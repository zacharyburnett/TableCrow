from datetime import date, datetime
from functools import partial
from logging import Logger
import re
from typing import (
    Any,
    Collection,
    Mapping,
    Sequence,
    Union,
    get_args as typing_get_args,
)

import psycopg2
from pyproj import CRS
from shapely.geometry.base import BaseGeometry, BaseMultipartGeometry, GEOMETRY_TYPES
from sshtunnel import SSHTunnelForwarder

from ..table import (
    DatabaseTable,
    parse_record_values,
    random_open_tcp_port,
    split_URL_port,
)

DEFAULT_CRS = CRS.from_epsg(4326)
SSH_DEFAULT_PORT = 22


class PostGresTable(DatabaseTable):
    DEFAULT_PORT = 5432
    FIELD_TYPES = {
        'NoneType': 'NULL',
        'bool': 'BOOL',
        'float': 'REAL',
        'int': 'INTEGER',
        'str': 'VARCHAR',
        'bytes': 'BYTEA',
        'date': 'DATE',
        'time': 'TIME',
        'datetime': 'TIMESTAMP',
        'timedelta': 'INTERVAL',
        'dict': 'HSTORE',
        'ipaddress': 'INET',
        **{geometry_type: 'GEOMETRY' for geometry_type in GEOMETRY_TYPES},
    }

    def __init__(
            self,
            hostname: str,
            database: str,
            name: str,
            fields: {str: type},
            primary_key: Union[str, Sequence[str]] = None,
            crs: CRS = None,
            username: str = None,
            password: str = None,
            users: [str] = None,
            logger: Logger = None,
            **kwargs,
    ):
        super().__init__(
            hostname, database, name, fields, primary_key, username, password, users, logger
        )
        self.crs = crs if crs is not None else DEFAULT_CRS

        connector = partial(
            psycopg2.connect,
            database=self.database,
            user=self.username,
            password=self.password,
        )
        if 'ssh_hostname' in kwargs and kwargs['ssh_hostname'] is not None:
            ssh_hostname, ssh_port = split_URL_port(kwargs['ssh_hostname'])
            if ssh_port is None:
                ssh_port = SSH_DEFAULT_PORT

            if '@' in ssh_hostname:
                ssh_username, ssh_hostname = ssh_hostname.split('@', 1)

            ssh_username = kwargs['ssh_username'] if 'ssh_username' in kwargs else None

            if ssh_username is not None and ':' in ssh_username:
                ssh_username, ssh_password = ssh_hostname.split(':', 1)

            ssh_password = kwargs['ssh_password'] if 'ssh_password' in kwargs else None

            self.tunnel = SSHTunnelForwarder(
                (ssh_hostname, ssh_port),
                ssh_username=ssh_username,
                ssh_password=ssh_password,
                remote_bind_address=('localhost', self.port),
                local_bind_address=('localhost', random_open_tcp_port()),
            )
            try:
                self.tunnel.start()
            except Exception as error:
                raise ConnectionError(error)
            self.connection = connector(
                host=self.tunnel.local_bind_host, port=self.tunnel.local_bind_port
            )
        else:
            self.tunnel = None
            self.connection = connector(host=self.hostname, port=self.port)

        if not self.connected:
            raise ConnectionError(
                f'no connection to {self.username}@{self.hostname}:{self.port}/{self.database}/{self.name}'
            )

        with self.connection:
            with self.connection.cursor() as cursor:
                if database_has_table(cursor, self.name):
                    if database_table_is_inherited(cursor, self.name):
                        raise RuntimeError(
                            f'inheritance of table "{self.database}/{self.name}" will cause unexpected behaviour; aborting'
                        )

                    remote_fields = self.remote_fields
                    if list(remote_fields) != list(self.fields):
                        self.logger.warning(
                            f'schema of existing table "{self.database}/{self.name}" differs from given fields'
                        )

                        remote_fields_not_in_local_table = {
                            field: value
                            for field, value in remote_fields.items()
                            if field not in self.fields
                        }
                        if len(remote_fields_not_in_local_table) > 0:
                            self.logger.warning(
                                f'remote table has {len(remote_fields_not_in_local_table)} fields not in local table: {list(remote_fields_not_in_local_table)}'
                            )
                            self.logger.warning(
                                f'adding {len(remote_fields_not_in_local_table)} fields to local table: {list(remote_fields_not_in_local_table)}'
                            )

                            self._DatabaseTable__fields.update(
                                remote_fields_not_in_local_table
                            )
                            self._DatabaseTable__fields = {
                                field: self._DatabaseTable__fields[field]
                                for field in remote_fields
                            }

                        local_fields_not_in_remote_table = {
                            field: value
                            for field, value in self.fields.items()
                            if field not in remote_fields
                        }
                        if len(local_fields_not_in_remote_table) > 0:
                            self.logger.warning(
                                f'local table has {len(local_fields_not_in_remote_table)} fields not in remote table: {list(local_fields_not_in_remote_table)}'
                            )
                            self.logger.warning(
                                f'adding {len(local_fields_not_in_remote_table)} fields to remote table: {list(local_fields_not_in_remote_table)}'
                            )

                        if list(remote_fields) != list(self.fields):
                            self.logger.warning(
                                f'altering schema of "{self.database}/{self.name}"'
                            )
                            self.logger.debug(self.remote_fields)
                            self.logger.debug(self.fields)

                            copy_table_name = f'old_{self.name}'

                            if database_has_table(cursor, copy_table_name):
                                cursor.execute(f'DROP TABLE {copy_table_name};')

                            cursor.execute(
                                f'ALTER TABLE {self.name} RENAME TO {copy_table_name};'
                            )

                            cursor.execute(f'CREATE TABLE {self.name} ({self.schema});')
                            for user in self.users:
                                cursor.execute(
                                    f'GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE public.{self.name} TO {user};'
                                )

                            cursor.execute(
                                'SELECT column_name FROM information_schema.columns WHERE table_name=%s;',
                                [copy_table_name],
                            )
                            copy_table_fields = [record[0] for record in cursor.fetchall()]

                            cursor.execute(
                                f'INSERT INTO {self.name} ({", ".join(copy_table_fields)}) '
                                f'SELECT * FROM {copy_table_name};'
                            )

                            cursor.execute(f'DROP TABLE {copy_table_name};')
                else:
                    self.logger.debug(f'creating remote table "{self.database}/{self.name}"')
                    cursor.execute(f'CREATE TABLE {self.name} ({self.schema});')

                    for user in self.users:
                        cursor.execute(
                            f'GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE public.{self.name} TO {user};'
                        )

    @property
    def exists(self) -> bool:
        with self.connection:
            with self.connection.cursor() as cursor:
                return database_has_table(cursor, self.name)

    @property
    def schema(self) -> str:
        """ PostGreSQL schema string """

        schema = []
        for field, field_type in self.fields.items():
            if field_type in [list, tuple, Sequence, Collection]:
                field_type = [typing_get_args(field_type)[0]]
            dimensions = 0
            while isinstance(field_type, Sequence) and not isinstance(field_type, str):
                if len(field_type) > 0:
                    field_type = field_type[0]
                else:
                    field_type = list
                dimensions += 1
            if isinstance(field_type, Mapping):
                field_type = dict

            schema.append(
                f'{field} {self.FIELD_TYPES[field_type.__name__]}{"[]" * dimensions}'
            )

        schema.append(f'PRIMARY KEY({", ".join(self.primary_key)})')

        return ', '.join(schema)

    @property
    def remote_fields(self) -> {str: type}:
        if not self.connected:
            raise ConnectionError(
                f'no connection to {self.username}@{self.hostname}:{self.port}/{self.database}/{self.name}'
            )

        with self.connection:
            with self.connection.cursor() as cursor:
                if database_has_table(cursor, self.name):
                    fields = database_table_fields(cursor, self.name)

                    for field, field_type in fields.items():
                        dimensions = field_type.count('_')
                        field_type = field_type.strip('_')

                        field_type = field_type.lower()
                        if field_type == 'geometry':
                            if field in self.fields:
                                fields[field] = self.fields[field]
                                continue

                        for python_type, postgres_type in self.FIELD_TYPES.items():
                            if postgres_type.lower() == field_type:
                                if field_type == 'geometry':
                                    if python_type not in globals():
                                        exec(f'from shapely.geometry import {python_type}')
                                field_type = eval(python_type)
                                break
                        else:
                            for python_type, postgres_type in self.FIELD_TYPES.items():
                                if python_type.lower() in field_type:
                                    field_type = eval(python_type)
                                    break
                            else:
                                field_type = str

                        for _ in range(dimensions):
                            field_type = [field_type]
                        fields[field] = field_type

                    return fields

    @property
    def connected(self) -> bool:
        with self.connection:
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute('SELECT 1;')
                return True
            except psycopg2.OperationalError:
                return False

    def records_where(
            self, where: Union[Mapping[str, Any], str, Sequence[str]]
    ) -> [{str: Any}]:
        if not self.connected:
            raise ConnectionError(
                f'no connection to {self.username}@{self.hostname}:{self.port}/{self.database}/{self.name}'
            )

        where_clause, where_values = self.__where_clause(where)

        with self.connection:
            with self.connection.cursor() as cursor:
                if where_clause is None:
                    cursor.execute(f'SELECT {", ".join(self.fields.keys())} FROM {self.name}')
                else:
                    try:
                        cursor.execute(
                            f'SELECT * FROM {self.name} WHERE {where_clause}', where_values
                        )
                    except psycopg2.errors.UndefinedColumn as error:
                        raise KeyError(error)
                    except psycopg2.errors.SyntaxError as error:
                        raise SyntaxError(f'invalid SQL syntax - {error}')
                matching_records = cursor.fetchall()

        matching_records = [
            parse_record_values(dict(zip(self.fields.keys(), record)), self.fields)
            for record in matching_records
        ]

        return matching_records

    def insert(self, records: [{str: Any}]):
        if isinstance(records, dict):
            records = [records]

        if not all(field in record for field in self.primary_key for record in records):
            raise KeyError(
                f'one or more records does not contain primary key(s) "{self.primary_key}"'
            )

        if not self.connected:
            raise ConnectionError(
                f'no connection to {self.username}@{self.hostname}:{self.port}/{self.database}/{self.name}'
            )

        with self.connection:
            with self.connection.cursor() as cursor:
                for record in records:
                    if len(self.primary_key) == 1:
                        primary_key_string = self.primary_key[0]
                        primary_key_value = record[self.primary_key[0]]
                    else:
                        primary_key_string = f'({", ".join(self.primary_key)})'
                        primary_key_value = tuple(
                            record[primary_key] for primary_key in self.primary_key
                        )

                    record_fields_not_in_local_table = [
                        field for field in record if field not in self.fields
                    ]
                    if len(record_fields_not_in_local_table) > 0:
                        self.logger.warning(
                            f'record has {len(record_fields_not_in_local_table)} fields not in the local table'
                            f' that will not be inserted: {record_fields_not_in_local_table}'
                        )

                    local_fields_in_record = [
                        field for field in self.fields if field in record
                    ]
                    geometry_fields = [
                        field for field in self.geometry_fields if field in record
                    ]

                    columns = [
                        field
                        for field in local_fields_in_record
                        if field not in geometry_fields
                    ]
                    values = [
                        record[field]
                        for field in local_fields_in_record
                        if field not in geometry_fields
                    ]

                    if primary_key_value in self:
                        record_without_primary_key = {
                            column: value
                            for column, value in zip(columns, values)
                            if column not in self.primary_key
                        }
                        if len(record_without_primary_key) > 0:
                            if len(record_without_primary_key) > 1:
                                cursor.execute(
                                    f'UPDATE {self.name} SET ({", ".join(record_without_primary_key.keys())}) = %s'
                                    f' WHERE {primary_key_string} = %s;',
                                    [
                                        tuple(record_without_primary_key.values()),
                                        primary_key_value,
                                    ],
                                )
                            else:
                                cursor.execute(
                                    f'UPDATE {self.name} SET {tuple(record_without_primary_key.keys())[0]} = %s'
                                    f' WHERE {primary_key_string} = %s;',
                                    [
                                        tuple(record_without_primary_key.values())[0],
                                        primary_key_value,
                                    ],
                                )
                    else:
                        cursor.execute(
                            f'INSERT INTO {self.name} ({", ".join(columns)}) VALUES %s;',
                            [tuple(values)],
                        )

                    if len(geometry_fields) > 0:
                        geometries = {
                            field: record[field]
                            for field in geometry_fields
                            if record[field] is not None
                        }

                        for field, geometry in geometries.items():
                            cursor.execute(
                                f'UPDATE {self.name} SET {field} = ST_GeomFromText(%s, %s) '
                                f'WHERE {primary_key_string} = %s;',
                                [geometry.wkt, self.crs.to_epsg(), primary_key_value],
                            )

    def delete_where(self, where: Union[Mapping[str, Any], str, Sequence[str]]):
        if not self.connected:
            raise ConnectionError(
                f'no connection to {self.username}@{self.hostname}:{self.port}/{self.database}/{self.name}'
            )

        where_clause, where_values = self.__where_clause(where)

        with self.connection:
            with self.connection.cursor() as cursor:
                if where_clause is None:
                    cursor.execute(f'TRUNCATE {self.name};')
                else:
                    try:
                        cursor.execute(
                            f'DELETE FROM {self.name} WHERE {where_clause};', where_values
                        )
                    except psycopg2.errors.UndefinedColumn as error:
                        raise KeyError(error)
                    except psycopg2.errors.SyntaxError as error:
                        raise SyntaxError(f'invalid SQL syntax - {error}')

    def __len__(self) -> int:
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(f'SELECT COUNT(*) FROM {self.name};')
                return cursor.fetchone()[0]

    def records_intersecting(
            self, geometry: BaseGeometry, crs: CRS = None, geometry_fields: [str] = None
    ) -> [{str: Any}]:
        """
        records in the table that intersect the given geometry

        :param geometry: Shapely geometry object
        :param crs: coordinate reference system of input geometry
        :param geometry_fields: geometry fields to query
        :return: dictionaries of matching records
        """

        if crs is None:
            crs = self.crs

        if crs.to_epsg() is None:
            raise NotImplementedError(f'no EPSG code found for CRS "{crs}"')

        if geometry_fields is None or len(geometry_fields) == 0:
            geometry_fields = list(self.geometry_fields)

        where_clause = []
        where_values = []
        for field in geometry_fields:
            where_values.extend([geometry.wkt, crs.to_epsg()])
            geometry_string = f'ST_GeomFromText(%s, %s)'
            if crs != self.crs:
                geometry_string = f'ST_Transform({geometry_string}, %s)'
                where_values.append(self.crs.to_epsg())
            where_clause.append(f'ST_Intersects({field}, {geometry_string})')
        where_clause = ' OR '.join(where_clause)

        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(f'SELECT * FROM {self.name} WHERE {where_clause}', where_values)
                records = cursor.fetchall()

        return [
            parse_record_values(dict(zip(self.fields.keys(), record)), self.fields)
            for record in records
        ]

    def delete_table(self):
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(f'DROP TABLE {self.name};')

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}({repr(self.hostname)}, {repr(self.database)}, {repr(self.name)}, '
            f'{repr(self.fields)}, {repr(self.primary_key)}, {repr(self.crs.to_epsg())}, '
            f'{repr(self.username)}, {repr(re.sub("..", "*", self.password))}, {repr(self.users)})'
        )

    def __del__(self):
        if self.tunnel is not None:
            self.tunnel.stop()

    def __where_clause(self, where: {str: Union[Any, list]}) -> (str, [Any]):
        if (
                where is not None
                and not isinstance(where, Sequence)
                and not isinstance(where, dict)
        ):
            raise NotImplementedError(f'unsupported query type "{type(where)}"')

        if where is None or len(where) == 0:
            where_clause = None
            where_values = None
        else:
            fields = None
            where_values = []
            if isinstance(where, str):
                where_clause = where
            elif isinstance(where, dict):
                where_clause = []
                for field, value in where.items():
                    field_type = self.fields[field]
                    if isinstance(value, BaseGeometry) or isinstance(
                            value, BaseMultipartGeometry
                    ):
                        where_clause.append(f'{field} = ST_GeomFromText(%s, %s)')
                        where_values.extend([value.wkt, self.crs.to_epsg()])
                    else:
                        if isinstance(field_type, list):
                            if not isinstance(value, Sequence) or isinstance(value, str):
                                statement = f'%s = ANY({field})'
                            else:
                                if fields is None:
                                    with self.connection:
                                        with self.connection.cursor() as cursor:
                                            fields = database_table_fields(cursor, self.name)
                                field_type = fields[field]
                                dimensions = field_type.count('_')
                                field_type = field_type.strip('_')
                                statement = f'{field} = %s::{field_type}{"[]" * dimensions}'
                        elif value is None:
                            statement = f'{field} IS %s'
                        elif isinstance(value, Sequence) and not isinstance(value, str):
                            statement = f'{field} IN %s'
                            value = tuple(value)
                        elif isinstance(value, str) and '%' in value:
                            statement = f'{field} ILIKE %s'
                        else:
                            if isinstance(value, datetime):
                                value = f'{value:%Y%m%d %H%M%S}'
                            elif isinstance(value, date):
                                value = f'{value:%Y%m%d}'
                            statement = f'{field} = %s'
                        where_values.append(value)
                        where_clause.append(statement)
                where_clause = ' AND '.join(where_clause)
            else:
                where_clause = ' AND '.join(where)

            if len(where_values) == 0:
                where_values = None

        return where_clause, where_values


def database_has_table(cursor: psycopg2._psycopg.cursor, table: str) -> bool:
    """
    Whether the given table exists within the given database.

    :param cursor: psycopg2 cursor
    :param table: name of table
    :return: whether table exists
    """

    cursor.execute(
        f'SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=%s);',
        [table.lower()],
    )
    return cursor.fetchone()[0]


def database_table_is_inherited(cursor: psycopg2._psycopg.cursor, table: str) -> bool:
    """
    Whether the given table is inherited.

    :param cursor: psycopg2 cursor
    :param table: name of table
    :return: whether table is inherited
    """

    cursor.execute(
        f'SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_inherits WHERE inhrelid=%s::regclass);',
        [f'public.{table}'],
    )
    return cursor.fetchone()[0]


def database_table_fields(cursor: psycopg2._psycopg.cursor, table: str) -> {str: str}:
    """
    Get field names and data types of the given table, within the given database.

    :param cursor: psycopg2 cursor
    :param table: name of table
    :return: mapping of column names to the PostGres data type
    """

    cursor.execute(
        f'SELECT column_name, udt_name FROM information_schema.columns WHERE table_name=%s;',
        [table],
    )
    return {record[0]: record[1] for record in cursor.fetchall()}
