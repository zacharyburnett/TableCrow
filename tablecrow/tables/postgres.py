from datetime import date, datetime
from functools import partial
from getpass import getpass
from logging import Logger
from sqlite3 import Cursor
from typing import Any, Collection, Dict
from typing import List, Mapping, Sequence, Union
from typing import get_args as typing_get_args

import psycopg2
from psycopg2._psycopg import connection
from pyproj import CRS
from shapely.geometry.base import BaseGeometry, BaseMultipartGeometry, GEOMETRY_TYPES
from sshtunnel import SSHTunnelForwarder
from typepigeon import guard_generic_alias

from tablecrow.tables.base import (
    DatabaseTable,
    parse_record_values,
    random_open_tcp_port,
)

from ..utilities import parse_hostname, split_hostname_port

SSH_DEFAULT_PORT = 22


class PostGresTable(DatabaseTable):
    DEFAULT_PORT = 5432
    FIELD_TYPES = {
        "NoneType": "NULL",
        "bool": "BOOL",
        "float": "REAL",
        "int": "INTEGER",
        "str": "VARCHAR",
        "bytes": "BYTEA",
        "date": "DATE",
        "time": "TIME",
        "datetime": "TIMESTAMP",
        "timedelta": "INTERVAL",
        "dict": "HSTORE",
        "ipaddress": "INET",
        **{geometry_type: "GEOMETRY" for geometry_type in GEOMETRY_TYPES},
    }

    def __init__(
        self,
        hostname: str,
        table_name: str,
        database: str = None,
        fields: Dict[str, type] = None,
        primary_key: Union[str, List[str]] = None,
        crs: CRS = None,
        username: str = None,
        users: List[str] = None,
        logger: Logger = None,
        **kwargs,
    ):
        self.tunnel_credentials = {}

        if "ssh_hostname" in kwargs and kwargs["ssh_hostname"] is not None:
            credentials = parse_hostname(kwargs["ssh_hostname"])
            ssh_hostname = credentials["hostname"]
            ssh_port = credentials["port"]
            if ssh_port is None:
                ssh_port = SSH_DEFAULT_PORT

            ssh_username = kwargs["ssh_username"] if "ssh_username" in kwargs else None
            ssh_password = kwargs["ssh_password"] if "ssh_password" in kwargs else None

            self.tunnel_credentials["ssh_hostname"] = ssh_hostname
            self.tunnel_credentials["ssh_port"] = ssh_port
            self.tunnel_credentials["ssh_username"] = ssh_username
            self.tunnel_credentials["ssh_password"] = ssh_password

        password = None
        if username is not None and ":" in username:
            username, password = username.split(":", 1)
        if "password" in kwargs:
            password = kwargs["password"]
        if password is None:
            password = getpass()
        self._DatabaseTable__password = password

        if database is None:
            database = "postgres"

        super().__init__(
            resource=hostname,
            table_name=table_name,
            database=database,
            fields=fields,
            primary_key=primary_key,
            crs=crs,
            username=username,
            password=password,
            users=users,
            logger=logger,
        )

        if not self.connected:
            raise ConnectionError(
                f"no connection to {self.username}@{self.hostname}:{self.port}/{self.database}/{self.name}"
            )

        if self.fields is None:
            with self.connection as connection:
                with connection.cursor() as cursor:
                    self._DatabaseTable__fields = database_table_fields(
                        cursor, self.name
                    )
            connection.close()

            if self.primary_key is None:
                self._DatabaseTable__primary_key = list(self.fields)[0]

        with self.connection as connection:
            with connection.cursor() as cursor:
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
                                f"remote table has {len(remote_fields_not_in_local_table)} fields not in local table: {list(remote_fields_not_in_local_table)}"
                            )
                            self.logger.warning(
                                f"adding {len(remote_fields_not_in_local_table)} fields to local table: {list(remote_fields_not_in_local_table)}"
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
                                f"local table has {len(local_fields_not_in_remote_table)} fields not in remote table: {list(local_fields_not_in_remote_table)}"
                            )
                            self.logger.warning(
                                f"adding {len(local_fields_not_in_remote_table)} fields to remote table: {list(local_fields_not_in_remote_table)}"
                            )

                        if list(remote_fields) != list(self.fields):
                            self.logger.warning(
                                f'altering schema of "{self.database}/{self.name}"'
                            )
                            self.logger.debug(self.remote_fields)
                            self.logger.debug(self.fields)

                            copy_table_name = f"old_{self.name}"

                            if database_has_table(cursor, copy_table_name):
                                cursor.execute(f"DROP TABLE {copy_table_name};")

                            cursor.execute(
                                f"ALTER TABLE {self.name} RENAME TO {copy_table_name};"
                            )

                            cursor.execute(f"CREATE TABLE {self.name} ({self.schema});")
                            for user in self.users:
                                cursor.execute(
                                    f"GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE public.{self.name} TO {user};"
                                )

                            copy_table_fields = list(
                                database_table_fields(cursor, copy_table_name)
                            )

                            cursor.execute(
                                f'INSERT INTO {self.name} ({", ".join(copy_table_fields)}) SELECT {", ".join(copy_table_fields)} FROM {copy_table_name};'
                            )

                            cursor.execute(f"DROP TABLE {copy_table_name};")
                else:
                    self.logger.debug(
                        f'creating remote table "{self.database}/{self.name}"'
                    )
                    cursor.execute(f"CREATE TABLE {self.name} ({self.schema});")

                    for user in self.users:
                        cursor.execute(
                            f"GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE public.{self.name} TO {user};"
                        )
        connection.close()
        if "password" in kwargs:
            kwargs["password"] = "*****"
        self.kwargs = kwargs

    @property
    def hostname(self) -> str:
        return self.resource

    @property
    def tunnel(self) -> SSHTunnelForwarder:
        if "ssh_hostname" in self.tunnel_credentials:
            port = split_hostname_port(self.resource)[-1]
            if port is None:
                port = self.DEFAULT_PORT
            tunnel = SSHTunnelForwarder(
                (
                    self.tunnel_credentials["ssh_hostname"],
                    self.tunnel_credentials["ssh_port"],
                ),
                ssh_username=self.tunnel_credentials["ssh_username"],
                ssh_password=self.tunnel_credentials["ssh_password"],
                remote_bind_address=("localhost", port),
                local_bind_address=("localhost", random_open_tcp_port()),
            )
            try:
                tunnel.start()
            except Exception as error:
                raise ConnectionError(error)
        else:
            tunnel = None
        return tunnel

    @property
    def connection(self) -> connection:
        connector = partial(
            psycopg2.connect,
            database=self.database,
            user=self.username,
            password=self._DatabaseTable__password,
        )

        tunnel = self.tunnel
        if tunnel is not None:
            connection = connector(
                host=tunnel.local_bind_host, port=tunnel.local_bind_port
            )
        else:
            connection = connector(host=self.hostname, port=self.port)

        return connection

    @property
    def exists(self) -> bool:
        with self.connection as connection:
            with connection.cursor() as cursor:
                exists = database_has_table(cursor, self.name)
        connection.close()
        return exists

    @property
    def schema(self) -> str:
        """PostGres schema string"""

        schema = []
        for field, field_type in self.fields.items():
            field_type = guard_generic_alias(field_type)

            if field_type in [list, tuple, Sequence, Collection]:
                field_type = [typing_get_args(field_type[0])]
            dimensions = 0
            while isinstance(field_type, Sequence) and not isinstance(field_type, str):
                if len(field_type) > 0:
                    field_type = field_type[0]
                else:
                    field_type = list
                dimensions += 1
            if isinstance(field_type, Mapping):
                field_type = dict

            try:
                field_type = self.FIELD_TYPES[field_type.__name__]
            except KeyError:
                raise TypeError(f'PostGres does not support type "{field_type}"')

            schema.append(f'{field} {field_type}{"[]" * dimensions}')

        schema.append(f'PRIMARY KEY({", ".join(self.primary_key)})')

        return ", ".join(schema)

    @property
    def remote_fields(self) -> Dict[str, type]:
        if not self.connected:
            raise ConnectionError(
                f"no connection to {self.username}@{self.hostname}:{self.port}/{self.database}/{self.name}"
            )

        fields = None
        with self.connection as connection:
            with connection.cursor() as cursor:
                if database_has_table(cursor, self.name):
                    fields = database_table_fields(cursor, self.name)

                    for field, field_type in fields.items():
                        dimensions = field_type.count("_")
                        field_type = field_type.strip("_")

                        field_type = field_type.lower()
                        if field_type == "geometry":
                            if field in self.fields:
                                fields[field] = self.fields[field]
                                continue

                        for python_type, postgres_type in self.FIELD_TYPES.items():
                            if postgres_type.lower() == field_type:
                                if field_type == "geometry":
                                    if python_type not in globals():
                                        exec(
                                            f"from shapely.geometry import {python_type}"
                                        )
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
                else:
                    fields = None
        connection.close()

        return fields

    @property
    def connected(self) -> bool:
        with self.connection as connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1;")
                    cursor.fetchone()
                connected = True
            except:
                connected = False
        connection.close()
        return connected

    def records_where(
        self, where: Union[Mapping[str, Any], str, List[str]]
    ) -> List[Dict[str, Any]]:
        if not self.connected:
            raise ConnectionError(
                f"no connection to {self.username}@{self.hostname}:{self.port}/{self.database}/{self.name}"
            )

        where_clause, where_values = self.__where_clause(where)

        with self.connection as connection:
            with connection.cursor() as cursor:
                if where_clause is None:
                    cursor.execute(
                        f'SELECT {", ".join(self.fields.keys())} FROM {self.name};'
                    )
                else:
                    try:
                        cursor.execute(
                            f"SELECT * FROM {self.name} WHERE {where_clause};",
                            where_values,
                        )
                    except psycopg2.errors.UndefinedColumn as error:
                        raise KeyError(error)
                    except psycopg2.errors.SyntaxError as error:
                        raise SyntaxError(f"invalid SQL syntax - {error}")
                matching_records = cursor.fetchall()
        connection.close()

        matching_records = [
            parse_record_values(dict(zip(self.fields.keys(), record)), self.fields)
            for record in matching_records
        ]

        return matching_records

    def records_intersecting(
        self, geometry: BaseGeometry, crs: CRS = None, geometry_fields: List[str] = None
    ) -> List[Dict[str, Any]]:
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
            geometry_string = "ST_GeomFromText(%s, %s)"
            if crs != self.crs:
                geometry_string = f"ST_Transform({geometry_string}, %s)"
                where_values.append(self.crs.to_epsg())
            where_clause.append(f"ST_Intersects({field}, {geometry_string})")
        where_clause = " OR ".join(where_clause)

        with self.connection as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT * FROM {self.name} WHERE {where_clause};", where_values
                )
                records = cursor.fetchall()
        connection.close()

        return [
            parse_record_values(dict(zip(self.fields.keys(), record)), self.fields)
            for record in records
        ]

    def insert(self, records: List[Dict[str, Any]]):
        if isinstance(records, dict):
            records = [records]

        if not all(field in record for field in self.primary_key for record in records):
            raise KeyError(
                f'one or more records does not contain primary key(s) "{self.primary_key}"'
            )

        if not self.connected:
            raise ConnectionError(
                f"no connection to {self.username}@{self.hostname}:{self.port}/{self.database}/{self.name}"
            )

        with self.connection as connection:
            with connection.cursor() as cursor:
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
                            f"record has {len(record_fields_not_in_local_table)} fields not in the local table"
                            f" that will not be inserted: {record_fields_not_in_local_table}"
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

                    for index, value in enumerate(values):
                        if isinstance(value, Collection) and not isinstance(
                            value, (str, list)
                        ):
                            values[index] = list(value)

                    if primary_key_value in self:
                        record_without_primary_key = {
                            column: value
                            for column, value in zip(columns, values)
                            if column not in self.primary_key
                        }
                        if len(record_without_primary_key) > 0:
                            if len(record_without_primary_key) > 1:
                                cursor.execute(
                                    f'UPDATE {self.name} SET ({", ".join(record_without_primary_key.keys())}) = %s '
                                    f"WHERE {primary_key_string} = %s;",
                                    [
                                        tuple(record_without_primary_key.values()),
                                        primary_key_value,
                                    ],
                                )
                            else:
                                cursor.execute(
                                    f"UPDATE {self.name} SET {tuple(record_without_primary_key.keys())[0]} = %s "
                                    f"WHERE {primary_key_string} = %s;",
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
                                f"UPDATE {self.name} SET {field} = ST_GeomFromText(%s, %s) "
                                f"WHERE {primary_key_string} = %s;",
                                [geometry.wkt, self.crs.to_epsg(), primary_key_value],
                            )
        connection.close()

    def delete_where(self, where: Union[Mapping[str, Any], str, List[str]]):
        if not self.connected:
            raise ConnectionError(
                f"no connection to {self.username}@{self.hostname}:{self.port}/{self.database}/{self.name}"
            )

        where_clause, where_values = self.__where_clause(where)

        with self.connection as connection:
            with connection.cursor() as cursor:
                if where_clause is None:
                    cursor.execute(f"TRUNCATE {self.name};")
                else:
                    try:
                        cursor.execute(
                            f"DELETE FROM {self.name} WHERE {where_clause};",
                            where_values,
                        )
                    except psycopg2.errors.UndefinedColumn as error:
                        raise KeyError(error)
                    except psycopg2.errors.SyntaxError as error:
                        raise SyntaxError(f"invalid SQL syntax - {error}")
        connection.close()

    def __len__(self) -> int:
        with self.connection as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {self.name};")
                length = cursor.fetchone()[0]
        connection.close()
        return length

    def delete_table(self):
        with self.connection as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE {self.name};")
        connection.close()

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}({repr(self.database)}, {repr(self.name)}, {repr(self.fields)}, {repr(self.primary_key)}, "
            f"{repr(self.hostname)}, {repr(self.crs.to_epsg()) if self.crs is not None else None}, "
            f"{repr(self.username)}, {repr(self.users)}"
            f'{", " if len(self.kwargs) > 0 else ""}'
            f'{", ".join(key + "=" + repr(value) for key, value in self.kwargs.items())})'
        )

    def __where_clause(self, where: Dict[str, Union[Any, List]]) -> (str, List):
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
                        where_clause.append(f"{field} = ST_GeomFromText(%s, %s)")
                        where_values.extend([value.wkt, self.crs.to_epsg()])
                    else:
                        if isinstance(field_type, list):
                            if not isinstance(value, Sequence) or isinstance(
                                value, str
                            ):
                                statement = f"%s = ANY({field})"
                            else:
                                if fields is None:
                                    with self.connection as connection:
                                        with connection.cursor() as cursor:
                                            fields = database_table_fields(
                                                cursor, self.name
                                            )
                                    connection.close()
                                field_type = fields[field]
                                dimensions = field_type.count("_")
                                field_type = field_type.strip("_")
                                statement = (
                                    f'{field} = %s::{field_type}{"[]" * dimensions}'
                                )
                        elif value is None:
                            statement = f"{field} IS %s"
                        elif isinstance(value, Sequence) and not isinstance(value, str):
                            statement = f"{field} IN %s"
                            value = tuple(value)
                        elif isinstance(value, str) and "%" in value:
                            statement = f"{field} ILIKE %s"
                        else:
                            if isinstance(value, datetime):
                                value = f"{value:%Y%m%d %H%M%S}"
                            elif isinstance(value, date):
                                value = f"{value:%Y%m%d}"
                            statement = f"{field} = %s"
                        where_values.append(value)
                        where_clause.append(statement)
                where_clause = " AND ".join(where_clause)
            else:
                where_clause = " AND ".join(where)

            if len(where_values) == 0:
                where_values = None

        return where_clause, where_values


def database_tables(cursor: Cursor, user_defined: bool = True) -> List[str]:
    """
    list of tables within the given PostGreSQL database

    :param cursor: psycopg2 cursor
    :return: list of table names
    """

    # query = "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
    query = "SELECT relname FROM pg_class WHERE relkind='r'"
    if user_defined:
        query += " AND relname !~ '^(pg_|sql_)'"
    query += ";"

    cursor.execute(query)
    return [record[0] for record in cursor.fetchall()]


def database_has_table(cursor: psycopg2._psycopg.cursor, table: str) -> bool:
    """
    whether the given table exists within the given PostGreSQL database

    :param cursor: psycopg2 cursor
    :param table: name of table
    :return: whether table exists
    """

    cursor.execute(
        # "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=%s);",
        "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname=%s);",
        [table.lower()],
    )
    return cursor.fetchone()[0]


def database_table_is_inherited(cursor: psycopg2._psycopg.cursor, table: str) -> bool:
    """
    whether the given PostGreSQL table is inherited

    :param cursor: psycopg2 cursor
    :param table: name of table
    :return: whether table is inherited
    """

    cursor.execute(
        "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_inherits WHERE inhrelid=%s::regclass);",
        [f"public.{table}"],
    )
    return cursor.fetchone()[0]


def database_table_fields(
    cursor: psycopg2._psycopg.cursor, table: str
) -> Dict[str, str]:
    """
    field names and data types of the given table, within the given PostGreSQL database

    :param cursor: psycopg2 cursor
    :param table: name of table
    :return: mapping of column names to the PostGres data type
    """

    cursor.execute(
        "SELECT column_name, udt_name FROM information_schema.columns WHERE table_name=%s;",
        [table],
    )
    return {record[0]: record[1] for record in cursor.fetchall()}
