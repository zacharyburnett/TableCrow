from datetime import date, datetime
from functools import partial
from logging import Logger
import re
from typing import Any, Generator, Sequence, Union

import psycopg2
from pyproj import CRS
from shapely.geometry.base import BaseGeometry, GEOMETRY_TYPES
from sshtunnel import SSHTunnelForwarder

from .table import DatabaseTable, InheritedTableError, parse_record_values, random_open_tcp_port, split_URL_port

DEFAULT_CRS = CRS.from_epsg(4326)
SSH_DEFAULT_PORT = 22


class PostGresTable(DatabaseTable):
    DEFAULT_PORT = 5432
    FIELD_TYPES = {
        'NoneType' : 'NULL',
        'bool'     : 'BOOL',
        'float'    : 'REAL',
        'int'      : 'INTEGER',
        'str'      : 'VARCHAR',
        'bytes'    : 'BYTEA',
        'date'     : 'DATE',
        'time'     : 'TIME',
        'datetime' : 'TIMESTAMP',
        'timedelta': 'INTERVAL',
        'list'     : 'VARCHAR[]',
        'dict'     : 'HSTORE',
        'ipaddress': 'INET',
        **{geometry_type: 'GEOMETRY' for geometry_type in GEOMETRY_TYPES}
    }

    def __init__(self, hostname: str, database: str, table: str, fields: {str: type}, primary_key: str = None, crs: CRS = None,
                 username: str = None, password: str = None, users: [str] = None, logger: Logger = None, **kwargs):
        super().__init__(hostname, database, table, fields, primary_key, username, password, users, logger)
        self.crs = crs if crs is not None else DEFAULT_CRS

        connector = partial(psycopg2.connect, database=self.database, user=self.username, password=self.password)
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

            self.tunnel = SSHTunnelForwarder((ssh_hostname, ssh_port),
                    ssh_username=ssh_username, ssh_password=ssh_password,
                    remote_bind_address=('localhost', self.port), local_bind_address=('localhost', random_open_tcp_port()))
            try:
                self.tunnel.start()
            except Exception as error:
                raise ConnectionError(error)
            self.connection = connector(host=self.tunnel.local_bind_host, port=self.tunnel.local_bind_port)
        else:
            self.tunnel = None
            self.connection = connector(host=self.hostname, port=self.port)

        if not self.connected:
            raise ConnectionError(f'no connection to {self.username}@{self.hostname}:{self.port}/{self.database}/{self.table}')

        with self.connection:
            with self.connection.cursor() as cursor:
                if database_has_table(cursor, self.table):
                    if database_table_is_inherited(cursor, self.table):
                        raise InheritedTableError(f'inheritance of table "{self.database}/{self.table}" '
                                                  f'will cause unexpected behaviour; aborting')

                    remote_fields = self.remote_fields
                    if list(remote_fields) != list(self.fields):
                        self.logger.warning(f'schema of existing table "{self.database}/{self.table}" '
                                            f'differs from given fields')

                        remote_fields_not_in_local_table = {field: value for field, value in remote_fields.items() if
                                                            field not in self.fields}
                        if len(remote_fields_not_in_local_table) > 0:
                            self.logger.warning(f'remote table has {len(remote_fields_not_in_local_table)} fields '
                                                f'not in local table: {list(remote_fields_not_in_local_table)}')
                            self.logger.warning(f'adding {len(remote_fields_not_in_local_table)} fields '
                                                f'to local table: {list(remote_fields_not_in_local_table)}')

                            for field, field_type in remote_fields_not_in_local_table.items():
                                previous_field = list(remote_fields)[list(remote_fields).index(field) - 1]

                                local_fields = {}
                                for local_field, local_value in self.fields.items():
                                    local_fields[local_field] = local_value
                                    if local_field == previous_field:
                                        local_fields[field] = field_type

                                self._DatabaseTable__fields = local_fields

                        local_fields_not_in_remote_table = {field: value for field, value in self.fields.items()
                                                            if field not in remote_fields}
                        if len(local_fields_not_in_remote_table) > 0:
                            self.logger.warning(f'local table has {len(local_fields_not_in_remote_table)} fields '
                                                f'not in remote table: {list(local_fields_not_in_remote_table)}')
                            self.logger.warning(f'adding {len(local_fields_not_in_remote_table)} fields '
                                                f'to remote table: {list(local_fields_not_in_remote_table)}')

                        if list(remote_fields) != list(self.fields):
                            self.logger.warning(f'altering schema of "{self.database}/{self.table}"')
                            self.logger.debug(self.remote_fields)
                            self.logger.debug(self.fields)

                            copy_table_name = f'old_{self.table}'

                            if database_has_table(cursor, copy_table_name):
                                cursor.execute(f'DROP TABLE {copy_table_name};')

                            cursor.execute(f'ALTER TABLE {self.table} RENAME TO {copy_table_name};')

                            cursor.execute(f'CREATE TABLE {self.table} ({self.schema});')
                            for user in self.users:
                                cursor.execute(f'GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE public.{self.table} TO {user};')

                            cursor.execute('SELECT column_name FROM information_schema.columns WHERE table_name=%s;',
                                    [copy_table_name])
                            copy_table_fields = [record[0] for record in cursor.fetchall()]

                            cursor.execute(f'INSERT INTO {self.table} ({", ".join(copy_table_fields)}) '
                                           f'SELECT * FROM {copy_table_name};')

                            cursor.execute(f'DROP TABLE {copy_table_name};')
                else:
                    self.logger.debug(f'creating remote table "{self.database}/{self.table}"')
                    cursor.execute(f'CREATE TABLE {self.table} ({self.schema});')

                    for user in self.users:
                        cursor.execute(f'GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE public.{self.table} TO {user};')

    @property
    def schema(self) -> str:
        """ PostGres schema string of local table, given field names and types """

        schema = []
        for field, field_type in self.fields.items():
            dimensions = 0
            while type(field_type) is list:
                field_type = field_type[0]
                dimensions += 1

            schema.append(f'{field} {self.FIELD_TYPES[field_type.__name__]}{"[]" * dimensions}')

        schema.append(f'PRIMARY KEY({", ".join(self.primary_key)})')

        return ', '.join(schema)

    @property
    def remote_fields(self) -> {str: type}:
        if not self.connected:
            raise ConnectionError(f'no connection to {self.username}@{self.hostname}:{self.port}/{self.database}/{self.table}')

        with self.connection:
            with self.connection.cursor() as cursor:
                if database_has_table(cursor, self.table):
                    cursor.execute(f'SELECT column_name, udt_name FROM information_schema.columns WHERE table_name=%s;',
                            [self.table])
                    fields = {field[0]: field[1] for field in cursor.fetchall()}

                    for field, field_type in fields.items():
                        dimensions = field_type.count('_')
                        field_type = field_type.strip('_')

                        field_type = field_type.lower()
                        for python_type, postgres_type in self.FIELD_TYPES.items():
                            if postgres_type.lower() == field_type:
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

    def records_where(self, where: {str: Union[Any, list]}) -> [{str: Any}]:
        """
        records in the table that match the given key-value pairs

        :param where: dictionary mapping keys to values, with which to match records
        :return: dictionaries of matching records
        """

        if not self.connected:
            raise ConnectionError(f'no connection to {self.username}@{self.hostname}:{self.port}/{self.database}/{self.table}')

        if where is None or len(where) == 0:
            with self.connection:
                with self.connection.cursor() as cursor:
                    cursor.execute(f'SELECT {", ".join(self.fields.keys())} FROM {self.table}')
                    matching_records = cursor.fetchall()
        else:
            where_values = []
            if isinstance(where, str):
                where_clause = where
            elif isinstance(where, dict):
                where_clause = []
                for key, value in where.items():
                    value_is_list = type(value) in [list, tuple, range, slice]
                    value = value if not value_is_list else tuple(value)
                    if value is None:
                        statement = f'{key} IS %s'
                    elif value_is_list:
                        statement = f'{key} IN %s'
                    elif type(value) is str and '%' in value:
                        statement = f'{key} ILIKE %s'
                    else:
                        statement = f'{key} = %s'
                    where_values.append(value)
                    where_clause.append(statement)
                where_clause = ' AND '.join(where_clause)
            elif isinstance(where, Sequence):
                where_clause = ' AND '.join(where)
            else:
                raise NotImplementedError(f'unsupported query type {type(where)}')

            if len(where_values) == 0:
                where_values = None

            try:
                with self.connection:
                    with self.connection.cursor() as cursor:
                        cursor.execute(f'SELECT * FROM {self.table} WHERE {where_clause}', where_values)
                        matching_records = cursor.fetchall()
            except psycopg2.errors.UndefinedColumn as error:
                raise KeyError(error)

        matching_records = [parse_record_values(dict(zip(self.fields.keys(), record)), self.fields)
                            for record in matching_records]

        return matching_records

    def insert(self, records: [{str: Any}]):
        """
        Insert the list of records into the table.

        :param records: dictionary records
        """

        if type(records) is dict:
            records = [records]

        assert all(primary_key in record for primary_key in self.primary_key for record in records), \
            f'one or more records does not contain primary key(s) "{self.primary_key}"'

        records = [record for record in records if (record[key] for key in self.primary_key) not in self]

        if not self.connected:
            raise ConnectionError(f'no connection to {self.username}@{self.hostname}:{self.port}/{self.database}/{self.table}')

        with self.connection:
            with self.connection.cursor() as cursor:
                for record in records:
                    if len(self.primary_key) == 1:
                        primary_key_string = self.primary_key[0]
                        primary_key_value = record[self.primary_key[0]]
                    else:
                        primary_key_string = f'({", ".join(self.primary_key)})'
                        primary_key_value = tuple(record[primary_key] for primary_key in self.primary_key)

                    record_fields_not_in_local_table = [field for field in record if field not in self.fields]
                    if len(record_fields_not_in_local_table) > 0:
                        self.logger.warning(f'record has {len(record_fields_not_in_local_table)} fields not in the local table'
                                            f' that will not be inserted: {record_fields_not_in_local_table}')

                    local_fields_in_record = [field for field in self.fields if field in record]
                    geometry_fields = [field for field in self.geometry_fields if field in record]

                    columns = [field for field in local_fields_in_record if field not in geometry_fields]
                    values = [record[field] for field in local_fields_in_record if field not in geometry_fields]

                    if database_table_has_record(cursor, self.table, record, self.primary_key):
                        record_without_primary_key = {column: value for column, value in zip(columns, values)
                                                      if column not in self.primary_key}
                        if len(record_without_primary_key) > 0:
                            if len(record_without_primary_key) > 1:
                                cursor.execute(f'UPDATE {self.table} SET ({", ".join(record_without_primary_key.keys())}) = %s'
                                               f' WHERE {primary_key_string} = %s;',
                                        [tuple(record_without_primary_key.values()), primary_key_value])
                            else:
                                cursor.execute(f'UPDATE {self.table} SET {tuple(record_without_primary_key.keys())[0]} = %s'
                                               f' WHERE {primary_key_string} = %s;',
                                        [tuple(record_without_primary_key.values())[0], primary_key_value])
                    else:
                        cursor.execute(f'INSERT INTO {self.table} ({", ".join(columns)}) VALUES %s;',
                                [tuple(values)])

                    if len(geometry_fields) > 0:
                        geometries = {field: record[field] for field in geometry_fields if record[field] is not None}

                        for field, geometry in geometries.items():
                            cursor.execute(f'UPDATE {self.table} SET {field} = ST_GeomFromWKB(%s::geometry, %s) '
                                           f'WHERE {primary_key_string} = %s;',
                                    [geometry.wkb, self.crs.to_epsg(), primary_key_value])

    def execute(self, sql: str):
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)

    def records_intersecting(self, geometry: BaseGeometry, crs: CRS, geometry_fields: [str] = None) -> [{str: Any}]:
        """
        records in the table that intersect the given geometry

        :param geometry: Shapely geometry object
        :param crs: coordinate reference system of input geometry
        :param geometry_fields: geometry fields to query
        :return: dictionaries of matching records
        """

        if crs.to_epsg() is None:
            raise NotImplementedError(f'no EPSG code found for CRS "{crs}"')

        if geometry_fields is None or len(geometry_fields) == 0:
            geometry_fields = list(self.geometry_fields)

        where_clause = ' OR '.join([f'ST_Intersects({field}, %s::geometry)' for field in geometry_fields])
        where_values = [f'SRID={crs.to_epsg()};{geometry.wkt}' for _ in geometry_fields]

        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(f'SELECT * FROM {self.table} WHERE {where_clause}', where_values)
                records = cursor.fetchall()

        return [parse_record_values(dict(zip(self.fields.keys(), record)), self.fields) for record in records]

    def __contains__(self, key: Any) -> bool:
        if isinstance(key, Generator):
            key = tuple(key)
        elif not isinstance(key, Sequence) or isinstance(key, str):
            key = [key]

        if not self.connected:
            raise ConnectionError(f'no connection to {self.username}@{self.hostname}:{self.port}/{self.database}/{self.table}')

        with self.connection:
            with self.connection.cursor() as cursor:
                return database_table_has_record(cursor, self.table, dict(zip(self.primary_key, key)), self.primary_key)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({repr(self.hostname)}, {repr(self.database)}, {repr(self.table)}, ' \
               f'{repr(self.fields)}, {repr(self.primary_key)}, {repr(self.crs.to_epsg())}, ' \
               f'{repr(self.username)}, {repr(re.sub(".", "*", self.password))}, {repr(self.users)})'


def database_has_table(cursor: psycopg2._psycopg.cursor, table: str) -> bool:
    """
    Whether the given table exists within the given database.

    :param cursor: psycopg2 cursor
    :param table: name of table
    :return: whether table exists
    """

    cursor.execute(f'SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=%s);', [table.lower()])
    return cursor.fetchone()[0]


def database_table_has_record(cursor: psycopg2._psycopg.cursor, table: str, record: {str: Any},
                              primary_key: str = None) -> bool:
    """
    Whether the given record exists within the given table.

    :param cursor: psycopg2 cursor
    :param table: name of table
    :param record: dictionary record
    :param primary_key: name of primary key
    :return: whether record exists in table
    """

    if primary_key is None:
        # cursor.execute(f'SELECT 1 FROM information_schema.table_constraints WHERE table_name=\'{table}\' AND
        # constraint_type= \'PRIMARY KEY\';')
        # primary_key_index = cursor.fetchone()[0] - 1
        #
        # cursor.execute(f'SELECT * FROM information_schema.columns WHERE table_name=\'{table}\';')
        # primary_key = cursor.fetchall()[primary_key_index]
        primary_key = list(record)[0]

    if not isinstance(primary_key, Sequence) or isinstance(primary_key, str):
        primary_key = [primary_key]

    values = []
    for key in primary_key:
        value = record[key]
        if type(value) is date:
            value = f'{value:%Y%m%d}'
        elif type(value) is datetime:
            value = f'{value:%Y%m%d %H%M%S}'
        values.append(value)
    values = tuple(values)

    cursor.execute(f'SELECT EXISTS(SELECT 1 FROM {table} WHERE ({", ".join(primary_key)}) = %s);', [values])
    return cursor.fetchone()[0]


def database_table_is_inherited(cursor: psycopg2._psycopg.cursor, table: str) -> bool:
    """
    Whether the given table is inherited.

    :param cursor: psycopg2 cursor
    :param table: name of table
    :return: whether table is inherited
    """

    cursor.execute(f'SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_inherits WHERE inhrelid=%s::regclass);', [f'public.{table}'])
    return cursor.fetchone()[0]


def database_table_fields(cursor: psycopg2._psycopg.cursor, table: str) -> {str: str}:
    """
    Get field names and data types of the given table, within the given database.

    :param cursor: psycopg2 cursor
    :param table: name of table
    :return: mapping of column names to the PostGres data type
    """

    cursor.execute(f'SELECT column_name, udt_name FROM information_schema.columns WHERE table_name=%s;', [table])
    return {record[0]: record[1] for record in cursor.fetchall()}


def postgis_geometry(geometry: BaseGeometry, epsg: int = None) -> str:
    """
    Convert Shapely geometry to a PostGIS geometry string.

    :param geometry: Shapely geometry
    :param epsg: EPSG code of CRS
    :return: PostGIS input string
    """

    if epsg is None:
        epsg = 4326

    return f'ST_SetSRID(\'{geometry.wkb_hex}\'::geometry, {epsg})'
