from datetime import date, datetime
from functools import lru_cache
from logging import Logger
from os import PathLike
from pathlib import Path
import sqlite3
from sqlite3 import Connection, Cursor
from typing import Any, Mapping, Sequence, Union

from pyproj import CRS
from shapely.geometry.base import BaseGeometry, BaseMultipartGeometry

from tablecrow.tables.base import DatabaseTable, parse_record_values

SSH_DEFAULT_PORT = 22

GEOMETRY_TYPES = [
    'Point',
    'LineString',
    'Polygon',
    'MultiPoint',
    'MultiLineString',
    'MultiPolygon',
]


class SQLiteTable(DatabaseTable):
    FIELD_TYPES = {
        'NoneType': 'NULL',
        'bool': 'BOOLEAN',
        'float': 'REAL',
        'int': 'INTEGER',
        'str': 'TEXT',
        'date': 'DATE',
        'datetime': 'DATETIME',
        'bytes': 'BLOB',
        **{geometry_type: geometry_type.upper() for geometry_type in GEOMETRY_TYPES},
    }
    DEFAULT_PORT = None

    def __init__(
        self,
        path: PathLike,
        table_name: str,
        fields: {str: type} = None,
        primary_key: Union[str, Sequence[str]] = None,
        crs: CRS = None,
        logger: Logger = None,
    ):
        if '://' not in str(path):
            path = str(Path(path).expanduser().resolve())

        super().__init__(
            resource=path,
            table_name=table_name,
            database=None,
            fields=fields,
            primary_key=primary_key,
            crs=crs,
            logger=logger,
        )

        if not self.connected:
            raise ConnectionError(f'no connection to {self.path}/{self.name}')

        if self.fields is None:
            with self.connection:
                cursor = self.connection.cursor()
                self._DatabaseTable__fields = database_table_fields(cursor, self.name)

            if self.primary_key is None:
                self._DatabaseTable__primary_key = list(self.fields)[0]

        if len(self.geometry_fields) > 0:
            self.connection.enable_load_extension(True)

            try:
                self.connection.execute('SELECT load_extension("mod_spatialite")')
            except:
                import platform

                system_platform = platform.system()
                if system_platform == 'Windows':
                    message = 'download the module from `http://www.gaia-gis.it/gaia-sins/windows-bin-amd64/spatialite-loadable-modules-5.0.0-win-amd64.7z` and place `mod_spatialite.dll` / `mod_spatialite.so` in your `PATH`'
                elif system_platform in ['Linux', 'Darwin']:
                    message = (
                        'run the following: \n'
                        'sudo apt install libsqlite3-mod-spatialite \n'
                        'ln -sf /usr/lib/x86_64-linux-gnu/mod_spatialite.so /usr/lib/x86_64-linux-gnu/mod_spatialite'
                    )
                raise EnvironmentError(f'SpatiaLite module was not found; {message}')

        with self.connection:
            cursor = self.connection.cursor()
            if database_has_table(cursor, self.name):
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

                        self._DatabaseTable__fields.update(remote_fields_not_in_local_table)
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

                        cursor.execute(f'ALTER TABLE {self.name} RENAME TO {copy_table_name};')

                        cursor.execute(f'CREATE TABLE {self.name} ({self.schema});')
                        cursor.execute(f'PRAGMA table_info({copy_table_name})')
                        copy_table_fields = [record[1] for record in cursor.fetchall()]

                        cursor.execute(
                            f'INSERT INTO {self.name} ({", ".join(copy_table_fields)}) SELECT * FROM {copy_table_name};'
                        )

                        cursor.execute(f'DROP TABLE {copy_table_name};')
            else:
                self.logger.debug(f'creating remote table "{self.database}/{self.name}"')
                cursor.execute(f'CREATE TABLE {self.name} ({self.schema});')

    @property
    @lru_cache(maxsize=1)
    def path(self) -> Path:
        return Path(self.resource)

    @property
    @lru_cache(maxsize=1)
    def connection(self) -> Connection:
        return sqlite3.connect(database=self.path)

    @property
    def database(self) -> str:
        return str(self.path)

    @property
    def exists(self) -> bool:
        if self.connected:
            with self.connection:
                cursor = self.connection.cursor()
                return database_has_table(cursor, self.name)

    @property
    def schema(self) -> str:
        """ SQLite schema string """

        schema = []
        for field, field_type in self.fields.items():
            if (
                isinstance(field_type, Sequence)
                and not isinstance(field_type, str)
                or isinstance(field_type, Mapping)
            ):
                raise sqlite3.OperationalError('SQLite does not support arrays or mappings')

            try:
                field_type = self.FIELD_TYPES[field_type.__name__]
            except KeyError:
                raise TypeError(f'SQLite does not support type "{field_type}"')

            schema.append(f'{field} {field_type}')

        schema.append(f'PRIMARY KEY({", ".join(self.primary_key)})')

        return ', '.join(schema)

    @property
    def remote_fields(self) -> {str: type}:
        if not self.connected:
            raise ConnectionError(f'no connection to {self.database}/{self.name}')

        geometry_fields = [geometry_type.lower() for geometry_type in GEOMETRY_TYPES]

        with self.connection:
            cursor = self.connection.cursor()
            if database_has_table(cursor, self.name):
                fields = database_table_fields(cursor, self.name)

                for field, field_type in fields.items():
                    field_type = field_type.lower()
                    if field_type in geometry_fields:
                        if field in self.fields:
                            fields[field] = self.fields[field]
                            continue

                    for python_type, sqlite_type in self.FIELD_TYPES.items():
                        if sqlite_type.lower() == field_type:
                            if field_type in geometry_fields:
                                if python_type not in globals():
                                    exec(f'from shapely.geometry import {python_type}')
                            field_type = eval(python_type)
                            break
                    else:
                        for python_type, sqlite_type in self.FIELD_TYPES.items():
                            if python_type.lower() in field_type:
                                field_type = eval(python_type)
                                break
                        else:
                            field_type = str

                    fields[field] = field_type
            else:
                fields = None

            return fields

    @property
    def connected(self) -> bool:
        connected = False
        if self.path.exists():
            with self.connection:
                try:
                    cursor = self.connection.cursor()
                    cursor.execute('SELECT 1;')
                    cursor.fetchone()
                    connected = True
                except:
                    connected = False
        return connected

    def records_where(
        self, where: Union[Mapping[str, Any], str, Sequence[str]]
    ) -> [{str: Any}]:
        if not self.connected:
            raise ConnectionError(f'no connection to {self.database}/{self.name}')

        where_clause, where_values = self.__where_clause(where)

        with self.connection:
            cursor = self.connection.cursor()
            if where_clause is None:
                cursor.execute(f'SELECT {", ".join(self.fields.keys())} FROM {self.name}')
            else:
                try:
                    if where_values is not None:
                        cursor.execute(
                            f'SELECT * FROM {self.name} WHERE {where_clause}', where_values
                        )
                    else:
                        cursor.execute(f'SELECT * FROM {self.name} WHERE {where_clause}')
                except sqlite3.OperationalError:
                    raise
            matching_records = cursor.fetchall()

        matching_records = [
            parse_record_values(dict(zip(self.fields.keys(), record)), self.fields)
            for record in matching_records
        ]

        return matching_records

    def records_intersecting(
        self, geometry: BaseGeometry, crs: CRS = None, geometry_fields: [str] = None
    ) -> [{str: Any}]:
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
            geometry_string = f'GeomFromText(?, ?)'
            if crs != self.crs:
                geometry_string = f'Transform({geometry_string}, ?)'
                where_values.append(self.crs.to_epsg())
            where_clause.append(f'Intersects({field}, {geometry_string})')
        where_clause = ' OR '.join(where_clause)

        non_geometry_fields = {
            field: field_type
            for field, field_type in self.fields.items()
            if field_type.__name__ not in GEOMETRY_TYPES
        }

        with self.connection:
            cursor = self.connection.cursor()
            cursor.execute(
                f'SELECT {", ".join(non_geometry_fields)} '
                f'FROM {self.name} WHERE {where_clause};',
                where_values,
            )
            non_geometry_records = cursor.fetchall()
            non_geometry_records = [
                parse_record_values(
                    dict(zip(non_geometry_fields.keys(), record)), non_geometry_fields
                )
                for record in non_geometry_records
            ]

            geometry_field_string = ', '.join(
                f'asbinary({geometry_field})' for geometry_field in self.geometry_fields
            )
            cursor.execute(
                f'SELECT {geometry_field_string} ' f'FROM {self.name} WHERE {where_clause};',
                where_values,
            )
            geometry_records = cursor.fetchall()
            geometry_records = [
                parse_record_values(
                    dict(zip(self.geometry_fields.keys(), record)), self.geometry_fields
                )
                for record in geometry_records
            ]

        records = [
            {**non_geometry_records[index], **geometry_records[index]}
            for index in range(len(non_geometry_records))
        ]
        return [{field: record[field] for field in self.fields} for record in records]

    def insert(self, records: [{str: Any}]):
        if isinstance(records, dict):
            records = [records]

        if not all(field in record for field in self.primary_key for record in records):
            raise KeyError(
                f'one or more records does not contain primary key(s) "{self.primary_key}"'
            )

        if not self.connected:
            raise ConnectionError(f'no connection to {self.database}/{self.name}')

        with self.connection:
            cursor = self.connection.cursor()
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

                local_fields_in_record = [field for field in self.fields if field in record]
                geometry_fields = [field for field in self.geometry_fields if field in record]

                columns = [
                    field for field in local_fields_in_record if field not in geometry_fields
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
                    if not isinstance(primary_key_value, Sequence) or isinstance(
                        primary_key_value, str
                    ):
                        primary_key_value = [primary_key_value]
                    if len(record_without_primary_key) > 0:
                        cursor.execute(
                            f'UPDATE {self.name} '
                            f'SET ({", ".join(record_without_primary_key.keys())}) = ({", ".join("?" for _ in record_without_primary_key)})'
                            f' WHERE {primary_key_string} = ({", ".join("?" for _ in primary_key_value)});',
                            [*record_without_primary_key.values(), *primary_key_value,],
                        )
                else:
                    cursor.execute(
                        f'INSERT INTO {self.name} ({", ".join(columns)}) VALUES ({", ".join("?" for _ in values)});',
                        values,
                    )

                if len(geometry_fields) > 0:
                    geometries = {
                        field: record[field]
                        for field in geometry_fields
                        if record[field] is not None
                    }

                    for field, geometry in geometries.items():
                        cursor.execute(
                            f'UPDATE {self.name} SET {field} = GeomFromText(?, ?) '
                            f'WHERE {primary_key_string} = ?;',
                            [geometry.wkt, self.crs.to_epsg(), primary_key_value],
                        )

    def delete_where(self, where: Union[Mapping[str, Any], str, Sequence[str]]):
        if not self.connected:
            raise ConnectionError(f'no connection to {self.database}/{self.name}')

        where_clause, where_values = self.__where_clause(where)

        with self.connection:
            cursor = self.connection.cursor()
            if where_clause is None:
                cursor.execute(f'TRUNCATE {self.name};')
            else:
                try:
                    cursor.execute(
                        f'DELETE FROM {self.name} WHERE {where_clause};', where_values
                    )
                except sqlite3.OperationalError as error:
                    raise SyntaxError(f'invalid SQL syntax - {error}')
                except sqlite3.DatabaseError as error:
                    raise KeyError(error)

    def __len__(self) -> int:
        with self.connection:
            cursor = self.connection.cursor()
            cursor.execute(f'SELECT COUNT(*) FROM {self.name};')
            return cursor.fetchone()[0]

    def delete_table(self):
        with self.connection:
            cursor = self.connection.cursor()
            cursor.execute(f'DROP TABLE {self.name};')

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}({repr(self.database)}, {repr(self.name)}, '
            f'{repr(self.fields)}, {repr(self.primary_key)}, {repr(self.crs.to_epsg()) if self.crs is not None else None})'
        )

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
                        where_clause.append(f'{field} = GeomFromText(?, ?)')
                        where_values.extend([value.wkt, self.crs.to_epsg()])
                    else:
                        if isinstance(field_type, list):
                            if not isinstance(value, Sequence) or isinstance(value, str):
                                statement = f'? = ANY({field})'
                            else:
                                if fields is None:
                                    with self.connection:
                                        cursor = self.connection.cursor()
                                        fields = database_table_fields(cursor, self.name)
                                field_type = fields[field]
                                dimensions = field_type.count('_')
                                field_type = field_type.strip('_')
                                statement = f'{field} = ?::{field_type}{"[]" * dimensions}'
                        elif value is None:
                            statement = f'{field} IS ?'
                        elif isinstance(value, Sequence) and not isinstance(value, str):
                            statement = f'{field} IN ({", ".join("?" for _ in value)})'
                        elif isinstance(value, str) and '%' in value:
                            statement = f'UPPER({field}) LIKE ?'
                            value = value.upper()
                        else:
                            if isinstance(value, datetime) or isinstance(value, date):
                                value = f'{value:%Y-%m-%d %H:%M:%S}'
                            statement = f'{field} = ?'
                        if isinstance(value, Sequence) and not isinstance(value, str):
                            where_values.extend(value)
                        else:
                            where_values.append(value)
                        where_clause.append(statement)
                where_clause = ' AND '.join(where_clause)
            else:
                where_clause = ' AND '.join(where)

            if len(where_values) == 0:
                where_values = None

        return where_clause, where_values


def database_tables(cursor: Cursor) -> [str]:
    """
    List tables within the given database.

    :param cursor: sqlite3 cursor
    :return: list of table names
    """

    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table';")
    return [record[0] for record in cursor.fetchall()]


def database_has_table(cursor: Cursor, table: str) -> bool:
    """
    Whether the given table exists within the given database.

    :param cursor: sqlite3 cursor
    :param table: name of table
    :return: whether table exists
    """

    return table in database_tables(cursor)


def database_table_fields(cursor: Cursor, table: str) -> {str: str}:
    """
    Get field names and data types of the given table, within the given database.

    :param cursor: sqlite3 cursor
    :param table: name of table
    :return: mapping of column names to the SQLite data type
    """

    cursor.execute(f'PRAGMA table_info({table});')
    return {record[1]: record[2] for record in cursor.fetchall()}
