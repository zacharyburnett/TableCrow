from abc import ABC, abstractmethod
from ast import literal_eval
from datetime import date, datetime
import logging
from logging import Logger
import re
import socket
from typing import Any, Generator, Mapping, Sequence, Union

from shapely import wkb, wkt
from shapely.errors import WKBReadingError, WKTReadingError
from shapely.geometry import LinearRing, MultiPolygon, Polygon, shape as shapely_shape
from shapely.geometry.base import BaseGeometry, GEOMETRY_TYPES


class DatabaseTable(ABC):
    DEFAULT_PORT = NotImplementedError
    FIELD_TYPES: {str: str} = NotImplementedError

    def __init__(
            self,
            hostname: str,
            database: str,
            name: str,
            fields: {str: type},
            primary_key: Union[str, Sequence[str]] = None,
            username: str = None,
            password: str = None,
            users: [str] = None,
            logger: Logger = None,
    ):
        """
        Create a new database table interface.

        :param hostname: URL of database server as `hostname:port`
        :param database: name of database in server
        :param name: name of table in database
        :param fields: dictionary of fields
        :param primary_key: primary key field(s)
        :param username: username to connect ot database
        :param password: password to connect to database
        :param users: list of database users / roles
        """

        hostname, port = split_URL_port(hostname)
        if port is None:
            port = self.DEFAULT_PORT
        if '@' in hostname:
            username, hostname = hostname.split('@', 1)
        if ':' in username:
            username, password = username.split(':', 1)
        if primary_key is None:
            primary_key = [list(fields)[0]]
        elif not isinstance(primary_key, Sequence) or isinstance(primary_key, str):
            primary_key = [primary_key]
        if users is None:
            users = []
        if logger is None:
            logger = logging.getLogger('dummy')

        self.__hostname = hostname
        self.__port = port

        self.__database = database
        self.__name = name

        self.__fields = fields
        self.__primary_key = primary_key

        self.__username = username
        self.__password = password

        self.__users = users

        self.logger = logger

    @property
    def hostname(self) -> str:
        return self.__hostname

    @property
    def port(self) -> int:
        return self.__port

    @property
    def database(self) -> str:
        return self.__database

    @property
    def name(self) -> str:
        return self.__name

    @property
    def fields(self) -> {str: type}:
        return self.__fields

    @property
    def primary_key(self) -> [str]:
        return self.__primary_key

    @property
    def username(self) -> str:
        return self.__username

    @property
    def password(self) -> str:
        return self.__password

    @property
    def users(self) -> [str]:
        return self.__users

    @property
    def exists(self) -> bool:
        raise NotImplementedError

    @property
    def schema(self) -> str:
        """ SQL schema string """
        raise NotImplementedError

    @property
    def geometry_fields(self) -> {str: type}:
        """ local fields with geometry type """
        geometry_fields = {}
        for field, field_type in self.fields.items():
            while isinstance(field_type, Sequence) and not isinstance(field_type, str):
                if len(field_type) > 0:
                    field_type = field_type[0]
                else:
                    field_type = list
            if field_type.__name__ in GEOMETRY_TYPES:
                geometry_fields[field] = field_type
        return geometry_fields

    @property
    def remote_fields(self) -> {str: type}:
        """ fields at remote table """
        raise NotImplementedError

    @property
    def connected(self) -> bool:
        """ whether network connection exists to database server """
        try:
            socket.setdefaulttimeout(2)
            socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect(
                (self.hostname, self.port)
            )
            return True
        except socket.error:
            return False

    @property
    def records(self) -> [{str: Any}]:
        """ list of records in the table """
        return self.records_where(None)

    def records_where(
            self, where: Union[Mapping[str, Any], str, Sequence[str]]
    ) -> [{str: Any}]:
        """
        list of records in the table that match the query

        :param where: dictionary mapping keys to values, with which to match records
        :return: dictionaries of matching records
        """

        raise NotImplementedError

    def insert(self, records: [{str: Any}]):
        """
        insert the list of records into the table

        :param records: dictionary records
        """

        raise NotImplementedError

    def delete_where(self, where: Union[Mapping[str, Any], str, Sequence[str]]):
        """
        delete records from the table matching the given query

        :param where: dictionary mapping keys to values, with which to match records
        """

        raise NotImplementedError

    def __getitem__(self, key: Any) -> {str: Any}:
        """
        Return the record matching the given primary key value.

        :param key: value of primary key
        :return: dictionary record
        """

        if isinstance(key, dict):
            if not all(field in key for field in self.primary_key):
                raise ValueError(f'does not contain "{self.primary_key}"')
            where = key
        else:
            if isinstance(key, Generator):
                key = list(key)
            elif not isinstance(key, Sequence) or isinstance(key, str):
                key = [key]
            if len(key) != len(self.primary_key):
                raise ValueError(f'ambiguous value for primary key "{self.primary_key}"')
            where = {field: key[index] for index, field in enumerate(self.primary_key)}

        if not self.connected:
            raise ConnectionError(
                f'no connection to {self.username}@{self.hostname}:{self.port}/{self.database}/{self.name}'
            )

        try:
            records = self.records_where(where)

            if len(records) > 1:
                self.logger.warning(
                    f'found more than one record matching query {where}: {records}'
                )

            if len(records) > 0:
                return records[0]
            else:
                raise KeyError(f'no record with primary key "{key}"')
        except:
            raise KeyError(f'no record with primary key "{key}"')

    def __setitem__(self, key: Any, record: {str: Any}):
        """
        Insert the given record into the table.

        :param key: value of primary key at which to insert record
        :param record: dictionary record
        """

        if isinstance(key, Generator):
            key = list(key)
        elif isinstance(key, dict):
            if not all(field in key for field in self.primary_key):
                raise KeyError(f'does not contain "{self.primary_key}"')
            key = [key[field] for field in self.primary_key]
        elif not isinstance(key, Sequence) or isinstance(key, str):
            key = [key]

        for key_index, primary_key in enumerate(self.primary_key):
            record[primary_key] = key[key_index]

        if not self.connected:
            raise ConnectionError(
                f'no connection to {self.username}@{self.hostname}:{self.port}/{self.database}/{self.name}'
            )

        self.insert([record])

    def __delitem__(self, key: Any):
        """
        Delete the record matching the given primary key value.

        :param key: value of primary key
        """

        if isinstance(key, dict):
            if not all(field in key for field in self.primary_key):
                raise ValueError(f'does not contain "{self.primary_key}"')
            where = key
        else:
            if isinstance(key, Generator):
                key = list(key)
            elif not isinstance(key, Sequence) or isinstance(key, str):
                key = [key]
            if len(key) != len(self.primary_key):
                raise ValueError(f'ambiguous value for primary key "{self.primary_key}"')
            where = {field: key[index] for index, field in enumerate(self.primary_key)}

        if not self.connected:
            raise ConnectionError(
                f'no connection to {self.username}@{self.hostname}:{self.port}/{self.database}/{self.name}'
            )

        try:
            self.delete_where(where)
        except:
            raise KeyError(f'no record with primary key "{key}"')

    def __len__(self) -> int:
        return len(self.records)

    def __contains__(self, key: Any) -> bool:
        if not self.connected:
            raise ConnectionError(
                f'no connection to {self.username}@{self.hostname}:{self.port}/{self.database}/{self.name}'
            )

        try:
            self[key]
            return True
        except KeyError:
            return False

    @abstractmethod
    def delete_table(self):
        raise NotImplementedError

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}({repr(self.hostname)}, {repr(self.database)}, {repr(self.name)}, '
            f'{repr(self.fields)}, {repr(self.primary_key)}, '
            f'{repr(self.username)}, {repr(re.sub(".", "*", self.password))}, {repr(self.users)})'
        )


def parse_record_values(record: {str: Any}, field_types: {str: type}) -> {str: Any}:
    """
    Parse the values in the given record into their respective field types.

    :param record: dictionary mapping fields to values
    :param field_types: dictionary mapping fields to types
    :return: record with values parsed into their respective types
    """

    for field, value in record.items():
        if field in field_types:
            field_type = field_types[field]
            value_type = type(value)

            if value_type is not field_type and value is not None:
                if field_type is bool:
                    value = (
                        bool(value)
                        if value_type is not str
                        else literal_eval(value.capitalize())
                    )
                elif field_type is int:
                    value = int(value)
                elif field_type is float:
                    value = float(value)
                elif field_type is str:
                    value = str(value)
                elif value_type is str:
                    if field_type is list:
                        value = literal_eval(value)
                    elif field_type in (date, datetime):
                        value = datetime.strptime(value, '%Y%m%d')
                        if field_type is date:
                            value = value.date()
                    elif field_type.__name__ in GEOMETRY_TYPES:
                        try:
                            value = wkb.loads(value, hex=True)
                        except WKBReadingError:
                            try:
                                value = wkt.loads(value)
                            except WKTReadingError:
                                try:
                                    value = wkb.loads(value)
                                except TypeError:
                                    value = shapely_shape(literal_eval(value))
                record[field] = value
    return record


def random_open_tcp_port() -> int:
    open_socket = socket.socket()
    open_socket.bind(('', 0))
    return open_socket.getsockname()[1]


def split_URL_port(url: str) -> (str, Union[str, None]):
    """
    Split the given URL into host and port, assuming port is appended after a colon.

    :param url: URL string
    :return: URL and port (if found)
    """

    port = None

    if url.count(':') > 0:
        url = url.split(':')
        if 'http' in url:
            url = ':'.join(url[:2])
            if len(url) > 2:
                port = int(url[2])
        else:
            url, port = url
            port = int(port)

    return url, port


def flatten_geometry(geometry: BaseGeometry) -> BaseGeometry:
    geometry_type = type(geometry)

    # strip 3rd dimension
    if 'POLYGON Z' in geometry.wkt:
        polygons = (
            [polygon for polygon in geometry] if geometry_type is MultiPolygon else [geometry]
        )
        for polygon_index, polygon in enumerate(polygons):
            exterior_2d = LinearRing([vertex[:2] for vertex in polygon.exterior.coords])
            interiors_2d = [
                LinearRing([vertex[:2] for vertex in interior.coords])
                for interior in polygon.interiors
            ]
            polygons[polygon_index] = Polygon(exterior_2d, interiors_2d)
        geometry = (
            MultiPolygon(polygons) if geometry_type is MultiPolygon else Polygon(polygons[0])
        )

    if not geometry.is_valid:
        geometry = geometry.buffer(0)
    return geometry
