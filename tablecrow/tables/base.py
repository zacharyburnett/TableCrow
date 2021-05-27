from abc import ABC, abstractmethod
import logging
from logging import Logger
from pathlib import Path
import socket
from typing import Any, Generator, Mapping, Sequence, Union

from pyproj import CRS
from shapely.geometry import LinearRing, MultiPolygon, Polygon
from shapely.geometry.base import BaseGeometry, GEOMETRY_TYPES

from tablecrow.utilities import convert_value, get_logger, parse_hostname

DEFAULT_CRS = CRS.from_epsg(4326)


class TableNotFoundError(FileNotFoundError):
    pass


class DatabaseTable(ABC):
    DEFAULT_PORT = NotImplementedError
    FIELD_TYPES: {str: str} = NotImplementedError

    def __init__(
        self,
        resource: str,
        table_name: str,
        database: str = None,
        fields: {str: type} = None,
        primary_key: Union[str, Sequence[str]] = None,
        crs: CRS = None,
        username: str = None,
        password: str = None,
        users: [str] = None,
        logger: Logger = None,
    ):
        """
        Create a new database table interface.

        :param resource: URL of database server as `hostname:port`
        :param table_name: name of table in database
        :param database: name of database in server
        :param fields: dictionary of fields
        :param primary_key: primary key field(s)
        :param crs: coordinate reference system of table geometries
        :param username: username to connect ot database
        :param password: password to connect to database
        :param users: list of database users / roles
        """

        self.__database = database
        self.__name = table_name
        self.__fields = fields

        if logger is None:
            logger = get_logger('dummy', console_level=logging.NOTSET)

        self.logger = logger

        if resource is not None:
            if Path(resource).exists():
                port = None
            else:
                credentials = parse_hostname(resource)
                resource = credentials['hostname']
                port = credentials['port']
                if port is None:
                    port = self.DEFAULT_PORT
                if username is None:
                    username = credentials['username']
                if password is None:
                    password = credentials['password']
        else:
            port = None

        self.__resource = resource
        self.__port = port

        if username is not None and ':' in username:
            username, password = username.split(':', 1)

        self.__username = username
        self.__password = password

        if primary_key is None:
            primary_key = [list(self.fields)[0]] if self.fields is not None else None
        elif not isinstance(primary_key, Sequence) or isinstance(primary_key, str):
            primary_key = [primary_key]

        self.__primary_key = primary_key

        if crs is not None:
            crs = parse_crs(crs)
        elif len(self.geometry_fields) > 0:
            crs = DEFAULT_CRS
            self.logger.warning(
                f'no CRS provided for geometry fields; defaulting to EPSG:{crs.to_epsg()}'
            )
        else:
            crs = None
        self.__crs = crs

        if users is None:
            users = []

        self.__users = users

        if self.fields is None and not self.exists:
            raise TableNotFoundError(
                f'fields must be specified when creating a table; table does not exist at "{self.database}:{self.name}"'
            )

    @property
    def resource(self) -> str:
        return self.__resource

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
        if self.__fields is None:
            self.__fields = self.remote_fields
        return self.__fields

    @property
    def primary_key(self) -> [str]:
        return self.__primary_key

    @property
    def crs(self) -> CRS:
        return self.__crs

    @property
    def username(self) -> str:
        return self.__username

    @property
    def users(self) -> [str]:
        return self.__users

    @property
    @abstractmethod
    def exists(self) -> bool:
        raise NotImplementedError

    @property
    @abstractmethod
    def schema(self) -> str:
        """ SQL schema string """
        raise NotImplementedError

    @property
    def geometry_fields(self) -> {str: type}:
        """ local fields with geometry type """
        geometry_fields = {}
        if self.fields is not None:
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
    @abstractmethod
    def remote_fields(self) -> {str: type}:
        """ fields at remote table """
        raise NotImplementedError

    @property
    def connected(self) -> bool:
        """ whether network connection exists to database server """
        try:
            socket.setdefaulttimeout(2)
            socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect(
                (self.resource, self.port)
            )
            return True
        except socket.error:
            return False

    @property
    def records(self) -> [{str: Any}]:
        """ list of records in the table """
        return self.records_where(None)

    @abstractmethod
    def records_where(
        self, where: Union[Mapping[str, Any], str, Sequence[str]]
    ) -> [{str: Any}]:
        """
        list of records in the table that match the query

        :param where: dictionary mapping keys to values, with which to match records
        :return: dictionaries of matching records
        """

        raise NotImplementedError

    @abstractmethod
    def insert(self, records: [{str: Any}]):
        """
        insert the list of records into the table

        :param records: dictionary records
        """

        raise NotImplementedError

    @abstractmethod
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
                f'no connection to {self.username}@{self.resource}:{self.port}/{self.database}/{self.name}'
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
                f'no connection to {self.username}@{self.resource}:{self.port}/{self.database}/{self.name}'
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
                f'no connection to {self.username}@{self.resource}:{self.port}/{self.database}/{self.name}'
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
                f'no connection to {self.username}@{self.resource}:{self.port}/{self.database}/{self.name}'
            )

        try:
            self[key]
            return True
        except KeyError:
            return False

    def __iter__(self) -> Generator:
        yield from self.records

    @abstractmethod
    def delete_table(self):
        raise NotImplementedError

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}({repr(self.database)}, {repr(self.name)}, {repr(self.fields)}, {repr(self.primary_key)}, '
            f'{repr(self.resource)}, {repr(self.username)}, {repr("*" * len(self.password))}, {repr(self.users)})'
        )


def random_open_tcp_port() -> int:
    open_socket = socket.socket()
    open_socket.bind(('', 0))
    return open_socket.getsockname()[1]


def crs_key(crs: CRS) -> str:
    if not isinstance(crs, CRS):
        crs = parse_crs(crs)
    return crs.wkt.split('"')[1]


def is_compound_crs(crs: CRS) -> bool:
    if not isinstance(crs, CRS):
        crs = parse_crs(crs)
    return 'COMPD_CS' in crs.wkt or 'COMPOUNDCRS' in crs.wkt


def split_compound_crs(crs: CRS) -> [CRS]:
    """
    Split the given compound coordinate reference system into its constituent CRS parts.

    :param crs: compound CRS
    :returns: list of CRS parts
    """

    if type(crs) is not CRS:
        crs = parse_crs(crs)

    if is_compound_crs(crs):
        working_string = crs.wkt

        # remove the compound CRS keyword and name from the string, along with the closing bracket
        working_string = working_string.split(',', 1)[-1][:-1]

        wkts = []
        while len(working_string) > 0:
            opening_brackets = 0
            closing_brackets = 0
            for index, character in enumerate(working_string):
                if character == '[':
                    opening_brackets += 1
                elif character == ']':
                    closing_brackets += 1

                if opening_brackets > 0 and opening_brackets == closing_brackets:
                    wkts.append(working_string[: index + 1])
                    working_string = working_string[index + 2 :]
                    break
            else:
                wkts.append(working_string)
                break

        return [CRS.from_string(wkt) for wkt in wkts]


def compound_crs(crs_list: [CRS], key: str = None) -> CRS:
    """
    Build a compound coordinate reference system from the provided list of constituent CRSs.

    :param crs_list: list of coordinate reference systems
    :param key: name of CRS
    :returns: compound CRS
    """

    crs_list = [crs if type(crs) is CRS else parse_crs(crs) for crs in crs_list]

    if key is None:
        key = ' + '.join(crs_key(crs) for crs in crs_list)

    # TODO is keyword specced as COMPOUNDCRS?
    return CRS.from_string(f'COMPD_CS["{key}", {", ".join(crs.wkt for crs in crs_list)}]')


def parse_crs(crs: Union[str, int]) -> CRS:
    """
    Parse a CRS object from the given well-known text or EPSG code.

    :param crs: coordinate reference system; either well-known text or an EPSG code
    :returns: CRS object
    """

    if isinstance(crs, CRS):
        return crs
    elif (
        isinstance(crs, str)
        and '+' in crs
        and 'COMPD_CS' not in crs
        and 'COMPOUNDCRS' not in crs
    ):
        return compound_crs([parse_crs(crs_part.strip()) for crs_part in crs.split('+')])
    else:
        try:
            return CRS.from_epsg(int(crs))
        except ValueError:
            return CRS.from_string(str(crs))


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
            record[field] = convert_value(value, field_type)
    return record
