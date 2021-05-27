import configparser
from datetime import date, datetime, timedelta
from enum import Enum, EnumMeta
import json
import logging
from os import PathLike
from pathlib import Path
import re
import sys
from typing import Any, Collection, Iterable, Mapping, Union

from dateutil.parser import parse as parse_date
from pyproj import CRS
from shapely import wkb, wkt
from shapely.geometry import shape as shapely_shape
from shapely.geometry.base import GEOMETRY_TYPES


def read_configuration(filename: PathLike) -> {str: str}:
    configuration_file = configparser.ConfigParser()
    configuration_file.read(filename)
    return {
        section_name: {key: value for key, value in section.items()}
        for section_name, section in configuration_file.items()
        if section_name.upper() != 'DEFAULT'
    }


def repository_root(path: PathLike = None) -> Path:
    if path is None:
        path = __file__
    if not isinstance(path, Path):
        path = Path(path)
    if path.is_file():
        path = path.parent
    if '.git' in (child.name for child in path.iterdir()) or path == path.parent:
        return path
    else:
        return repository_root(path.parent)


def split_hostname_port(hostname: str) -> (str, Union[str, None]):
    """
    Split the given URL into host and port, assuming port is appended after a colon.

    :param hostname: hostname string
    :return: hostname and port (if found, otherwise `None`)
    """

    port = None

    if ':' in hostname:
        parts = hostname.rsplit(':', 1)
        try:
            port = int(parts[-1])
            hostname = parts[0]
        except ValueError:
            pass

    return hostname, port


def parse_hostname(hostname: str) -> {str: str}:
    username = None
    password = None

    hostname, port = split_hostname_port(hostname)

    protocol_pattern = re.compile(r'^(?:http|ftp)s?://')
    result = re.search(protocol_pattern, hostname)
    protocol = result.group(0) if result is not None else ''
    hostname = re.sub(protocol_pattern, '', hostname)

    if '@' in hostname:
        username, hostname = hostname.split('@', 1)

    hostname = protocol + hostname

    if username is not None and ':' in username:
        username, password = username.split(':', 1)

    return {
        'hostname': hostname,
        'port': port,
        'username': username,
        'password': password,
    }


def convert_value(value: Any, to_type: type) -> Any:
    if isinstance(to_type, str):
        to_type = eval(to_type)

    if isinstance(value, Enum):
        value = value.name

    if to_type is None:
        value = None
    elif isinstance(to_type, Collection):
        collection_type = type(to_type)
        if collection_type is not EnumMeta:
            if not issubclass(collection_type, Mapping):
                if value is not None:
                    to_type = list(to_type)
                    if not isinstance(value, Iterable) or isinstance(value, str):
                        value = [value]
                    if len(to_type) == 1:
                        to_type = [to_type[0] for _ in value]
                    elif len(to_type) == len(value):
                        to_type = to_type[: len(value)]
                    else:
                        raise ValueError(
                            f'unable to convert list of values of length {len(value)} '
                            f'to list of types of length {len(to_type)}: '
                            f'{value} -/> {to_type}'
                        )
                    value = collection_type(
                        convert_value(value[index], current_type)
                        for index, current_type in enumerate(to_type)
                    )
                else:
                    value = collection_type()
            elif isinstance(value, str):
                value = json.loads(value)
            elif isinstance(value, CRS):
                value = value.to_json_dict()
        elif value is not None:
            try:
                value = to_type[value]
            except (KeyError, ValueError):
                try:
                    value = to_type(value)
                except (KeyError, ValueError):
                    raise ValueError(
                        f'unrecognized entry "{value}"; must be one of {list(to_type)}'
                    )
    elif not isinstance(value, to_type) and value is not None:
        if issubclass(to_type, (datetime, date)):
            value = parse_date(value)
            if issubclass(to_type, date) and not issubclass(to_type, datetime):
                value = value.date()
        elif issubclass(to_type, timedelta):
            try:
                try:
                    time = datetime.strptime(value, '%H:%M:%S')
                    value = timedelta(
                        hours=time.hour, minutes=time.minute, seconds=time.second
                    )
                except:
                    parts = [float(part) for part in value.split(':')]
                    if len(parts) > 3:
                        days = parts.pop(0)
                    else:
                        days = 0
                    value = timedelta(
                        days=days, hours=parts[0], minutes=parts[1], seconds=parts[2]
                    )
            except:
                value = timedelta(seconds=float(value))
        elif to_type.__name__ in GEOMETRY_TYPES:
            try:
                value = wkb.loads(value, hex=True)
            except:
                try:
                    value = wkt.loads(value)
                except:
                    try:
                        value = wkb.loads(value)
                    except TypeError:
                        if isinstance(value, str):
                            value = eval(value)
                        try:
                            value = shapely_shape(value)
                        except:
                            value = to_type(value)
        elif issubclass(to_type, bool):
            try:
                value = eval(f'{value}')
            except:
                value = bool(value)

        if not isinstance(value, to_type):
            if isinstance(value, timedelta):
                if issubclass(to_type, str):
                    hours, remainder = divmod(value, timedelta(hours=1))
                    minutes, remainder = divmod(remainder, timedelta(minutes=1))
                    seconds = remainder / timedelta(seconds=1)
                    value = f'{hours:02}:{minutes:02}:{seconds:04.3}'
                else:
                    value /= timedelta(seconds=1)
            elif isinstance(value, CRS):
                if issubclass(to_type, str):
                    value = value.to_wkt()
                elif issubclass(to_type, dict):
                    value = value.to_json_dict()
                elif issubclass(to_type, int):
                    value = value.to_epsg()
            elif type(value).__name__ in GEOMETRY_TYPES and to_type.__name__ in GEOMETRY_TYPES:
                raise NotImplementedError('casting between geometric types not implemented')
            elif isinstance(value, (str, bytes)):
                try:
                    value = to_type.from_string(value)
                except:
                    value = to_type(value)
            else:
                value = to_type(value)

    return value


def get_logger(
    name: str,
    log_filename: PathLike = None,
    file_level: int = None,
    console_level: int = None,
    log_format: str = None,
) -> logging.Logger:
    if file_level is None:
        file_level = logging.DEBUG
    if console_level is None:
        console_level = logging.INFO
    logger = logging.getLogger(name)

    # check if logger is already configured
    if logger.level == logging.NOTSET and len(logger.handlers) == 0:
        # check if logger has a parent
        if '.' in name:
            logger.parent = get_logger(name.rsplit('.', 1)[0])
        else:
            # otherwise create a new split-console logger
            logger.setLevel(logging.DEBUG)
            if console_level != logging.NOTSET:
                if console_level <= logging.INFO:

                    class LoggingOutputFilter(logging.Filter):
                        def filter(self, rec):
                            return rec.levelno in (logging.DEBUG, logging.INFO)

                    console_output = logging.StreamHandler(sys.stdout)
                    console_output.setLevel(console_level)
                    console_output.addFilter(LoggingOutputFilter())
                    logger.addHandler(console_output)

                console_errors = logging.StreamHandler(sys.stderr)
                console_errors.setLevel(max((console_level, logging.WARNING)))
                logger.addHandler(console_errors)

    if log_filename is not None:
        if not isinstance(log_filename, Path):
            log_filename = Path(log_filename)
        log_filename = log_filename.resolve().expanduser()
        file_handler = logging.FileHandler(log_filename)
        file_handler.setLevel(file_level)
        for existing_file_handler in [
            handler for handler in logger.handlers if type(handler) is logging.FileHandler
        ]:
            logger.removeHandler(existing_file_handler)
        logger.addHandler(file_handler)

    if log_format is None:
        log_format = '%(asctime)s | %(levelname)-8s | %(message)s'
    log_formatter = logging.Formatter(log_format)
    for handler in logger.handlers:
        handler.setFormatter(log_formatter)

    return logger
