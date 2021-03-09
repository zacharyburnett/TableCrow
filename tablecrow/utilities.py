import configparser
from os import PathLike
from pathlib import Path
import re
from typing import Union


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
