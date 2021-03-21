import configparser
import logging
from os import PathLike
from pathlib import Path
import re
import sys
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
