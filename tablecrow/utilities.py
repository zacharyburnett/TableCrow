import configparser
from os import PathLike
from pathlib import Path


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
