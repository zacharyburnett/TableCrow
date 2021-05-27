from os import PathLike
from pathlib import Path
import sqlite3
from typing import Union

import psycopg2

from tablecrow.tables.base import DatabaseTable
from tablecrow.tables.postgres import PostGresTable
from tablecrow.tables.postgres import database_tables as postgres_database_tables
from tablecrow.tables.sqlite import SQLiteTable
from tablecrow.tables.sqlite import database_tables as sqlite_database_tables
from tablecrow.utilities import parse_hostname

DATABASE_FUNCTIONS = {
    'PostGres': {
        'connect': psycopg2.connect,
        'table_names': postgres_database_tables,
        'table': PostGresTable,
    },
    'SQLite': {
        'connect': sqlite3.connect,
        'table_names': sqlite_database_tables,
        'table': SQLiteTable,
    },
}


def connect(
    resource: Union[str, PathLike], table_names: [str] = None, **kwargs
) -> [DatabaseTable]:
    if table_names is None:
        table_names = []

    database_type = None
    connection = None
    messages = []

    credentials = parse_hostname(resource)

    database_types = list(DATABASE_FUNCTIONS)

    path = Path(credentials['hostname']).expanduser()
    if path.exists() or path.anchor != '':
        resource = Path(resource).resolve().absolute()
        database_types = ['SQLite']
        credentials = {'database': credentials['hostname']}
    else:
        database_types.remove('SQLite')

    for credential in credentials:
        if credential in kwargs and kwargs[credential] is not None:
            credentials[credential] = kwargs[credential]

    for current_database_type in database_types:
        try:
            current_credentials = credentials.copy()
            if current_database_type == 'PostGres':
                current_credentials['host'] = current_credentials['hostname']
                current_credentials['user'] = current_credentials['username']
                del current_credentials['hostname'], current_credentials['username']
            connection = DATABASE_FUNCTIONS[current_database_type]['connect'](
                **current_credentials
            )
            database_type = current_database_type
            break
        except Exception as error:
            messages.append(f'{current_database_type} - {error.__class__.__name__}: {error}')

    if database_type is not None:
        functions = DATABASE_FUNCTIONS[database_type]
        with connection:
            cursor = connection.cursor()
            table_names.extend(functions['table_names'](cursor))
            if 'table_name' in kwargs:
                table_names.append(kwargs['table_name'])
                del kwargs['table_name']
            table_names = set(table_names)
            return [
                functions['table'](resource, table_name=table_name, **kwargs)
                for table_name in table_names
            ]
    else:
        message = ''.join(messages)
        raise ConnectionError(
            f'could not connect to "{resource}" with {[functions["connect"].__module__ + "." + functions["connect"].__name__ for functions in DATABASE_FUNCTIONS.values()]}'
            f'\n'
            f'{message}'
        )
