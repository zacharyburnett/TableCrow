from os import PathLike
import sqlite3
from typing import Union

import psycopg2

from tablecrow.table import DatabaseTable
from tablecrow.tables.postgres import PostGresTable, database_tables as postgres_database_tables
from tablecrow.tables.sqlite import SQLiteTable, database_tables as sqlite_database_tables

DATABASE_FUNCTIONS = {
    'PostGres': {'connect': psycopg2.connect, 'table_names': postgres_database_tables, 'table': PostGresTable},
    'SQLite': {'connect': sqlite3.connect, 'table_names': sqlite_database_tables, 'table': SQLiteTable}
}


def connect(resource: Union[str, PathLike], table_names: [str] = None, **kwargs) -> [DatabaseTable]:
    if table_names is None:
        table_names = []

    database_type = None
    connection = None
    for current_database_type, functions in DATABASE_FUNCTIONS.items():
        try:
            connection = functions['connect'](str(resource))
            database_type = current_database_type
            break
        except:
            pass

    if database_type is not None:
        functions = DATABASE_FUNCTIONS[database_type]
        with connection:
            cursor = connection.cursor()
            table_names.extend(functions['table_names'](cursor))
            if 'table_name' in kwargs:
                table_names.append(kwargs['table_name'])
                del kwargs['table_name']
            table_names = set(table_names)
            return [functions['table'](resource, table_name=table_name, **kwargs) for table_name in table_names]
    else:
        raise ConnectionError(f'could not connect to "{resource}" with {[functions["connect"] for functions in DATABASE_FUNCTIONS.values()]}')


if __name__ == '__main__':
    tables = connect('test_database.db', table_name='test_table', fields={'name': str, 'value': float})

    print('done')
