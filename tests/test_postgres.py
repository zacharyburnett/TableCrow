from datetime import datetime
from functools import partial
import os

import psycopg2
from pyproj import CRS
import pytest
from shapely.geometry import MultiPolygon, Point, box
from sshtunnel import SSHTunnelForwarder

from tablecrow import PostGresTable
from tablecrow.table import random_open_tcp_port, split_URL_port
from tablecrow.tables.postgres import (
    SSH_DEFAULT_PORT,
    database_has_table,
    database_table_fields,
)
from tablecrow.utilities import read_configuration, repository_root

CREDENTIALS_FILENAME = repository_root() / 'credentials.config'
CREDENTIALS = read_configuration(CREDENTIALS_FILENAME)

if 'database' not in CREDENTIALS:
    CREDENTIALS['database'] = {
        'hostname': os.environ['POSTGRES_HOSTNAME'],
        'database': os.environ['POSTGRES_DATABASE'],
        'username': os.environ['POSTGRES_USERNAME'],
        'password': os.environ['POSTGRES_PASSWORD']
    }
    if 'ssh_hostname' in os.environ:
        CREDENTIALS['database']['ssh_hostname'] = os.environ['SSH_HOSTNAME']
    if 'ssh_username' in os.environ:
        CREDENTIALS['database']['ssh_username'] = os.environ['SSH_USERNAME']
    if 'ssh_password' in os.environ:
        CREDENTIALS['database']['ssh_password'] = os.environ['SSH_PASSWORD']

if 'ssh_hostname' in CREDENTIALS['database'] and CREDENTIALS['database']['ssh_hostname'] is not None:
    hostname, port = split_URL_port(CREDENTIALS['database']['hostname'])
    if port is None:
        port = PostGresTable.DEFAULT_PORT

    ssh_hostname, ssh_port = split_URL_port(CREDENTIALS['database']['ssh_hostname'])
    if ssh_port is None:
        ssh_port = SSH_DEFAULT_PORT

    if '@' in ssh_hostname:
        ssh_username, ssh_hostname = ssh_hostname.split('@', 1)

    ssh_username = CREDENTIALS['database']['ssh_username']

    if ssh_username is not None and ':' in ssh_username:
        ssh_username, ssh_password = ssh_hostname.split(':', 1)

    ssh_password = CREDENTIALS['database']['ssh_password']

    TUNNEL = SSHTunnelForwarder((ssh_hostname, ssh_port),
                                ssh_username=ssh_username, ssh_password=ssh_password,
                                remote_bind_address=('localhost', port),
                                local_bind_address=('localhost', random_open_tcp_port()))
    try:
        TUNNEL.start()
    except Exception as error:
        raise ConnectionError(error)
else:
    TUNNEL = None


@pytest.fixture
def connection() -> psycopg2.connect:
    hostname, port = split_URL_port(CREDENTIALS['database']['hostname'])
    if port is None:
        port = PostGresTable.DEFAULT_PORT

    connector = partial(psycopg2.connect, database=CREDENTIALS['database']['database'], user=CREDENTIALS['database']['username'],
                        password=CREDENTIALS['database']['password'])
    if tunnel := TUNNEL is not None:
        try:
            tunnel.start()
        except Exception as error:
            raise ConnectionError(error)
        connection = connector(host=tunnel.local_bind_host, port=tunnel.local_bind_port)
    else:
        connection = connector(host=hostname, port=port)

    return connection


def test_table_creation(connection):
    table_name = 'test_table_creation'

    fields = {
        'primary_key_field': int,
        'field_1': datetime,
        'field_2': float,
        'field_3': str,
        'field_4': [str],
        'field_5': Point,
        'field_6': MultiPolygon,
    }

    with connection:
        with connection.cursor() as cursor:
            if database_has_table(cursor, table_name):
                cursor.execute(f'DROP TABLE {table_name};')

    table = PostGresTable(
        name=table_name,
        fields=fields,
        primary_key='primary_key_field',
        **CREDENTIALS['database'],
    )

    test_remote_fields = table.remote_fields

    with connection:
        with connection.cursor() as cursor:
            test_raw_remote_fields = database_table_fields(cursor, table_name)
            if table.exists:
                table.delete_table()
                table_exists = database_has_table(cursor, table_name)
                if table_exists:
                    cursor.execute(f'DROP TABLE {table_name};')

    assert test_remote_fields == fields
    assert list(test_raw_remote_fields) == list(fields)
    assert not table_exists


def test_compound_primary_key(connection):
    table_name = 'test_compound_primary_key'

    fields = {
        'primary_key_field_1': int,
        'primary_key_field_2': str,
        'primary_key_field_3': datetime,
        'field_1': float,
        'field_2': str,
    }

    records = [
        {
            'primary_key_field_1': 1,
            'primary_key_field_2': 'test 1',
            'primary_key_field_3': datetime(2020, 1, 1),
            'field_1': None,
            'field_2': 'test 1',
        },
        {
            'primary_key_field_1': 2,
            'primary_key_field_2': 'test 1',
            'primary_key_field_3': datetime(2020, 1, 2),
            'field_1': 5.67,
            'field_2': None,
        },
    ]

    extra_record = {
        'primary_key_field_1': 3,
        'primary_key_field_2': 'test 3',
        'primary_key_field_3': datetime(2020, 1, 3),
        'field_1': 2.5,
        'field_2': None,
    }
    extra_record_to_insert = {
        'primary_key_field_2': 'overwritten value',
        'primary_key_field_3': datetime(2020, 1, 3),
        'field_1': 2.5,
        'field_2': None,
    }

    primary_key = ('primary_key_field_1', 'primary_key_field_2', 'primary_key_field_3')

    with connection:
        with connection.cursor() as cursor:
            if database_has_table(cursor, table_name):
                cursor.execute(f'DROP TABLE {table_name};')

    table = PostGresTable(
        name=table_name,
        fields=fields,
        primary_key=primary_key,
        **CREDENTIALS['database'],
    )

    test_primary_key = primary_key
    table.insert(records)

    with pytest.raises(ValueError):
        table[1]
    with pytest.raises(IndexError):
        table[1] = extra_record_to_insert

    table[3, 'test 3', datetime(2020, 1, 3)] = extra_record_to_insert

    test_record = table[1, 'test 1', datetime(2020, 1, 1)]
    test_records = table.records

    with connection:
        with connection.cursor() as cursor:
            test_raw_remote_fields = database_table_fields(cursor, table_name)
            if database_has_table(cursor, table_name):
                cursor.execute(f'DROP TABLE {table_name};')

    assert test_primary_key == primary_key
    assert test_records == records + [extra_record]
    assert test_record == records[0]
    assert list(test_raw_remote_fields) == list(fields)


def test_record_insertion(connection):
    table_name = 'test_record_insertion'

    fields = {
        'primary_key_field': int,
        'field_1': datetime,
        'field_2': float,
        'field_3': str,
    }

    records = [
        {'primary_key_field': 1, 'field_1': datetime(2020, 1, 1), 'field_3': 'test 1'},
        {'primary_key_field': 2, 'field_1': datetime(2020, 1, 2), 'field_2': 5.67},
    ]

    extra_record = {
        'primary_key_field': 3,
        'field_1': datetime(2020, 1, 3),
        'field_2': 3,
        'field_3': 'test 3',
    }

    with connection:
        with connection.cursor() as cursor:
            if database_has_table(cursor, table_name):
                cursor.execute(f'DROP TABLE {table_name};')

    table = PostGresTable(
        name=table_name,
        fields=fields,
        primary_key='primary_key_field',
        **CREDENTIALS['database'],
    )
    table.insert(records)
    test_records_before_addition = table.records
    table[extra_record['primary_key_field']] = extra_record
    test_records_after_addition = table.records

    del table[extra_record['primary_key_field']]
    test_records_after_deletion = table.records

    records[0]['field_2'] = None
    records[1]['field_3'] = None

    assert records[0] in table
    assert records[0]['primary_key_field'] in table
    assert (records[0][field] for field in ['primary_key_field']) in table
    assert 'nonexistant' not in table
    assert len(table) == 2

    with pytest.raises(ValueError):
        key_without_primary_key = {
            field: records[0][field]
            for field in records[0]
            if field not in ['primary_key_field']
        }
        key_without_primary_key in table

    table.insert(records[0])

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(f'DROP TABLE {table_name};')

    assert test_records_before_addition == records
    assert test_records_after_addition == records + [extra_record]
    assert test_records_after_deletion == records


def test_table_flexibility(connection):
    table_name = 'test_table_flexibility'

    fields = {
        'primary_key_field': int,
        'field_1': datetime,
        'field_2': float,
        'field_3': str,
    }

    incomplete_fields = {'primary_key_field': int, 'field_3': str}

    records = [
        {'primary_key_field': 1, 'field_1': datetime(2020, 1, 1), 'field_3': 'test 1'}
    ]

    with connection:
        with connection.cursor() as cursor:
            if database_has_table(cursor, table_name):
                cursor.execute(f'DROP TABLE {table_name};')

    # create table with incomplete fields
    incomplete_table = PostGresTable(
        name=table_name,
        fields=incomplete_fields,
        primary_key='primary_key_field',
        **CREDENTIALS['database'],
    )
    incomplete_table.insert(records)
    incomplete_records = incomplete_table.records

    with connection:
        with connection.cursor() as cursor:
            test_incomplete_remote_fields = database_table_fields(cursor, table_name)

    # create table with complete fields, pointing to existing remote table with incomplete fields
    complete_table = PostGresTable(
        name=table_name,
        fields=fields,
        primary_key='primary_key_field',
        **CREDENTIALS['database'],
    )
    complete_records = complete_table.records

    with connection:
        with connection.cursor() as cursor:
            test_complete_remote_fields = database_table_fields(cursor, table_name)

    # create table with incomplete fields, pointing to existing remote table with complete fields
    completed_table = PostGresTable(
        name=table_name,
        fields=incomplete_fields,
        primary_key='primary_key_field',
        **CREDENTIALS['database'],
    )
    completed_records = completed_table.records

    with connection:
        with connection.cursor() as cursor:
            test_completed_remote_fields = database_table_fields(cursor, table_name)
            cursor.execute(f'DROP TABLE {table_name};')

    assert list(test_complete_remote_fields) == list(fields)
    assert list(test_completed_remote_fields) == list(fields)

    for test_records in (incomplete_records, complete_records, completed_records):
        for record_index, record in enumerate(test_records):
            record = records[record_index]
            for field, value in record.items():
                assert value == record[field]


def test_list_type(connection):
    table_name = 'test_list_type'

    fields = {'primary_key_field': int, 'field_1': [str], 'field_2': tuple([str])}

    records = [
        {'primary_key_field': 1, 'field_1': ['test 1', 'test 2']},
        {'primary_key_field': 2, 'field_1': ['test 3', 'test 1']},
        {'primary_key_field': 3, 'field_2': ['test 1', 'test 2']},
    ]

    with connection:
        with connection.cursor() as cursor:
            if database_has_table(cursor, table_name):
                cursor.execute(f'DROP TABLE {table_name};')

    table = PostGresTable(
        name=table_name,
        fields=fields,
        primary_key='primary_key_field',
        **CREDENTIALS['database'],
    )

    table.insert(records)

    test_records = table.records

    test_record_query_1 = table.records_where("'test 1' = ANY(field_1)")
    test_record_query_2 = table.records_where({'field_1': 'test 1'})

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(f'DROP TABLE {table_name};')

    records[0]['field_2'] = None
    records[1]['field_2'] = None
    records[2]['field_1'] = None

    assert test_records == records
    assert test_record_query_1 == records[:2]
    assert test_record_query_2 == records[:2]


def test_records_where(connection):
    table_name = 'test_records_where'

    fields = {'primary_key_field': int, 'field_1': datetime, 'field_2': str}

    records = [
        {'primary_key_field': 1, 'field_1': datetime(2020, 1, 1), 'field_2': 'test 1'},
        {'primary_key_field': 2, 'field_1': datetime(2020, 1, 2), 'field_2': 'test 2'},
        {'primary_key_field': 3, 'field_1': datetime(2020, 1, 3), 'field_2': 'test 3'},
        {'primary_key_field': 4, 'field_1': datetime(2020, 1, 4), 'field_2': None},
    ]

    with connection:
        with connection.cursor() as cursor:
            if database_has_table(cursor, table_name):
                cursor.execute(f'DROP TABLE {table_name};')

    table = PostGresTable(
        name=table_name,
        fields=fields,
        primary_key='primary_key_field',
        **CREDENTIALS['database'],
    )

    table.insert(records)

    test_record_query_1 = table.records_where({'field_1': datetime(2020, 1, 1)})
    test_record_query_2 = table.records_where({'field_2': ['test 1', 'test 3']})
    test_record_query_3 = table.records_where({'primary_key_field': range(3)})
    test_record_query_4 = table.records_where({'field_2': 'test%'})
    test_record_query_5 = table.records_where("field_1 = '2020-01-02'")
    test_record_query_6 = table.records_where(
        ["field_1 = '2020-01-02'", "field_2 IN ('test 1', 'test 2')"]
    )
    test_record_query_7 = table.records_where({'field_2': None})

    with pytest.raises(KeyError):
        table.records_where('nonexistent_field = 4')

    with pytest.raises(SyntaxError):
        table.records_where('bad_ syn = tax')

    with pytest.raises(NotImplementedError):
        table.records_where(1)

    table.delete_where({'field_1': datetime(2020, 1, 1)})
    test_records_after_deletion = table.records

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(f'DROP TABLE {table_name};')

    assert test_record_query_1 == [records[0]]
    assert test_record_query_2 == [records[0], records[2]]
    assert test_record_query_3 == records[:2]
    assert test_record_query_4 == records[:3]
    assert test_record_query_5 == [records[1]]
    assert test_record_query_6 == [records[1]]
    assert test_record_query_7 == [records[3]]
    assert test_records_after_deletion == records[1:]


def test_field_reorder(connection):
    table_name = 'test_field_reorder'

    fields = {
        'primary_key_field': int,
        'field_1': datetime,
        'field_2': float,
        'field_3': str,
    }

    reordered_fields = {
        'field_2': float,
        'primary_key_field': int,
        'field_1': datetime,
        'field_3': str,
    }

    records = [
        {'primary_key_field': 1, 'field_1': datetime(2020, 1, 1), 'field_3': 'test 1'}
    ]

    with connection:
        with connection.cursor() as cursor:
            if database_has_table(cursor, table_name):
                cursor.execute(f'DROP TABLE {table_name};')

    table = PostGresTable(
        name=table_name,
        fields=fields,
        primary_key='primary_key_field',
        **CREDENTIALS['database'],
    )
    table.insert(records)
    test_records = table.records

    with connection:
        with connection.cursor() as cursor:
            test_fields = database_table_fields(cursor, table_name)

    reordered_table = PostGresTable(
        name=table_name,
        fields=reordered_fields,
        primary_key='primary_key_field',
        **CREDENTIALS['database'],
    )
    test_reordered_records = reordered_table.records

    with connection:
        with connection.cursor() as cursor:
            test_reordered_fields = database_table_fields(cursor, table_name)
            cursor.execute(f'DROP TABLE {table_name};')

    assert list(test_fields) == list(fields)
    assert list(test_reordered_fields) == list(reordered_fields)

    for test_records in (test_records, test_reordered_records):
        for record_index, record in enumerate(records):
            test_record = test_records[record_index]
            for field, value in record.items():
                assert test_record[field] == value


def test_nonexistent_field_in_inserted_record(connection):
    table_name = 'test_nonexistent_field_in_inserted_record'

    fields = {
        'primary_key_field': int,
        'field_1': datetime,
        'field_2': float,
        'field_3': str,
    }

    record_with_extra_field = {
        'primary_key_field': 2,
        'field_1': datetime(2020, 1, 2),
        'nonexistent_field': 'test',
    }

    with connection:
        with connection.cursor() as cursor:
            if database_has_table(cursor, table_name):
                cursor.execute(f'DROP TABLE {table_name};')

    table = PostGresTable(
        name=table_name,
        fields=fields,
        primary_key='primary_key_field',
        **CREDENTIALS['database'],
    )
    table[record_with_extra_field['primary_key_field']] = record_with_extra_field
    test_records = table.records

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(f'DROP TABLE {table_name};')

    del record_with_extra_field['nonexistent_field']
    record_with_extra_field['field_2'] = None
    record_with_extra_field['field_3'] = None

    assert test_records == [record_with_extra_field]


def test_records_intersecting_polygon(connection):
    table_name = 'test_records_intersecting_polygon'

    fields = {
        'primary_key_field': int,
        'field_1': str,
        'field_2': MultiPolygon,
        'field_3': MultiPolygon,
    }

    crs = CRS.from_epsg(4326)

    inside_polygon = box(-77.7, 39.725, -77.4, 39.8)
    touching_polygon = box(-77.1, 39.575, -76.8, 39.65)
    outside_polygon = box(-77.7, 39.425, -77.4, 39.5)
    containing_polygon = box(-77.7, 39.65, -77.1, 39.8)
    projected_containing_polygon = box(268397.8, 4392279.8, 320292.0, 4407509.6)
    multipolygon = MultiPolygon([inside_polygon, touching_polygon])

    records = [
        {
            'primary_key_field': 1,
            'field_1': 'inside box',
            'field_2': MultiPolygon([inside_polygon]),
            'field_3': None,
        },
        {
            'primary_key_field': 2,
            'field_1': 'containing box',
            'field_2': MultiPolygon([containing_polygon]),
            'field_3': None,
        },
        {
            'primary_key_field': 3,
            'field_1': 'outside box with multipolygon',
            'field_2': MultiPolygon([outside_polygon]),
            'field_3': multipolygon,
        },
    ]

    with connection:
        with connection.cursor() as cursor:
            if database_has_table(cursor, table_name):
                cursor.execute(f'DROP TABLE {table_name};')

    table = PostGresTable(
        name=table_name,
        fields=fields,
        primary_key='primary_key_field',
        **CREDENTIALS['database'],
    )
    table.insert(records)

    test_query_1 = table.records_intersecting(inside_polygon)
    test_query_2 = table.records_intersecting(containing_polygon)
    test_query_3 = table.records_intersecting(inside_polygon, geometry_fields=['field_2'])
    test_query_4 = table.records_intersecting(
        containing_polygon, geometry_fields=['field_2']
    )
    test_query_5 = table.records_intersecting(
        projected_containing_polygon, crs=CRS.from_epsg(32618), geometry_fields=['field_2']
    )

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(f'DROP TABLE {table_name};')

    assert test_query_1 == records
    assert test_query_2 == records
    assert test_query_3 == records[:2]
    assert test_query_4 == records[:2]
    assert test_query_5 == records[:2]
