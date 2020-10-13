from datetime import datetime
import os
import unittest

import psycopg2
from pyproj import CRS
from shapely import wkt
from shapely.geometry import MultiPolygon, box
from shapely.ops import unary_union

from tablecrow.postgres import PostGresTable, database_has_table, database_table_fields
from tablecrow.table import split_URL_port
from tablecrow.utilities import read_configuration, repository_root

CREDENTIALS_FILENAME = repository_root() / 'credentials.config'


class TestPostGresTable(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.database = 'metadata_develop'

        credentials = read_configuration(CREDENTIALS_FILENAME)

        if 'database' not in credentials:
            credentials['database'] = {
                'hostname': os.environ['POSTGRES_HOSTNAME'],
                'database': os.environ['POSTGRES_DATABASE'],
                'username': os.environ['POSTGRES_USERNAME'],
                'password': os.environ['POSTGRES_PASSWORD']
            }
            if 'ssh_hostname' in os.environ:
                credentials['database']['ssh_hostname'] = os.environ['ssh_hostname']
            if 'ssh_username' in os.environ:
                credentials['database']['ssh_username'] = os.environ['ssh_username']
            if 'ssh_password' in os.environ:
                credentials['database']['ssh_password'] = os.environ['ssh_password']

        self.hostname = credentials['database']['hostname']
        self.database = credentials['database']['database']
        self.table = credentials['database']['table']
        self.username = credentials['database']['username']
        self.password = credentials['database']['password']

        self.ssh_hostname = credentials['database']['ssh_hostname'] if 'ssh_hostname' in credentials['database'] else None
        self.ssh_username = credentials['database']['ssh_username'] if 'ssh_username' in credentials['database'] else None
        self.ssh_password = credentials['database']['ssh_password'] if 'ssh_password' in credentials['database'] else None

        hostname, port = split_URL_port(self.hostname)
        if port is None:
            port = PostGresTable.DEFAULT_PORT

        self.connection = psycopg2.connect(database=self.database, user=self.username, password=self.password, host=hostname,
                port=port)

        self.table = 'test_table'

    def test_table_creation(self):
        table_name = 'test_table_creation'

        fields = {
            'primary_key_field': int,
            'field_1'          : datetime,
            'field_2'          : float,
            'field_3'          : str
        }

        with self.connection:
            with self.connection.cursor() as cursor:
                if database_has_table(cursor, table_name):
                    cursor.execute(f'DROP TABLE {table_name};')

        PostGresTable(self.hostname, self.database, self.username, self.password, table_name, fields=fields,
                primary_key='primary_key_field')

        with self.connection:
            with self.connection.cursor() as cursor:
                remote_fields = database_table_fields(cursor, table_name)
                table_exists = database_has_table(cursor, table_name)
                if table_exists:
                    cursor.execute(f'DROP TABLE {table_name};')

        assert list(remote_fields) == list(fields)
        assert table_exists

    def test_table_flexibility(self):
        table_name = 'test_table_flexibility'

        fields = {
            'primary_key_field': int,
            'field_1'          : datetime,
            'field_2'          : float,
            'field_3'          : str
        }

        incomplete_fields = {
            'primary_key_field': int,
            'field_3'          : str
        }

        test_records = [
            {'primary_key_field': 1, 'field_1': datetime(2020, 1, 1), 'field_3': 'test 1'}
        ]

        with self.connection:
            with self.connection.cursor() as cursor:
                if database_has_table(cursor, table_name):
                    cursor.execute(f'DROP TABLE {table_name};')

        # create table with incomplete fields
        incomplete_table = PostGresTable(self.hostname, self.database, self.username, self.password, table_name,
                fields=incomplete_fields, primary_key='primary_key_field')
        incomplete_table.insert(test_records)
        incomplete_records = incomplete_table.records

        with self.connection:
            with self.connection.cursor() as cursor:
                incomplete_remote_fields = database_table_fields(cursor, table_name)

        # create table with complete fields, pointing to existing remote table with incomplete fields
        complete_table = PostGresTable(self.hostname, self.database, self.username, self.password, table_name, fields=fields,
                primary_key='primary_key_field')
        complete_records = complete_table.records

        with self.connection:
            with self.connection.cursor() as cursor:
                complete_remote_fields = database_table_fields(cursor, table_name)

        # create table with incomplete fields, pointing to existing remote table with complete fields
        completed_table = PostGresTable(self.hostname, self.database, self.username, self.password, table_name,
                fields=incomplete_fields, primary_key='primary_key_field')
        completed_records = completed_table.records

        with self.connection:
            with self.connection.cursor() as cursor:
                completed_remote_fields = database_table_fields(cursor, table_name)
                cursor.execute(f'DROP TABLE {table_name};')

        assert list(incomplete_remote_fields) == list(incomplete_fields)
        assert list(complete_remote_fields) == list(fields)
        assert list(completed_remote_fields) == list(fields)

        for table_records in (incomplete_records, complete_records, completed_records):
            for record_index, record in enumerate(table_records):
                test_record = test_records[record_index]
                for field, value in record.items():
                    assert value == test_record[field]

    def test_records_where(self):
        table_name = 'test_records_where'

        fields = {
            'primary_key_field': int,
            'field_1'          : datetime,
            'field_2'          : str
        }

        test_records = [
            {'primary_key_field': 1, 'field_1': datetime(2020, 1, 1), 'field_2': 'test 1'},
            {'primary_key_field': 2, 'field_1': datetime(2020, 1, 2), 'field_2': 'test 2'},
            {'primary_key_field': 3, 'field_1': datetime(2020, 1, 3), 'field_2': 'test 3'},
            {'primary_key_field': 4, 'field_1': datetime(2020, 1, 4), 'field_2': None}
        ]

        with self.connection:
            with self.connection.cursor() as cursor:
                if database_has_table(cursor, table_name):
                    cursor.execute(f'DROP TABLE {table_name};')

        table = PostGresTable(self.hostname, self.database, self.username, self.password, table_name, fields=fields,
                primary_key='primary_key_field')

        table.insert(test_records)

        record_query_1 = table.records_where({'field_1': datetime(2020, 1, 1)})
        record_query_2 = table.records_where({'field_2': ['test 1', 'test 3']})
        record_query_3 = table.records_where({'primary_key_field': range(3)})
        record_query_4 = table.records_where({'field_2': 'test%'})
        record_query_5 = table.records_where('script_field_1 = \'2020-01-02\'')
        record_query_6 = table.records_where({'field_2': None})

        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(f'DROP TABLE {table_name};')

        assert record_query_1 == [test_records[0]]
        assert record_query_2 == [test_records[0], test_records[2]]
        assert record_query_3 == test_records[:2]
        assert record_query_4 == test_records[:3]
        assert record_query_5 == [test_records[1]]
        assert record_query_6 == [{field: value for field, value in test_records[3].items() if value is not None}]

    def test_field_reorder(self):
        table_name = 'test_field_reorder'

        fields = {
            'primary_key_field': int,
            'field_1'          : datetime,
            'field_2'          : float,
            'field_3'          : str
        }

        reordered_fields = {
            'field_2'          : float,
            'primary_key_field': int,
            'field_1'          : datetime,
            'field_3'          : str
        }

        corrected_reordered_fields = {
            'primary_key_field': int,
            'field_2'          : float,
            'field_1'          : datetime,
            'field_3'          : str
        }

        test_records = [
            {'primary_key_field': 1, 'field_1': datetime(2020, 1, 1), 'field_3': 'test 1'}
        ]

        with self.connection:
            with self.connection.cursor() as cursor:
                if database_has_table(cursor, table_name):
                    cursor.execute(f'DROP TABLE {table_name};')

        table = PostGresTable(self.hostname, self.database, self.username, self.password, table_name, fields=fields,
                primary_key='primary_key_field')
        table.insert(test_records)
        records = table.records

        with self.connection:
            with self.connection.cursor() as cursor:
                test_fields = database_table_fields(cursor, table_name)

        reordered_table = PostGresTable(self.hostname, self.database, self.username, self.password, table_name,
                fields=reordered_fields, primary_key='primary_key_field')
        reordered_records = reordered_table.records

        with self.connection:
            with self.connection.cursor() as cursor:
                test_reordered_fields = database_table_fields(cursor, table_name)
                cursor.execute(f'DROP TABLE {table_name};')

        assert list(test_fields) == list(fields)
        assert list(test_reordered_fields) == list(corrected_reordered_fields)

        for table_records in (records, reordered_records):
            for record_index, record in enumerate(table_records):
                test_record = test_records[record_index]
                for field, value in record.items():
                    assert value == test_record[field]

    def test_record_insertion(self):
        table_name = 'test_record_insertion'

        fields = {
            'primary_key_field': int,
            'field_1'          : datetime,
            'field_2'          : float,
            'field_3'          : str
        }

        test_records = [
            {'primary_key_field': 1, 'field_1': datetime(2020, 1, 1), 'field_3': 'test 1'},
            {'primary_key_field': 2, 'field_1': datetime(2020, 1, 2), 'field_2': 5.67}
        ]

        extra_record = {'primary_key_field': 3, 'field_1': datetime(2020, 1, 3), 'field_2': 3, 'field_3': 'test 3'}

        with self.connection:
            with self.connection.cursor() as cursor:
                if database_has_table(cursor, table_name):
                    cursor.execute(f'DROP TABLE {table_name};')

        table = PostGresTable(self.hostname, self.database, self.username, self.password, table_name, fields=fields,
                primary_key='primary_key_field')
        table.insert(test_records)
        records_before_addition = table.records
        table[extra_record['primary_key_field']] = extra_record
        records_after_addition = table.records

        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(f'DROP TABLE {table_name};')

        assert records_before_addition == test_records
        assert records_after_addition == test_records + [extra_record]

    def test_nonexistent_field_in_inserted_record(self):
        table_name = 'test_nonexistent_field_in_inserted_record'

        fields = {
            'primary_key_field': int,
            'field_1'          : datetime,
            'field_2'          : float,
            'field_3'          : str
        }

        record_with_extra_field = {'primary_key_field': 2, 'field_1': datetime(2020, 1, 2), 'nonexistent_field': 'test'}

        with self.connection:
            with self.connection.cursor() as cursor:
                if database_has_table(cursor, table_name):
                    cursor.execute(f'DROP TABLE {table_name};')

        table = PostGresTable(self.hostname, self.database, self.username, self.password, table_name, fields=fields,
                primary_key='primary_key_field')
        table[record_with_extra_field['primary_key_field']] = record_with_extra_field
        records = table.records

        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(f'DROP TABLE {table_name};')

        del record_with_extra_field['nonexistent_field']

        assert records == [record_with_extra_field]

    def test_records_intersecting_polygon(self):
        table_name = 'test_records_intersecting_polygon'

        fields = {
            'primary_key_field': int,
            'field_1'          : str,
            'field_2'          : MultiPolygon,
            'field_3'          : MultiPolygon
        }

        crs = CRS.from_epsg(4326)

        inside_polygon_1 = wkt.loads('POLYGON ((-164.7 67.725, -164.7 67.8, -164.4 67.8, -164.4 67.725, -164.7 67.725))')
        inside_polygon_2 = wkt.loads('POLYGON ((-164.4 67.65, -164.4 67.725, -164.1 67.725, -164.1 67.65, -164.4 67.65))')
        touching_polygon = wkt.loads('POLYGON ((-164.1 67.575, -164.1 67.65, -163.8 67.65, -163.8 67.575, -164.1 67.575))')
        outside_polygon = wkt.loads('POLYGON ((-164.7 67.425, -164.7 67.5, -164.4 67.5, -164.4 67.425, -164.7 67.425))')
        containing_polygon = box(*unary_union([inside_polygon_1, inside_polygon_2]).bounds)
        multipolygon = MultiPolygon([inside_polygon_1, touching_polygon])

        test_records = [
            {'primary_key_field': 1, 'field_1': 'inside box', 'field_2': MultiPolygon([inside_polygon_1])},
            {'primary_key_field': 2, 'field_1': 'containing box', 'field_2': MultiPolygon([containing_polygon])},
            {
                'primary_key_field': 3,
                'field_1'          : 'outside box with multipolygon',
                'field_2'          : MultiPolygon([outside_polygon]),
                'field_3'          : multipolygon
            }
        ]

        with self.connection:
            with self.connection.cursor() as cursor:
                if database_has_table(cursor, table_name):
                    cursor.execute(f'DROP TABLE {table_name};')

        metadata_table = PostGresTable(self.hostname, self.database, self.username, self.password, table_name, fields=fields,
                primary_key='primary_key_field')
        metadata_table.insert(test_records)

        query_1 = metadata_table.records_intersecting(inside_polygon_1, crs)
        query_2 = metadata_table.records_intersecting(containing_polygon, crs)
        query_3 = metadata_table.records_intersecting(inside_polygon_1, crs, ['field_2'])
        query_4 = metadata_table.records_intersecting(containing_polygon, crs, ['field_2'])

        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(f'DROP TABLE {table_name};')

        assert query_1 == test_records
        assert query_2 == test_records
        assert query_3 == test_records[:2]
        assert query_4 == test_records[:2]


if __name__ == '__main__':
    unittest.main()
