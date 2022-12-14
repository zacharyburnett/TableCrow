from datetime import date, datetime
import os
import sqlite3

import pytest
from shapely.geometry import box, MultiPolygon, Point
from pyproj import CRS

from tablecrow import SQLiteTable
from tablecrow.tables.base import DEFAULT_CRS
from tablecrow.tables.sqlite import database_has_table, database_table_fields
from tablecrow.utilities import read_configuration, repository_root

CREDENTIALS_FILENAME = repository_root() / "credentials.config"
CREDENTIALS = read_configuration(CREDENTIALS_FILENAME)

if "sqlite" not in CREDENTIALS:
    CREDENTIALS["sqlite"] = {}

default_credentials = {
    "path": ("SQLITE_DATABASE", "test_database.db"),
}

for credential, details in default_credentials.items():
    if credential not in CREDENTIALS["sqlite"]:
        CREDENTIALS["sqlite"][credential] = os.getenv(*details)


def sqlite_connection() -> sqlite3.Connection:
    return sqlite3.connect(CREDENTIALS["sqlite"]["path"])


@pytest.mark.sqlite
def test_table_creation():
    table_name = "test_table_creation"

    fields = {
        "primary_key_field": int,
        "field_1": str,
        "field_2": float,
        "field_3": datetime,
        "field_4": date,
        "field_5": bool,
    }

    with sqlite_connection() as connection:
        cursor = connection.cursor()
        if database_has_table(cursor, table_name):
            cursor.execute(f"DROP TABLE {table_name};")

    table = SQLiteTable(
        table_name=table_name,
        fields=fields,
        primary_key="primary_key_field",
        **CREDENTIALS["sqlite"],
    )

    test_remote_fields = table.remote_fields

    with sqlite_connection() as connection:
        cursor = connection.cursor()
        test_raw_remote_fields = database_table_fields(cursor, table_name)
        if table.exists:
            table.delete_table()
            table_exists = database_has_table(cursor, table_name)
            if table_exists:
                cursor.execute(f"DROP TABLE {table_name};")

    assert test_remote_fields == fields
    assert list(test_raw_remote_fields) == list(fields)
    assert not table_exists


@pytest.mark.sqlite
@pytest.mark.spatial
def test_table_creation_spatial():
    table_name = "test_table_creation"

    fields = {
        "primary_key_field": int,
        "field_6": Point,
        "field_7": MultiPolygon,
    }

    with sqlite_connection() as connection:
        cursor = connection.cursor()
        if database_has_table(cursor, table_name):
            cursor.execute(f"DROP TABLE {table_name};")

    table = SQLiteTable(
        table_name=table_name,
        fields=fields,
        primary_key="primary_key_field",
        **CREDENTIALS["sqlite"],
    )

    test_remote_fields = table.remote_fields

    with sqlite_connection() as connection:
        cursor = connection.cursor()
        test_raw_remote_fields = database_table_fields(cursor, table_name)
        if table.exists:
            table.delete_table()
            table_exists = database_has_table(cursor, table_name)
            if table_exists:
                cursor.execute(f"DROP TABLE {table_name};")

    assert test_remote_fields == fields
    assert list(test_raw_remote_fields) == list(fields)
    assert not table_exists


@pytest.mark.sqlite
def test_compound_primary_key():
    table_name = "test_compound_primary_key"

    fields = {
        "primary_key_field_1": int,
        "primary_key_field_2": str,
        "primary_key_field_3": datetime,
        "field_1": float,
        "field_2": str,
    }

    records = [
        {
            "primary_key_field_1": 1,
            "primary_key_field_2": "test 1",
            "primary_key_field_3": datetime(2020, 1, 1),
            "field_1": None,
            "field_2": "test 1",
        },
        {
            "primary_key_field_1": 2,
            "primary_key_field_2": "test 1",
            "primary_key_field_3": datetime(2020, 1, 2),
            "field_1": 5.67,
            "field_2": None,
        },
    ]

    extra_record = {
        "primary_key_field_1": 3,
        "primary_key_field_2": "test 3",
        "primary_key_field_3": datetime(2020, 1, 3),
        "field_1": 2.5,
        "field_2": None,
    }
    extra_record_to_insert = {
        "primary_key_field_2": "overwritten value",
        "primary_key_field_3": datetime(2020, 1, 3),
        "field_1": 2.5,
        "field_2": None,
    }

    primary_key = ("primary_key_field_1", "primary_key_field_2", "primary_key_field_3")

    with sqlite_connection() as connection:
        cursor = connection.cursor()
        if database_has_table(cursor, table_name):
            cursor.execute(f"DROP TABLE {table_name};")

    table = SQLiteTable(
        table_name=table_name,
        fields=fields,
        primary_key=primary_key,
        **CREDENTIALS["sqlite"],
    )

    test_primary_key = primary_key
    table.insert(records)

    with pytest.raises(ValueError):
        table[1]
    with pytest.raises(IndexError):
        table[1] = extra_record_to_insert

    table[3, "test 3", datetime(2020, 1, 3)] = extra_record_to_insert

    test_record = table[1, "test 1", datetime(2020, 1, 1)]
    test_records = table.records

    with sqlite_connection() as connection:
        cursor = connection.cursor()
        test_raw_remote_fields = database_table_fields(cursor, table_name)
        if database_has_table(cursor, table_name):
            cursor.execute(f"DROP TABLE {table_name};")

    assert test_primary_key == primary_key
    assert test_records == records + [extra_record]
    assert test_record == records[0]
    assert list(test_raw_remote_fields) == list(fields)


@pytest.mark.sqlite
def test_record_insertion():
    table_name = "test_record_insertion"

    fields = {
        "primary_key_field": int,
        "field_1": datetime,
        "field_2": float,
        "field_3": str,
        "field_4": bool,
    }

    records = [
        {
            "primary_key_field": 1,
            "field_1": datetime(2020, 1, 1),
            "field_3": "test 1",
            "field_4": None,
        },
        {
            "primary_key_field": 2,
            "field_1": datetime(2020, 1, 2),
            "field_2": 5.67,
            "field_4": True,
        },
    ]

    extra_record = {
        "primary_key_field": 3,
        "field_1": datetime(2020, 1, 3),
        "field_2": 3,
        "field_3": "test 3",
        "field_4": False,
    }

    with sqlite_connection() as connection:
        cursor = connection.cursor()
        if database_has_table(cursor, table_name):
            cursor.execute(f"DROP TABLE {table_name};")

    table = SQLiteTable(
        table_name=table_name,
        fields=fields,
        primary_key="primary_key_field",
        **CREDENTIALS["sqlite"],
    )
    table.insert(records)
    test_records_before_addition = table.records
    table[extra_record["primary_key_field"]] = extra_record
    test_records_after_addition = table.records

    del table[extra_record["primary_key_field"]]
    test_records_after_deletion = table.records

    records[0]["field_2"] = None
    records[1]["field_3"] = None

    assert records[0] in table
    assert records[0]["primary_key_field"] in table
    assert (records[0][field] for field in ["primary_key_field"]) in table
    assert "nonexistant" not in table
    assert len(table) == 2

    with pytest.raises(ValueError):
        key_without_primary_key = {
            field: records[0][field]
            for field in records[0]
            if field not in ["primary_key_field"]
        }
        key_without_primary_key in table

    table.insert(records[0])

    with sqlite_connection() as connection:
        cursor = connection.cursor()
        cursor.execute(f"DROP TABLE {table_name};")

    assert test_records_before_addition == records
    assert test_records_after_addition == records + [extra_record]
    assert test_records_after_deletion == records


@pytest.mark.sqlite
def test_table_flexibility():
    table_name = "test_table_flexibility"

    fields = {
        "primary_key_field": int,
        "field_1": datetime,
        "field_2": float,
        "field_3": str,
    }

    incomplete_fields = {"primary_key_field": int, "field_3": str}

    records = [
        {"primary_key_field": 1, "field_1": datetime(2020, 1, 1), "field_3": "test 1"}
    ]

    with sqlite_connection() as connection:
        cursor = connection.cursor()
        if database_has_table(cursor, table_name):
            cursor.execute(f"DROP TABLE {table_name};")

    # create table with incomplete fields
    incomplete_table = SQLiteTable(
        table_name=table_name,
        fields=incomplete_fields.copy(),
        primary_key="primary_key_field",
        **CREDENTIALS["sqlite"],
    )
    incomplete_table.insert(records)
    incomplete_records = incomplete_table.records

    with sqlite_connection() as connection:
        cursor = connection.cursor()
        test_incomplete_remote_fields = database_table_fields(cursor, table_name)

    # create table with complete fields, pointing to existing remote table with incomplete fields
    complete_table = SQLiteTable(
        table_name=table_name,
        fields=fields,
        primary_key="primary_key_field",
        **CREDENTIALS["sqlite"],
    )
    complete_records = complete_table.records

    with sqlite_connection() as connection:
        cursor = connection.cursor()
        test_complete_remote_fields = database_table_fields(cursor, table_name)

    # create table with incomplete fields, pointing to existing remote table with complete fields
    completed_table = SQLiteTable(
        table_name=table_name,
        fields=incomplete_fields.copy(),
        primary_key="primary_key_field",
        **CREDENTIALS["sqlite"],
    )
    completed_records = completed_table.records

    with sqlite_connection() as connection:
        cursor = connection.cursor()
        test_completed_remote_fields = database_table_fields(cursor, table_name)
        cursor.execute(f"DROP TABLE {table_name};")

    assert list(test_incomplete_remote_fields) == list(incomplete_fields)
    assert list(test_complete_remote_fields) == list(fields)
    assert list(test_completed_remote_fields) == list(fields)

    for test_records in (incomplete_records, complete_records, completed_records):
        for record_index, record in enumerate(test_records):
            record = records[record_index]
            for field, value in record.items():
                assert value == record[field]


@pytest.mark.sqlite
def test_records_where():
    table_name = "test_records_where"

    fields = {"primary_key_field": int, "field_1": datetime, "field_2": str}

    records = [
        {"primary_key_field": 1, "field_1": datetime(2020, 1, 1), "field_2": "test 1"},
        {"primary_key_field": 2, "field_1": datetime(2020, 1, 2), "field_2": "test 2"},
        {"primary_key_field": 3, "field_1": datetime(2020, 1, 3), "field_2": "test 3"},
        {"primary_key_field": 4, "field_1": datetime(2020, 1, 4), "field_2": None},
    ]

    with sqlite_connection() as connection:
        cursor = connection.cursor()
        if database_has_table(cursor, table_name):
            cursor.execute(f"DROP TABLE {table_name};")

    table = SQLiteTable(
        table_name=table_name,
        fields=fields,
        primary_key="primary_key_field",
        **CREDENTIALS["sqlite"],
    )

    table.insert(records)

    test_record_query_1 = table.records_where({"field_1": datetime(2020, 1, 1)})
    test_record_query_2 = table.records_where({"field_2": ["test 1", "test 3"]})
    test_record_query_3 = table.records_where({"primary_key_field": range(3)})
    test_record_query_4 = table.records_where({"field_2": "test%"})
    test_record_query_5 = table.records_where("field_1 = '2020-01-02 00:00:00'")
    test_record_query_6 = table.records_where(
        ["field_1 = '2020-01-02 00:00:00'", "field_2 IN ('test 1', 'test 2')"]
    )
    test_record_query_7 = table.records_where({"field_2": None})

    with pytest.raises(sqlite3.OperationalError):
        table.records_where("nonexistent_field = 4")

    with pytest.raises(sqlite3.OperationalError):
        table.records_where("bad_ syn = tax")

    with pytest.raises(NotImplementedError):
        table.records_where(1)

    table.delete_where({"field_1": datetime(2020, 1, 1)})
    test_records_after_deletion = table.records

    with sqlite_connection() as connection:
        cursor = connection.cursor()
        cursor.execute(f"DROP TABLE {table_name};")

    assert test_record_query_1 == [records[0]]
    assert test_record_query_2 == [records[0], records[2]]
    assert test_record_query_3 == records[:2]
    assert test_record_query_4 == records[:3]
    assert test_record_query_5 == [records[1]]
    assert test_record_query_6 == [records[1]]
    assert test_record_query_7 == [records[3]]
    assert test_records_after_deletion == records[1:]


@pytest.mark.sqlite
def test_field_reorder():
    table_name = "test_field_reorder"

    fields = {
        "primary_key_field": int,
        "field_1": datetime,
        "field_2": float,
        "field_3": str,
        "field_4": date,
    }

    reordered_fields = {
        "field_4": date,
        "field_2": float,
        "primary_key_field": int,
        "field_1": datetime,
        "field_3": str,
    }

    records = [
        {
            "primary_key_field": 1,
            "field_1": datetime(2020, 1, 1),
            "field_3": "test 1",
            "field_4": date(2020, 1, 2),
        }
    ]

    with sqlite_connection() as connection:
        cursor = connection.cursor()
        if database_has_table(cursor, table_name):
            cursor.execute(f"DROP TABLE {table_name};")

    table = SQLiteTable(
        table_name=table_name,
        fields=fields,
        primary_key="primary_key_field",
        **CREDENTIALS["sqlite"],
    )
    table.insert(records)
    test_records = table.records

    with sqlite_connection() as connection:
        cursor = connection.cursor()
        test_fields = database_table_fields(cursor, table_name)

    reordered_table = SQLiteTable(
        table_name=table_name,
        fields=reordered_fields,
        primary_key="primary_key_field",
        **CREDENTIALS["sqlite"],
    )
    test_reordered_records = reordered_table.records

    with sqlite_connection() as connection:
        cursor = connection.cursor()
        test_reordered_fields = database_table_fields(cursor, table_name)
        cursor.execute(f"DROP TABLE {table_name};")

    assert list(test_fields) == list(fields)
    assert list(test_reordered_fields) == list(reordered_fields)

    for test_records in (test_records, test_reordered_records):
        for record_index, record in enumerate(records):
            test_record = test_records[record_index]
            for field, value in record.items():
                assert test_record[field] == value


@pytest.mark.sqlite
def test_nonexistent_field_in_inserted_record():
    table_name = "test_nonexistent_field_in_inserted_record"

    fields = {
        "primary_key_field": int,
        "field_1": str,
        "field_2": float,
        "field_3": str,
    }

    record_with_extra_field = {
        "primary_key_field": 2,
        "field_1": datetime(2020, 1, 2),
        "nonexistent_field": "test",
    }

    with sqlite_connection() as connection:
        cursor = connection.cursor()
        if database_has_table(cursor, table_name):
            cursor.execute(f"DROP TABLE {table_name};")

    table = SQLiteTable(
        table_name=table_name,
        fields=fields,
        primary_key="primary_key_field",
        **CREDENTIALS["sqlite"],
    )
    table[record_with_extra_field["primary_key_field"]] = record_with_extra_field
    test_records = table.records

    with sqlite_connection() as connection:
        cursor = connection.cursor()
        cursor.execute(f"DROP TABLE {table_name};")

    del record_with_extra_field["nonexistent_field"]
    record_with_extra_field["field_1"] = f'{record_with_extra_field["field_1"]}'
    record_with_extra_field["field_2"] = None
    record_with_extra_field["field_3"] = None

    assert test_records == [record_with_extra_field]


@pytest.mark.sqlite
@pytest.mark.spatial
def test_missing_crs():
    table_name = "test_missing_crs"

    fields = {
        "primary_key_field": int,
        "field_1": str,
        "field_2": MultiPolygon,
        "field_3": MultiPolygon,
    }

    table = SQLiteTable(
        table_name=table_name,
        fields=fields,
        primary_key="primary_key_field",
        crs=None,
        **CREDENTIALS["sqlite"],
    )

    assert table.crs == DEFAULT_CRS


@pytest.mark.sqlite
@pytest.mark.spatial
def test_records_intersecting_polygon():
    table_name = "test_records_intersecting_polygon"

    fields = {
        "primary_key_field": int,
        "field_1": str,
        "field_2": MultiPolygon,
        "field_3": MultiPolygon,
    }

    crs = CRS.from_epsg(4326)

    inside_polygon = box(-77.7, 39.725, -77.4, 39.8)
    touching_polygon = box(-77.1, 39.575, -76.8, 39.65)
    outside_polygon = box(-77.7, 39.425, -77.4, 39.5)
    containing_polygon = box(-77.7, 39.65, -77.1, 39.8)
    # projected_containing_polygon = box(268397.8, 4392279.8, 320292.0, 4407509.6)
    multipolygon = MultiPolygon([inside_polygon, touching_polygon])

    records = [
        {
            "primary_key_field": 1,
            "field_1": "inside box",
            "field_2": MultiPolygon([inside_polygon]),
            "field_3": None,
        },
        {
            "primary_key_field": 2,
            "field_1": "containing box",
            "field_2": MultiPolygon([containing_polygon]),
            "field_3": None,
        },
        {
            "primary_key_field": 3,
            "field_1": "outside box with multipolygon",
            "field_2": MultiPolygon([outside_polygon]),
            "field_3": multipolygon,
        },
    ]

    with sqlite_connection() as connection:
        cursor = connection.cursor()
        if database_has_table(cursor, table_name):
            cursor.execute(f"DROP TABLE {table_name};")

    table = SQLiteTable(
        table_name=table_name,
        fields=fields,
        primary_key="primary_key_field",
        crs=crs,
        **CREDENTIALS["sqlite"],
    )
    table.insert(records)

    test_query_1 = table.records_intersecting(inside_polygon)
    test_query_2 = table.records_intersecting(containing_polygon)
    test_query_3 = table.records_intersecting(
        inside_polygon, geometry_fields=["field_2"]
    )
    test_query_4 = table.records_intersecting(
        containing_polygon, geometry_fields=["field_2"]
    )

    # TODO fix SRID transformation from 32618
    # test_query_5 = table.records_intersecting(
    #     projected_containing_polygon, crs=CRS.from_epsg(32618), geometry_fields=['field_2']
    # )

    with sqlite_connection() as connection:
        cursor = connection.cursor()
        cursor.execute(f"DROP TABLE {table_name};")

    assert test_query_1 == records
    assert test_query_2 == records
    assert test_query_3 == records[:2]
    assert test_query_4 == records[:2]

    # TODO fix SRID transformation from 32618
    # assert test_query_5 == records[:2]
