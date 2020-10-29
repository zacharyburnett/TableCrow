# TableCrow 

[![tests](https://github.com/zacharyburnett/TableCrow/workflows/tests/badge.svg)](https://github.com/zacharyburnett/TableCrow/actions?query=workflow%3Atests)
[![build](https://github.com/zacharyburnett/TableCrow/workflows/build/badge.svg)](https://github.com/zacharyburnett/TableCrow/actions?query=workflow%3Abuild)
[![version](https://img.shields.io/pypi/v/tablecrow)](https://pypi.org/project/tablecrow)
[![license](https://img.shields.io/github/license/zacharyburnett/tablecrow)](https://opensource.org/licenses/MIT)
[![style](https://sourceforge.net/p/oitnb/code/ci/default/tree/_doc/_static/oitnb.svg?format=raw)](https://sourceforge.net/p/oitnb/code)

`tablecrow` is an abstraction library over a generalized database table.
Currently, `tablecrow` offers an abstraction for PostGreSQL tables with simple PostGIS operations. 
```bash
pip install tablecrow
```

## Data Model
`tablecrow` sees a database schema as a mapping of field names to Python types, 
and a database record / row as a mapping of field names to values:
```python
from datetime import datetime

fields = {'id': int, 'time': datetime, 'length': float, 'name': str}
record = {'id': 1, 'time': datetime(2020, 1, 1), 'length': 4.4, 'name': 'long boi'}
```
For databases with a spatial extension, you can use [Shapely geometries](https://shapely.readthedocs.io/en/stable/manual.html#geometric-objects):
```python
from shapely.geometry import Polygon

fields = {'id': int, 'polygon': Polygon}
record = {'id': 1, 'polygon': Polygon([(-77.1, 39.65), (-77.1, 39.725), (-77.4, 39.725), (-77.4, 39.65), (-77.1, 39.65)])}
```

## Usage
#### create a simple table (single primary key, no geometries)
```python
from datetime import datetime
from tablecrow import PostGresTable

table = PostGresTable(
    hostname='localhost:5432',
    database='postgres',
    name='testing',
    fields={'id': int, 'time': datetime, 'length': float, 'name': str},
    primary_key='id',
    username='postgres',
    password='<password>',
)

# add a list of records
table.insert([
    {'id': 1, 'time': datetime(2020, 1, 1), 'length': 4.4, 'name': 'long boi'},
    {'id': 3, 'time': datetime(2020, 1, 3), 'length': 2, 'name': 'short boi'},
    {'id': 2},
    {'id': 15, 'time': datetime(2020, 3, 3)},
])

# set, access, or delete a single record using its primary key value
table[4] = {'time': datetime(2020, 1, 4), 'length': 5, 'name': 'long'}
record = table[3]
del table[2]

# list of records in the table
num_records = len(table)
records = table.records

# query the database with a dictionary, or a SQL `WHERE` clause as a string
records = table.records_where({'name': 'short boi'})
records = table.records_where({'name': None})
records = table.records_where({'name': '%long%'})
records = table.records_where("time <= '20200102'::date")
records = table.records_where("length > 2 OR name ILIKE '%short%'")

# delete records with a query
table.delete_where({'name': None})
```
#### create a table with multiple primary key fields
```python
from datetime import datetime
from tablecrow import PostGresTable

table = PostGresTable(
    hostname='localhost:5432',
    database='postgres',
    name='testing',
    fields={'id': int, 'time': datetime, 'length': float, 'name': str},
    primary_key=('id', 'name'),
    username='postgres',
    password='<password>',
)

# a compound primary key allows more flexibility in ID
table.insert([
    {'id': 1, 'time': datetime(2020, 1, 1), 'length': 4.4, 'name': 'long boi'},
    {'id': 1, 'time': datetime(2020, 1, 1), 'length': 3, 'name': 'short boi'},
    {'id': 3, 'time': datetime(2020, 1, 3), 'length': 2, 'name': 'short boi'},
    {'id': 3, 'time': datetime(2020, 1, 3), 'length': 6, 'name': 'long boi'},
    {'id': 2, 'name':'short boi'},
])

# key accessors must include entire primary key
table[4, 'long'] = {'time': datetime(2020, 1, 4), 'length': 5}
record = table[3, 'long boi']
```
#### create a table with geometry fields
the database must have a spatial extension (such as PostGIS) installed
```python
from pyproj import CRS
from shapely.geometry import MultiPolygon, Polygon, box
from tablecrow import PostGresTable

table = PostGresTable(
    hostname='localhost:5432',
    database='postgres',
    name='testing',
    fields={'id': int, 'polygon': Polygon, 'multipolygon': MultiPolygon},
    primary_key='id',
    username='postgres',
    password='<password>',
    crs=CRS.from_epsg(4326),
)

big_box = box(-77.4, 39.65, -77.1, 39.725)
little_box_inside_big_box = box(-77.7, 39.725, -77.4, 39.8)
little_box_touching_big_box = box(-77.1, 39.575, -76.8, 39.65)
disparate_box = box(-77.7, 39.425, -77.4, 39.5)
big_box_in_utm18n = box(268397.8, 4392279.8, 320292.0, 4407509.6)

multi_box = MultiPolygon([little_box_inside_big_box, little_box_touching_big_box])

table.insert([
    {'id': 1, 'polygon': little_box_inside_big_box},
    {'id': 2, 'polygon': little_box_touching_big_box},
    {'id': 3, 'polygon': disparate_box, 'multipolygon': multi_box},
])

# find all records with any geometry intersecting the given geometry
records = table.records_intersecting(big_box)

# find all records with only specific geometry fields intersecting the given geometry
records = table.records_intersecting(big_box, geometry_fields=['polygon'])

# you can also provide geometries in a different CRS
records = table.records_intersecting(
    big_box_in_utm18n,
    crs=CRS.from_epsg(32618),
    geometry_fields=['polygon'],
)
```

## Extending
to write your own custom table interface, extend `DatabaseTable`:
```python
from typing import Any, Mapping, Sequence, Union
from tablecrow.table import DatabaseTable

class CustomDatabaseTable(DatabaseTable):
    # mapping from Python types to database types
    FIELD_TYPES = {
        'NoneType': '',
        'bool': '',
        'float': '',
        'int': '',
        'str': '',
        'bytes': '',
        'date': '',
        'time': '',
        'datetime': '',
        'timedelta': '',
    }

    def __init__(self, hostname: str, database: str, name: str, fields: {str: type}):
        super().__init__(hostname, database, name, fields)
        raise NotImplementedError('implement database connection and table creation here')

    @property
    def exists(self) -> bool:
        raise NotImplementedError('implement database table existence check here')

    @property
    def schema(self) -> str:
        raise NotImplementedError('implement string generation for the database schema here')

    @property
    def remote_fields(self) -> {str: type}:
        raise NotImplementedError('implement accessor for database fields here')

    def records_where(self, where: Union[Mapping[str, Any], str, Sequence[str]]) -> [{str: Any}]:
        raise NotImplementedError('implement database record query here')

    def insert(self, records: [{str: Any}]):
        raise NotImplementedError('implement database record insertion here')

    def delete_where(self, where: Union[Mapping[str, Any], str, Sequence[str]]):
        raise NotImplementedError('implement database record deletion here')

    def delete_table(self):
        raise NotImplementedError('implement database table deletion here')
```

## Acknowledgements
The original core code and methodology of `tablecrow` was developed for the National Bathymetric Source project under the [Office of Coast Survey of the National Oceanic and Atmospheric Administration (NOAA)](https://nauticalcharts.noaa.gov), a part of the United States Department of Commerce.