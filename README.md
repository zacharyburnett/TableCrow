# TableCrow 

[![tests](https://github.com/zacharyburnett/TableCrow/workflows/tests/badge.svg)](https://github.com/zacharyburnett/TableCrow/actions?query=workflow%3Atests)
[![build](https://github.com/zacharyburnett/TableCrow/workflows/build/badge.svg)](https://github.com/zacharyburnett/TableCrow/actions?query=workflow%3Abuild)
[![version](https://img.shields.io/pypi/v/tablecrow)](https://pypi.org/project/tablecrow)
[![license](https://img.shields.io/badge/license-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

`tablecrow` is an abstraction library over a generalized database table.
Currently, `tablecrow` offers an abstraction for PostGreSQL tables with simple PostGIS operations. 

```bash
pip install tablecrow
```

## Data Model:
`tablecrow` sees a database record / row as a dictionary of field names to values:
```python
record = {'id': 1, 'time': datetime(2020, 1, 1), 'length': 4.4, 'name': 'long boi'}
```

Similarly, a database schema is seen as a dictionary of field names to Python types:
```python
fields = {'id': int, 'time': datetime, 'length': float, 'name': str}
```

This also includes [Shapely geometric types](https://shapely.readthedocs.io/en/stable/manual.html#geometric-objects):
```python
fields = {'id': int, 'polygon': Polygon}
```

## Usage:
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

# you can add a list of records with `.insert()`
table.insert([
    {'id': 1, 'time': datetime(2020, 1, 1), 'length': 4.4, 'name': 'long boi'},
    {'id': 3, 'time': datetime(2020, 1, 3), 'length': 2, 'name': 'short boi'},
    {'id': 2},
])

# or alternatively set or access a primary key value with square bracket indexing
table[4] = {'time': datetime(2020, 1, 4), 'length': 5, 'name': 'long'}
record = table[3]

# you can query the database with a filtering dictionary or a SQL `WHERE` clause
records = table.records_where({'name': 'short boi'})
records = table.records_where({'name': '%long%'})
records = table.records_where("time <= '20200102'::date")
records = table.records_where("length > 2 OR name ILIKE '%short%'")
```
#### compound primary key
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

#### geometries
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
records = table.records_intersecting(box(268397.8, 4392279.8, 320292.0, 4407509.6), crs=CRS.from_epsg(32618),
                                     geometry_fields=['polygon'])
```
