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

## Python API:
```python
from datetime import datetime

from tablecrow import PostGresTable


hostname = 'localhost:5432'
database = 'postgres'
table = 'test'

username = 'postgres'
password = '<password>'

# parameters for an SSH tunnel
ssh_hostname = None
ssh_username = None
ssh_password = None

fields = {
    'id'    : int,
    'time'  : datetime,
    'length': float,
    'name'  : str
}

table = PostGresTable(hostname, database, table, fields, username=username, password=password,
        ssh_hostname=ssh_hostname, ssh_username=ssh_username, ssh_password=ssh_password)

table.insert([
    {'id': 1, 'time': datetime(2020, 1, 1), 'length': 4.4, 'name': 'long boi'},
    {'id': 3, 'time': datetime(2020, 1, 3), 'length': 2, 'name': 'short boi'},
    {'id': 2, 'time': datetime(2020, 1, 2)}
])

table[4] = {'time': datetime(2020, 1, 4), 'length': 5, 'name': 'long'}

record_with_id_3 = table[3]
short_records = table.records_where({'name': 'short boi'})
long_records = table.records_where({'name': '%long%'})
early_records = table.records_where("time <= '20200102'::date")
```
