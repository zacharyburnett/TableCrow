# TableCrow 

[![tests](https://github.com/zacharyburnett/TableCrow/workflows/tests/badge.svg)](https://github.com/zacharyburnett/TableCrow/actions?query=workflow%3Atests)
[![build](https://github.com/zacharyburnett/TableCrow/workflows/build/badge.svg)](https://github.com/zacharyburnett/TableCrow/actions?query=workflow%3Abuild)
[![version](https://img.shields.io/pypi/v/tablecrow)](https://pypi.org/project/tablecrow)
[![license](https://img.shields.io/badge/license-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

TableCrow is an abstraction library over a generalized database table.

```bash
pip install tablecrow
```

## Database Implementations
- PostGreSQL w/ PostGIS

## Python API:
create new PostGres database table at the specified location:
```python
from packetraven import APRSfi

callsigns = ['W3EAX-8', 'W3EAX-12', 'KC3FXX', 'KC3ZRB']
api_key = '<api_key>' # enter your APRS.fi API key here - you can get one from https://aprs.fi/page/api

aprs_fi = APRSfi(callsigns, api_key)
aprs_fi_packets = aprs_fi.packets

print(aprs_fi_packets)
```
