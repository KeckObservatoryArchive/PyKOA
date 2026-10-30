[![Tests](https://github.com/KeckObservatoryArchive/PyKOA/actions/workflows/tests.yml/badge.svg)](https://github.com/KeckObservatoryArchive/PyKOA/actions/workflows/tests.yml)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Python](https://img.shields.io/pypi/pyversions/pykoa)](https://pypi.org/project/pykoa/)

KOA Table Access Protocol (TAP) Client
=======================================

PyKOA provides access to data stored in the Keck Observatory Archive (KOA). Queries are performed via the nexsciTAP server, a NExScI Python-based server that implements the IVOA Table Access Protocol (TAP). This design enables a rich variety of searches, including cone, box, polygon, or all-sky spatial searches; temporal searches; searches on program information; and complex searches on multiple attributes.

Keck PIs can use the KOA credentials assigned to them when data were acquired to search for their proprietary data.

The Keck Observatory Archive (KOA) is a collaboration between the NASA Exoplanet Science Institute (NExScI) and the W. M. Keck Observatory (WMKO). NExScI is sponsored by NASA's Exoplanet Exploration Program, and operated by the California Institute of Technology in coordination with the Jet Propulsion Laboratory (JPL).

## Supported Instruments

DEIMOS, ESI, HIRES, KCWI, KPF, LRIS, LWS, MOSFIRE, NIRC, NIRC2, NIRES, NIRSPEC, OSIRIS

Moving objects are supported via the MOSS service.

## Installation

```
pip install pykoa
```

Requires Python 3.11+.

## Quick Start

No login or account is required for publicly available data.

```python
from pykoa.koa import Koa

Koa.query_datetime(
    'hires',
    '2023-01-01 00:00:00/2023-01-31 23:59:59',
    outpath='hires_results.tbl'
)
```

## Querying

All query methods write results to a file. The format is controlled by the `format` keyword (default: `ipac`).

### By date and time

```python
Koa.query_datetime('hires', '2023-01-01 00:00:00/2023-01-31 23:59:59', 'results.tbl')
```

### By date

```python
Koa.query_date('deimos', '2023-01-01/2023-01-31', 'results.tbl')
```

### By sky position

The `pos` argument describes the search region:

```python
# Cone search: circle RA Dec radius (degrees)
Koa.query_position('hires', 'circle 83.82 -5.39 0.5', 'results.tbl')

# Box search: box RA Dec width height (degrees)
Koa.query_position('hires', 'box 83.82 -5.39 1.0 1.0', 'results.tbl')

# Polygon: polygon RA1 Dec1 RA2 Dec2 RA3 Dec3 ...
Koa.query_position('hires', 'polygon 209.80 53.34 209.80 55.34 211.80 55.34 211.80 53.34', 'results.tbl')
```

### By object name

Object names are resolved via the ExoPlanet Archive, SIMBAD, and NED.

```python
Koa.query_object('hires', 'Vega', 'results.tbl')
```

### Custom criteria

```python
Koa.query_criteria(
    {
        'instrument': 'hires',
        'datetime': '2023-01-01 00:00:00/2023-12-31 23:59:59'
    },
    'results.tbl'
)
```

### ADQL

```python
Koa.query_adql(
    "SELECT koaid, instrume, ra, dec, elaptime FROM koa_hires WHERE elaptime > 600",
    'results.tbl'
)
```

### Moving objects

NAIF IDs (SPK-IDs) are unique identifiers for solar system objects. They can be looked up at the [JPL Small-Body Database Browser](https://ssd.jpl.nasa.gov/tools/sbdb_lookup.html). Object names can be ambiguous, so including the NAIF ID is recommended.

```python
Koa.query_moving_object(
    instrument='nirspec',
    object='pluto',
    naifid='999',
    startdate='1995-01-01',
    enddate='2022-09-20',
    outdir='./pluto_results',
    outfile='pluto.json'
)
```

For unambiguous objects the `naifid` can be omitted:

```python
Koa.query_moving_object(
    instrument='nirc2',
    object='pallas',
    startdate='1995-01-01',
    outdir='./pallas_results',
    outfile='pallas.json'
)
```

## Output Formats

| Format | Keyword |
|--------|---------|
| IPAC table (default) | `format='ipac'` |
| VOTable | `format='votable'` |
| CSV | `format='csv'` |
| TSV | `format='tsv'` |

## Downloading Data

Use the metadata file from a query to download the FITS files:

```python
Koa.download('results.tbl', 'ipac', './fits_files/')
```

## Proprietary Data

Keck PIs can access their proprietary data using the credentials assigned to them when data were acquired.

```python
# prompts if not supplied
Koa.login('./koacookie.txt')

# Or pass credentials directly
Koa.login('./koacookie.txt', userid='your_koa_id', password='your_password')

# Pass the cookie to subsequent queries
Koa.query_datetime(
    'hires',
    '2023-01-01 00:00:00/2023-01-31 23:59:59',
    'results.tbl',
    cookiepath='./koacookie.txt'
)
```

## Configuration

## Notebooks and Documentation

Jupyter notebooks for each instrument and use case are available on the [KOA User Guide](https://koa.ipac.caltech.edu/UserGuide/PyKOA/TAPClients.html).

## Acknowledgement

If you used PyKOA in your research, please acknowledge KOA as follows:


    "This research has made use of the Keck Observatory Archive (KOA), which
    is operated by the W. M. Keck Observatory and the NASA Exoplanet Science
    Institute (NExScI), under contract with the National Aeronautics and
    Space Administration."


Please also acknowledge the PI(s) of datasets that have been accessed through PyKOA.

If your research uses data reduced by the DEIMOS, KCWI, MOSFIRE, or NIRES [PypeIt DRP](https://pypeit.readthedocs.io/en/release/), please also cite PypeIt. For KPF DRP-reduced data, see the [KPF Pipeline README](https://github.com/Keck-DataReductionPipelines/KPF-Pipeline/blob/develop/README.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). To report a security issue, see [SECURITY.md](SECURITY.md).
