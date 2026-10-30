"""End-to-end tests against the live KOA TAP service.

These tests make real HTTP requests to koa.ipac.caltech.edu.
No mocking, no credentials required -- all queries use public data.

Run selectively:
    pytest tests/test_e2e.py -m e2e -v

These tests are intentionally excluded from the main test suite
and run nightly via .github/workflows/e2e.yml.
"""

import os
import tempfile

import pytest
import requests
from astropy.table import Table

from pykoa.koa import Koa
from pykoa.koa.core import Archive

pytestmark = pytest.mark.e2e

# Short-timeout Archive instance for query_object tests.
# The name resolver (ExoPlanet/SIMBAD/NED) can hang on degraded services.
# 30s read timeout fails fast with a clear skip message rather than
# blocking the full 300s default.
_KOA_RESOLVER = Archive(timeout=(30, 30))

# Known public HIRES date range with data
HIRES_DATE = "2021-06-01 00:00:00/2021-06-30 23:59:59"

# Known sky position with HIRES data: M42 (Orion Nebula)
M42_RA = 83.82
M42_DEC = -5.39

# Known empty date range (Keck was not operating in 1990)
EMPTY_DATE = "1990-01-01 00:00:00/1990-01-02 00:00:00"


# ---------------------------------------------------------------------------
# Basic connectivity
# ---------------------------------------------------------------------------


def test_tap_service_reachable():
    """KOA TAP service responds to a minimal query."""
    r = requests.get(
        "https://koa.ipac.caltech.edu/TAP/sync",
        params={
            "REQUEST": "doQuery",
            "LANG": "ADQL",
            "QUERY": "SELECT TOP 1 koaid FROM koa_hires",
        },
        timeout=(30, 60),
    )
    assert r.status_code == 200, f"TAP service returned {r.status_code}"


# ---------------------------------------------------------------------------
# query_datetime
# ---------------------------------------------------------------------------


def test_query_datetime_returns_rows():
    """query_datetime returns rows for a known date range with HIRES data."""
    with tempfile.NamedTemporaryFile(suffix=".tbl", delete=False) as f:
        outfile = f.name
    try:
        Koa.query_datetime(
            "hires", HIRES_DATE, outpath=outfile, format="ipac", maxrec=5
        )
        t = Table.read(outfile, format="ascii.ipac")
        assert len(t) > 0, "Expected rows for known HIRES date range"
        assert all(k.startswith("HI.") for k in t["koaid"]), "Expected HIRES koaids"
        assert all(r == "HIRES" for r in t["instrume"]), "All rows should be HIRES"
    finally:
        if os.path.exists(outfile):
            os.unlink(outfile)


def test_query_date_returns_rows():
    """query_date (date-only, no time component) returns rows for a known date."""
    with tempfile.NamedTemporaryFile(suffix=".tbl", delete=False) as f:
        outfile = f.name
    try:
        Koa.query_date(
            "hires", "2021-06-01/2021-06-30", outpath=outfile, format="ipac", maxrec=3
        )
        t = Table.read(outfile, format="ascii.ipac")
        assert len(t) > 0, "Expected rows for known HIRES date range"
        assert all(k.startswith("HI.") for k in t["koaid"]), "Expected HIRES koaids"
    finally:
        if os.path.exists(outfile):
            os.unlink(outfile)


def test_query_datetime_empty_result():
    """query_datetime returns zero rows for a date range with no data."""
    with tempfile.NamedTemporaryFile(suffix=".tbl", delete=False) as f:
        outfile = f.name
    try:
        Koa.query_datetime(
            "hires", EMPTY_DATE, outpath=outfile, format="ipac", maxrec=5
        )
        t = Table.read(outfile, format="ascii.ipac")
        assert len(t) == 0, "Expected no rows for 1990 date range"
    finally:
        if os.path.exists(outfile):
            os.unlink(outfile)


def test_query_datetime_maxrec_respected():
    """maxrec limits the number of rows returned."""
    with tempfile.NamedTemporaryFile(suffix=".tbl", delete=False) as f:
        outfile = f.name
    try:
        Koa.query_datetime(
            "hires", HIRES_DATE, outpath=outfile, format="ipac", maxrec=2
        )
        t = Table.read(outfile, format="ascii.ipac")
        assert len(t) > 0, "Expected rows for known HIRES date range"
        assert len(t) <= 2, f"Expected at most 2 rows, got {len(t)}"
        assert all(k.startswith("HI.") for k in t["koaid"]), "Expected HIRES koaids"
    finally:
        if os.path.exists(outfile):
            os.unlink(outfile)


# ---------------------------------------------------------------------------
# query_position
# ---------------------------------------------------------------------------


def test_query_position_cone_returns_rows():
    """query_position cone search returns rows for a known sky position."""
    with tempfile.NamedTemporaryFile(suffix=".tbl", delete=False) as f:
        outfile = f.name
    try:
        Koa.query_position(
            "hires",
            f"circle {M42_RA} {M42_DEC} 0.5",
            outpath=outfile,
            format="ipac",
            maxrec=3,
        )
        t = Table.read(outfile, format="ascii.ipac")
        assert len(t) > 0, "Expected rows for M42 cone search"
        assert "koaid" in t.colnames
    finally:
        if os.path.exists(outfile):
            os.unlink(outfile)


# ---------------------------------------------------------------------------
# query_object
# ---------------------------------------------------------------------------


def test_query_object_resolves_and_returns_rows():
    """query_object resolves an object name and returns rows.

    Uses _KOA_RESOLVER (30s read timeout) so a hung resolver skips
    cleanly rather than blocking for 5 minutes.
    """
    with tempfile.NamedTemporaryFile(suffix=".tbl", delete=False) as f:
        outfile = f.name
    try:
        _KOA_RESOLVER.query_object(
            "hires", "Vega", outpath=outfile, format="ipac", maxrec=3
        )
        t = Table.read(outfile, format="ascii.ipac")
        assert len(t) > 0, "Expected HIRES rows for Vega"
        assert "koaid" in t.colnames
    except requests.Timeout:
        pytest.skip("Name resolver timed out (SIMBAD/NED may be down)")
    finally:
        if os.path.exists(outfile):
            os.unlink(outfile)


# ---------------------------------------------------------------------------
# query_criteria
# ---------------------------------------------------------------------------


def test_query_criteria_dict_returns_rows():
    """query_criteria accepts a dict and returns rows."""
    with tempfile.NamedTemporaryFile(suffix=".tbl", delete=False) as f:
        outfile = f.name
    try:
        Koa.query_criteria(
            {
                "instrument": "hires",
                "datetime": HIRES_DATE,
            },
            outpath=outfile,
            format="ipac",
            maxrec=3,
        )
        t = Table.read(outfile, format="ascii.ipac")
        assert len(t) > 0, "Expected rows from query_criteria"
        assert "koaid" in t.colnames
    finally:
        if os.path.exists(outfile):
            os.unlink(outfile)


# ---------------------------------------------------------------------------
# query_adql
# ---------------------------------------------------------------------------


def test_query_adql_raw_sql():
    """query_adql executes raw ADQL and returns rows."""
    with tempfile.NamedTemporaryFile(suffix=".tbl", delete=False) as f:
        outfile = f.name
    try:
        Koa.query_adql(
            "SELECT TOP 3 koaid, instrume, ra, dec FROM koa_hires "
            "WHERE instrume = 'HIRES'",
            outpath=outfile,
            format="ipac",
        )
        t = Table.read(outfile, format="ascii.ipac")
        assert len(t) == 3
        assert all(r == "HIRES" for r in t["instrume"])
    finally:
        if os.path.exists(outfile):
            os.unlink(outfile)


# ---------------------------------------------------------------------------
# Output formats
# ---------------------------------------------------------------------------


def test_query_returns_votable():
    """query_datetime returns a valid VOTable when format='votable'."""
    with tempfile.NamedTemporaryFile(suffix=".xml", delete=False) as f:
        outfile = f.name
    try:
        Koa.query_datetime(
            "hires", HIRES_DATE, outpath=outfile, format="votable", maxrec=2
        )
        assert os.path.getsize(outfile) > 0
        with open(outfile) as fp:
            content = fp.read()
        assert "VOTABLE" in content, "Output should be a VOTable"
    finally:
        if os.path.exists(outfile):
            os.unlink(outfile)


def test_query_returns_csv():
    """query_datetime returns CSV when format='csv'."""
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        outfile = f.name
    try:
        Koa.query_datetime("hires", HIRES_DATE, outpath=outfile, format="csv", maxrec=2)
        assert os.path.getsize(outfile) > 0
        with open(outfile) as fp:
            first_line = fp.readline()
        assert "koaid" in first_line, "First line should be CSV header with koaid"
    finally:
        if os.path.exists(outfile):
            os.unlink(outfile)


# ---------------------------------------------------------------------------
# Timeout
# ---------------------------------------------------------------------------


def test_timeout_fires_on_connect():
    """Archive(timeout=0.001) raises requests.Timeout against the live server."""
    koa = Archive(timeout=(0.001, 0.001))
    outfile = os.path.join(tempfile.gettempdir(), "koa_timeout_test.tbl")
    try:
        with pytest.raises(requests.Timeout):
            koa.query_datetime("hires", HIRES_DATE, outpath=outfile)
    finally:
        if os.path.exists(outfile):
            os.unlink(outfile)


def test_default_timeout_completes_query():
    """Default timeout (30, 300) is sufficient for a normal query."""
    koa = Archive()
    assert koa.timeout == (30, 300)
    with tempfile.NamedTemporaryFile(suffix=".tbl", delete=False) as f:
        outfile = f.name
    try:
        koa.query_datetime(
            "hires", HIRES_DATE, outpath=outfile, format="ipac", maxrec=1
        )
        t = Table.read(outfile, format="ascii.ipac")
        assert len(t) > 0
    finally:
        if os.path.exists(outfile):
            os.unlink(outfile)
