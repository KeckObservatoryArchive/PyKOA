"""End-to-end tests for each KOA instrument, mirroring the PyKOA notebooks.

Each test uses the exact date ranges, sky positions, and object names from
the official PyKOA instrument notebooks at:
https://koa.ipac.caltech.edu/UserGuide/PyKOA/TAPClients.html

Run selectively:
    pytest tests/test_e2e_instruments.py -m e2e -v

These tests are excluded from the main test suite and run nightly
via .github/workflows/e2e.yml.
"""

import json
import os
import tempfile

import pytest
import requests
from astropy.table import Table

from pykoa.koa import Koa
from pykoa.koa.core import Archive

pytestmark = pytest.mark.e2e

# Short timeout for object lookups -- the name resolver (ExoPlanet/SIMBAD/NED)
# can hang for the full read timeout when those services are degraded.
# 30s connect, 30s read is enough for a healthy resolver; if it takes longer
# the service is down and we want a fast, clear failure not a 5-minute hang.
_KOA_OBJECT = Archive(timeout=(30, 30))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def query_datetime_rows(instrument, datetime_range, maxrec=3):
    """Run query_datetime and return an astropy Table."""
    with tempfile.NamedTemporaryFile(suffix=".tbl", delete=False) as f:
        outfile = f.name
    try:
        Koa.query_datetime(
            instrument, datetime_range, outpath=outfile, format="ipac", maxrec=maxrec
        )
        return Table.read(outfile, format="ascii.ipac")
    finally:
        if os.path.exists(outfile):
            os.unlink(outfile)


def query_position_rows(instrument, pos, maxrec=3):
    """Run query_position and return an astropy Table."""
    with tempfile.NamedTemporaryFile(suffix=".tbl", delete=False) as f:
        outfile = f.name
    try:
        Koa.query_position(
            instrument, pos, outpath=outfile, format="ipac", maxrec=maxrec
        )
        return Table.read(outfile, format="ascii.ipac")
    finally:
        if os.path.exists(outfile):
            os.unlink(outfile)


def query_object_rows(instrument, obj, maxrec=3):
    """Run query_object with a short timeout and return an astropy Table.

    Uses a 30s read timeout so hung resolver calls (SIMBAD/NED outage)
    fail quickly rather than waiting the full default 300s.
    """
    with tempfile.NamedTemporaryFile(suffix=".tbl", delete=False) as f:
        outfile = f.name
    try:
        _KOA_OBJECT.query_object(
            instrument, obj, outpath=outfile, format="ipac", maxrec=maxrec
        )
        return Table.read(outfile, format="ascii.ipac")
    except requests.Timeout:
        pytest.skip("Name resolver timed out (SIMBAD/NED may be down)")
    finally:
        if os.path.exists(outfile):
            os.unlink(outfile)


def query_adql_rows(adql, maxrec=3):
    """Run query_adql and return an astropy Table."""
    with tempfile.NamedTemporaryFile(suffix=".tbl", delete=False) as f:
        outfile = f.name
    try:
        Koa.query_adql(adql, outpath=outfile, format="ipac")
        return Table.read(outfile, format="ascii.ipac")
    finally:
        if os.path.exists(outfile):
            os.unlink(outfile)


# ---------------------------------------------------------------------------
# HIRES
# Notebook: PyKOA_HIRES_introduction — ref date 2018-08-07, object hr1679
# ---------------------------------------------------------------------------


def test_hires_query_datetime():
    """HIRES: query_datetime returns rows for notebook reference date."""
    t = query_datetime_rows("hires", "2018-08-07 00:00:00/2018-08-08 00:00:00")
    assert len(t) > 0
    assert all(k.startswith("HI.") for k in t["koaid"]), "Expected HIRES koaids"


def test_hires_query_position():
    """HIRES: cone search around HR1679 (notebook ref position)."""
    t = query_position_rows("hires", "circle 77.28 -8.748 0.5")
    assert len(t) > 0
    assert "koaid" in t.colnames


def test_hires_query_object():
    """HIRES: query_object resolves hr1679 and returns rows."""
    t = query_object_rows("hires", "hr1679")
    assert len(t) > 0
    assert "koaid" in t.colnames


def test_hires_query_adql():
    """HIRES: raw ADQL cone query around HR1679."""
    t = query_adql_rows(
        "SELECT TOP 3 koaid, instrume, ra, dec FROM koa_hires "
        "WHERE contains(point('J2000', ra, dec), "
        "circle('J2000', 77.28659645, -8.75408078, 0.5)) = 1"
    )
    assert len(t) > 0
    assert "koaid" in t.colnames


# ---------------------------------------------------------------------------
# DEIMOS
# Notebook: PyKOA_DEIMOS_introduction — ref date 2015-09, object ngc_1614
# ---------------------------------------------------------------------------


def test_deimos_query_datetime():
    """DEIMOS: query_datetime returns rows for notebook reference date."""
    t = query_datetime_rows("deimos", "2015-09-01 00:00:00/2015-09-30 23:59:59")
    assert len(t) > 0
    assert all(k.startswith("DE.") for k in t["koaid"]), "Expected DEIMOS koaids"


def test_deimos_query_position():
    """DEIMOS: cone search around notebook reference position."""
    t = query_position_rows("deimos", "circle 268.288 65.097 1.0")
    assert len(t) > 0
    assert "koaid" in t.colnames


def test_deimos_query_object():
    """DEIMOS: query_object resolves ngc_1614 and returns rows."""
    t = query_object_rows("deimos", "ngc_1614")
    assert len(t) > 0
    assert "koaid" in t.colnames


# ---------------------------------------------------------------------------
# ESI
# Notebook: PyKOA_ESI_introduction — ref date 2015-09, object M32
# ---------------------------------------------------------------------------


def test_esi_query_datetime():
    """ESI: query_datetime returns rows for notebook reference date."""
    t = query_datetime_rows("esi", "2015-09-01 00:00:00/2015-09-30 23:59:59")
    assert len(t) > 0
    assert all(k.startswith("ES.") for k in t["koaid"]), "Expected ESI koaids"


def test_esi_query_position():
    """ESI: cone search around notebook reference position."""
    t = query_position_rows("esi", "circle 255.03913 -11.82214 1.0")
    assert len(t) > 0
    assert "koaid" in t.colnames


def test_esi_query_object():
    """ESI: query_object resolves M32 and returns rows."""
    t = query_object_rows("esi", "M32")
    assert len(t) > 0
    assert "koaid" in t.colnames


# ---------------------------------------------------------------------------
# KCWI
# Notebook: PyKOA_KCWI_introduction — ref date 2019-05, object feige34
# ---------------------------------------------------------------------------


def test_kcwi_query_datetime():
    """KCWI: query_datetime returns rows for notebook reference date."""
    t = query_datetime_rows("kcwi", "2019-05-09 02:00:00/2019-05-09 08:00:00")
    assert len(t) > 0
    assert all(k.startswith("KB.") for k in t["koaid"]), "Expected KCWI koaids"


def test_kcwi_query_position():
    """KCWI: cone search around Feige34 (notebook ref position)."""
    t = query_position_rows("kcwi", "circle 159.90296 43.10281 0.5")
    assert len(t) > 0
    assert "koaid" in t.colnames


def test_kcwi_query_object():
    """KCWI: query_object resolves feige34 and returns rows."""
    t = query_object_rows("kcwi", "feige34")
    assert len(t) > 0
    assert "koaid" in t.colnames


# ---------------------------------------------------------------------------
# KPF
# Notebook: PyKOA_KPF_introduction — ref date 2023-02
# Note: KPF is new (2023), no position queries in notebook
# ---------------------------------------------------------------------------


def test_kpf_query_datetime():
    """KPF: query_datetime returns rows for notebook reference date."""
    t = query_datetime_rows("kpf", "2023-02-03 00:00:00/2023-02-04 23:59:59")
    assert len(t) > 0
    assert all(k.startswith("KP.") for k in t["koaid"]), "Expected KPF koaids"


def test_kpf_query_adql():
    """KPF: raw ADQL query by program ID returns rows."""
    t = query_adql_rows(
        "SELECT TOP 3 koaid, instrume, progid FROM koa_kpf WHERE progid = 'ENG'"
    )
    assert len(t) > 0
    assert "koaid" in t.colnames


# ---------------------------------------------------------------------------
# LRIS
# Notebook: PyKOA_LRIS_introduction — ref date 2015-01, object Aldebaran
# ---------------------------------------------------------------------------


def test_lris_query_datetime():
    """LRIS: query_datetime returns rows for notebook reference date."""
    t = query_datetime_rows("lris", "2015-01-01 00:00:00/2015-03-01 23:59:59")
    assert len(t) > 0
    assert all(k.startswith(("LB.", "LR.")) for k in t["koaid"]), (
        "Expected LRIS koaids (blue or red)"
    )


def test_lris_query_position():
    """LRIS: cone search around Aldebaran (notebook ref position)."""
    t = query_position_rows("lris", "circle 174.27 29.8 1.0")
    assert len(t) > 0
    assert "koaid" in t.colnames


def test_lris_query_object():
    """LRIS: query_object resolves Aldebaran and returns rows."""
    t = query_object_rows("lris", "Aldebaran")
    assert len(t) > 0
    assert "koaid" in t.colnames


# ---------------------------------------------------------------------------
# LWS
# Notebook: PyKOA_LWS_NIRC_introduction — ref date 1998-12, object Aldebaran
# ---------------------------------------------------------------------------


def test_lws_query_datetime():
    """LWS: query_datetime returns rows for notebook reference date."""
    t = query_datetime_rows("lws", "1998-12-03 00:00:00/2005-02-20 23:59:59")
    assert len(t) > 0
    assert all(k.startswith("LW.") for k in t["koaid"]), "Expected LWS koaids"


def test_lws_query_position():
    """LWS: cone search around Aldebaran (notebook ref position)."""
    t = query_position_rows("lws", "circle 68.98 16.51 1.0")
    assert len(t) > 0
    assert "koaid" in t.colnames


# ---------------------------------------------------------------------------
# MOSFIRE
# Notebook: PyKOA_MOSFIRE_introduction — ref date 2012-11, object HIP17971
# ---------------------------------------------------------------------------


def test_mosfire_query_datetime():
    """MOSFIRE: query_datetime returns rows for notebook reference date."""
    t = query_datetime_rows("mosfire", "2012-11-28 03:00:00/2012-11-28 04:00:00")
    assert len(t) > 0
    assert all(k.startswith("MF.") for k in t["koaid"]), "Expected MOSFIRE koaids"


def test_mosfire_query_position():
    """MOSFIRE: cone search around HIP17971 (notebook ref position)."""
    t = query_position_rows("mosfire", "circle 57.63 29.74 0.5")
    assert len(t) > 0
    assert "koaid" in t.colnames


def test_mosfire_query_object():
    """MOSFIRE: query_object resolves HIP17971 and returns rows."""
    t = query_object_rows("mosfire", "HIP17971")
    assert len(t) > 0
    assert "koaid" in t.colnames


# ---------------------------------------------------------------------------
# NIRC
# Notebook: PyKOA_LWS_NIRC_introduction — ref date 1995-03, object NGC 3384
# ---------------------------------------------------------------------------


def test_nirc_query_datetime():
    """NIRC: query_datetime returns rows for notebook reference date."""
    t = query_datetime_rows("nirc", "1995-03-16 00:00:00/1995-04-01 23:59:59")
    assert len(t) > 0
    assert all(k.startswith("N1.") for k in t["koaid"]), "Expected NIRC koaids"


def test_nirc_query_position():
    """NIRC: cone search around notebook reference position."""
    t = query_position_rows("nirc", "circle 162.07 12.63 1.0")
    assert len(t) > 0
    assert "koaid" in t.colnames


# ---------------------------------------------------------------------------
# NIRC2
# Notebook: PyKOA_NIRC2_introduction — ref date 2014-09, object grb140903a
# ---------------------------------------------------------------------------


def test_nirc2_query_datetime():
    """NIRC2: query_datetime returns rows for notebook reference date."""
    t = query_datetime_rows("nirc2", "2014-09-06 05:00:00/2014-09-06 06:00:00")
    assert len(t) > 0
    assert all(k.startswith("N2.") for k in t["koaid"]), "Expected NIRC2 koaids"


def test_nirc2_query_position():
    """NIRC2: cone search around GRB140903A (notebook ref position)."""
    t = query_position_rows("nirc2", "circle 238.0 27.6 0.5")
    assert len(t) > 0
    assert "koaid" in t.colnames


def test_nirc2_query_object():
    """NIRC2: query_object resolves grb140903a and returns rows."""
    t = query_object_rows("nirc2", "grb140903a")
    assert len(t) > 0
    assert "koaid" in t.colnames


# ---------------------------------------------------------------------------
# NIRES
# Notebook: PyKOA_NIRES_introduction — ref date 2018-12, object GD153
# ---------------------------------------------------------------------------


def test_nires_query_datetime():
    """NIRES: query_datetime returns rows for notebook reference date."""
    t = query_datetime_rows("nires", "2018-12-18 15:00:00/2018-12-18 15:30:00")
    assert len(t) > 0
    assert all(k.startswith("NR.") for k in t["koaid"]), "Expected NIRES koaids"


def test_nires_query_position():
    """NIRES: cone search around GD153 (notebook ref position)."""
    t = query_position_rows("nires", "circle 194.25 22.03 0.5")
    assert len(t) > 0
    assert "koaid" in t.colnames


def test_nires_query_object():
    """NIRES: query_object resolves GD153 and returns rows."""
    t = query_object_rows("nires", "GD153")
    assert len(t) > 0
    assert "koaid" in t.colnames


# ---------------------------------------------------------------------------
# NIRSPEC
# Notebook: PyKOA_NIRSPEC_introduction — ref date 2017-06, object WR 118
# ---------------------------------------------------------------------------


def test_nirspec_query_datetime():
    """NIRSPEC: query_datetime returns rows for notebook reference date."""
    t = query_datetime_rows("nirspec", "2017-06-05 13:00:00/2017-06-05 14:00:00")
    assert len(t) > 0
    assert all(k.startswith("NS.") for k in t["koaid"]), "Expected NIRSPEC koaids"


def test_nirspec_query_position():
    """NIRSPEC: cone search around WR 118 (notebook ref position)."""
    t = query_position_rows("nirspec", "circle 277.93 -9.985 0.5")
    assert len(t) > 0
    assert "koaid" in t.colnames


def test_nirspec_query_object():
    """NIRSPEC: query_object resolves WR 118 and returns rows."""
    t = query_object_rows("nirspec", "WR 118")
    assert len(t) > 0
    assert "koaid" in t.colnames


# ---------------------------------------------------------------------------
# OSIRIS
# Notebook: PyKOA_OSIRIS_introduction — ref date 2019-05, object 3C273
# ---------------------------------------------------------------------------


def test_osiris_query_datetime():
    """OSIRIS: query_datetime returns rows for notebook reference date."""
    t = query_datetime_rows("osiris", "2019-05-29 00:00:00/2019-05-31 23:59:59")
    assert len(t) > 0
    assert all(k.startswith("OS.") for k in t["koaid"]), "Expected OSIRIS koaids"


def test_osiris_query_position():
    """OSIRIS: cone search around notebook reference position."""
    t = query_position_rows("osiris", "circle 270.4 -27.2 1.0")
    assert len(t) > 0
    assert "koaid" in t.colnames


def test_osiris_query_object():
    """OSIRIS: query_object resolves 3C273 and returns rows."""
    t = query_object_rows("osiris", "3C273")
    assert len(t) > 0
    assert "koaid" in t.colnames


# ---------------------------------------------------------------------------
# MOSS — Moving Object Search Service
# Notebook: PyKOA_MOSS_introduction — Pluto/NIRSPEC, Pallas/NIRC2
# ---------------------------------------------------------------------------


def test_moss_pluto_nirspec():
    """MOSS: query_moving_object finds NIRSPEC observations of Pluto."""
    with tempfile.TemporaryDirectory() as outdir:
        Koa.query_moving_object(
            instrument="nirspec",
            object="pluto",
            naifid="999",
            startdate="1995-01-01",
            enddate="2022-09-20",
            outdir=outdir,
            outfile="pluto.json",
        )
        json_path = os.path.join(outdir, "pluto.json")
        assert os.path.exists(json_path), "Expected JSON output file"

        with open(json_path) as f:
            data = json.load(f)
        assert len(data) > 0, "Expected Pluto NIRSPEC observations in JSON"


def test_moss_pallas_nirc2():
    """MOSS: query_moving_object finds NIRC2 observations of Pallas (no NAIF ID needed)."""
    with tempfile.TemporaryDirectory() as outdir:
        Koa.query_moving_object(
            instrument="nirc2",
            object="pallas",
            startdate="1995-01-01",
            outdir=outdir,
            outfile="pallas.json",
        )
        json_path = os.path.join(outdir, "pallas.json")
        assert os.path.exists(json_path), "Expected JSON output file"

        with open(json_path) as f:
            data = json.load(f)
        assert len(data) > 0, "Expected Pallas NIRC2 observations in JSON"
