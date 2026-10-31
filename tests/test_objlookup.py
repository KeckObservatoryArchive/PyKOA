"""Tests for objLookup.__init__ data-parsing paths.

Covers:
- stat=OK: all coordinate and metadata fields populated from JSON
- stat=error: self.status set to "error", self.msg extracted from JSON
- malformed JSON body: raises Exception
- missing "stat" key in JSON: raises Exception

Timeout configuration and URL construction are covered by test_timeout.py
and are not duplicated here.
"""

import json

import pytest
import requests_mock as req_mock

from pykoa.koa.core import objLookup

# Use ANY to avoid coupling to the exact conf.server value or URL encoding.
LOOKUP_OK_JSON = json.dumps(
    {
        "stat": "OK",
        "source": "SIMBAD",
        "objname": "Vega",
        "objtype": "Star",
        "parsename": "Vega",
        "objdesc": "Variable Star of delta Sct type (DSCT*)",
        "ra2000": "279.23473",
        "dec2000": "38.78369",
        "cra2000": "18:36:56.34",
        "cdec2000": "+38:47:01.3",
    }
)

LOOKUP_ERROR_JSON = json.dumps(
    {
        "stat": "ERROR",
        "msg": "Object name 'NoSuchObject' could not be resolved.",
    }
)


# ---------------------------------------------------------------------------
# stat=OK — all fields populated
# ---------------------------------------------------------------------------


def test_objlookup_ok_populates_coordinates():
    """stat=OK response populates ra2000 and dec2000."""
    with req_mock.Mocker() as m:
        m.get(req_mock.ANY, text=LOOKUP_OK_JSON,
              headers={"Content-type": "application/json"})
        obj = objLookup("Vega")

    assert obj.status == "OK"
    assert obj.ra2000 == "279.23473"
    assert obj.dec2000 == "38.78369"


def test_objlookup_ok_populates_all_metadata():
    """stat=OK response populates source, objname, objtype, parsename, cra2000, cdec2000."""
    with req_mock.Mocker() as m:
        m.get(req_mock.ANY, text=LOOKUP_OK_JSON,
              headers={"Content-type": "application/json"})
        obj = objLookup("Vega")

    assert obj.source == "SIMBAD"
    assert obj.objname == "Vega"
    assert obj.objtype == "Star"
    assert obj.parsename == "Vega"
    assert obj.cra2000 == "18:36:56.34"
    assert obj.cdec2000 == "+38:47:01.3"


# ---------------------------------------------------------------------------
# stat=error — error fields extracted
# ---------------------------------------------------------------------------


def test_objlookup_error_stat_sets_status_and_msg():
    """A non-OK stat sets self.status='error' and self.msg from the JSON msg field."""
    with req_mock.Mocker() as m:
        m.get(req_mock.ANY, text=LOOKUP_ERROR_JSON,
              headers={"Content-type": "application/json"})
        obj = objLookup("NoSuchObject")

    assert obj.status == "error"
    assert "NoSuchObject" in obj.msg or "could not be resolved" in obj.msg



# ---------------------------------------------------------------------------
# Malformed JSON body
# ---------------------------------------------------------------------------


def test_objlookup_malformed_json_raises():
    """A non-JSON response body raises an Exception during JSON parsing."""
    with req_mock.Mocker() as m:
        m.get(req_mock.ANY, text="<html>Internal Server Error</html>",
              headers={"Content-type": "text/html"})
        with pytest.raises(Exception, match="jsondata"):
            objLookup("Vega")


# ---------------------------------------------------------------------------
# Missing "stat" key
# ---------------------------------------------------------------------------


def test_objlookup_missing_stat_key_raises():
    """JSON response missing the 'stat' key raises an Exception."""
    body = json.dumps({"ra2000": "279.23473", "dec2000": "38.78369"})
    with req_mock.Mocker() as m:
        m.get(req_mock.ANY, text=body,
              headers={"Content-type": "application/json"})
        with pytest.raises(Exception, match="stat"):
            objLookup("Vega")


# ---------------------------------------------------------------------------
# stat=error with missing msg key — raises
# ---------------------------------------------------------------------------


def test_objlookup_error_stat_missing_msg_key_raises():
    """stat=ERROR with no 'msg' key raises instead of silently swallowing.

    The error branch unconditionally reads jsondata["msg"]; a missing key
    triggers the except path which re-raises with an explanatory message.
    """
    body = json.dumps({"stat": "ERROR"})  # no "msg" key
    with req_mock.Mocker() as m:
        m.get(req_mock.ANY, text=body,
              headers={"Content-type": "application/json"})
        with pytest.raises(Exception, match="(?i)msg"):
            objLookup("NoSuchObject")
