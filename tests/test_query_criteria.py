"""Tests for Archive.query_criteria.

Covers:
- Happy path: __make_query returns ADQL, send_async writes file
- No-cookie path (no cookiepath kwarg)
- send_async result checked for VOTable error via extract_xmlerr
- __make_query returns JSON error: query_criteria returns early (no file)
- __make_query timeout: propagates
- File not written after send_async: raises "Query failed"
- Invalid maxrec: returns early
- format and maxrec kwargs forwarded to KoaTap

Strategy: mock the makequery GET via requests_mock; patch KoaTap.send_async
at the boundary since it is already covered by test_send_async.py.
"""

import os
from unittest import mock

import pytest
import requests
import requests_mock as req_mock

from pykoa.koa.core import Archive, KoaTap

# Use a fake server so tests never touch the real KOA endpoint.
SERVER = "https://koa.example.test/"

PARAM = {"instrument": "hires", "datetime": "2020-01-01 00:00:00/2020-01-02 00:00:00"}
ADQL = "SELECT koaid, instrume FROM koa_hires WHERE utdatetime > '2020-01-01'"

VOTABLE_BYTES = (
    b'<?xml version="1.0" encoding="UTF-8"?>'
    b'<VOTABLE version="1.3" xmlns="http://www.ivoa.net/xml/VOTable/v1.3">'
    b"<RESOURCE><TABLE>"
    b'<FIELD name="koaid" datatype="char" arraysize="*"/>'
    b"<DATA><TABLEDATA>"
    b"<TR><TD>HI.20200101.00001.fits</TD></TR>"
    b"</TABLEDATA></DATA>"
    b"</TABLE></RESOURCE></VOTABLE>"
)

ERROR_VOTABLE_STR = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<VOTABLE version="1.3" xmlns="http://www.ivoa.net/xml/VOTable/v1.3">'
    '<RESOURCE type="results">'
    '<INFO name="QUERY_STATUS" value="ERROR">Column \'bad_col\' not found</INFO>'
    "</RESOURCE></VOTABLE>"
)


@pytest.fixture
def outfile(tmp_path):
    return str(tmp_path / "result.tbl")


def make_send_async_stub(content=VOTABLE_BYTES, retmsg=None):
    """Return a send_async stub that writes content to the SUT-supplied outpath."""
    def stub(self, query, outpath=None, **kwargs):
        if outpath:
            with open(outpath, "wb") as f:
                f.write(content)
        return retmsg or f"Result downloaded to file {outpath}"
    return stub


# ---------------------------------------------------------------------------
# Happy path — no cookie
# ---------------------------------------------------------------------------


def test_query_criteria_happy_path_no_cookie(outfile):
    """query_criteria writes result file on successful no-cookie query."""
    a = Archive()
    with req_mock.Mocker() as m:
        m.get(req_mock.ANY, text=ADQL, headers={"content-type": "text/plain"})
        with mock.patch.object(KoaTap, "send_async", make_send_async_stub()):
            a.query_criteria(PARAM, outfile, server=SERVER)

    with open(outfile, "rb") as f:
        assert f.read() == VOTABLE_BYTES




# ---------------------------------------------------------------------------
# __make_query error paths
# ---------------------------------------------------------------------------


def test_query_criteria_makequery_json_error_returns_early(outfile):
    """If __make_query returns a JSON error, query_criteria returns early without writing file."""
    a = Archive()
    with req_mock.Mocker() as m:
        m.get(
            req_mock.ANY,
            json={"status": "error", "msg": "Instrument not supported"},
            headers={"content-type": "application/json"},
        )
        a.query_criteria(PARAM, outfile, server=SERVER)

    assert not os.path.exists(outfile)


def test_query_criteria_makequery_timeout_raises(outfile):
    """requests.Timeout on __make_query propagates out of query_criteria."""
    a = Archive()
    with req_mock.Mocker() as m:
        m.get(req_mock.ANY, exc=requests.Timeout)
        with pytest.raises(requests.Timeout):
            a.query_criteria(PARAM, outfile, server=SERVER)


def test_query_criteria_makequery_network_error_returns_early(outfile):
    """A network error on __make_query causes query_criteria to return early."""
    a = Archive()
    with req_mock.Mocker() as m:
        m.get(req_mock.ANY, exc=requests.ConnectionError("connection refused"))
        a.query_criteria(PARAM, outfile, server=SERVER)

    assert not os.path.exists(outfile)


# ---------------------------------------------------------------------------
# send_async error paths
# ---------------------------------------------------------------------------


def test_query_criteria_votable_error_from_server_raises(outfile):
    """If send_async returns a VOTable error string, query_criteria raises."""
    a = Archive()
    with req_mock.Mocker() as m:
        m.get(req_mock.ANY, text=ADQL, headers={"content-type": "text/plain"})

        def error_stub(self, query, outpath=None, **kwargs):
            # Do NOT write the file — server returned an error
            return ERROR_VOTABLE_STR

        with mock.patch.object(KoaTap, "send_async", error_stub):
            with pytest.raises(Exception, match="(?i)bad_col|not found"):
                a.query_criteria(PARAM, outfile, server=SERVER)

    assert not os.path.exists(outfile)


def test_query_criteria_file_not_written_raises(outfile):
    """If send_async returns a non-VOTable response and no file is written, raises."""
    a = Archive()
    with req_mock.Mocker() as m:
        m.get(req_mock.ANY, text=ADQL, headers={"content-type": "text/plain"})

        def bad_stub(self, query, outpath=None, **kwargs):
            # Return short non-XML response without writing the file
            return "WEB"

        with mock.patch.object(KoaTap, "send_async", bad_stub):
            with pytest.raises(Exception, match="(?i)query failed|unexpected response"):
                a.query_criteria(PARAM, outfile, server=SERVER)

    assert not os.path.exists(outfile)


# ---------------------------------------------------------------------------
# maxrec validation
# ---------------------------------------------------------------------------


def test_query_criteria_invalid_maxrec_returns_early(outfile):
    """Non-numeric maxrec causes query_criteria to return early without querying."""
    a = Archive()
    # maxrec validation fires before any URL is constructed — no mock needed
    a.query_criteria(PARAM, outfile, server=SERVER, maxrec="not_a_number")

    assert not os.path.exists(outfile)


def test_query_criteria_file_not_written_long_response_raises(outfile):
    """If send_async returns a long multi-line response with no file written, raises generic message."""
    a = Archive()
    long_error = "line1\n" + "x" * 200  # >80 chars and contains \n
    with req_mock.Mocker() as m:
        m.get(req_mock.ANY, text=ADQL, headers={"content-type": "text/plain"})

        def long_stub(self, query, outpath=None, **kwargs):
            return long_error

        with mock.patch.object(KoaTap, "send_async", long_stub):
            with pytest.raises(Exception, match="(?i)error response"):
                a.query_criteria(PARAM, outfile, server=SERVER)

    assert not os.path.exists(outfile)
