"""Tests for KoaTap.send_async against a fake TAP server.

The fake server sits at the requests_mock boundary, covering:
- 303 happy path (COMPLETED phase, outpath and in-memory)
- EXECUTING → COMPLETED poll transition
- ERROR phase (status set, error summary returned)
- Non-303 response on the initial POST (JSON error, XML error, plain text)
- Timeout propagation
- 303 with no Location header (statusurl missing)
- Non-303 with application/json error body
"""

import os

import pytest
import requests
import requests_mock as req_mock

from pykoa.koa.core import KoaTap

TAP_URL = "https://koa.example.test/TAP"
ASYNC_URL = TAP_URL + "/async"
STATUS_URL = TAP_URL + "/async/job123"
RESULT_URL = TAP_URL + "/async/job123/results/result"
QUERY = "SELECT TOP 3 koaid FROM koa_hires"

VOTABLE_BYTES = (
    b'<?xml version="1.0" encoding="UTF-8"?>'
    b'<VOTABLE version="1.3" xmlns="http://www.ivoa.net/xml/VOTable/v1.3">'
    b"<RESOURCE><TABLE>"
    b'<FIELD name="koaid" datatype="char" arraysize="*"/>'
    b"<DATA><TABLEDATA>"
    b"<TR><TD>HI.20200101.00001.fits</TD></TR>"
    b"<TR><TD>HI.20200101.00002.fits</TD></TR>"
    b"<TR><TD>HI.20200101.00003.fits</TD></TR>"
    b"</TABLEDATA></DATA>"
    b"</TABLE></RESOURCE></VOTABLE>"
)

ERROR_VOTABLE_BYTES = (
    b'<?xml version="1.0" encoding="UTF-8"?>'
    b'<VOTABLE version="1.3" xmlns="http://www.ivoa.net/xml/VOTable/v1.3">'
    b'<RESOURCE type="results">'
    b'<INFO name="QUERY_STATUS" value="ERROR">Table \'koa_bad\' not found</INFO>'
    b"</RESOURCE></VOTABLE>"
)

UWS_COMPLETED_XML = f"""<?xml version="1.0" encoding="UTF-8"?>
<uws:job xmlns:uws="http://www.ivoa.net/xml/UWS/v1.0"
         xmlns:xlink="http://www.w3.org/1999/xlink">
  <uws:jobId>job123</uws:jobId>
  <uws:phase>COMPLETED</uws:phase>
  <uws:results>
    <uws:result id="result" xlink:href="{RESULT_URL}"/>
  </uws:results>
  <uws:parameters/>
</uws:job>"""

UWS_EXECUTING_XML = """<?xml version="1.0" encoding="UTF-8"?>
<uws:job xmlns:uws="http://www.ivoa.net/xml/UWS/v1.0"
         xmlns:xlink="http://www.w3.org/1999/xlink">
  <uws:jobId>job123</uws:jobId>
  <uws:phase>EXECUTING</uws:phase>
  <uws:results/>
  <uws:parameters/>
</uws:job>"""

UWS_ERROR_XML = """<?xml version="1.0" encoding="UTF-8"?>
<uws:job xmlns:uws="http://www.ivoa.net/xml/UWS/v1.0"
         xmlns:xlink="http://www.w3.org/1999/xlink">
  <uws:jobId>job123</uws:jobId>
  <uws:phase>ERROR</uws:phase>
  <uws:errorSummary type="transient">
    <uws:message>Query execution failed</uws:message>
  </uws:errorSummary>
  <uws:results/>
  <uws:parameters/>
</uws:job>"""


@pytest.fixture
def tmp_outpath(tmp_path):
    return str(tmp_path / "result.tbl")


# ---------------------------------------------------------------------------
# Happy path — 303 → COMPLETED, with outpath
# ---------------------------------------------------------------------------


def test_send_async_happy_path_outpath(tmp_outpath):
    """send_async writes result to outpath on 303 → COMPLETED."""
    tap = KoaTap(TAP_URL)
    with req_mock.Mocker() as m:
        m.post(ASYNC_URL, status_code=303, headers={"Location": STATUS_URL})
        m.get(STATUS_URL, text=UWS_COMPLETED_XML)
        m.get(RESULT_URL, content=VOTABLE_BYTES)

        msg = tap.send_async(QUERY, outpath=tmp_outpath, format="votable")

    assert os.path.exists(tmp_outpath), "result file not written"
    assert open(tmp_outpath, "rb").read() == VOTABLE_BYTES
    assert f"Result downloaded to file [{tmp_outpath}]" == msg


def test_send_async_happy_path_in_memory():
    """send_async loads result into astropytbl when no outpath given."""
    tap = KoaTap(TAP_URL)
    with req_mock.Mocker() as m:
        m.post(ASYNC_URL, status_code=303, headers={"Location": STATUS_URL})
        m.get(STATUS_URL, text=UWS_COMPLETED_XML)
        m.get(RESULT_URL, content=VOTABLE_BYTES)

        msg = tap.send_async(QUERY, format="votable")

    assert len(tap.astropytbl) == 3
    assert msg == "Result saved in memory (astropy table)."


# ---------------------------------------------------------------------------
# Poll loop — EXECUTING → COMPLETED
# ---------------------------------------------------------------------------


def test_send_async_poll_loop(tmp_outpath):
    """send_async polls until EXECUTING transitions to COMPLETED."""
    tap = KoaTap(TAP_URL)
    with req_mock.Mocker() as m:
        m.post(ASYNC_URL, status_code=303, headers={"Location": STATUS_URL})
        # First poll: EXECUTING; second poll: COMPLETED
        m.get(
            STATUS_URL,
            [
                {"text": UWS_EXECUTING_XML},
                {"text": UWS_COMPLETED_XML},
            ],
        )
        m.get(RESULT_URL, content=VOTABLE_BYTES)

        tap.send_async(QUERY, outpath=tmp_outpath, format="votable")

    with open(tmp_outpath, "rb") as f:
        assert f.read() == VOTABLE_BYTES


# ---------------------------------------------------------------------------
# ERROR phase
# ---------------------------------------------------------------------------


def test_send_async_error_phase(tmp_outpath):
    """send_async sets status='error' and returns error message on ERROR phase."""
    tap = KoaTap(TAP_URL)
    with req_mock.Mocker() as m:
        m.post(ASYNC_URL, status_code=303, headers={"Location": STATUS_URL})
        m.get(STATUS_URL, text=UWS_ERROR_XML)

        msg = tap.send_async(QUERY, outpath=tmp_outpath, format="votable")

    assert not os.path.exists(tmp_outpath), "no file should be written on ERROR"
    assert tap.status == "error"
    assert "Query execution failed" in msg


# ---------------------------------------------------------------------------
# Non-303 response on initial POST
# ---------------------------------------------------------------------------


def test_send_async_non303_xml_error(tmp_outpath):
    """send_async returns error message when POST returns 400 with VOTable error."""
    tap = KoaTap(TAP_URL)
    with req_mock.Mocker() as m:
        m.post(
            ASYNC_URL,
            status_code=400,
            content=ERROR_VOTABLE_BYTES,
            headers={"Content-Type": "text/xml"},
        )

        msg = tap.send_async(QUERY, outpath=tmp_outpath, format="votable")

    assert not os.path.exists(tmp_outpath)
    assert "koa_bad" in msg


def test_send_async_non303_plain_text_error(tmp_outpath):
    """send_async returns plain-text error body when POST returns 400 text/plain."""
    tap = KoaTap(TAP_URL)
    with req_mock.Mocker() as m:
        m.post(
            ASYNC_URL,
            status_code=400,
            text="Bad request: missing QUERY parameter",
            headers={"Content-Type": "text/plain"},
        )

        msg = tap.send_async(QUERY, outpath=tmp_outpath, format="votable")

    assert not os.path.exists(tmp_outpath)
    assert "bad request" in msg.lower()


def test_send_async_non303_500(tmp_outpath):
    """send_async handles 500 Internal Server Error gracefully."""
    tap = KoaTap(TAP_URL)
    with req_mock.Mocker() as m:
        m.post(
            ASYNC_URL,
            status_code=500,
            text="Internal Server Error",
            headers={"Content-Type": "text/plain"},
        )

        msg = tap.send_async(QUERY, outpath=tmp_outpath, format="votable")

    assert not os.path.exists(tmp_outpath)
    assert "internal server error" in msg.lower()


# ---------------------------------------------------------------------------
# 303 with no Location header — statusurl not provided
# ---------------------------------------------------------------------------


def test_send_async_303_no_location_returns_error(tmp_outpath):
    """303 with no Location header returns an error string instead of crashing.

    Regression for core.py: headers["Location"] was used without .get(),
    causing KeyError when the Location header was absent on a 303 response.
    The guard at line ~4961 (if len(self.statusurl) == 0) now fires correctly.
    """
    tap = KoaTap(TAP_URL)
    with req_mock.Mocker() as m:
        m.post(ASYNC_URL, status_code=303, headers={})  # no Location
        result = tap.send_async(QUERY, outpath=tmp_outpath, format="votable")
    assert "error" in result.lower() or "statusurl" in result.lower()
    assert not os.path.exists(tmp_outpath)


# ---------------------------------------------------------------------------
# Non-303 with application/json error body
# ---------------------------------------------------------------------------


def test_send_async_non303_json_error(tmp_outpath):
    """send_async returns the JSON msg field when POST returns non-303 with JSON body."""
    tap = KoaTap(TAP_URL)
    with req_mock.Mocker() as m:
        m.post(
            ASYNC_URL,
            status_code=400,
            json={"status": "error", "msg": "Instrument not supported"},
            headers={"Content-Type": "application/json"},
        )

        msg = tap.send_async(QUERY, outpath=tmp_outpath, format="votable")

    assert not os.path.exists(tmp_outpath)
    assert "not supported" in msg.lower()


# ---------------------------------------------------------------------------
# Timeout propagation
# ---------------------------------------------------------------------------


def test_send_async_timeout_raises(tmp_outpath):
    """requests.Timeout on the async POST propagates out of send_async."""
    tap = KoaTap(TAP_URL, timeout=0.001)
    with req_mock.Mocker() as m:
        m.post(ASYNC_URL, exc=requests.Timeout)
        with pytest.raises(requests.Timeout):
            tap.send_async(QUERY, outpath=tmp_outpath, format="votable")


def test_send_async_poll_timeout_raises(tmp_outpath):
    """requests.Timeout during status polling propagates out of send_async."""
    tap = KoaTap(TAP_URL, timeout=0.001)
    with req_mock.Mocker() as m:
        m.post(ASYNC_URL, status_code=303, headers={"Location": STATUS_URL})
        m.get(STATUS_URL, exc=requests.Timeout)
        with pytest.raises(requests.Timeout):
            tap.send_async(QUERY, outpath=tmp_outpath, format="votable")
