"""Tests for Archive.__submit_request (regression + behavior).

Covers:
- HTTP 200 success: response streamed to filepath
- HTTP 200 success with cookiejar: cookies forwarded to requests.get
- HTTP 403 + generic body: raises with "Access denied" prefix
- HTTP 403 + VOTable body: VOTable error text included in exception (regression
  for SUMMARY.md item 8 — Archive had no extract_xmlerr; fixed via
  _parse_votable_error module-level function)
- HTTP 400 + generic body: raises with "Bad request" prefix
- HTTP 400 + VOTable body: VOTable error text included in exception
- HTTP 500: raises with HTTP status code in message
- HTTP 200 + application/json status="error": raises
- Network error (non-Timeout): raises
- requests.Timeout: re-raised
"""

import os

import pytest
import requests
import requests.cookies as req_cookies
import requests_mock as req_mock

from pykoa.koa.core import Archive

DOWNLOAD_URL = "https://koa.ipac.caltech.edu/cgi-bin/getKOA/nph-getKOA?filehand=/fits/HI.20200101.00001.fits"

FITS_BYTES = b"SIMPLE  =                    T / file conforms to FITS standard"

ERROR_VOTABLE = (
    b'<?xml version="1.0" encoding="UTF-8"?>'
    b'<VOTABLE version="1.3" xmlns="http://www.ivoa.net/xml/VOTable/v1.3">'
    b'<RESOURCE type="results">'
    b'<INFO name="QUERY_STATUS" value="ERROR">Table \'koa_bad\' not found</INFO>'
    b"</RESOURCE></VOTABLE>"
)


@pytest.fixture
def archive():
    return Archive()


@pytest.fixture
def outfile(tmp_path):
    return str(tmp_path / "download.fits")


def submit(archive, url, outfile, cookiejar=None):
    """Call the private __submit_request via name-mangled accessor."""
    archive._Archive__submit_request(url, outfile, cookiejar)


# ---------------------------------------------------------------------------
# 200 success — file written
# ---------------------------------------------------------------------------


def test_submit_request_200_writes_file(archive, outfile):
    """HTTP 200 response is streamed to the filepath."""
    with req_mock.Mocker() as m:
        m.get(DOWNLOAD_URL, content=FITS_BYTES, status_code=200)
        submit(archive, DOWNLOAD_URL, outfile)

    assert os.path.exists(outfile)
    with open(outfile, "rb") as f:
        assert f.read() == FITS_BYTES


def test_submit_request_200_with_cookiejar(archive, outfile):
    """HTTP 200 with a cookiejar passes cookies to requests.get."""
    jar = req_cookies.RequestsCookieJar()
    jar.set("koasession", "abc123", domain="koa.ipac.caltech.edu", path="/")
    with req_mock.Mocker() as m:
        m.get(DOWNLOAD_URL, content=FITS_BYTES, status_code=200)
        submit(archive, DOWNLOAD_URL, outfile, cookiejar=jar)

    assert os.path.exists(outfile)
    assert "koasession=abc123" in m.last_request.headers.get("Cookie", "")


# ---------------------------------------------------------------------------
# 403 — Access denied
# ---------------------------------------------------------------------------


def test_submit_request_403_no_body_raises(archive, outfile):
    """HTTP 403 with no parseable body raises with generic access-denied message."""
    with req_mock.Mocker() as m:
        m.get(DOWNLOAD_URL, status_code=403, text="Forbidden")
        with pytest.raises(Exception, match="(?i)access denied"):
            submit(archive, DOWNLOAD_URL, outfile)

    assert not os.path.exists(outfile)


def test_submit_request_403_votable_body_returns_specific_error(archive, outfile):
    """HTTP 403 with a VOTable body includes the specific server error in the message."""
    with req_mock.Mocker() as m:
        m.get(
            DOWNLOAD_URL,
            status_code=403,
            content=ERROR_VOTABLE,
            headers={"Content-Type": "text/xml"},
        )
        with pytest.raises(Exception) as exc_info:
            submit(archive, DOWNLOAD_URL, outfile)

    assert "koa_bad" in str(exc_info.value), (
        "VOTable error text must appear in the exception"
    )
    assert "access denied" in str(exc_info.value).lower()
    assert not os.path.exists(outfile)


# ---------------------------------------------------------------------------
# 400 — Bad request
# ---------------------------------------------------------------------------


def test_submit_request_400_no_body_raises(archive, outfile):
    """HTTP 400 with no parseable body raises with generic bad-request message."""
    with req_mock.Mocker() as m:
        m.get(DOWNLOAD_URL, status_code=400, text="Bad Request")
        with pytest.raises(Exception, match="(?i)bad request"):
            submit(archive, DOWNLOAD_URL, outfile)

    assert not os.path.exists(outfile)


def test_submit_request_400_votable_body_returns_specific_error(archive, outfile):
    """HTTP 400 with a VOTable body includes the specific server error in the message."""
    with req_mock.Mocker() as m:
        m.get(
            DOWNLOAD_URL,
            status_code=400,
            content=ERROR_VOTABLE,
            headers={"Content-Type": "text/xml"},
        )
        with pytest.raises(Exception) as exc_info:
            submit(archive, DOWNLOAD_URL, outfile)

    assert "koa_bad" in str(exc_info.value), (
        "VOTable error text must appear in the exception"
    )
    assert "bad request" in str(exc_info.value).lower()
    assert not os.path.exists(outfile)


# ---------------------------------------------------------------------------
# 500 — generic HTTP error
# ---------------------------------------------------------------------------


def test_submit_request_500_raises_with_status_code(archive, outfile):
    """HTTP 500 raises with the status code in the message."""
    with req_mock.Mocker() as m:
        m.get(DOWNLOAD_URL, status_code=500, text="Internal Server Error")
        with pytest.raises(Exception, match="HTTP status 500"):
            submit(archive, DOWNLOAD_URL, outfile)

    assert not os.path.exists(outfile)


# ---------------------------------------------------------------------------
# 200 + application/json
# ---------------------------------------------------------------------------


def test_submit_request_200_json_error_raises(archive, outfile):
    """HTTP 200 with JSON body containing status='error' raises Exception."""
    with req_mock.Mocker() as m:
        m.get(
            DOWNLOAD_URL,
            status_code=200,
            json={"status": "error", "msg": "Quota exceeded"},
            headers={"Content-Type": "application/json"},
        )
        with pytest.raises(Exception, match="(?i)quota exceeded"):
            submit(archive, DOWNLOAD_URL, outfile)

    assert not os.path.exists(outfile)


def test_submit_request_200_json_with_error_field_raises(archive, outfile):
    """HTTP 200 with JSON body containing 'error' key raises Exception."""
    with req_mock.Mocker() as m:
        m.get(
            DOWNLOAD_URL,
            status_code=200,
            json={"error": "File not found on archive"},
            headers={"Content-Type": "application/json"},
        )
        with pytest.raises(Exception, match="(?i)file not found"):
            submit(archive, DOWNLOAD_URL, outfile)

    assert not os.path.exists(outfile)


# ---------------------------------------------------------------------------
# Timeout and network errors
# ---------------------------------------------------------------------------


def test_submit_request_timeout_reraises(archive, outfile):
    """requests.Timeout propagates out of __submit_request."""
    with req_mock.Mocker() as m:
        m.get(DOWNLOAD_URL, exc=requests.Timeout)
        with pytest.raises(requests.Timeout):
            submit(archive, DOWNLOAD_URL, outfile)


def test_submit_request_network_error_raises(archive, outfile):
    """Non-Timeout network errors are wrapped and re-raised."""
    with req_mock.Mocker() as m:
        m.get(DOWNLOAD_URL, exc=requests.ConnectionError("connection refused"))
        with pytest.raises(Exception, match="(?i)failed to submit"):
            submit(archive, DOWNLOAD_URL, outfile)
