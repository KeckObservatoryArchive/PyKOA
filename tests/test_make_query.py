"""Tests for Archive.__make_query.

Covers:
- Plain text response: returns response.text
- JSON content-type with parseable JSON: always raises Exception(jsondata["msg"])
- JSON content-type with unparseable JSON: raises "returned JSON object parse error"
- Non-Timeout network error: raises Exception("Error: ...")
- requests.Timeout: re-raised

Note: any JSON content-type response always raises — the method treats JSON as
an error envelope. Successful data comes back as plain text or XML.
"""

import pytest
import requests
import requests_mock as req_mock

SERVER = "https://koa.example.test/"
MAKEQUERY_URL = SERVER + "cgi-bin/KoaAPI/nph-makeQuery?instrument=hires"
ADQL = "SELECT koaid FROM koa_hires WHERE instrume='HIRES'"


def make_query(archive, url):
    """Call the private __make_query via name-mangling."""
    return archive._Archive__make_query(url)


# ---------------------------------------------------------------------------
# Happy path — plain text response
# ---------------------------------------------------------------------------


def test_make_query_returns_plain_text(archive):
    """Returns response.text when content-type is not application/json."""
    with req_mock.Mocker() as m:
        m.get(MAKEQUERY_URL, text=ADQL, headers={"content-type": "text/plain"})
        result = make_query(archive, MAKEQUERY_URL)
    assert result == ADQL


# ---------------------------------------------------------------------------
# JSON content-type — always raises
# ---------------------------------------------------------------------------


def test_make_query_json_error_raises_with_server_msg(archive):
    """JSON response raises Exception containing the server msg field."""
    with req_mock.Mocker() as m:
        m.get(
            MAKEQUERY_URL,
            json={"status": "error", "msg": "Instrument not supported"},
            headers={"content-type": "application/json"},
        )
        with pytest.raises(Exception, match="Instrument not supported"):
            make_query(archive, MAKEQUERY_URL)


def test_make_query_malformed_json_raises_parse_error(archive):
    """Malformed JSON body raises Exception with parse-error message."""
    with req_mock.Mocker() as m:
        m.get(
            MAKEQUERY_URL,
            text="not json at all",
            headers={"content-type": "application/json"},
        )
        with pytest.raises(Exception, match=r"(?i)json.*parse"):
            make_query(archive, MAKEQUERY_URL)


# ---------------------------------------------------------------------------
# Network errors
# ---------------------------------------------------------------------------


def test_make_query_timeout_propagates(archive):
    """requests.Timeout propagates out of __make_query unchanged."""
    with req_mock.Mocker() as m:
        m.get(MAKEQUERY_URL, exc=requests.Timeout)
        with pytest.raises(requests.Timeout):
            make_query(archive, MAKEQUERY_URL)


def test_make_query_network_error_raises(archive):
    """Non-Timeout network errors are wrapped and raised as Exception."""
    with req_mock.Mocker() as m:
        m.get(MAKEQUERY_URL, exc=ConnectionError("connection refused"))
        with pytest.raises(Exception, match=r"(?i)^error:"):
            make_query(archive, MAKEQUERY_URL)
