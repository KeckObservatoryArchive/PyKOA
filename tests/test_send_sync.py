"""Tests for KoaTap.send_sync against a fake TAP server at HTTPAdapter.send."""

import io
import os
import tempfile
from unittest import mock
from urllib.parse import parse_qs

import pytest
import requests
from astropy.table import Table
from requests.adapters import HTTPAdapter
from requests.structures import CaseInsensitiveDict

from pykoa.koa.core import KoaTap

TAP_URL = "https://koa.example.test/TAP"
QUERY = "select top 1 koaid from koa_hires"
KOAID = "HI.20200101.12345.fits"

TABLE_ERR = (
    "Table 'nosuchtable' is not available for querying. "
    "Use TAP_SCHEMA.tables to see available tables."
)
COLUMN_ERR = (
    "Input query [select nosuchcolumn from koa_hires] syntax error: "
    "ORA-00904: 'NOSUCHCOLUMN': invalid identifier"
)
# Non-ASCII error text; verifies that extract_xmlerr decodes via the XML
# declaration rather than letting requests guess ISO-8859-1 for text/xml.
UNICODE_ERR = "Table '\u0152uvre_\xe9' is not available for querying."


def votable_bytes():
    buf = io.BytesIO()
    Table({"koaid": [KOAID]}).write(buf, format="votable")
    return buf.getvalue()


def ipac_bytes():
    fd, fpath = tempfile.mkstemp(suffix=".tbl")
    os.close(fd)
    Table({"koaid": [KOAID], "instrume": ["HIRES"]}).write(
        fpath, format="ipac", overwrite=True
    )
    with open(fpath, "rb") as fh:
        data = fh.read()
    os.unlink(fpath)
    return data


def csv_bytes():
    fd, fpath = tempfile.mkstemp(suffix=".csv")
    os.close(fd)
    Table({"koaid": [KOAID], "instrume": ["HIRES"]}).write(
        fpath, format="ascii.csv", overwrite=True
    )
    with open(fpath, "rb") as fh:
        data = fh.read()
    os.unlink(fpath)
    return data


def tsv_bytes():
    fd, fpath = tempfile.mkstemp(suffix=".tsv")
    os.close(fd)
    Table({"koaid": [KOAID], "instrume": ["HIRES"]}).write(
        fpath, format="ascii.tab", overwrite=True
    )
    with open(fpath, "rb") as fh:
        data = fh.read()
    os.unlink(fpath)
    return data


def error_votable(msg):
    # Shape of nexsciTAP's error response for format=votable.
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<VOTABLE version="1.4" xmlns="http://www.ivoa.net/xml/VOTable/v1.3">\n'
        '<RESOURCE type="results">\n'
        '<INFO name="QUERY_STATUS" value="ERROR">\n'
        f"{msg}\n"
        "</INFO>\n"
        "</RESOURCE>\n"
        "</VOTABLE>\n"
    ).encode("utf-8")


# Modelled on KOA's OVERFLOW response (HTTP 200, no rows).
EMPTY_VOTABLE = (
    b'<?xml version="1.0" encoding="utf-8"?>\n'
    b'<VOTABLE version="1.3" xmlns="http://www.ivoa.net/xml/VOTable/v1.3">\n'
    b'  <RESOURCE type="results">\n'
    b'  <INFO name="QUERY_STATUS" value="OVERFLOW"/>\n'
    b"  <TABLE>\n"
    b'    <FIELD ID="koaid" arraysize="*" datatype="char" name="koaid"/>\n'
    b"  </TABLE>\n"
    b"  </RESOURCE>\n"
    b"</VOTABLE>\n"
)


@pytest.fixture
def tap_server(request):
    """Fake TAP server; returns the list of requests it received.

    Parametrize indirectly with (status, content_type, body); the default is
    a 200 VOTable with one row.
    """
    status, content_type, body = getattr(
        request, "param", (200, "text/xml", votable_bytes())
    )
    received = []

    def send(adapter, prepared, **kwargs):
        received.append(prepared)
        resp = requests.Response()
        resp.status_code = status
        resp.headers = CaseInsensitiveDict({"Content-type": content_type})
        # Mirror what HTTPAdapter.build_response does so charset-related decoding
        # tests are meaningful.
        resp.encoding = requests.utils.get_encoding_from_headers(resp.headers)
        resp.raw = io.BytesIO(body)
        resp.url = prepared.url
        resp.request = prepared
        return resp

    with mock.patch.object(HTTPAdapter, "send", autospec=True, side_effect=send):
        yield received


@pytest.fixture
def cookie_file(tmp_path):
    path = tmp_path / "cookies.txt"
    path.write_text(
        "# Netscape HTTP Cookie File\n"
        "koa.example.test\tFALSE\t/\tFALSE\t2147483647\tKOA_SESSION\tabc123\n"
    )
    return str(path)


@pytest.fixture
def workdir(tmp_path, monkeypatch):
    # Isolate the cwd: save_data writes temp files into './', so any leak
    # appears in os.listdir(workdir). Update this fixture if save_data moves
    # its temp files to the system temp directory.
    path = tmp_path / "work"
    path.mkdir()
    monkeypatch.chdir(path)
    return path


def make_tap(with_cookie, cookie_file):
    return KoaTap(TAP_URL, cookiefile=cookie_file) if with_cookie else KoaTap(TAP_URL)


@pytest.mark.parametrize("with_cookie", [False, True], ids=["no-cookie", "cookie"])
def test_send_sync_writes_result_to_outpath(
    tmp_path, workdir, tap_server, cookie_file, with_cookie
):
    outpath = str(tmp_path / "result.xml")
    tap = make_tap(with_cookie, cookie_file)

    msg = tap.send_sync(QUERY, format="votable", outpath=outpath)

    assert msg == f"Result downloaded to file [{outpath}]"
    with open(outpath, "rb") as fp:
        assert fp.read() == votable_bytes()
    assert os.listdir(workdir) == []

    [sent] = tap_server
    assert sent.method == "POST"
    assert sent.url == TAP_URL + "/sync"
    form = parse_qs(sent.body)
    assert form["query"] == [QUERY]
    assert form["format"] == ["votable"]
    if with_cookie:
        assert sent.headers["Cookie"] == "KOA_SESSION=abc123"
    else:
        assert "Cookie" not in sent.headers


@pytest.mark.parametrize("with_cookie", [False, True], ids=["no-cookie", "cookie"])
def test_send_sync_without_outpath_returns_table_and_removes_temp_file(
    workdir, tap_server, cookie_file, with_cookie
):
    tap = make_tap(with_cookie, cookie_file)

    msg = tap.send_sync(QUERY, format="votable")

    assert msg == "Result saved in memory (astropy table)."
    assert list(tap.astropytbl["koaid"]) == [KOAID]
    assert os.listdir(workdir) == []


@pytest.mark.parametrize(
    "fmt,body_fn",
    [
        ("votable", votable_bytes),
        ("ipac", ipac_bytes),
        ("csv", csv_bytes),
        ("tsv", tsv_bytes),
    ],
    ids=["votable", "ipac", "csv", "tsv"],
)
def test_send_sync_without_outpath_respects_format(workdir, fmt, body_fn):
    """save_data must translate TAP format names to astropy reader names.

    Previously save_data hard-coded format='votable', so non-votable formats
    raised IORegistryError (tsv) or ValueError (ipac/csv) and left a temp file
    in the cwd. Covers all four formats advertised by Archive query methods.
    """
    body = body_fn()

    def send(adapter, prepared, **kwargs):
        resp = requests.Response()
        resp.status_code = 200
        resp.headers = CaseInsensitiveDict({"Content-type": "text/xml"})
        resp.encoding = requests.utils.get_encoding_from_headers(resp.headers)
        resp.raw = io.BytesIO(body)
        resp.url, resp.request = prepared.url, prepared
        return resp

    with mock.patch.object(HTTPAdapter, "send", autospec=True, side_effect=send):
        tap = KoaTap(TAP_URL)
        msg = tap.send_sync(QUERY, format=fmt)

    assert msg == "Result saved in memory (astropy table)."
    assert list(tap.astropytbl["koaid"]) == [KOAID]
    assert os.listdir(workdir) == []


@pytest.mark.parametrize(
    "tap_server", [(200, "text/xml", EMPTY_VOTABLE)], indirect=True
)
def test_send_sync_empty_result_is_not_an_error(workdir, tap_server):
    """HTTP 200 + QUERY_STATUS=OVERFLOW (valid empty result) must not trigger the non-200 guard.

    nexsciTAP returns OVERFLOW — not an error — when the result set is valid
    but empty. The guard keys on status code only, so a 200 OVERFLOW stays
    on the success path and produces a 0-row astropytbl.
    """
    tap = KoaTap(TAP_URL)

    msg = tap.send_sync(QUERY, format="votable")

    assert msg == "Result saved in memory (astropy table)."
    assert len(tap.astropytbl) == 0
    assert tap.status != "error"
    assert os.listdir(workdir) == []


ERROR_CASES = [
    pytest.param(
        (403, "text/xml", error_votable(TABLE_ERR)), TABLE_ERR, id="403-votable"
    ),
    pytest.param(
        (400, "text/xml;charset=UTF-8", error_votable(COLUMN_ERR)),
        COLUMN_ERR,
        id="400-votable",
    ),
    # Non-ASCII in a text/xml body with no charset: guard must use .content so
    # the XML declaration governs encoding, not requests' ISO-8859-1 default.
    pytest.param(
        (403, "text/xml", error_votable(UNICODE_ERR)),
        UNICODE_ERR,
        id="403-votable-non-ascii",
    ),
    pytest.param(
        (400, "text/plain; charset=UTF-8", (COLUMN_ERR + "\n").encode()),
        COLUMN_ERR,
        id="400-plain",
    ),
    pytest.param(
        (500, "text/html", b"<html>Internal error</html>"),
        "Request failed with HTTP status 500.",
        id="500-html",
    ),
    # XML body that extract_xmlerr cannot parse: guard must fall back to the
    # generic message rather than raising.
    pytest.param(
        (403, "application/xml", b"<not-xml"),
        "Request failed with HTTP status 403.",
        id="403-unparseable-xml",
    ),
    # Empty text/plain body: guard must fall back cleanly.
    pytest.param(
        (400, "text/plain", b""),
        "Request failed with HTTP status 400.",
        id="400-empty-plain",
    ),
    # Uppercase content type: guard must match case-insensitively after .lower().
    pytest.param(
        (403, "Text/XML", error_votable(TABLE_ERR)), TABLE_ERR, id="403-uppercase-ctype"
    ),
]


def test_send_sync_error_message_survives_close_raising(workdir):
    """status/msg are set before close(); a close() exception must not hide them."""
    tap = KoaTap(TAP_URL)

    def send(adapter, prepared, **kwargs):
        resp = requests.Response()
        resp.status_code = 403
        resp.headers = CaseInsensitiveDict({"Content-type": "text/plain"})
        resp.encoding = "utf-8"
        resp.raw = io.BytesIO(b"Access denied")
        resp.url, resp.request = prepared.url, prepared
        return resp

    with mock.patch.object(HTTPAdapter, "send", autospec=True, side_effect=send):
        with mock.patch.object(
            requests.Response, "close", side_effect=RuntimeError("pool error")
        ):
            msg = tap.send_sync(QUERY, format="votable")

    assert msg == "Access denied"
    assert tap.status == "error"


@pytest.mark.parametrize("use_outpath", [True, False], ids=["outpath", "in-memory"])
@pytest.mark.parametrize("tap_server, expected", ERROR_CASES, indirect=["tap_server"])
def test_send_sync_error_returns_message_and_writes_nothing(
    tmp_path, workdir, tap_server, expected, use_outpath
):
    outpath = str(tmp_path / "result.xml")
    kwargs = {"outpath": outpath} if use_outpath else {}
    tap = KoaTap(TAP_URL)

    with mock.patch.object(requests.Response, "close", autospec=True) as close:
        msg = tap.send_sync(QUERY, format="votable", **kwargs)

    assert msg == expected
    assert tap.status == "error"
    if use_outpath:
        assert not os.path.exists(outpath)
    assert os.listdir(workdir) == []
    # Call check: confirms the response is released on every error path.
    close.assert_called_once_with(tap.response)
