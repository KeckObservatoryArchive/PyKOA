"""Tests for request timeout configuration on Archive and KoaTap.

Covers:
- Default timeout value
- User-configurable timeout at instantiation (scalar, tuple, None)
- Timeout passed through to every requests.get/post call
- Timeout exception propagates correctly
- KoaJob timeout configuration (via requests_mock for full UWS flow)
"""

from unittest import mock

import pytest
import requests
import requests_mock as req_mock

from pykoa.koa.core import Archive, KoaTap

TAP_URL = "https://koa.example.test/TAP"


# ---------------------------------------------------------------------------
# Archive timeout configuration
# ---------------------------------------------------------------------------


def test_archive_default_timeout():
    """Archive default timeout is (30, 300) -- connect 30s, read 300s."""
    a = Archive()
    assert a.timeout == (30, 300)


def test_archive_timeout_scalar():
    """Archive accepts a scalar timeout."""
    a = Archive(timeout=60)
    assert a.timeout == 60


def test_archive_timeout_tuple():
    """Archive accepts a (connect, read) tuple."""
    a = Archive(timeout=(10, 600))
    assert a.timeout == (10, 600)


def test_archive_timeout_none():
    """Archive accepts timeout=None (user explicitly opts out)."""
    a = Archive(timeout=None)
    assert a.timeout is None


def test_archive_timeout_does_not_affect_other_instances():
    """Setting timeout on one instance does not affect another."""
    a1 = Archive(timeout=60)
    a2 = Archive()
    assert a1.timeout == 60
    assert a2.timeout == (30, 300)


# ---------------------------------------------------------------------------
# KoaTap timeout configuration
# ---------------------------------------------------------------------------


def test_koatap_default_timeout():
    """KoaTap default timeout is (30, 300)."""
    tap = KoaTap(TAP_URL)
    assert tap.timeout == (30, 300)


def test_koatap_timeout_scalar():
    """KoaTap accepts a scalar timeout."""
    tap = KoaTap(TAP_URL, timeout=60)
    assert tap.timeout == 60


def test_koatap_timeout_tuple():
    """KoaTap accepts a (connect, read) tuple."""
    tap = KoaTap(TAP_URL, timeout=(10, 600))
    assert tap.timeout == (10, 600)


def test_koatap_timeout_none():
    """KoaTap accepts timeout=None."""
    tap = KoaTap(TAP_URL, timeout=None)
    assert tap.timeout is None


# ---------------------------------------------------------------------------
# Timeout passed to requests calls
# ---------------------------------------------------------------------------


def test_koatap_send_sync_passes_timeout():
    """KoaTap.send_sync passes self.timeout to requests.post."""
    tap = KoaTap(TAP_URL, timeout=(5, 10))
    captured = {}

    def fake_post(url, **kwargs):
        captured["timeout"] = kwargs.get("timeout")
        resp = requests.models.Response()
        resp.status_code = 200
        resp.headers = requests.structures.CaseInsensitiveDict(
            {"Content-Type": "text/plain"}
        )
        resp._content = b"result"
        return resp

    with mock.patch("requests.post", side_effect=fake_post):
        try:
            tap.send_sync(
                "SELECT TOP 1 koaid FROM koa_hires",
                outpath="/tmp/out.tbl",
                format="ipac",
            )
        except Exception:
            pass

    assert captured, "requests.post was never called in send_sync -- mock did not fire"
    assert captured.get("timeout") == (5, 10), (
        "send_sync must pass self.timeout to requests.post"
    )


def test_archive_login_passes_timeout():
    """Archive.login passes self.timeout to session.get."""
    a = Archive(timeout=(5, 10))
    captured = {}

    def fake_get(url, **kwargs):
        captured["timeout"] = kwargs.get("timeout")
        resp = requests.models.Response()
        resp.status_code = 200
        resp._content = b'{"status": "ok", "msg": ""}'
        resp.headers = requests.structures.CaseInsensitiveDict(
            {"Content-type": "application/json"}
        )
        return resp

    with mock.patch("requests.Session.get", side_effect=fake_get):
        try:
            a.login(cookiepath="/tmp/test.txt", userid="test", password="test")
        except Exception:
            pass

    assert captured, "session.get was never called in login() -- mock did not fire"
    assert captured.get("timeout") == (5, 10), (
        "session.get in login() must pass self.timeout"
    )


# ---------------------------------------------------------------------------
# Timeout exception propagation
# ---------------------------------------------------------------------------


def test_koatap_send_sync_timeout_raises():
    """A requests.Timeout from the server propagates out of send_sync."""
    tap = KoaTap(TAP_URL, timeout=0.001)

    with mock.patch("requests.post", side_effect=requests.Timeout("timed out")):
        with pytest.raises(requests.Timeout):
            tap.send_sync(
                "SELECT TOP 1 koaid FROM koa_hires",
                outpath="/tmp/out.tbl",
                format="ipac",
            )


# ---------------------------------------------------------------------------
# KoaJob timeout configuration — tested via requests_mock for full UWS flow
# ---------------------------------------------------------------------------

# Minimal valid UWS job status XML that satisfies __get_statusjob parsing.
# Uses the real UWS 1.0 namespace so BeautifulSoup + xmltodict both parse it.
KOAJOB_STATUS_URL = "https://koa.example.test/TAP/async/123"
KOAJOB_RESULT_URL = "https://koa.example.test/TAP/async/123/results/result"

UWS_COMPLETED_XML = """<?xml version="1.0" encoding="UTF-8"?>
<uws:job xmlns:uws="http://www.ivoa.net/xml/UWS/v1.0"
         xmlns:xlink="http://www.w3.org/1999/xlink">
  <uws:jobId>123</uws:jobId>
  <uws:phase>COMPLETED</uws:phase>
  <uws:results>
    <uws:result id="result" xlink:href="{result_url}"/>
  </uws:results>
</uws:job>""".format(result_url=KOAJOB_RESULT_URL)


def test_koajob_default_timeout():
    """KoaJob default timeout is (30, 300) when no timeout kwarg is passed."""
    from pykoa.koa.core import KoaJob

    with req_mock.Mocker() as m:
        m.get(KOAJOB_STATUS_URL, text=UWS_COMPLETED_XML)
        job = KoaJob(KOAJOB_STATUS_URL)

    assert job.timeout == (30, 300)


def test_koajob_timeout_kwarg():
    """KoaJob stores a custom timeout kwarg correctly."""
    from pykoa.koa.core import KoaJob

    with req_mock.Mocker() as m:
        m.get(KOAJOB_STATUS_URL, text=UWS_COMPLETED_XML)
        job = KoaJob(KOAJOB_STATUS_URL, timeout=(10, 120))

    assert job.timeout == (10, 120)


def test_koajob_timeout_none():
    """KoaJob accepts timeout=None."""
    from pykoa.koa.core import KoaJob

    with req_mock.Mocker() as m:
        m.get(KOAJOB_STATUS_URL, text=UWS_COMPLETED_XML)
        job = KoaJob(KOAJOB_STATUS_URL, timeout=None)

    assert job.timeout is None


def test_koajob_status_request_passes_timeout():
    """KoaJob.__get_statusjob passes self.timeout to requests.get."""
    from pykoa.koa.core import KoaJob

    with req_mock.Mocker() as m:
        m.get(KOAJOB_STATUS_URL, text=UWS_COMPLETED_XML)
        job = KoaJob(KOAJOB_STATUS_URL, timeout=(5, 15))

    assert job.timeout == (5, 15)
    assert m.last_request.timeout == (5, 15), (
        "__get_statusjob must pass self.timeout to requests.get"
    )


def test_koatap_send_async_passes_timeout_to_koajob():
    """KoaTap.send_async passes self.timeout to KoaJob via the 303 redirect path.

    send_async POSTs to the async URL, receives a 303 with a Location header
    pointing to the status URL, then instantiates KoaJob(statusurl, timeout=self.timeout).
    This test verifies the timeout flows through that full path.
    """
    from pykoa.koa.core import KoaJob

    tap = KoaTap(TAP_URL, timeout=(10, 120))
    async_url = TAP_URL + "/async"
    captured = {}
    original_init = KoaJob.__init__

    def intercepting_init(self, statusurl, **kwargs):
        captured["timeout"] = kwargs.get("timeout")
        original_init(self, statusurl, **kwargs)

    with req_mock.Mocker() as m:
        # send_async POSTs to /async and expects a 303 redirect to the status URL
        m.post(async_url, status_code=303, headers={"Location": KOAJOB_STATUS_URL})
        # KoaJob.__init__ GETs the status URL
        m.get(KOAJOB_STATUS_URL, text=UWS_COMPLETED_XML)
        # send_async fetches the result after COMPLETED status
        m.get(KOAJOB_RESULT_URL, content=b"koaid\nHI.20200101.12345.fits\n")

        with mock.patch.object(KoaJob, "__init__", intercepting_init):
            tap.send_async("SELECT TOP 1 koaid FROM koa_hires", outpath="/tmp/out.tbl")

    assert captured, "KoaJob.__init__ was never called -- interceptor did not fire"
    assert captured.get("timeout") == (10, 120), (
        "KoaTap.send_async must pass self.timeout to KoaJob"
    )


# ---------------------------------------------------------------------------
# objLookup timeout configuration — tested via requests_mock (live call in __init__)
# ---------------------------------------------------------------------------

LOOKUP_URL = "https://koa.ipac.caltech.edu/cgi-bin/Lookup/nph-lookup?location=Vega"
LOOKUP_OK_JSON = '{"stat": "OK", "source": "SIMBAD", "objname": "Vega", "objtype": "Star", "parsename": "Vega", "objdesc": "", "ra2000": "279.23473", "dec2000": "38.78369", "cra2000": "18:36:56.34", "cdec2000": "+38:47:01.3"}'


def test_objlookup_default_timeout():
    """objLookup default timeout is (30, 300)."""
    from pykoa.koa.core import objLookup

    with req_mock.Mocker() as m:
        m.get(
            LOOKUP_URL,
            text=LOOKUP_OK_JSON,
            headers={"Content-type": "application/json"},
        )
        obj = objLookup("Vega")

    assert obj.timeout == (30, 300)


def test_objlookup_timeout_kwarg():
    """objLookup accepts a custom timeout kwarg."""
    from pykoa.koa.core import objLookup

    with req_mock.Mocker() as m:
        m.get(
            LOOKUP_URL,
            text=LOOKUP_OK_JSON,
            headers={"Content-type": "application/json"},
        )
        obj = objLookup("Vega", timeout=(5, 60))

    assert obj.timeout == (5, 60)


def test_objlookup_timeout_none():
    """objLookup accepts timeout=None."""
    from pykoa.koa.core import objLookup

    with req_mock.Mocker() as m:
        m.get(
            LOOKUP_URL,
            text=LOOKUP_OK_JSON,
            headers={"Content-type": "application/json"},
        )
        obj = objLookup("Vega", timeout=None)

    assert obj.timeout is None


def test_objlookup_passes_timeout_to_requests():
    """objLookup.__init__ passes self.timeout to requests.get."""
    from pykoa.koa.core import objLookup

    with req_mock.Mocker() as m:
        m.get(
            LOOKUP_URL,
            text=LOOKUP_OK_JSON,
            headers={"Content-type": "application/json"},
        )
        objLookup("Vega", timeout=(7, 77))

    assert m.last_request.timeout == (7, 77), (
        "objLookup.__init__ must pass self.timeout to requests.get"
    )


def test_objlookup_timeout_raises():
    """A requests.Timeout from the lookup server propagates out of objLookup.__init__."""
    from pykoa.koa.core import objLookup

    with req_mock.Mocker() as m:
        m.get(LOOKUP_URL, exc=requests.Timeout("timed out"))
        with pytest.raises(requests.Timeout):
            objLookup("Vega")


def test_objlookup_lookupurl_uses_conf_server():
    """objLookup.lookupurl is built from conf.server, not a hardcoded URL."""
    from pykoa.koa import conf
    from pykoa.koa.core import objLookup

    with req_mock.Mocker() as m:
        m.get(
            req_mock.ANY,
            text=LOOKUP_OK_JSON,
            headers={"Content-type": "application/json"},
        )
        obj = objLookup("Vega")

    assert obj.lookupurl == conf.server + "cgi-bin/Lookup/nph-lookup?", (
        "objLookup.lookupurl must be built from conf.server"
    )


def test_objlookup_lookupurl_not_exoplanet():
    """objLookup must not use the ExoPlanet Archive URL for name resolution.

    Regression test: the original code hardcoded exoplanetarchive.ipac.caltech.edu.
    KOA has its own resolver at conf.server that should be used instead.
    """
    from pykoa.koa.core import objLookup

    with req_mock.Mocker() as m:
        m.get(
            req_mock.ANY,
            text=LOOKUP_OK_JSON,
            headers={"Content-type": "application/json"},
        )
        obj = objLookup("Vega")

    assert "exoplanetarchive" not in obj.lookupurl, (
        "objLookup must use KOA resolver, not ExoPlanet Archive"
    )


# ---------------------------------------------------------------------------
# Additional Timeout propagation tests
# ---------------------------------------------------------------------------


def test_koatap_send_async_timeout_raises():
    """A requests.Timeout on the async POST propagates out of send_async."""
    tap = KoaTap(TAP_URL, timeout=0.001)

    with mock.patch("requests.post", side_effect=requests.Timeout("timed out")):
        with pytest.raises(requests.Timeout):
            tap.send_async("SELECT TOP 1 koaid FROM koa_hires", outpath="/tmp/out.tbl")


def test_koajob_statusjob_timeout_raises():
    """A requests.Timeout on the status poll propagates out of KoaJob.__init__."""
    from pykoa.koa.core import KoaJob

    with req_mock.Mocker() as m:
        m.get(KOAJOB_STATUS_URL, exc=requests.Timeout("timed out"))
        with pytest.raises(requests.Timeout):
            KoaJob(KOAJOB_STATUS_URL)


def test_koajob_resultfile_timeout_raises():
    """A requests.Timeout fetching the result file propagates out of KoaJob.get_result."""
    from pykoa.koa.core import KoaJob

    with req_mock.Mocker() as m:
        m.get(KOAJOB_STATUS_URL, text=UWS_COMPLETED_XML)
        job = KoaJob(KOAJOB_STATUS_URL)

    with req_mock.Mocker() as m:
        m.get(KOAJOB_RESULT_URL, exc=requests.Timeout("timed out"))
        with pytest.raises(requests.Timeout):
            job.get_result("/tmp/out.tbl")


def test_koajob_get_status_timeout_raises():
    """A requests.Timeout during status polling propagates out of KoaJob.get_status."""
    from pykoa.koa.core import KoaJob

    with req_mock.Mocker() as m:
        m.get(KOAJOB_STATUS_URL, text=UWS_COMPLETED_XML)
        job = KoaJob(KOAJOB_STATUS_URL)

    # Force phase to non-completed so get_status calls __get_statusjob
    job.phase = "EXECUTING"
    with req_mock.Mocker() as m:
        m.get(KOAJOB_STATUS_URL, exc=requests.Timeout("timed out"))
        with pytest.raises(requests.Timeout):
            job.get_status()


def test_koajob_get_resulturl_timeout_raises():
    """A requests.Timeout during status polling propagates out of KoaJob.get_resulturl."""
    from pykoa.koa.core import KoaJob

    with req_mock.Mocker() as m:
        m.get(KOAJOB_STATUS_URL, text=UWS_COMPLETED_XML)
        job = KoaJob(KOAJOB_STATUS_URL)

    job.phase = "EXECUTING"
    with req_mock.Mocker() as m:
        m.get(KOAJOB_STATUS_URL, exc=requests.Timeout("timed out"))
        with pytest.raises(requests.Timeout):
            job.get_resulturl()


def test_koajob_get_phase_timeout_raises():
    """A requests.Timeout during status polling propagates out of KoaJob.get_phase."""
    from pykoa.koa.core import KoaJob

    with req_mock.Mocker() as m:
        m.get(KOAJOB_STATUS_URL, text=UWS_COMPLETED_XML)
        job = KoaJob(KOAJOB_STATUS_URL)

    job.phase = "EXECUTING"
    with req_mock.Mocker() as m:
        m.get(KOAJOB_STATUS_URL, exc=requests.Timeout("timed out"))
        with pytest.raises(requests.Timeout):
            job.get_phase()


# ---------------------------------------------------------------------------
# Archive threading timeout to KoaTap and objLookup
# ---------------------------------------------------------------------------

KOA_TAP_URL = "https://koa.ipac.caltech.edu/TAP"
KOAJOB_ASYNC_URL = KOA_TAP_URL + "/async"


def test_archive_query_criteria_passes_timeout_to_koatap():
    """Archive.query_criteria passes self.timeout to the KoaTap it creates internally."""
    a = Archive(timeout=(5, 10))
    captured = {}
    original_init = KoaTap.__init__

    def intercepting_init(self, url, **kwargs):
        captured["timeout"] = kwargs.get("timeout")
        original_init(self, url, **kwargs)

    # Mock __make_query so it doesn't make a real HTTP call before reaching KoaTap
    with mock.patch(
        "pykoa.koa.core.Archive._Archive__make_query",
        return_value="SELECT TOP 1 koaid FROM koa_hires",
    ):
        with mock.patch.object(KoaTap, "__init__", intercepting_init):
            try:
                a.query_criteria(
                    {"instrument": "hires"},
                    outpath="/tmp/out.tbl",
                    format="ipac",
                )
            except Exception:
                pass

    assert captured, "KoaTap.__init__ was never called -- interceptor did not fire"
    assert captured.get("timeout") == (5, 10), (
        "Archive.query_criteria must pass self.timeout to KoaTap"
    )


def test_archive_query_object_passes_timeout_to_objlookup():
    """Archive.query_object passes self.timeout to objLookup."""
    from pykoa.koa.core import objLookup

    a = Archive(timeout=(5, 10))
    captured = {}
    original_init = objLookup.__init__

    def intercepting_init(self, object, **kwargs):
        captured["timeout"] = kwargs.get("timeout")
        original_init(self, object, **kwargs)

    lookup_url = "https://koa.ipac.caltech.edu/cgi-bin/Lookup/nph-lookup?location=Vega"
    with req_mock.Mocker() as m:
        m.get(
            lookup_url,
            text=LOOKUP_OK_JSON,
            headers={"Content-type": "application/json"},
        )
        with mock.patch.object(objLookup, "__init__", intercepting_init):
            try:
                a.query_object(
                    "hires",
                    "Vega",
                    outpath="/tmp/out.tbl",
                    format="ipac",
                )
            except Exception:
                pass

    assert captured, "objLookup.__init__ was never called -- interceptor did not fire"
    assert captured.get("timeout") == (5, 10), (
        "Archive.query_object must pass self.timeout to objLookup"
    )


def test_archive_query_object_timeout_raises():
    """A requests.Timeout from objLookup propagates out of Archive.query_object."""

    a = Archive(timeout=(5, 10))
    lookup_url = "https://koa.ipac.caltech.edu/cgi-bin/Lookup/nph-lookup?location=Vega"

    with req_mock.Mocker() as m:
        m.get(lookup_url, exc=requests.Timeout("timed out"))
        with pytest.raises(requests.Timeout):
            a.query_object("hires", "Vega", outpath="/tmp/out.tbl")


def test_archive_query_criteria_timeout_raises():
    """A requests.Timeout during __make_query propagates out of Archive.query_criteria."""
    a = Archive(timeout=(5, 10))

    with mock.patch(
        "pykoa.koa.core.Archive._Archive__make_query",
        side_effect=requests.Timeout("timed out"),
    ):
        with pytest.raises(requests.Timeout):
            a.query_criteria(
                {"instrument": "hires"},
                outpath="/tmp/out.tbl",
                format="ipac",
            )
