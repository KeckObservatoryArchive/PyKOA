"""Tests for KoaJob.get_phase and KoaJob.get_result.

Covers:
- get_phase: COMPLETED/ERROR phases skip re-poll; EXECUTING triggers poll;
  timeout and network errors propagate
- get_result: empty outpath guard; file download on COMPLETED job;
  timeout propagation during download
"""

import os
from unittest import mock

import pytest
import requests
import requests_mock as req_mock

from pykoa.koa.core import KoaJob

STATUS_URL = "https://koa.example.test/TAP/async/123"
RESULT_URL = "https://koa.example.test/TAP/async/123/results/result"
RESULT_BYTES = b"koaid\nHI.20200101.12345.fits\n"

UWS_COMPLETED_XML = f"""<?xml version="1.0" encoding="UTF-8"?>
<uws:job xmlns:uws="http://www.ivoa.net/xml/UWS/v1.0"
         xmlns:xlink="http://www.w3.org/1999/xlink">
  <uws:jobId>123</uws:jobId>
  <uws:phase>COMPLETED</uws:phase>
  <uws:results>
    <uws:result id="result" xlink:href="{RESULT_URL}"/>
  </uws:results>
  <uws:parameters/>
</uws:job>"""


UWS_ERROR_XML = """<?xml version="1.0" encoding="UTF-8"?>
<uws:job xmlns:uws="http://www.ivoa.net/xml/UWS/v1.0"
         xmlns:xlink="http://www.w3.org/1999/xlink">
  <uws:jobId>123</uws:jobId>
  <uws:phase>ERROR</uws:phase>
  <uws:errorSummary type="transient">
    <uws:message>Query execution failed</uws:message>
  </uws:errorSummary>
  <uws:results/>
  <uws:parameters/>
</uws:job>"""


@pytest.fixture
def completed_job():
    """KoaJob pre-initialized in COMPLETED phase.

    resulturl and phase are plain string attributes — they persist after the
    Mocker context exits, so tests can freely re-mock only the download URL.
    """
    with req_mock.Mocker() as m:
        m.get(STATUS_URL, text=UWS_COMPLETED_XML)
        job = KoaJob(STATUS_URL)
    return job


@pytest.fixture
def error_job():
    """KoaJob pre-initialized in ERROR phase."""
    with req_mock.Mocker() as m:
        m.get(STATUS_URL, text=UWS_ERROR_XML)
        job = KoaJob(STATUS_URL)
    return job


# ---------------------------------------------------------------------------
# get_phase — cached phases skip re-poll
# ---------------------------------------------------------------------------


def test_get_phase_completed_returns_without_network(completed_job):
    """get_phase returns COMPLETED immediately without a network call."""
    with mock.patch.object(completed_job, "_KoaJob__get_statusjob") as mock_poll:
        phase = completed_job.get_phase()
    assert phase == "COMPLETED"
    mock_poll.assert_not_called()


def test_get_phase_error_returns_without_network(error_job):
    """get_phase returns ERROR immediately without a network call."""
    with mock.patch.object(error_job, "_KoaJob__get_statusjob") as mock_poll:
        phase = error_job.get_phase()
    assert phase == "ERROR"
    mock_poll.assert_not_called()


# ---------------------------------------------------------------------------
# get_phase — poll when EXECUTING
# ---------------------------------------------------------------------------


def test_get_phase_executing_polls_and_returns_completed():
    """get_phase polls __get_statusjob when phase is EXECUTING."""
    with req_mock.Mocker() as m:
        m.get(STATUS_URL, text=UWS_COMPLETED_XML)
        job = KoaJob(STATUS_URL)

    job.phase = "EXECUTING"
    with req_mock.Mocker() as m:
        m.get(STATUS_URL, text=UWS_COMPLETED_XML)
        phase = job.get_phase()

    assert phase == "COMPLETED"
    assert m.call_count == 1


def test_get_phase_timeout_propagates():
    """requests.Timeout during get_phase poll propagates out."""
    with req_mock.Mocker() as m:
        m.get(STATUS_URL, text=UWS_COMPLETED_XML)
        job = KoaJob(STATUS_URL)

    job.phase = "EXECUTING"
    with req_mock.Mocker() as m:
        m.get(STATUS_URL, exc=requests.Timeout)
        with pytest.raises(requests.Timeout):
            job.get_phase()


def test_get_phase_network_error_raises():
    """A network error during get_phase poll raises Exception."""
    with req_mock.Mocker() as m:
        m.get(STATUS_URL, text=UWS_COMPLETED_XML)
        job = KoaJob(STATUS_URL)

    job.phase = "EXECUTING"
    with req_mock.Mocker() as m:
        m.get(STATUS_URL, exc=ConnectionError("refused"))
        with pytest.raises(Exception, match=r"refused"):
            job.get_phase()


# ---------------------------------------------------------------------------
# get_result — empty outpath guard
# ---------------------------------------------------------------------------


def test_get_result_empty_outpath_sets_error_no_raise(completed_job):
    """get_result("") sets status=error and returns without raising."""
    completed_job.get_result("")
    assert completed_job.status == "error"
    assert "required" in completed_job.msg.lower()


# ---------------------------------------------------------------------------
# get_result — downloads file on COMPLETED job
# ---------------------------------------------------------------------------


def test_get_result_writes_file(completed_job, tmp_path):
    """get_result downloads resulturl content to the specified file."""
    outpath = str(tmp_path / "result.tbl")
    with req_mock.Mocker() as m:
        m.get(RESULT_URL, content=RESULT_BYTES)
        completed_job.get_result(outpath)

    assert os.path.exists(outpath)
    with open(outpath, "rb") as f:
        assert f.read() == RESULT_BYTES
    assert completed_job.status == "ok"
    assert outpath in completed_job.msg


# ---------------------------------------------------------------------------
# get_result — timeout during download
# ---------------------------------------------------------------------------


def test_get_result_download_timeout_propagates(completed_job, tmp_path):
    """requests.Timeout fetching the result file propagates out of get_result."""
    outpath = str(tmp_path / "result.tbl")
    with req_mock.Mocker() as m:
        m.get(RESULT_URL, exc=requests.Timeout)
        with pytest.raises(requests.Timeout):
            completed_job.get_result(outpath)
