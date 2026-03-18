"""
Tests for TAP error handling in query_criteria and query_adql.

Branch under test: fix/tap-error-status-handling

These tests verify that server error responses are raised as exceptions
instead of being silently swallowed. Run against both branches to see
the before/after difference:

    # On fix branch — all tests should pass
    pytest tests/test_error_handling.py -v

    # On main branch — tests marked REGRESSION will fail,
    # showing exactly what the fix changed
"""

import os
import pytest
from unittest.mock import patch, MagicMock

from pykoa.koa import Koa, conf


# ── Fixtures ────────────────────────────────────────────────────────────────

# Minimal nexsciTAP VOTable error body (DALI 1.1 §4.4 format)
VOTABLE_ERROR = """\
<?xml version="1.0" encoding="UTF-8"?>
<VOTABLE version="1.3" xmlns="http://www.ivoa.net/xml/VOTable/v1.3">
  <RESOURCE type="results">
    <INFO name="QUERY_STATUS" value="ERROR">ORA-00942: table or view does not exist</INFO>
  </RESOURCE>
</VOTABLE>"""

# Raw CGI stdout contamination from nexsciTAP configparam.py init failure
WEB_RESPONSE = "'WEB'"

# Long raw connection exception trace (requests library format)
CONNECTION_ERROR = (
    "HTTPConnectionPool(host='localhost', port=19999): Max retries exceeded "
    "with url: /TAP (Caused by NewConnectionError('<urllib3.connection."
    "HTTPConnection object>: Failed to establish a new connection: "
    "[Errno 111] Connection refused'))"
)


@pytest.fixture
def outfile(tmp_path):
    """Temporary output file path that does NOT exist yet."""
    return str(tmp_path / 'result.tbl')


@pytest.fixture
def existing_outfile(tmp_path):
    """Temporary output file path that DOES exist (simulates successful write)."""
    p = tmp_path / 'result.tbl'
    p.write_text('\\c IPAC table placeholder')
    return str(p)


# ── Helper ──────────────────────────────────────────────────────────────────

def run_query_adql(retstr, outfile):
    """Run query_adql with send_async mocked to return retstr."""
    with patch('pykoa.koa.core.KoaTap.send_async', return_value=retstr):
        Koa.query_adql(
            "select koaid from koa_nirc2 limit 1",
            outfile, overwrite=True
        )


def run_query_criteria(retstr, outfile):
    """Run query_criteria with send_async mocked to return retstr."""
    with patch('pykoa.koa.core.KoaTap.send_async', return_value=retstr):
        Koa.query_criteria(
            {'instrument': 'nirc2', 'datetime': '2021-01-01 00:00:00/2021-01-02 00:00:00'},
            outfile, overwrite=True
        )


# ── Bug 2: QUERY_STATUS=ERROR VOTable silently swallowed ────────────────────
#
# Old behavior (main): indx = retstr.find('error') >= 0, so print(retstr) and
# return — no exception raised, caller has no way to detect the failure.
#
# New behavior (branch): extract_xmlerr parses the VOTable, finds
# QUERY_STATUS=ERROR, returns the error message, and raises Exception.

class TestBug2VoTableError:

    def test_query_adql_raises_on_votable_error(self, outfile):
        """REGRESSION: query_adql must raise when server returns QUERY_STATUS=ERROR."""
        with pytest.raises(Exception) as exc_info:
            run_query_adql(VOTABLE_ERROR, outfile)
        assert 'ORA-00942' in str(exc_info.value), \
            "Exception should contain the extracted server error message"

    def test_query_criteria_raises_on_votable_error(self, outfile):
        """REGRESSION: query_criteria must raise when server returns QUERY_STATUS=ERROR."""
        with pytest.raises(Exception) as exc_info:
            run_query_criteria(VOTABLE_ERROR, outfile)
        assert 'ORA-00942' in str(exc_info.value), \
            "Exception should contain the extracted server error message"

    def test_query_adql_no_file_written_on_error(self, outfile):
        """REGRESSION: no output file should exist after a QUERY_STATUS=ERROR response."""
        try:
            run_query_adql(VOTABLE_ERROR, outfile)
        except Exception:
            pass
        assert not os.path.exists(outfile), \
            "Output file must not be created when the server returned an error"

    def test_query_criteria_no_file_written_on_error(self, outfile):
        """REGRESSION: no output file should exist after a QUERY_STATUS=ERROR response."""
        try:
            run_query_criteria(VOTABLE_ERROR, outfile)
        except Exception:
            pass
        assert not os.path.exists(outfile), \
            "Output file must not be created when the server returned an error"


# ── Bug 3: 'WEB' CGI contamination silently swallowed ───────────────────────
#
# Old behavior (main): 'WEB' does not contain the word 'error', so indx = -1,
# error block skipped, falls through to print(retstr); return — no exception.
#
# New behavior (branch): extract_xmlerr raises (not XML), os.path.exists check
# detects no file was written, raises Exception with the 'WEB' response shown.

class TestBug3WebResponse:

    def test_query_adql_raises_on_web_response(self, outfile):
        """REGRESSION: query_adql must raise when server returns 'WEB' garbage."""
        with pytest.raises(Exception) as exc_info:
            run_query_adql(WEB_RESPONSE, outfile)
        assert "'WEB'" in str(exc_info.value), \
            "Exception should identify the unexpected 'WEB' response"

    def test_query_criteria_raises_on_web_response(self, outfile):
        """REGRESSION: query_criteria must raise when server returns 'WEB' garbage."""
        with pytest.raises(Exception) as exc_info:
            run_query_criteria(WEB_RESPONSE, outfile)
        assert "'WEB'" in str(exc_info.value), \
            "Exception should identify the unexpected 'WEB' response"

    def test_query_adql_no_file_written_on_web_response(self, outfile):
        """REGRESSION: no output file should exist after a 'WEB' response."""
        try:
            run_query_adql(WEB_RESPONSE, outfile)
        except Exception:
            pass
        assert not os.path.exists(outfile), \
            "Output file must not be created when server returned 'WEB'"

    def test_query_criteria_no_file_written_on_web_response(self, outfile):
        """REGRESSION: no output file should exist after a 'WEB' response."""
        try:
            run_query_criteria(WEB_RESPONSE, outfile)
        except Exception:
            pass
        assert not os.path.exists(outfile), \
            "Output file must not be created when server returned 'WEB'"


# ── Connection errors: clean message ────────────────────────────────────────
#
# When send_async returns a long raw connection exception trace, the error
# message shown to the caller should be clean — not a raw urllib3 dump.

class TestConnectionError:

    def test_query_adql_raises_clean_message_on_connection_error(self, outfile):
        """Connection errors should raise with a clean message, not a raw trace."""
        with pytest.raises(Exception) as exc_info:
            run_query_adql(CONNECTION_ERROR, outfile)
        msg = str(exc_info.value)
        assert 'HTTPConnectionPool' not in msg, \
            "Raw urllib3 connection pool trace should not be shown to the caller"
        assert 'Query failed' in msg, \
            "Exception should contain a human-readable 'Query failed' prefix"

    def test_query_criteria_raises_clean_message_on_connection_error(self, outfile):
        """Connection errors should raise with a clean message, not a raw trace."""
        with pytest.raises(Exception) as exc_info:
            run_query_criteria(CONNECTION_ERROR, outfile)
        msg = str(exc_info.value)
        assert 'HTTPConnectionPool' not in msg, \
            "Raw urllib3 connection pool trace should not be shown to the caller"
        assert 'Query failed' in msg, \
            "Exception should contain a human-readable 'Query failed' prefix"


# ── Regression: success path unchanged ──────────────────────────────────────
#
# When send_async returns a valid response and the output file is written,
# no exception should be raised. This verifies the fix does not break
# normal successful queries.

class TestSuccessPath:

    def test_query_adql_does_not_raise_on_success(self, existing_outfile):
        """SUCCESS PATH: query_adql must not raise when the output file is written."""
        # Simulate a successful async query: send_async returns a job URL,
        # and the file has already been written to disk (fixture provides it).
        run_query_adql("http://vmkoatest.ipac.caltech.edu:8000/TAP/async/job123", existing_outfile)

    def test_query_criteria_does_not_raise_on_success(self, existing_outfile):
        """SUCCESS PATH: query_criteria must not raise when the output file is written."""
        run_query_criteria("http://vmkoatest.ipac.caltech.edu:8000/TAP/async/job123", existing_outfile)
