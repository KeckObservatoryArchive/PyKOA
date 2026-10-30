"""Regression tests for three F821 undefined-name bugs fixed in core.py.

Bug 1 (core.py:1939): query_moving_object — print(msg) → print(str(e))
  msg was never assigned before the os.makedirs except block; crashed with
  NameError instead of printing the OS error.

Bug 2 (core.py:3974): __download_calibfiles — caliblist → listpath, plus
  unsafe fp.close() and dead return removed.
  caliblist is the caller's variable name; inside the method the parameter
  is listpath. Also: deubg=1 typo at the call site fixed to debug=1.

Bug 3 (core.py:5447): extract_xmlerr — str(e) → self.msg in debug log
  inside if (votbl is None): block. e is unbound there — the preceding
  try succeeded, so the except clause never ran; Python 3 deletes e on
  except-block exit. Crashed with NameError if self.debug=1.
"""

import logging
import pathlib
from unittest import mock

import pytest

from pykoa.koa.core import Archive, KoaTap

# ── Bug 3 — extract_xmlerr ────────────────────────────────────────────────────
# This is the cleanest to unit-test: extract_xmlerr takes a plain string/bytes
# and is directly accessible on KoaTap.

VALID_ERROR_VOTABLE = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    '<VOTABLE version="1.4" xmlns="http://www.ivoa.net/xml/VOTable/v1.3">\n'
    '<RESOURCE type="results">\n'
    '<INFO name="QUERY_STATUS" value="ERROR">Table not found</INFO>\n'
    "</RESOURCE>\n"
    "</VOTABLE>\n"
)

# A VOTable where VOTABLE key maps to None — the rare case that hit the bug.
NULL_VOTABLE_XML = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    "<VOTABLE/>\n"  # self-closing — xmltodict gives None for the VOTABLE value
)

# A VOTable with QUERY_STATUS != ERROR — triggers the "not a VOTable error" path.
NON_ERROR_VOTABLE = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    '<VOTABLE version="1.4" xmlns="http://www.ivoa.net/xml/VOTable/v1.3">\n'
    '<RESOURCE type="results">\n'
    '<INFO name="QUERY_STATUS" value="OK"/>\n'
    "</RESOURCE>\n"
    "</VOTABLE>\n"
)


def make_tap(debug=False):
    tap = KoaTap("https://koa.example.test/TAP")
    tap.debug = 1 if debug else 0
    return tap


def test_extract_xmlerr_happy_path():
    """extract_xmlerr returns the INFO text from a well-formed error VOTable."""
    tap = make_tap()
    result = tap.extract_xmlerr(VALID_ERROR_VOTABLE)
    assert result == "Table not found"


def test_extract_xmlerr_happy_path_bytes():
    """extract_xmlerr accepts bytes (as send_sync passes .content)."""
    tap = make_tap()
    result = tap.extract_xmlerr(VALID_ERROR_VOTABLE.encode("utf-8"))
    assert result == "Table not found"


def test_extract_xmlerr_votbl_none_raises_not_crashes():
    """Bug 3 regression: votbl is None must raise Exception, not NameError.

    Previously: logging.debug(f'exception: e= {str(e):s}') with e unbound
    crashed with NameError when self.debug=1. Now uses self.msg instead.
    """
    tap = make_tap(debug=True)  # debug=1 activates the formerly-broken path
    with pytest.raises(Exception, match="Not a votbl format."):
        tap.extract_xmlerr(NULL_VOTABLE_XML)
    # Confirm no NameError escaped — the right exception type was raised.


def test_extract_xmlerr_non_error_status_raises():
    """extract_xmlerr raises when QUERY_STATUS is not ERROR."""
    tap = make_tap()
    with pytest.raises(Exception, match="Failed to extract infoval"):
        tap.extract_xmlerr(NON_ERROR_VOTABLE)


def test_extract_xmlerr_votbl_none_debug1_logs_self_msg(caplog):
    """Bug 3 regression: with debug=1 the fixed path must log self.msg — not
    crash with NameError on the undefined 'e'. Verifies the log content."""
    tap = make_tap(debug=True)
    with caplog.at_level(logging.DEBUG):
        with pytest.raises(Exception, match="Not a votbl format."):
            tap.extract_xmlerr(NULL_VOTABLE_XML)
    assert any("Not a votbl format." in m for m in caplog.messages), (
        "Expected self.msg in debug log; got: " + repr(caplog.messages)
    )


# ── Bug 1 — query_moving_object os.makedirs failure ──────────────────────────


def test_query_moving_object_makedirs_failure_prints_error_not_crashes(capsys):
    """Bug 1 regression: os.makedirs failure must print the OS error, not crash.

    Previously: print(msg) with msg undefined crashed with NameError.
    Now: print(str(e)) prints the actual OS error (e.g. PermissionError).
    """
    from pykoa.koa import Koa  # Koa = Archive()

    with mock.patch("os.makedirs", side_effect=PermissionError("Permission denied")):
        result = Koa.query_moving_object(
            instrument="hires",
            object="pluto",
            outdir="/nonexistent",
            outfile="out.json",
            startdate="2020-01-01",
        )

    assert result is None  # method returns on error, never raises
    captured = capsys.readouterr()
    assert "Permission denied" in captured.out  # the OS error was printed


# ── Bug 2 — __download_calibfiles listpath/fp.close ──────────────────────────


def test_download_calibfiles_missing_file_raises_readable_error():
    """Bug 2 regression: a missing caliblist file must raise Exception('Failed
    to read <path>'), not crash with NameError on 'caliblist' or 'fp'.

    __download_calibfiles is name-mangled; call it directly via the mangled name.
    """
    archive = Archive()
    bad_path = "/nonexistent/caliblist.json"

    with pytest.raises(Exception, match="Failed to read " + bad_path):
        archive._Archive__download_calibfiles(bad_path, None, "/tmp", debug=1)


def test_no_deubg_typo_at_download_calibfiles_callsite():
    """Bug 2 regression: 'deubg=1' at the __download_calibfiles call site
    silently passed debug=0 into the method, disabling all debug logging.
    Guard against the typo being reintroduced."""
    src = pathlib.Path(__file__).parent.parent / "pykoa" / "koa" / "core.py"
    assert "deubg" not in src.read_text(), (
        "Typo 'deubg' found in core.py — should be 'debug' at the "
        "__download_calibfiles call site (line ~3610)."
    )


def test_baseurl_trailing_slash_added_when_missing():
    """Archive adds trailing slash to server kwarg that lacks one.

    Regression test: Archive.__init__ had 'self.baseurl + "/"' (no-op string
    concatenation, result discarded) instead of 'self.baseurl += "/"'.
    If a user passes server='https://koa.ipac.caltech.edu' without a trailing
    slash, all URL construction would silently break.
    """
    a = Archive(server="https://koa.ipac.caltech.edu")
    assert a.baseurl.endswith("/"), f"baseurl must end with '/'; got {a.baseurl!r}"


def test_baseurl_trailing_slash_preserved_when_present():
    """Archive does not double-slash a server kwarg that already ends with '/'."""
    a = Archive(server="https://koa.ipac.caltech.edu/")
    assert a.baseurl == "https://koa.ipac.caltech.edu/", (
        f"baseurl should be unchanged; got {a.baseurl!r}"
    )
