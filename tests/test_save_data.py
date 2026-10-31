"""Tests for KoaTap.save_data.

Covers:
- With outpath: streams response to file, returns "Result downloaded to file [...]"
- Without outpath (in-memory): reads result into tap.astropytbl via temp file
- File open fails: returns error string without raising
- iter_content fails: returns error string without raising

Note: save_data reads self.outpath (instance attribute) for the message text
but uses the outpath parameter as the actual write destination. In normal use
both are the same path.
"""

import os
from unittest import mock

import pytest

from pykoa.koa.core import KoaTap

TAP_URL = "https://koa.example.test/TAP"

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


def _make_response(content=VOTABLE_BYTES):
    """Return a mock response whose iter_content yields the given bytes."""
    resp = mock.MagicMock()
    resp.iter_content.return_value = [content]
    return resp


@pytest.fixture
def tap():
    return KoaTap(TAP_URL)


# ---------------------------------------------------------------------------
# With outpath — streams to file
# ---------------------------------------------------------------------------


def test_save_data_writes_file(tap, tmp_path):
    """save_data with outpath streams response to the file and returns a downloaded message."""
    outpath = str(tmp_path / "result.tbl")
    tap.outpath = outpath
    tap.response_result = _make_response()

    msg = tap.save_data(outpath)

    assert os.path.exists(outpath)
    with open(outpath, "rb") as f:
        assert f.read() == VOTABLE_BYTES
    assert "downloaded" in msg.lower()
    assert outpath in msg


# ---------------------------------------------------------------------------
# Without outpath — in-memory astropy table
# ---------------------------------------------------------------------------


def test_save_data_in_memory_populates_astropytbl(tap, tmp_path, monkeypatch):
    """save_data with empty outpath reads result into tap.astropytbl.

    Relies on KoaTap.__init__ default: self.format = "votable".
    save_data creates a temp file in cwd; monkeypatch.chdir isolates it.
    """
    # tap.outpath is "" by default (KoaTap.__init__) — no assignment needed
    tap.response_result = _make_response()

    monkeypatch.chdir(tmp_path)
    msg = tap.save_data("")

    assert msg == "Result saved in memory (astropy table)."
    assert tap.astropytbl is not None
    assert len(tap.astropytbl) == 1


# ---------------------------------------------------------------------------
# File open fails
# ---------------------------------------------------------------------------


def test_save_data_open_fails_returns_error_string(tap, tmp_path):
    """save_data returns an error string (no raise) when open() fails."""
    # tmp_path/missing_dir/ is never created — guaranteed non-existent
    outpath = str(tmp_path / "missing_dir" / "result.tbl")
    tap.outpath = outpath
    tap.response_result = _make_response()

    msg = tap.save_data(outpath)

    assert "failed to open file" in msg.lower()


# ---------------------------------------------------------------------------
# iter_content fails
# ---------------------------------------------------------------------------


def test_save_data_iter_content_fails_returns_error_string(tap, tmp_path):
    """save_data returns an error string (no raise) when iter_content raises."""
    outpath = str(tmp_path / "result.tbl")
    tap.outpath = outpath

    bad_resp = mock.MagicMock()
    bad_resp.iter_content.side_effect = ConnectionError("broken pipe")
    tap.response_result = bad_resp

    msg = tap.save_data(outpath)

    assert "save_data error" in msg.lower()


# ---------------------------------------------------------------------------
# self.outpath (message) vs outpath parameter (file) divergence
# ---------------------------------------------------------------------------


def test_save_data_message_uses_instance_outpath_not_parameter(tap, tmp_path):
    """Message uses self.outpath (instance attr); file is written to outpath (parameter).

    These are independent — a caller can write to one path and have the message
    report another. This test documents that contract and will catch a refactor
    that accidentally unifies them.
    """
    actual_write_path = str(tmp_path / "actual.tbl")
    tap.outpath = str(tmp_path / "reported.tbl")  # instance attr differs from param
    tap.response_result = _make_response()

    msg = tap.save_data(actual_write_path)

    assert os.path.exists(actual_write_path)
    assert not os.path.exists(tap.outpath)
    assert "reported.tbl" in msg  # message uses self.outpath
    assert "actual.tbl" not in msg  # file written here but not in message
