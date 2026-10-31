"""Regression tests for pykoa.koa.core._parse_votable_error.

This function was introduced to fix a bug where Archive.__submit_request
called self.extract_xmlerr() which does not exist on Archive (only on KoaTap),
causing AttributeError to be silently swallowed and server error messages lost.

The function has 7 raise paths. This file covers all 7:
- Branch 1 (happy path): test_parse_votable_error_returns_error_message
- Branch 2 (non-VOTABLE root): test_parse_votable_error_raises_on_missing_votable_key
- Branch 3 (votbl is None): test_parse_votable_error_votable_element_is_none
- Branch 4 (INFO key missing): test_parse_votable_error_raises_on_missing_info
- Branch 5 (info is None): test_parse_votable_error_info_element_is_none
- Branch 6 (INFO has no #text): test_parse_votable_error_info_element_has_no_text
- Branch 7 (@value != ERROR): test_parse_votable_error_raises_when_status_not_error
"""

import pytest

from pykoa.koa.core import _parse_votable_error

VALID_ERROR_VOTABLE = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<VOTABLE version="1.3" xmlns="http://www.ivoa.net/xml/VOTable/v1.3">'
    '<RESOURCE type="results">'
    '<INFO name="QUERY_STATUS" value="ERROR">Table not found</INFO>'
    "</RESOURCE></VOTABLE>"
)

VALID_ERROR_VOTABLE_BYTES = VALID_ERROR_VOTABLE.encode()


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_parse_votable_error_returns_error_message():
    """Returns the INFO text when QUERY_STATUS=ERROR is present."""
    assert _parse_votable_error(VALID_ERROR_VOTABLE_BYTES) == "Table not found"


def test_parse_votable_error_accepts_string():
    """Accepts a str as well as bytes."""
    assert _parse_votable_error(VALID_ERROR_VOTABLE) == "Table not found"


# ---------------------------------------------------------------------------
# Exception branches
# ---------------------------------------------------------------------------


def test_parse_votable_error_raises_on_malformed_xml():
    """Raises on input that cannot be parsed as XML."""
    with pytest.raises(Exception, match="(?i)parse"):
        _parse_votable_error(b"not xml at all <<<")


def test_parse_votable_error_raises_on_missing_votable_key():
    """Raises when the root element is not VOTABLE."""
    xml = b'<?xml version="1.0"?><OTHER><INFO name="QUERY_STATUS" value="ERROR">x</INFO></OTHER>'
    with pytest.raises(Exception, match="(?i)votable"):
        _parse_votable_error(xml)


def test_parse_votable_error_raises_on_missing_info():
    """Raises when VOTABLE/RESOURCE has no INFO element (branch 4)."""
    xml = (
        b'<?xml version="1.0"?>'
        b'<VOTABLE xmlns="http://www.ivoa.net/xml/VOTable/v1.3">'
        b"<RESOURCE/>"
        b"</VOTABLE>"
    )
    with pytest.raises(Exception, match="(?i)info|votable"):
        _parse_votable_error(xml)


def test_parse_votable_error_votable_element_is_none():
    """Raises when VOTABLE root is self-closing (votbl is None, branch 3)."""
    xml = b'<?xml version="1.0"?><VOTABLE/>'
    with pytest.raises(Exception, match="(?i)votable"):
        _parse_votable_error(xml)


def test_parse_votable_error_info_element_is_none():
    """Raises when INFO element is self-closing and parses to None (branch 5).

    xmltodict returns None for a bare self-closing <INFO/> tag with no
    attributes and no text, so info is None triggers the explicit None guard.
    """
    xml = (
        b'<?xml version="1.0"?>'
        b'<VOTABLE xmlns="http://www.ivoa.net/xml/VOTable/v1.3">'
        b"<RESOURCE><INFO/></RESOURCE>"
        b"</VOTABLE>"
    )
    with pytest.raises(Exception, match="(?i)no error|info"):
        _parse_votable_error(xml)


def test_parse_votable_error_info_element_has_no_text():
    """Raises when INFO has @value but no text content (branch 6).

    A self-closing INFO element like <INFO name="QUERY_STATUS" value="ERROR"/>
    xmltodict returns {"@name": ..., "@value": "ERROR"} with no "#text" key.
    """
    xml = (
        b'<?xml version="1.0"?>'
        b'<VOTABLE xmlns="http://www.ivoa.net/xml/VOTable/v1.3">'
        b"<RESOURCE>"
        b'<INFO name="QUERY_STATUS" value="ERROR"/>'
        b"</RESOURCE>"
        b"</VOTABLE>"
    )
    with pytest.raises(Exception, match="(?i)extract error text"):
        _parse_votable_error(xml)


def test_parse_votable_error_raises_when_status_not_error():
    """Raises when INFO @value is not 'ERROR' (e.g. OVERFLOW, OK)."""
    xml = (
        b'<?xml version="1.0" encoding="UTF-8"?>'
        b'<VOTABLE version="1.3" xmlns="http://www.ivoa.net/xml/VOTable/v1.3">'
        b'<RESOURCE type="results">'
        b'<INFO name="QUERY_STATUS" value="OVERFLOW">0 rows returned</INFO>'
        b"</RESOURCE></VOTABLE>"
    )
    with pytest.raises(Exception, match="(?i)no error"):
        _parse_votable_error(xml)
