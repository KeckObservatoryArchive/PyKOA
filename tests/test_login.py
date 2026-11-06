"""Tests for Archive.login.

Covers:
- Empty cookiepath: prints message and returns, no network call
- Success (status="ok"): cookie_loaded=1, prints success message
- Bad credentials (status!="ok"): prints failure message
- Network error (non-Timeout): prints failure message, returns
- requests.Timeout: re-raised
- Credentials are URL-encoded exactly once (special characters round-trip)
"""

import json
from unittest import mock
from urllib.parse import parse_qs, urlsplit

import pytest
import requests
import requests_mock as req_mock


def _login_response(status="ok", msg=""):
    """Return a fake requests.Response for the login endpoint."""
    resp = requests.models.Response()
    resp.status_code = 200
    resp._content = json.dumps({"status": status, "msg": msg}).encode()
    resp.headers = requests.structures.CaseInsensitiveDict(
        {"Content-type": "application/json"}
    )
    return resp


def _login_query(archive, tmp_path, userid, password):
    """Log in against a mocked server; return the decoded query it received."""
    with req_mock.Mocker() as m:
        m.get(
            req_mock.ANY,
            json={"status": "ok", "msg": ""},
            headers={"Content-type": "application/json"},
        )
        archive.login(str(tmp_path / "koa.cookie"), userid=userid, password=password)
    return parse_qs(urlsplit(m.last_request.url).query, keep_blank_values=True)


# ---------------------------------------------------------------------------
# Empty cookiepath guard
# ---------------------------------------------------------------------------


def test_login_empty_cookiepath_returns_early(archive, capsys):
    """login("") prints the required-cookie message without making a network call."""
    with req_mock.Mocker() as m:
        archive.login("")
    assert not m.called
    assert "cookiepath" in capsys.readouterr().out.lower()


# ---------------------------------------------------------------------------
# Success path
# ---------------------------------------------------------------------------


def test_login_success_sets_cookie_loaded(archive, tmp_path, capsys):
    """login sets cookie_loaded=1 and prints success on status=ok."""
    cookiepath = str(tmp_path / "koa.cookie")
    with (
        mock.patch("requests.Session.get", return_value=_login_response("ok")),
        mock.patch("os.chmod"),  # prevent cross-platform permission differences
    ):
        archive.login(cookiepath, userid="testuser", password="testpass")

    assert archive.cookie_loaded == 1
    out = capsys.readouterr().out
    assert "successfully login" in out.lower()
    assert "testuser" in out


# ---------------------------------------------------------------------------
# Bad credentials
# ---------------------------------------------------------------------------


def test_login_bad_credentials_prints_failure(archive, tmp_path, capsys):
    """login prints 'Failed to login' when server returns status != ok."""
    cookiepath = str(tmp_path / "koa.cookie")
    with mock.patch(
        "requests.Session.get",
        return_value=_login_response("error", "invalid credentials"),
    ):
        archive.login(cookiepath, userid="bad", password="bad")

    out = capsys.readouterr().out
    assert "failed to login" in out.lower()
    assert "invalid credentials" in out


# ---------------------------------------------------------------------------
# Network error
# ---------------------------------------------------------------------------


def test_login_network_error_prints_and_returns(archive, tmp_path, capsys):
    """login catches non-Timeout network errors, prints, and returns without raising."""
    cookiepath = str(tmp_path / "koa.cookie")
    with mock.patch("requests.Session.get", side_effect=ConnectionError("refused")):
        archive.login(cookiepath, userid="u", password="p")

    out = capsys.readouterr().out
    assert "failed to login" in out.lower()


# ---------------------------------------------------------------------------
# Timeout propagates
# ---------------------------------------------------------------------------


def test_login_timeout_raises(archive, tmp_path):
    """requests.Timeout from session.get propagates out of login."""
    cookiepath = str(tmp_path / "koa.cookie")
    with mock.patch("requests.Session.get", side_effect=requests.Timeout("timed out")):
        with pytest.raises(requests.Timeout, match="timed out"):
            archive.login(cookiepath, userid="u", password="p")


# ---------------------------------------------------------------------------
# Credentials survive URL encoding exactly once
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "password",
    [
        "p:ss",
        "p@ss word",
        "100%",
        "a+b",
        "a&b=c",
        "a/b?c#d",
        "caf\u00e9",
        "%41",
    ],
)
def test_login_password_reaches_server_unchanged(archive, tmp_path, password):
    """The server must decode the password back to exactly what the user typed.

    A password that is URL-quoted and then urlencoded again arrives with its
    special characters still escaped (":" becomes "%3A"), so logins fail for
    any password containing them.
    """
    query = _login_query(archive, tmp_path, "testuser", password)
    assert query["password"] == [password]


def test_login_special_userid_and_password_together(archive, tmp_path):
    """A userid and a password that both need escaping arrive intact side by side."""
    userid = "j.d\u00f6e+test@example.org"
    password = "p@ss:w%rd&1 #x"
    query = _login_query(archive, tmp_path, userid, password)
    assert query == {"userid": [userid], "password": [password]}
