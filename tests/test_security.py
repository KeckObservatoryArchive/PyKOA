"""Security tests for PyKOA.

Tests that credentials, cookies, and URLs are handled safely.
"""

import logging
import os
import stat
import tempfile
from unittest import mock

import requests

from pykoa.koa.core import Archive, _validate_server_url

# ---------------------------------------------------------------------------
# Cookie file permissions
# ---------------------------------------------------------------------------


def _make_ok_response(url, **kwargs):
    """Return a mocked successful login HTTP response."""
    resp = requests.models.Response()
    resp.status_code = 200
    resp._content = b'{"status": "ok", "msg": "logged in"}'
    resp.headers = requests.structures.CaseInsensitiveDict(
        {"Content-type": "application/json"}
    )
    return resp


def _fake_save(cookiepath):
    """Return a fake_save closure that mirrors real MozillaCookieJar.save().

    Python 3.11+ MozillaCookieJar.save() uses os.open(..., O_CREAT|O_WRONLY|
    O_TRUNC, 0o600) internally, so the file is created or truncated at 0o600.
    This closure replicates that behavior faithfully.
    """

    def fake_save():
        fd = os.open(cookiepath, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write("# Netscape HTTP Cookie File\n")

    return fake_save


def _run_login(cookiepath):
    """Run Archive.login with mocked network and cookie save.

    Uses _make_ok_response for the HTTP layer and _fake_save (which mirrors
    real Python 3.11+ MozillaCookieJar.save() behavior) for the cookie write.
    Returns the Archive instance so callers can inspect state.
    """
    a = Archive()
    with mock.patch("requests.Session.get", side_effect=_make_ok_response):
        with mock.patch(
            "http.cookiejar.MozillaCookieJar.save",
            side_effect=_fake_save(cookiepath),
        ):
            a.login(cookiepath=cookiepath, userid="test", password="test")
    return a


def test_re_login_existing_insecure_cookie_is_tightened():
    """Re-login: a pre-existing 0o644 cookie file must be restricted to 0o600.

    MozillaCookieJar.save() uses O_CREAT internally, but O_CREAT's mode
    argument only applies when the file is newly created — it is silently
    ignored for pre-existing files. The belt-and-suspenders os.chmod after
    save() is the mechanism that secures the re-login case.

    This test simulates re-login by pre-creating the file at 0o644, then
    verifies login() tightens it to 0o600 via os.chmod.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        cookiepath = os.path.join(tmpdir, "koa.cookie")

        # Simulate a pre-existing cookie from a prior login at wrong permissions
        with open(cookiepath, "w") as f:
            f.write("")
        os.chmod(cookiepath, 0o644)

        assert stat.S_IMODE(os.stat(cookiepath).st_mode) == 0o644, (
            "Test setup failed: file should start at 0o644"
        )

        a = _run_login(cookiepath)

        permissions = stat.S_IMODE(os.stat(cookiepath).st_mode)
        assert permissions == 0o600, (
            f"Cookie file must be 0o600 after re-login, "
            f"got 0o{permissions:03o}. Belt-and-suspenders os.chmod failed."
        )
        assert a.cookie_loaded == 1, "cookie_loaded must be 1 after successful login"


def test_first_login_new_cookie_file_created_at_0o600():
    """First login: end-to-end invariant — new cookie file must be 0o600.

    Verifies the security invariant for first login (no pre-existing file):
    the cookie file must exist at 0o600 after login() returns.

    Note: this test does NOT isolate whether save() or os.chmod is the
    mechanism — it tests the observable end state. os.chmod is specifically
    exercised by test_re_login_existing_insecure_cookie_is_tightened.
    The pre_chmod_perms assertion below separately verifies that the mock
    (mirroring real Python 3.11+ MozillaCookieJar.save()) creates the file
    at 0o600 before os.chmod even runs.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        cookiepath = os.path.join(tmpdir, "koa.cookie")
        pre_chmod_perms = []

        def spy_save():
            """Mirror real MozillaCookieJar.save() and capture pre-chmod perms."""
            fd = os.open(cookiepath, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w") as f:
                f.write("# Netscape HTTP Cookie File\n")
            # Capture permissions now — before os.chmod runs in login()
            pre_chmod_perms.append(stat.S_IMODE(os.stat(cookiepath).st_mode))

        assert not os.path.exists(cookiepath), (
            "Test setup failed: cookie file must not exist before first login"
        )

        a = Archive()
        with mock.patch("requests.Session.get", side_effect=_make_ok_response):
            with mock.patch(
                "http.cookiejar.MozillaCookieJar.save", side_effect=spy_save
            ):
                a.login(cookiepath=cookiepath, userid="test", password="test")

        assert os.path.exists(cookiepath), "Cookie file must exist after login"
        permissions = stat.S_IMODE(os.stat(cookiepath).st_mode)
        assert permissions == 0o600, (
            f"Newly created cookie file must be 0o600, got 0o{permissions:03o}."
        )
        assert pre_chmod_perms == [0o600], (
            f"save() must create new files at 0o600 via os.open; "
            f"got 0o{pre_chmod_perms[0]:03o} before os.chmod ran. "
            "This would be a TOCTOU exposure window."
        )
        assert a.cookie_loaded == 1, (
            "cookie_loaded must be 1 after successful first login"
        )


def test_chmod_failure_removes_cookie_and_clears_flag(capsys):
    """chmod OSError must remove the cookie file, clear cookie_loaded, and print failure.

    If os.chmod raises OSError (e.g. EPERM on some filesystems), the cookie
    file must be deleted so it cannot be read at whatever permissions the OS
    left it with. cookie_loaded must remain 0 so the caller does not attempt
    to use a potentially world-readable cookie. The user must receive a printed
    failure message since every other login exit path prints exactly one message.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        cookiepath = os.path.join(tmpdir, "koa.cookie")
        a = Archive()

        with mock.patch("requests.Session.get", side_effect=_make_ok_response):
            with mock.patch(
                "http.cookiejar.MozillaCookieJar.save",
                side_effect=_fake_save(cookiepath),
            ):
                with mock.patch(
                    "os.chmod", side_effect=OSError("Operation not permitted")
                ):
                    a.login(cookiepath=cookiepath, userid="test", password="test")

        assert a.cookie_loaded == 0, "cookie_loaded must remain 0 when chmod fails"
        assert not os.path.exists(cookiepath), (
            "Cookie file must be removed when chmod fails to prevent "
            "reading at unsafe permissions."
        )
        captured = capsys.readouterr()
        assert "Login failed" in captured.out, (
            f"print() must emit failure message on chmod error, got: {captured.out!r}"
        )


def test_chmod_failure_logs_warning(capsys):
    """chmod OSError must emit a logging warning and a printed failure message.

    logging.warning is the operator-visible signal; print(msg) is the
    user-visible signal. Both must fire — the test checks print via capsys
    and logging via assertLogs.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        cookiepath = os.path.join(tmpdir, "koa.cookie")
        a = Archive()

        with mock.patch("requests.Session.get", side_effect=_make_ok_response):
            with mock.patch(
                "http.cookiejar.MozillaCookieJar.save",
                side_effect=_fake_save(cookiepath),
            ):
                with mock.patch(
                    "os.chmod", side_effect=OSError("Operation not permitted")
                ):
                    with mock.patch.object(logging, "warning") as mock_warn:
                        a.login(cookiepath=cookiepath, userid="test", password="test")
                        assert mock_warn.called, (
                            "logging.warning must be called when chmod fails"
                        )
                        warning_text = " ".join(
                            str(arg) for arg in mock_warn.call_args_list[0][0]
                        )
                        assert "cookie" in warning_text.lower(), (
                            f"Warning must mention cookie, got: {warning_text!r}"
                        )

        captured = capsys.readouterr()
        assert "Login failed" in captured.out, (
            f"print() must emit failure message on chmod error, got: {captured.out!r}"
        )


def test_chmod_and_remove_both_fail_logs_and_clears_flag(capsys):
    """When both chmod and os.remove fail, log both warnings and clear cookie_loaded.

    If the cookie file cannot be removed after a chmod failure (e.g. immutable
    filesystem, EPERM on parent directory), the file may remain on disk at
    unsafe permissions. Both failures must be logged so an operator can act.
    cookie_loaded must still be 0 and a failure message must be printed.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        cookiepath = os.path.join(tmpdir, "koa.cookie")
        a = Archive()

        with mock.patch("requests.Session.get", side_effect=_make_ok_response):
            with mock.patch(
                "http.cookiejar.MozillaCookieJar.save",
                side_effect=_fake_save(cookiepath),
            ):
                with mock.patch(
                    "os.chmod", side_effect=OSError("Operation not permitted")
                ):
                    with mock.patch(
                        "os.remove", side_effect=OSError("Read-only file system")
                    ):
                        with mock.patch.object(logging, "warning") as mock_warn:
                            a.login(
                                cookiepath=cookiepath, userid="test", password="test"
                            )
                            # Both chmod failure and remove failure must be logged
                            assert mock_warn.call_count == 2, (
                                f"Expected exactly 2 logging.warning calls "
                                f"(chmod fail + remove fail), got {mock_warn.call_count}"
                            )
                            all_warnings = " ".join(
                                str(arg)
                                for call in mock_warn.call_args_list
                                for arg in call[0]
                            ).lower()
                            assert (
                                "remove" in all_warnings or "manual" in all_warnings
                            ), (
                                "Second warning must mention removal failure or manual action"
                            )

        assert a.cookie_loaded == 0, (
            "cookie_loaded must remain 0 even when os.remove also fails"
        )
        captured = capsys.readouterr()
        assert "Login failed" in captured.out, (
            "Failure message must be printed even when os.remove also fails"
        )


# ---------------------------------------------------------------------------
# Credential leakage in debug logs
# ---------------------------------------------------------------------------


def test_password_not_logged_in_debug_url(caplog):
    """Full login URL with credentials must not appear in debug logs.

    login() constructs url = login_url + userid=...&password=... and previously
    logged the full url. Only the base login_url should appear in logs.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        cookiepath = os.path.join(tmpdir, "koa.cookie")
        a = Archive()
        a.debug = 1

        with mock.patch("requests.Session.get", side_effect=_make_ok_response):
            with mock.patch(
                "http.cookiejar.MozillaCookieJar.save",
                side_effect=_fake_save(cookiepath),
            ):
                with caplog.at_level(logging.DEBUG):
                    a.login(cookiepath=cookiepath, userid="testuser", password="s3cr3t")

    all_logs = " ".join(caplog.messages)
    assert "s3cr3t" not in all_logs, "Password must not appear in any log message"
    assert "testuser" not in all_logs or "userid" not in all_logs.lower(), (
        "Userid must not appear in the url= log line (only in login_url base is ok)"
    )


def test_cookie_value_not_logged(caplog):
    """Session cookie value must not appear in debug logs.

    cookie.value contains the session token. Logging it would expose active
    sessions to anyone with access to the debug file.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        cookiepath = os.path.join(tmpdir, "koa.cookie")
        a = Archive()
        a.debug = 1

        fake_cookie = mock.MagicMock()
        fake_cookie.name = "KOA80"
        fake_cookie.value = "supersecrettoken123"
        fake_cookie.domain = "koa.ipac.caltech.edu"
        fake_cookie.discard = False
        fake_cookie.is_expired = mock.MagicMock(return_value=False)

        def fake_save_with_cookie():
            fd = os.open(cookiepath, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w") as f:
                f.write("# Netscape HTTP Cookie File\n")

        with mock.patch("requests.Session.get", side_effect=_make_ok_response):
            with mock.patch(
                "http.cookiejar.MozillaCookieJar.save",
                side_effect=fake_save_with_cookie,
            ):
                with mock.patch(
                    "http.cookiejar.MozillaCookieJar.__iter__",
                    return_value=iter([fake_cookie]),
                ):
                    with caplog.at_level(logging.DEBUG):
                        a.login(
                            cookiepath=cookiepath,
                            userid="testuser",
                            password="test",
                        )

    all_logs = " ".join(caplog.messages)
    assert "supersecrettoken123" not in all_logs, (
        "Cookie value must not appear in any log message"
    )
    assert "[REDACTED]" in all_logs, (
        "Redacted placeholder must appear in logs where cookie.value was"
    )


# ---------------------------------------------------------------------------
# URL injection via server kwarg
# ---------------------------------------------------------------------------


def test_http_server_raises():
    """Non-https server must raise ValueError.

    Passing server='http://...' would send credentials in plaintext.
    The KOA server accepts both http and https with no redirect, so the
    client must enforce https itself.
    """
    import pytest

    with pytest.raises(ValueError, match="https://"):
        Archive(server="http://koa.ipac.caltech.edu/")


def test_non_koa_server_warns():
    """Non-KOA https server must log a warning.

    A misconfigured or compromised server could redirect credentials.
    Warnings let dev and test servers work while alerting the user.
    """
    with mock.patch.object(logging, "warning") as mock_warn:
        Archive(server="https://example.com/")
        assert mock_warn.called, "Warning must fire for non-KOA domain"
        assert any("example.com" in str(call) for call in mock_warn.call_args_list), (
            "Warning must mention the server URL"
        )


def test_koa_server_accepted():
    """https://koa.ipac.caltech.edu/ must be accepted without warning or error."""
    with mock.patch.object(logging, "warning") as mock_warn:
        a = Archive(server="https://koa.ipac.caltech.edu/")
        assert not mock_warn.called, "No warning for the production KOA server"
        assert a.baseurl == "https://koa.ipac.caltech.edu/"


def test_ipac_subdomain_accepted():
    """https://*.ipac.caltech.edu servers must be accepted without warning."""
    with mock.patch.object(logging, "warning") as mock_warn:
        Archive(server="https://testdds.ipac.caltech.edu/")
        assert not mock_warn.called, "No warning for .ipac.caltech.edu subdomain"


def test_validate_server_url_rejects_http():
    """_validate_server_url raises ValueError for http://."""
    import pytest

    with pytest.raises(ValueError, match="https://"):
        _validate_server_url("http://koa.ipac.caltech.edu/")


def test_validate_server_url_accepts_https_koa():
    """_validate_server_url passes silently for https://koa.ipac.caltech.edu."""
    _validate_server_url("https://koa.ipac.caltech.edu/")
