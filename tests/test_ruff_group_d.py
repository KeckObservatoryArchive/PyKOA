"""Tests for Group D ruff fixes — four single-issue corrections.

Fix 1 — `pid = os.getpid()` removed (F841, core.py:4748)
    KoaTap.__init__ called os.getpid() but never used the result.
    The comment '# turn on server debug' was moved to above
    self.datadict['debug'] = 1, which is what actually turns on server debug.

Fix 2 — `except:` → `except Exception:` (E722, core.py:4785)
    KoaTap.__init__ used a bare except around cookiejar.load().
    Bare except catches KeyboardInterrupt and SystemExit — dangerous.
    except Exception catches all realistic cookie-file failures
    (http.cookiejar.LoadError, OSError, FileNotFoundError) without
    swallowing signals.

Fix 3 — `data = self.response.json()` → `self.response.json()` (F841)
    send_sync's JSON branch parsed the response body to validate it was
    valid JSON, but never used the parsed result. The bare call keeps the
    parse-validation behaviour while dropping the unused binding.

Fix 4 — `# noqa: E402` on `__init__.py:27` (E402)
    The `from .core import ...` line must come AFTER `conf = Conf()` to
    avoid a circular import (core.py does `from . import conf` at module
    scope). The noqa suppresses the ruff warning without changing the import
    order, which must stay as-is.
"""

import pathlib
import pytest

from pykoa.koa.core import KoaTap

SRC = pathlib.Path(__file__).parent.parent / 'pykoa' / 'koa' / 'core.py'
INIT = pathlib.Path(__file__).parent.parent / 'pykoa' / 'koa' / '__init__.py'


def _method_source(method_name, after_line=0):
    """Extract source lines for a named method, optionally starting search after a line.

    WARNING: Uses string-matching, not AST parsing. Stops at the next 'def'
    keyword, so it will return incomplete source if the target method ever gains
    a nested function. The methods currently tested have no nested functions.
    If that changes, switch to ast.parse() with FunctionDef.lineno/end_lineno.
    """
    src = SRC.read_text().splitlines()
    in_method, lines, started = False, [], 0
    for lineno, line in enumerate(src):
        if lineno < after_line:
            continue
        if f'def {method_name}' in line:
            in_method = True
            started = lineno
        elif in_method and lineno > started and line.strip().startswith('def '):
            break
        if in_method:
            lines.append(line)
    return '\n'.join(lines)


# ── Fix 1: pid removed ────────────────────────────────────────────────────────

def test_pid_assignment_removed_from_koatap_init():
    """pid = os.getpid() was dead code — process ID was never used.

    The comment '# turn on server debug' was moved to above
    self.datadict['debug'] = 1, which is the line that actually sends
    debug=1 to the TAP server.
    """
    src = _method_source('__init__', after_line=4672)  # KoaTap.__init__ at line 4673
    assert 'pid = os.getpid()' not in src, (
        "Dead 'pid = os.getpid()' found in KoaTap.__init__. "
        "pid is never used after assignment. Remove it."
    )


def test_server_debug_comment_is_above_datadict_debug():
    """'# turn on server debug' comment must sit above self.datadict['debug'] = 1,
    the line it actually describes."""
    src = SRC.read_text()
    debug_comment_pos = src.find('turn on server debug')
    datadict_debug_pos = src.find("self.datadict['debug'] = 1")
    assert debug_comment_pos != -1, "Comment 'turn on server debug' not found"
    assert datadict_debug_pos != -1, "self.datadict['debug'] = 1 not found"
    # Comment must appear just before the datadict['debug'] line
    between = src[debug_comment_pos:datadict_debug_pos]
    assert len(between) < 50, (
        "Comment 'turn on server debug' is too far from self.datadict['debug'] = 1. "
        "It should sit directly above that line."
    )


def test_koatap_init_still_sets_server_debug_flag():
    """Removing pid must not affect self.datadict['debug'] = 1.

    The TAP server debug flag must still be set after construction.
    """
    tap = KoaTap('https://koa.example.test/TAP')
    assert tap.datadict.get('debug') == 1, (
        "self.datadict['debug'] should be 1 after KoaTap.__init__. "
        "Removing pid may have accidentally removed the server debug flag."
    )


# ── Fix 2: bare except → except Exception ────────────────────────────────────

def test_no_bare_except_in_koatap_init():
    """KoaTap.__init__ must not use bare 'except:' around cookiejar.load().

    Bare except catches KeyboardInterrupt and SystemExit, which should never
    be swallowed. except Exception catches all realistic cookie-file errors:
    http.cookiejar.LoadError, OSError, FileNotFoundError, PermissionError.
    """
    src = _method_source('__init__', after_line=4672)  # KoaTap.__init__ at line 4673
    assert 'except:' not in src, (
        "Bare 'except:' found in KoaTap.__init__. "
        "Use 'except Exception:' to avoid catching KeyboardInterrupt/SystemExit."
    )
    assert 'except Exception:' in src, (
        "'except Exception:' not found in KoaTap.__init__. "
        "Should be 'except Exception:' not bare 'except:'."
    )


def test_koatap_init_bad_cookie_file_raises_exception():
    """A malformed or missing cookie file must raise Exception('Error: failed to load cookie file.').

    This confirms except Exception: catches the cookie-load error correctly
    and re-raises it as a clean Exception for callers.
    """
    with pytest.raises(Exception, match='Error: failed to load cookie file'):
        KoaTap('https://koa.example.test/TAP',
               cookiefile='/nonexistent/cookies.txt')


# ── Fix 3: data = response.json() → bare call ─────────────────────────────────

def test_no_data_assignment_in_send_sync_json_branch():
    """send_sync's JSON branch must validate parseability without binding the result.

    The parsed JSON was never used after assignment — only the try/except
    is needed to detect parse failures. The bare call self.response.json()
    keeps the validation while dropping the unused name.
    """
    src = _method_source('send_sync')
    assert 'data = self.response.json()' not in src, (
        "Dead 'data = self.response.json()' found in send_sync. "
        "The parsed JSON is never used. Use bare 'self.response.json()' instead."
    )
    assert 'self.response.json()' in src, (
        "Bare 'self.response.json()' not found in send_sync. "
        "The call must remain to validate that the response is parseable JSON."
    )
    assert 'data = None' not in src, (
        "Dead 'data = None' initialisation found in send_sync. Remove it."
    )


# ── Fix 4: noqa on deferred import ───────────────────────────────────────────

def test_noqa_e402_on_deferred_core_import():
    """__init__.py must suppress E402 on the deferred import, not move it.

    The import must stay below conf = Conf() to avoid a circular import.
    core.py does 'from . import conf' at module scope — conf must already
    exist in __init__.py's namespace when that line runs.
    """
    init_src = INIT.read_text()
    assert 'from .core import' in init_src
    import_line = next(
        line for line in init_src.splitlines() if 'from .core import' in line
    )
    assert 'noqa: E402' in import_line, (
        "The 'from .core import ...' line is missing '# noqa: E402'. "
        "This import must stay below conf = Conf() to avoid a circular import; "
        "add the noqa comment to suppress the ruff warning."
    )


def test_deferred_import_still_after_conf_instantiation():
    """The 'from .core import' line must come AFTER conf = Conf() in __init__.py.

    Moving it before would cause ImportError: core.py imports conf from __init__
    at module scope, and conf wouldn't exist yet if the import were first.
    """
    init_src = INIT.read_text()
    conf_pos = init_src.find('conf = Conf()')
    import_pos = init_src.find('from .core import')
    assert conf_pos != -1 and import_pos != -1
    assert conf_pos < import_pos, (
        "'from .core import' appears before 'conf = Conf()' in __init__.py. "
        "This will cause a circular import. Keep the import after conf = Conf()."
    )


def test_package_still_importable():
    """After all Group D fixes, pykoa must still import cleanly."""
    import importlib
    import pykoa.koa
    importlib.reload(pykoa.koa)
    from pykoa.koa import Koa, Archive, KoaTap, KoaJob, conf
    assert all([Koa, Archive, KoaTap, KoaJob, conf])
