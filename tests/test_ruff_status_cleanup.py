"""Tests for Group C ruff cleanup — dead `status = 'error'` assignments removed.

Background
----------
Three Archive private helper methods had this pattern:

    except Exception as e:
        status = 'error'        # assigned but never read
        msg = str(e)
        raise Exception(msg)    # exits the function here

Setting `status = 'error'` looks intentional but has no effect.
Because `raise` exits the function immediately, no code can ever
read that local `status` variable. The caller never sees it.

How errors ARE correctly signaled in these methods
--------------------------------------------------
These private helpers signal failure by **raising an Exception**.
Their callers use try/except to handle it:

    try:
        self.__get_moss_resultfile(url, path)
    except Exception as e:
        print(str(e))   # caller handles the raised exception

This is different from KoaTap.send_sync / send_async, which set
`self.status = 'error'` (an INSTANCE attribute) and return a message
string. Callers of those methods can check `tap.status` afterward.
Archive private helpers don't work that way — they raise.

What these tests verify
-----------------------
1. The Exception IS raised with the correct message (behavior is intact).
2. No `self.status` is set on the Archive instance (correct design —
   these methods use raise, not status fields).
3. The deleted `status = 'error'` lines are confirmed absent.
"""

import json
import pathlib
from unittest import mock

import pytest

from pykoa.koa.core import Archive

SRC = pathlib.Path(__file__).parent.parent / 'pykoa' / 'koa' / 'core.py'


def _method_source(method_name):
    """Extract source lines for a named method from core.py.

    WARNING: Uses string-matching, not AST parsing. Stops at the next 'def'
    keyword, so it will return incomplete source if the target method ever gains
    a nested function. The methods currently tested have no nested functions.
    If that changes, switch to ast.parse() with FunctionDef.lineno/end_lineno.
    """
    src = SRC.read_text().splitlines()
    in_method = False
    method_lines = []
    for line in src:
        if f'def {method_name}' in line:
            in_method = True
        elif in_method and line.strip().startswith('def '):
            break
        if in_method:
            method_lines.append(line)
    return '\n'.join(method_lines)


# ── Static guards: confirm the dead assignments are gone ──────────────────────

def test_status_error_removed_from_get_moss_resultfile():
    """__get_moss_resultfile must not have a dead local `status = 'error'`.

    This method raises an Exception when the network call fails.
    Setting a local `status` variable before the raise has no effect —
    the raise exits the function before anyone can read it.
    The correct signal to callers is the raised Exception itself.
    """
    method_src = _method_source('__get_moss_resultfile')
    assert "status = 'error'" not in method_src, (
        "__get_moss_resultfile still has 'status = \"error\"' — "
        "this is dead code. The method signals errors by raising Exception. "
        "Remove the assignment; keep the raise."
    )
    assert "status = ''" not in method_src, (
        "__get_moss_resultfile still has 'status = \"\"' initialisation — "
        "status is never read in this method. Remove it."
    )


def test_status_error_removed_from_download_calibfiles():
    """__download_calibfiles must not have a dead local `status = 'error'`.

    When nrec == 0 (empty caliblist), the method raises Exception immediately.
    The local `status = 'error'` set just before the raise is never read —
    the raise exits the function. The caller receives the Exception, not status.
    """
    method_src = _method_source('__download_calibfiles')
    assert "status = 'error'" not in method_src, (
        "__download_calibfiles still has 'status = \"error\"' before a raise — "
        "this is dead code. The raise already signals the error. Remove it."
    )


# ── Behavioural: Exception is raised with the right message ───────────────────

def test_get_moss_resultfile_network_failure_raises_correct_message():
    """When the network call fails, __get_moss_resultfile must raise an Exception
    whose message is the network error string.

    This confirms that removing `status = 'error'` did NOT break error signaling.
    The Exception is the error signal — not a status variable.
    """
    archive = Archive()

    with mock.patch('pykoa.koa.core.requests.get',
                    side_effect=ConnectionError('connection refused')):
        with pytest.raises(Exception, match='connection refused'):
            archive._Archive__get_moss_resultfile(
                'https://koa.ipac.caltech.edu/fake/result.tbl',
                '/tmp/out.tbl'
            )


def test_get_moss_resultfile_does_not_set_self_status_on_failure():
    """__get_moss_resultfile must NOT set self.status on the Archive instance.

    This method signals errors by raising — callers catch the Exception.
    Setting self.status would be the KoaTap pattern (send_sync / send_async),
    not the Archive private helper pattern. Confirm the design is correct.
    """
    archive = Archive()
    status_before = getattr(archive, 'status', 'NOT_SET')

    with mock.patch('pykoa.koa.core.requests.get',
                    side_effect=ConnectionError('connection refused')):
        with pytest.raises(Exception):
            archive._Archive__get_moss_resultfile(
                'https://koa.ipac.caltech.edu/fake/result.tbl',
                '/tmp/out.tbl'
            )

    status_after = getattr(archive, 'status', 'NOT_SET')
    assert status_after == status_before, (
        f"__get_moss_resultfile set self.status='{status_after}' on failure. "
        "This method signals errors by raising, not by setting self.status. "
        "Check if self.status was accidentally added."
    )


def test_download_calibfiles_empty_list_raises_correct_message(tmp_path):
    """When the caliblist JSON has no entries, __download_calibfiles must raise
    Exception('No data found in the caliblist: <path>').

    This confirms that removing `status = 'error'` before the raise did NOT
    break the error. The raise is the error signal — status was just dead code.
    """
    archive = Archive()
    caliblist = {'table': []}
    listpath = tmp_path / 'caliblist.json'
    listpath.write_text(json.dumps(caliblist))

    with pytest.raises(Exception, match='No data found in the caliblist'):
        archive._Archive__download_calibfiles(
            str(listpath), None, str(tmp_path)
        )
