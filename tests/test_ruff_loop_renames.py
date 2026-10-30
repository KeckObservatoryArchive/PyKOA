"""Tests for E741 loop variable renames (l → i / row / subdir_idx).

Ruff E741 flags single-letter variables that look like numbers.
These tests confirm:
1. The renamed variables work correctly (loop logic is intact).
2. The old ambiguous name 'l' is gone from the relevant code sections.
3. Debug log strings were updated to match the new names.

Loops fixed:
- core.py:2329 — __download_moving_object_metadata result tables:  l → i
- core.py:2423 — __download_moving_object_metadata graph tables:   l → i
- core.py:3155 — download row iteration:                           l → row
- core.py:3835 — __download_lev1files subdir iteration:            l → subdir_idx
"""

import json
import pathlib
from unittest import mock

from pykoa.koa.core import Archive

SRC = pathlib.Path(__file__).parent.parent / "pykoa" / "koa" / "core.py"

# ── Group A guard — confirm all 'as fdebug' bindings are gone ────────────────


def test_no_unused_fdebug_binding_in_open_statements():
    """Group A: all 10 'with open(...) as fdebug: pass' must drop the binding.

    The file handle 'fdebug' was bound by the 'with' statement but never used —
    the block body was only 'pass'. Ruff F841 flags this as an unused variable.
    The fix keeps the truncation side-effect by dropping 'as fdebug':
        with open(debugfname, 'w'):
            pass
    This guard catches any of the 10 sites being accidentally reverted.
    """
    src = SRC.read_text()
    assert "as fdebug" not in src, (
        "'as fdebug' found in core.py — the file handle is never used. "
        "Use 'with open(...):' without binding the handle. "
        "10 sites were fixed; check all of them."
    )


# ── Static guards — confirm 'l' is gone and new names are present ─────────────


def test_no_ambiguous_l_in_resulttbl_loop():
    """core.py:2329 must use 'i' not 'l' to index resulttbls."""
    src = SRC.read_text()
    # The old pattern: for l in range (nresulttbl)
    assert "for l in range(nresulttbl)" not in src
    assert "for i in range(nresulttbl)" in src


def test_no_ambiguous_l_in_graphtbl_loop():
    """core.py:2423 must use 'i' not 'l' to index graphtbls."""
    src = SRC.read_text()
    assert "for l in range(ngraphtbl)" not in src
    assert "for i in range(ngraphtbl)" in src


def test_no_ambiguous_l_in_download_row_loop():
    """core.py:3155 must use 'row' not 'l' to iterate table rows."""
    src = SRC.read_text()
    assert "for l in range(srow, erow+1)" not in src
    assert "for row in range(srow, erow + 1)" in src
    # Debug log and closing comment updated too
    assert "f'l= {l:d}'" not in src
    assert 'f"row= {row:d}"' in src
    assert "endfor l in range (srow, erow+1)" not in src
    assert "endfor row in range (srow, erow+1)" in src


def test_no_ambiguous_l_in_subdir_loop():
    """core.py:3835 must use 'subdir_idx' not 'l' to iterate subdirs.

    Cannot use 'i' because there is a nested 'for i in range(nrec)' inside
    the same loop body — naming both 'i' would be confusing and fragile.
    """
    src = SRC.read_text()
    assert "for l in range(nsubdir)" not in src
    assert "for subdir_idx in range(nsubdir)" in src
    assert "f'l= {l:d} subdir=" not in src
    assert 'f"subdir_idx= {subdir_idx:d} subdir=' in src


# ── Behavioural tests — loop logic is intact after rename ─────────────────────


def test_download_moving_object_metadata_result_loop_iterates_all_tables(tmp_path):
    """__download_moving_object_metadata iterates every result table entry.

    The 'i' loop (was 'l') must index all entries in resulttbls correctly.
    We mock __get_moss_resultfile to capture what URLs were requested.
    """
    archive = Archive()
    jsondata = {
        "urlprefix": "https://koa.ipac.caltech.edu",
        "results": {
            "nresulttbl": 3,
            "ngraphtbl": 0,
            "resulttbls": [
                {"fileurl": "/results/tbl0.tbl"},
                {"fileurl": "/results/tbl1.tbl"},
                {"fileurl": "/results/tbl2.tbl"},
            ],
            "graphtbls": [],
        },
    }
    jsonpath = tmp_path / "result.json"
    jsonpath.write_text(json.dumps(jsondata))

    called_urls = []

    def fake_get_moss(url, resultpath, **kw):
        called_urls.append(url)

    with mock.patch.object(
        archive, "_Archive__get_moss_resultfile", side_effect=fake_get_moss
    ):
        archive._Archive__download_moving_object_metadata(str(jsonpath), str(tmp_path))

    # All 3 entries must have been fetched — loop ran for each i in range(3)
    assert len(called_urls) == 3
    assert any("tbl0.tbl" in u for u in called_urls)
    assert any("tbl1.tbl" in u for u in called_urls)
    assert any("tbl2.tbl" in u for u in called_urls)


def test_download_moving_object_metadata_graph_loop_iterates_all_tables(tmp_path):
    """The 'i' graph loop (was 'l') must index all graphtbls entries."""
    archive = Archive()
    jsondata = {
        "urlprefix": "https://koa.ipac.caltech.edu",
        "results": {
            "nresulttbl": 1,
            "ngraphtbl": 2,
            "resulttbls": [{"fileurl": "/results/dummy.tbl"}],
            "graphtbls": [
                {"graphfileurl": "/graphs/g0.png", "nrec": 0},
                {"graphfileurl": "/graphs/g1.png", "nrec": 0},
            ],
        },
    }
    jsonpath = tmp_path / "result.json"
    jsonpath.write_text(json.dumps(jsondata))
    (tmp_path / "graphs").mkdir()

    called_urls = []

    def fake_get_moss(url, resultpath, **kw):
        called_urls.append(url)

    with mock.patch.object(
        archive, "_Archive__get_moss_resultfile", side_effect=fake_get_moss
    ):
        archive._Archive__download_moving_object_metadata(str(jsonpath), str(tmp_path))

    graph_urls = [u for u in called_urls if ".png" in u]
    assert len(graph_urls) == 2
    assert any("g0.png" in u for u in graph_urls)
    assert any("g1.png" in u for u in graph_urls)
