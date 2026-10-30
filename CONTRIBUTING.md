# Contributing to PyKOA

PyKOA is a Python client for the Keck Observatory Archive (KOA) TAP service.
TAP (Table Access Protocol) is an IVOA standard for querying astronomical
data archives via SQL-like queries.

## Reporting bugs

Use the KOA helpdesk:
https://koa.ipac.caltech.edu/cgi-bin/Helpdesk/nph-genTicketForm?projname=KOA

For security vulnerabilities, see [SECURITY.md](SECURITY.md).

## Getting started

Browse [open issues](https://github.com/KeckObservatoryArchive/PyKOA/issues) to find something to work on, or open a new issue to discuss a bug or feature before submitting a PR.

## Development setup

Python 3.11 or later is required. CI runs on Python 3.11, 3.12, and 3.13; you only need to test locally on one version.

```sh
git clone https://github.com/KeckObservatoryArchive/PyKOA.git
cd PyKOA
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install --upgrade pip
pip install -e .
pip install pytest ruff
```

## Running tests

Tests do not require a live KOA account or network access. All external calls are mocked.

```sh
pytest tests/                        # run all tests
pytest tests/test_send_sync.py       # run a single file
pytest tests/ -k test_name           # run a single test by name
```

Bug fixes and new features should include a test. Add tests to the relevant file in `tests/`.

## Code style

Ruff is used for linting, import ordering, and formatting. Before opening a PR:

```sh
ruff check .          # check for violations
ruff check . --fix    # auto-fix what can be fixed
ruff format .         # format code
```

All violations must be resolved. Ruff is enforced in CI:
https://github.com/KeckObservatoryArchive/PyKOA/actions/workflows/tests.yml

## Docstrings

Use [NumPy docstring format](https://numpydoc.readthedocs.io/en/latest/format.html):

```python
def query(self, sql, outpath, format="ipac"):
    """
    Submit a SQL query to the KOA TAP service.

    Parameters
    ----------
    sql : str
        ADQL query string.
    outpath : str
        Path to write the output file.
    format : str, optional
        Output format: votable, ipac, csv, or tsv. Default is ipac.

    Returns
    -------
    None

    Raises
    ------
    RuntimeError
        If the query fails or the server returns an error.
    """
```

## Opening a pull request

External contributors should fork the repository first. Then:

1. Create a branch off `main`. Use a prefix to match CI push triggers: `fix/`, `feature/`, or `ci/`. CI also runs on all PRs targeting `main` regardless of branch name.
2. Make your changes
3. Run the tests and ruff check
4. Open a PR against `main`
5. CI must pass before merging

Releases are handled by maintainers.
