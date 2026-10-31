"""Shared pytest fixtures for the pykoa test suite."""

import pytest

from pykoa.koa.core import Archive

# Fake server — tests never touch the real KOA endpoint.
TEST_SERVER = "https://koa.example.test/"


@pytest.fixture
def archive():
    """Archive instance pointed at the fake test server."""
    return Archive(server=TEST_SERVER)
