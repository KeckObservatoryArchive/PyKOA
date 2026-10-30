"""End-to-end tests for the name resolver services used by query_object.

query_object resolves object names via KOA's resolver at:
    https://koa.ipac.caltech.edu/cgi-bin/Lookup/nph-lookup

That resolver cascades through three services in order:
    1. ExoPlanet Archive (IPAC) -- bright stars, planets, known exoplanet hosts
    2. Sesame/CDS -- covers SIMBAD (most astronomical objects)
    3. NED -- NASA/IPAC Extragalactic Database (galaxies, AGN, etc.)

These tests hit each service independently so nightly reports can
identify exactly which resolver is degraded when query_object fails.

Run selectively:
    pytest tests/test_e2e_resolvers.py -m e2e -v
"""

import pytest
import requests

pytestmark = pytest.mark.e2e

KOA_RESOLVER = "https://koa.ipac.caltech.edu/cgi-bin/Lookup/nph-lookup"
EXOPLANET_RESOLVER = (
    "https://exoplanetarchive.ipac.caltech.edu/cgi-bin/Lookup/nph-lookup"
)

# Timeout for resolver checks -- 30s is generous for a healthy service
RESOLVER_TIMEOUT = (10, 30)


# ---------------------------------------------------------------------------
# KOA resolver endpoint
# ---------------------------------------------------------------------------


def test_koa_resolver_reachable():
    """KOA resolver endpoint responds to HTTP request."""
    r = requests.get(
        KOA_RESOLVER,
        params={"location": "Vega"},
        timeout=RESOLVER_TIMEOUT,
    )
    assert r.status_code == 200, f"KOA resolver returned {r.status_code}"
    data = r.json()
    assert "stat" in data, "Expected JSON with 'stat' field"


def test_koa_resolver_returns_coordinates():
    """KOA resolver returns RA/Dec for a well-known bright star (Vega).

    Vega is in the ExoPlanet Archive directly so this passes even when
    SIMBAD/NED are down.
    """
    r = requests.get(
        KOA_RESOLVER,
        params={"location": "Vega"},
        timeout=RESOLVER_TIMEOUT,
    )
    data = r.json()
    assert data.get("stat") == "OK", (
        f"KOA resolver failed for Vega: {data.get('msg', data)}"
    )
    assert "ra2000" in data
    assert "dec2000" in data
    ra = float(data["ra2000"])
    dec = float(data["dec2000"])
    # Vega: RA ~279.23, Dec ~38.78
    assert 279.0 < ra < 280.0, f"Unexpected RA for Vega: {ra}"
    assert 38.0 < dec < 39.0, f"Unexpected Dec for Vega: {dec}"


# ---------------------------------------------------------------------------
# ExoPlanet Archive resolver (upstream of KOA resolver)
# ---------------------------------------------------------------------------


def test_exoplanet_resolver_reachable():
    """ExoPlanet Archive resolver endpoint responds to HTTP request."""
    r = requests.get(
        EXOPLANET_RESOLVER,
        params={"location": "Vega"},
        timeout=RESOLVER_TIMEOUT,
    )
    assert r.status_code == 200, f"ExoPlanet resolver returned {r.status_code}"
    data = r.json()
    assert "stat" in data


def test_exoplanet_resolver_known_star():
    """ExoPlanet Archive resolves a known bright star (Aldebaran).

    Aldebaran is in the ExoPlanet Archive directly. This test passes
    independently of SIMBAD/NED.
    """
    r = requests.get(
        EXOPLANET_RESOLVER,
        params={"location": "Aldebaran"},
        timeout=RESOLVER_TIMEOUT,
    )
    data = r.json()
    assert data.get("stat") == "OK", (
        f"ExoPlanet resolver failed for Aldebaran: {data.get('msg', data)}"
    )
    assert "ra2000" in data


# ---------------------------------------------------------------------------
# SIMBAD via KOA resolver (object not in ExoPlanet Archive)
# ---------------------------------------------------------------------------


def test_simbad_via_koa_resolver():
    """KOA resolver can resolve a galaxy name via SIMBAD/Sesame.

    NGC 1614 is not in the ExoPlanet Archive but is in SIMBAD.
    If this test fails while test_koa_resolver_returns_coordinates passes,
    SIMBAD/Sesame is degraded but ExoPlanet Archive is fine.
    """
    r = requests.get(
        KOA_RESOLVER,
        params={"location": "NGC 1614"},
        timeout=RESOLVER_TIMEOUT,
    )
    data = r.json()
    assert data.get("stat") == "OK", (
        f"SIMBAD lookup via KOA resolver failed for NGC 1614: {data.get('msg', data)}"
    )
    assert "ra2000" in data


# ---------------------------------------------------------------------------
# NED via KOA resolver (extragalactic object)
# ---------------------------------------------------------------------------


def test_ned_via_koa_resolver():
    """KOA resolver resolves a well-known extragalactic object (3C 273).

    Note: 3C 273 is in both SIMBAD and NED, so this test cannot isolate NED
    specifically. If this fails while test_simbad_via_koa_resolver passes,
    it indicates a resolver issue with extragalactic objects specifically.
    True NED isolation would require an object absent from SIMBAD, which is
    hard to guarantee reliably.
    """
    r = requests.get(
        KOA_RESOLVER,
        params={"location": "3C 273"},
        timeout=RESOLVER_TIMEOUT,
    )
    data = r.json()
    assert data.get("stat") == "OK", (
        f"KOA resolver failed for 3C 273: {data.get('msg', data)}"
    )
    assert "ra2000" in data
    ra = float(data["ra2000"])
    # 3C 273: RA ~187.28
    assert 186.0 < ra < 189.0, f"Unexpected RA for 3C 273: {ra}"
