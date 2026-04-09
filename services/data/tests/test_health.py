"""Smoke tests for the FastAPI data service."""

from fastapi.testclient import TestClient

from main import app


def test_health_returns_json() -> None:
    client = TestClient(app)
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert "status" in body
    assert body["status"] in ("ok", "degraded")
    assert "pybaseball" in body


def test_season_defaults_import() -> None:
    from season_defaults import (
        DEFAULT_MIN_IP_SEASON_PITCHING,
        DEFAULT_MIN_PA_SEASON_BATTING,
        SEASON_STATS_ROW_CAP_MAX,
    )

    assert DEFAULT_MIN_IP_SEASON_PITCHING == 0.0
    assert DEFAULT_MIN_PA_SEASON_BATTING == 0.0
    assert SEASON_STATS_ROW_CAP_MAX == 300
