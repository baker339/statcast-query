"""Tests for optional pybaseball RestrictedPython sandbox."""

import pytest
from fastapi.testclient import TestClient

from pybaseball_sandbox import execute_sandbox_code, run_sandbox_in_subprocess, sandbox_feature_enabled


def test_sandbox_feature_flag_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ENABLE_PYBASEBALL_SANDBOX", raising=False)
    assert sandbox_feature_enabled() is False
    monkeypatch.setenv("ENABLE_PYBASEBALL_SANDBOX", "1")
    assert sandbox_feature_enabled() is True


def test_execute_sandbox_simple_json() -> None:
    r = execute_sandbox_code("RESULT = [{'x': 1}, {'x': 2}]", 10)
    assert r["ok"] is True
    assert r["result_kind"] == "json"
    assert r["value"] == [{"x": 1}, {"x": 2}]


def test_execute_sandbox_requires_result() -> None:
    r = execute_sandbox_code("x = 1", 10)
    assert r["ok"] is False
    assert "RESULT" in r["error"]


def test_execute_sandbox_rejects_import() -> None:
    r = execute_sandbox_code("import os\nRESULT = 1", 10)
    assert r["ok"] is False


def test_execute_sandbox_pandas_groupby() -> None:
    code = """
df = pd.DataFrame({"a": [1, 1], "b": [10, 20]})
RESULT = df.groupby("a")["b"].sum().reset_index().to_dict("records")
"""
    r = execute_sandbox_code(code, 10)
    assert r["ok"] is True
    assert r["result_kind"] == "json"
    assert r["value"] == [{"a": 1, "b": 30}]


def test_execute_sandbox_dataframe_table() -> None:
    code = """
df = pd.DataFrame({"a": [1, 1], "b": [10, 20]})
RESULT = df.groupby("a")["b"].sum().reset_index()
"""
    r = execute_sandbox_code(code, 10)
    assert r["ok"] is True
    assert r["result_kind"] == "table"
    assert r["columns"] == ["a", "b"]
    assert len(r["rows"]) == 1
    assert r["rows"][0]["a"] == 1
    assert r["rows"][0]["b"] == 30


def test_limited_pandas_blocks_read_csv() -> None:
    r = execute_sandbox_code("RESULT = pd.read_csv('foo.csv')", 10)
    assert r["ok"] is False


def test_subprocess_worker_smoke() -> None:
    r = run_sandbox_in_subprocess("RESULT = {'n': 2 + 2}", 50, timeout_sec=60)
    assert r["ok"] is True
    assert r["result_kind"] == "json"
    assert r["value"] == {"n": 4}


def test_http_route_disabled_without_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ENABLE_PYBASEBALL_SANDBOX", raising=False)
    from main import app

    client = TestClient(app)
    res = client.post("/v1/pybaseball_sandbox", json={"code": "RESULT = 1"})
    assert res.status_code == 403


def test_http_route_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENABLE_PYBASEBALL_SANDBOX", "1")
    from main import app

    client = TestClient(app)
    res = client.post(
        "/v1/pybaseball_sandbox",
        json={"code": "RESULT = [1, 2, 3]", "row_cap": 10, "timeout_sec": 60},
    )
    assert res.status_code == 200, res.text
    body = res.json()
    assert body.get("ok") is True
    assert body.get("value") == [1, 2, 3]
