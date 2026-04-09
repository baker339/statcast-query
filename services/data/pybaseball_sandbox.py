"""
Optional sandboxed execution for pybaseball + pandas workflows.

Runs user code in a subprocess with RestrictedPython, a denylisted pandas/numpy
surface, hard timeouts, and a mandatory RESULT assignment. Disabled unless
ENABLE_PYBASEBALL_SANDBOX is set.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import traceback
from pathlib import Path
from typing import Any, Final

import numpy as np
import pandas as pd
from RestrictedPython import compile_restricted
from RestrictedPython.Eval import default_guarded_getitem, default_guarded_getiter
from RestrictedPython.Guards import (
    full_write_guard,
    guarded_iter_unpack_sequence,
    safer_getattr,
    safe_builtins,
)
from RestrictedPython.PrintCollector import PrintCollector

_MAX_CODE_LEN: Final[int] = 12_000
_DEFAULT_ROW_CAP: Final[int] = 200
_DEFAULT_TIMEOUT_SEC: Final[int] = 90

_PD_DENY: Final[frozenset[str]] = frozenset(
    {
        "read_csv",
        "read_excel",
        "read_feather",
        "read_parquet",
        "read_pickle",
        "read_sql",
        "read_sql_query",
        "read_sql_table",
        "read_html",
        "read_clipboard",
        "read_sas",
        "read_spss",
        "read_stata",
        "read_gbq",
        "read_orc",
        "read_table",
        "read_json",
        "ExcelFile",
        "ExcelWriter",
        "to_pickle",
    }
)

_NP_DENY: Final[frozenset[str]] = frozenset(
    {
        "load",
        "loads",
        "fromfile",
        "frombuffer",
        "memmap",
        "lib",
        "ctypeslib",
        "distutils",
    }
)


class _LimitedModule:
    __slots__ = ("_mod", "_deny", "_label")

    def __init__(self, mod: Any, deny: frozenset[str], label: str) -> None:
        object.__setattr__(self, "_mod", mod)
        object.__setattr__(self, "_deny", deny)
        object.__setattr__(self, "_label", label)

    def __getattr__(self, name: str) -> Any:
        if name in object.__getattribute__(self, "_deny"):
            raise NotImplementedError(f"{object.__getattribute__(self, '_label')}.{name} is disabled in sandbox")
        return getattr(object.__getattribute__(self, "_mod"), name)


def sandbox_feature_enabled() -> bool:
    v = os.environ.get("ENABLE_PYBASEBALL_SANDBOX", "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _enable_pybaseball_cache() -> None:
    try:
        from pybaseball import cache as pyb_cache

        pyb_cache.enable()
    except Exception:  # pragma: no cover
        pass


def _inplacevar_(op: str, x: Any, y: Any) -> Any:
    if op == "+=":
        return x + y
    if op == "-=":
        return x - y
    if op == "*=":
        return x * y
    if op == "/=":
        return x / y
    if op == "//=":
        return x // y
    if op == "%=":
        return x % y
    if op == "**=":
        return x**y
    if op == "<<=":
        return x << y
    if op == ">>=":
        return x >> y
    if op == "&=":
        return x & y
    if op == "|=":
        return x | y
    if op == "^=":
        return x ^ y
    raise NotImplementedError(op)


def _print_(_getattr_: Any) -> PrintCollector:
    return PrintCollector(_getattr_)


def _df_to_payload(df: pd.DataFrame, row_cap: int) -> dict[str, Any]:
    if df is None or df.empty:
        return {"columns": [], "rows": []}
    df = df.head(int(row_cap)).copy()
    df = df.replace([float("inf"), float("-inf")], None)
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str)
        elif df[col].dtype == object:
            df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)
    rows = json.loads(df.to_json(orient="records", date_format="iso"))
    return {"columns": list(df.columns), "rows": rows}


def _serialize_result(raw: Any, row_cap: int) -> tuple[dict[str, Any], str]:
    if isinstance(raw, pd.DataFrame):
        return _df_to_payload(raw, row_cap), "table"
    if isinstance(raw, pd.Series):
        name = raw.name if raw.name is not None else "value"
        fr = raw.reset_index()
        if len(fr.columns) == 2:
            fr.columns = ["key", str(name)]
        return _df_to_payload(fr, row_cap), "table"
    try:
        out = json.loads(json.dumps(raw, default=str))
    except (TypeError, ValueError) as e:
        raise ValueError(f"RESULT is not JSON-serializable: {e}") from e
    if isinstance(out, list):
        out = out[: int(row_cap)]
    return {"value": out}, "json"


def build_restricted_globals(extra: dict[str, Any] | None = None) -> dict[str, Any]:
    """Execution globals for RestrictedPython ``exec`` (tests and worker)."""
    _enable_pybaseball_cache()
    try:
        from pybaseball import (
            batting_stats,
            fielding_stats,
            pitching_stats,
            playerid_lookup,
            statcast,
            statcast_batter,
            statcast_pitcher,
            statcast_pitcher_pitch_arsenal,
            statcast_single_game,
        )
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(f"pybaseball import failed: {e}") from e

    import datetime as datetime_mod

    g: dict[str, Any] = {
        "__builtins__": safe_builtins,
        "_getattr_": safer_getattr,
        "_getitem_": default_guarded_getitem,
        "_getiter_": default_guarded_getiter,
        "_write_": full_write_guard,
        "_iter_unpack_sequence_": guarded_iter_unpack_sequence,
        "_inplacevar_": _inplacevar_,
        "_print_": _print_,
        "pd": _LimitedModule(pd, _PD_DENY, "pandas"),
        "np": _LimitedModule(np, _NP_DENY, "numpy"),
        "statcast": statcast,
        "statcast_batter": statcast_batter,
        "statcast_pitcher": statcast_pitcher,
        "statcast_single_game": statcast_single_game,
        "statcast_pitcher_pitch_arsenal": statcast_pitcher_pitch_arsenal,
        "batting_stats": batting_stats,
        "pitching_stats": pitching_stats,
        "fielding_stats": fielding_stats,
        "playerid_lookup": playerid_lookup,
        "datetime": datetime_mod,
        "json": json,
    }
    if extra:
        g.update(extra)
    return g


def execute_sandbox_code(code: str, row_cap: int) -> dict[str, Any]:
    """
    Run restricted code in-process. Used by tests; production uses subprocess.
    """
    if len(code) > _MAX_CODE_LEN:
        return {"ok": False, "error": f"code exceeds max length {_MAX_CODE_LEN}"}
    try:
        byte_code = compile_restricted(code, "<sandbox>", "exec")
    except SyntaxError as e:
        return {"ok": False, "error": f"restricted compile: {e}"}

    g = build_restricted_globals()
    loc: dict[str, Any] = {}
    try:
        exec(byte_code, g, loc)
    except Exception as e:
        return {"ok": False, "error": str(e), "traceback": traceback.format_exc()}

    printed = ""
    pr = loc.get("_print")
    if callable(pr):
        printed = pr()

    if "RESULT" not in loc:
        return {
            "ok": False,
            "error": "Code must assign to RESULT (e.g. RESULT = df.head(20).to_dict('records')).",
            "printed": printed,
        }
    raw_result = loc["RESULT"]

    try:
        payload, kind = _serialize_result(raw_result, row_cap)
    except ValueError as e:
        return {"ok": False, "error": str(e), "printed": printed}

    out: dict[str, Any] = {
        "ok": True,
        "result_kind": kind,
        "printed": printed,
        "source": "pybaseball_sandbox",
        "note": (
            "RestrictedPython sandbox; pybaseball/pandas/numpy only (no imports in code). "
            "pandas file I/O helpers are disabled. Prefer named HTTP tools when they fit."
        ),
    }
    out.update(payload)
    return out


def run_sandbox_in_subprocess(code: str, row_cap: int, timeout_sec: int) -> dict[str, Any]:
    worker = Path(__file__).resolve()
    payload = json.dumps({"code": code, "row_cap": row_cap})
    try:
        proc = subprocess.run(
            [sys.executable, str(worker), "--sandbox-worker"],
            input=payload,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            env=os.environ.copy(),
            cwd=str(worker.parent),
        )
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": f"sandbox timeout after {timeout_sec}s"}

    if proc.returncode != 0 and not proc.stdout.strip():
        err = (proc.stderr or "").strip() or "worker exited non-zero"
        return {"ok": False, "error": err, "stderr": (proc.stderr or "")[:4000]}

    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError:
        return {
            "ok": False,
            "error": "invalid worker output",
            "stderr": (proc.stderr or "")[:2000],
            "stdout": (proc.stdout or "")[:2000],
        }


def _worker_main() -> None:
    data = json.load(sys.stdin)
    code = data.get("code", "")
    row_cap = int(data.get("row_cap", _DEFAULT_ROW_CAP))
    result = execute_sandbox_code(code, row_cap)
    sys.stdout.write(json.dumps(result, default=str))


if __name__ == "__main__" and "--sandbox-worker" in sys.argv:
    _worker_main()
