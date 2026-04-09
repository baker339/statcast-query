"""
Statcast + FanGraphs (pybaseball) + MLB Stats API leaders — microservice with TTL caching and strict caps.
FanGraphs tables are optional; official leaderboards use statsapi.mlb.com (no scrape).
"""

from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Literal

from season_defaults import DEFAULT_MIN_IP_SEASON_PITCHING, SEASON_STATS_ROW_CAP_MAX

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None  # type: ignore[misc, assignment]


def _load_service_env_files() -> None:
    """Load ``services/data/.env`` so uvicorn picks up keys without manual export."""
    if load_dotenv is None:
        return
    root = Path(__file__).resolve().parent
    for name in (".env", ".env.local"):
        env_path = root / name
        if env_path.is_file():
            load_dotenv(env_path, override=False)


_load_service_env_files()

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

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
    batting_stats = None  # type: ignore
    fielding_stats = None  # type: ignore
    pitching_stats = None  # type: ignore
    playerid_lookup = None  # type: ignore
    statcast = None  # type: ignore
    statcast_batter = None  # type: ignore
    statcast_pitcher = None  # type: ignore
    statcast_pitcher_pitch_arsenal = None  # type: ignore
    statcast_single_game = None  # type: ignore
    _IMPORT_ERROR = str(e)
else:
    _IMPORT_ERROR = ""
    # Disk cache: speeds repeat identical pybaseball calls and helps recover partial
    # progress on long Statcast pulls. See https://github.com/jldbc/pybaseball#caching
    try:
        from pybaseball import cache as pyb_cache

        pyb_cache.enable()
    except Exception:  # pragma: no cover
        pass

CACHE_TTL_SEC = 300
_cache: dict[str, tuple[float, Any]] = {}


def _cache_get(key: str) -> Any | None:
    now = time.time()
    hit = _cache.get(key)
    if not hit:
        return None
    ts, val = hit
    if now - ts > CACHE_TTL_SEC:
        del _cache[key]
        return None
    return val


def _cache_set(key: str, val: Any) -> None:
    _cache[key] = (time.time(), val)


MLB_STATS_API = "https://statsapi.mlb.com/api/v1"

# MLB Stats API `leaderCategories` query values (camelCase). See /api/v1/stats/leaders.
ALLOWED_MLB_LEADER_CATEGORIES = frozenset(
    {
        "strikeOuts",
        "homeRuns",
        "runs",
        "rbi",
        "hits",
        "doubles",
        "triples",
        "stolenBases",
        "battingAverage",
        "onBasePercentage",
        "sluggingPercentage",
        "onBasePlusSlugging",
        "baseOnBalls",
        "wins",
        "saves",
        "earnedRunAverage",
        "walksAndHitsPerInningPitched",
        "inningsPitched",
        "hitByPitch",
    }
)


def _http_get_json(url: str, timeout: float = 45.0) -> Any:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "statcast-query-data/0.1 (+local pybaseball wrapper)"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")[:2000]
        raise HTTPException(502, detail=f"MLB Stats API HTTP {e.code}: {body}") from e
    except urllib.error.URLError as e:
        raise HTTPException(502, detail=f"MLB Stats API network error: {e}") from e


def _default_stat_group_for_leader_category(leader_category: str) -> Literal["pitching", "hitting"]:
    pitching_only = {
        "wins",
        "saves",
        "earnedRunAverage",
        "walksAndHitsPerInningPitched",
        "inningsPitched",
    }
    if leader_category in pitching_only:
        return "pitching"
    if leader_category == "strikeOuts":
        return "pitching"
    return "hitting"


def _mlb_leaders_table(data: dict[str, Any]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for block in data.get("leagueLeaders") or []:
        cat = block.get("leaderCategory")
        for L in block.get("leaders") or []:
            person = L.get("person") or {}
            team = L.get("team") or {}
            league = L.get("league") or {}
            rows.append(
                {
                    "rank": L.get("rank"),
                    "value": L.get("value"),
                    "player": person.get("fullName"),
                    "mlbam_id": person.get("id"),
                    "team_abbr": team.get("abbreviation") or team.get("abbreviationName"),
                    "team_name": team.get("name"),
                    "league": league.get("abbreviation") or league.get("name"),
                    "leader_category": cat,
                }
            )
    if not rows:
        return {"columns": [], "rows": [], "note": "MLB leaders response contained no rows."}
    cols = [
        "rank",
        "value",
        "player",
        "mlbam_id",
        "team_abbr",
        "team_name",
        "league",
        "leader_category",
    ]
    return {"columns": cols, "rows": rows}


# Savant / common aliases → MLB Stats API team.abbreviation (from /teams?sportId=1&season=)
_MLB_TEAM_ABBR_ALIASES: dict[str, str] = {
    "WSN": "WSH",
    "WAS": "WSH",
    "TBR": "TB",
    "TBA": "TB",
    "SDP": "SD",
    "SFG": "SF",
    "KCR": "KC",
}


def _mlb_team_map_for_season(season: int) -> dict[str, int]:
    cache_key = f"mlb:teams:{season}"
    hit = _cache_get(cache_key)
    if isinstance(hit, dict):
        return hit
    data = _http_get_json(f"{MLB_STATS_API}/teams?sportId=1&season={season}")
    m: dict[str, int] = {}
    for t in data.get("teams") or []:
        ab = str(t.get("abbreviation") or "").upper()
        tid = t.get("id")
        if ab and tid is not None:
            m[ab] = int(tid)
    _cache_set(cache_key, m)
    return m


def _mlb_resolve_team_id(team_abbr: str, season: int) -> int | None:
    u = team_abbr.strip().upper()
    u = _MLB_TEAM_ABBR_ALIASES.get(u, u)
    return _mlb_team_map_for_season(season).get(u)


def _mlb_team_abbr_by_id(season: int) -> dict[int, str]:
    m = _mlb_team_map_for_season(season)
    return {int(tid): ab for ab, tid in m.items()}


def _safe_int_mlb(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _mlb_parse_rate(v: Any) -> float | str | None:
    if v is None:
        return None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    s = str(v).strip()
    if s in ("", ".---", "-.--", "---"):
        return None
    try:
        return float(s)
    except ValueError:
        return s


def _parse_innings_mlb(v: Any) -> float | None:
    """MLB innings strings use .0/.1/.2 for outs (e.g. 138.1 = 138⅓)."""
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    if "." not in s:
        try:
            return float(s)
        except ValueError:
            return None
    whole_s, outs_s = s.rsplit(".", 1)
    try:
        whole = float(whole_s) if whole_s else 0.0
        outs = int(outs_s)
        if outs not in (0, 1, 2):
            return None
        return whole + outs / 3.0
    except (TypeError, ValueError):
        return None


def _split_to_batting_metric_row(split: dict[str, Any], metrics: list[str]) -> dict[str, Any]:
    """Map one MLB /stats hitting split into FanGraphs-shaped column names."""
    player = split.get("player") or {}
    team = split.get("team") or {}
    st = split.get("stat") or {}
    pa = _safe_int_mlb(st.get("plateAppearances")) or 0
    bb = _safe_int_mlb(st.get("baseOnBalls")) or 0
    so = _safe_int_mlb(st.get("strikeOuts")) or 0
    out: dict[str, Any] = {}
    for m in metrics:
        if m == "Name":
            out[m] = player.get("fullName")
        elif m == "Team":
            out[m] = team.get("abbreviation") or team.get("name")
        elif m == "G":
            out[m] = _safe_int_mlb(st.get("gamesPlayed"))
        elif m == "PA":
            out[m] = pa if pa else None
        elif m == "HR":
            out[m] = _safe_int_mlb(st.get("homeRuns"))
        elif m == "R":
            out[m] = _safe_int_mlb(st.get("runs"))
        elif m == "RBI":
            out[m] = _safe_int_mlb(st.get("rbi"))
        elif m == "SB":
            out[m] = _safe_int_mlb(st.get("stolenBases"))
        elif m == "BB":
            out[m] = bb if bb else None
        elif m == "SO":
            out[m] = so if so else None
        elif m == "AVG":
            out[m] = _mlb_parse_rate(st.get("avg"))
        elif m == "OBP":
            out[m] = _mlb_parse_rate(st.get("obp"))
        elif m == "SLG":
            out[m] = _mlb_parse_rate(st.get("slg"))
        elif m == "OPS":
            out[m] = _mlb_parse_rate(st.get("ops"))
        elif m == "ISO":
            slg = _mlb_parse_rate(st.get("slg"))
            avg = _mlb_parse_rate(st.get("avg"))
            if isinstance(slg, float) and isinstance(avg, float):
                out[m] = round(slg - avg, 3)
            else:
                out[m] = None
        elif m == "BABIP":
            out[m] = _mlb_parse_rate(st.get("babip"))
        elif m == "BB%":
            out[m] = round(100.0 * bb / pa, 1) if pa > 0 else None
        elif m == "K%":
            out[m] = round(100.0 * so / pa, 1) if pa > 0 else None
        elif m in ("wOBA", "wRC+", "WAR"):
            out[m] = None
        else:
            out[m] = None
    return out


def _batting_season_from_mlb_api(body: BattingSeasonBody, fg_error: str | None) -> dict[str, Any]:
    """FanGraphs-free hitting lines via statsapi.mlb.com (season, group=hitting)."""
    team_id: int | None = None
    if body.team_abbr:
        team_id = _mlb_resolve_team_id(body.team_abbr, body.season)
        if team_id is None:
            raise HTTPException(
                400,
                detail=f"Unknown team_abbr {body.team_abbr!r} for MLB lookup (season {body.season}).",
            )

    need = int(body.row_cap)
    min_pa_f = float(body.min_pa)
    collected: list[dict[str, Any]] = []
    offset = 0
    total_splits = 10**9
    pages = 0
    max_pages = 10 if team_id is None else 3
    target_splits = max(need * 3, 200)
    if body.name_contains and team_id is None:
        max_pages = 30
        target_splits = min(6000, total_splits)

    while len(collected) < target_splits and offset < total_splits and pages < max_pages:
        pages += 1
        params: dict[str, str] = {
            "stats": "season",
            "group": "hitting",
            "season": str(body.season),
            "sportId": "1",
            "playerPool": "ALL",
            "limit": "200",
            "sortStat": "plateAppearances",
            "offset": str(offset),
        }
        if team_id is not None:
            params["teamId"] = str(team_id)
        url = f"{MLB_STATS_API}/stats?{urllib.parse.urlencode(params)}"
        payload = _http_get_json(url)
        stats_blk = (payload.get("stats") or [])
        if not stats_blk:
            break
        blk = stats_blk[0]
        splits = blk.get("splits") or []
        total_splits = int(blk.get("totalSplits") or 0)
        collected.extend(splits)
        offset += len(splits)
        if not splits:
            break
        if team_id is not None:
            break

    rows_out: list[dict[str, Any]] = []
    for sp in collected:
        row = _split_to_batting_metric_row(sp, list(body.metrics))
        pa = row.get("PA")
        if pa is None:
            continue
        try:
            if float(pa) < min_pa_f:
                continue
        except (TypeError, ValueError):
            continue
        rows_out.append(row)

    if body.name_contains:
        nc = body.name_contains.strip().lower()
        rows_out = [r for r in rows_out if r.get("Name") and nc in str(r["Name"]).lower()]

    rows_out = rows_out[:need]

    if not rows_out:
        raise HTTPException(
            502,
            detail=(
                "MLB Stats API hitting fallback returned no rows after filters "
                f"(min_pa={body.min_pa}, team_abbr={body.team_abbr!r}, "
                f"name_contains={body.name_contains!r}). "
                f"FanGraphs error was: {fg_error}"
            ),
        )

    df = pd.DataFrame(rows_out)
    table = _df_to_payload(df, body.row_cap)
    note_parts = [
        "Hitting data from **MLB Stats API** (official), not FanGraphs — works when FanGraphs blocks requests.",
        "Columns **wOBA**, **wRC+**, and **WAR** are not in this MLB endpoint; they are blank here. **OPS** is filled from MLB when requested.",
    ]
    if fg_error:
        note_parts.append(f"FanGraphs fetch failed: {fg_error}")
    return {**table, "source": "mlb_stats_api_hitting_season", "note": " ".join(note_parts)}


def _split_to_pitching_metric_row(
    split: dict[str, Any], metrics: list[str], abbr_by_tid: dict[int, str]
) -> dict[str, Any]:
    player = split.get("player") or {}
    team = split.get("team") or {}
    st = split.get("stat") or {}
    tid = team.get("id")
    team_lbl: str | None = None
    if tid is not None:
        try:
            team_lbl = abbr_by_tid.get(int(tid))
        except (TypeError, ValueError):
            team_lbl = None
    if not team_lbl:
        team_lbl = team.get("abbreviation") or team.get("name")
    out: dict[str, Any] = {}
    for m in metrics:
        if m == "Name":
            out[m] = player.get("fullName")
        elif m == "Team":
            out[m] = team_lbl
        elif m == "IP":
            out[m] = st.get("inningsPitched")
        elif m == "SO":
            out[m] = _safe_int_mlb(st.get("strikeOuts"))
        elif m == "BB":
            out[m] = _safe_int_mlb(st.get("baseOnBalls"))
        elif m == "ERA":
            out[m] = _mlb_parse_rate(st.get("era"))
        elif m == "G":
            out[m] = _safe_int_mlb(st.get("gamesPlayed"))
        elif m == "SV":
            out[m] = _safe_int_mlb(st.get("saves"))
        elif m == "H":
            out[m] = _safe_int_mlb(st.get("hits"))
        elif m == "HR":
            out[m] = _safe_int_mlb(st.get("homeRuns"))
        elif m == "WHIP":
            out[m] = _mlb_parse_rate(st.get("whip"))
        elif m == "WAR":
            out[m] = None
        else:
            out[m] = None
    return out


def _pitching_season_from_mlb_api(
    body: PitchingSeasonBody,
    fg_error: str | None,
    *,
    _relaxed_min_ip: bool = False,
) -> dict[str, Any]:
    team_id: int | None = None
    if body.team_abbr:
        team_id = _mlb_resolve_team_id(body.team_abbr, body.season)
        if team_id is None:
            raise HTTPException(
                400,
                detail=f"Unknown team_abbr {body.team_abbr!r} for MLB lookup (season {body.season}).",
            )

    need = int(body.row_cap)
    min_ip_f = float(body.min_ip)
    abbr_by_tid = _mlb_team_abbr_by_id(body.season)
    collected: list[dict[str, Any]] = []
    offset = 0
    total_splits = 10**9
    pages = 0
    max_pages = 10 if team_id is None else 3
    target_splits = max(need * 3, 200)
    if body.name_contains and team_id is None:
        max_pages = 30
        target_splits = min(6000, total_splits)

    while len(collected) < target_splits and offset < total_splits and pages < max_pages:
        pages += 1
        params: dict[str, str] = {
            "stats": "season",
            "group": "pitching",
            "season": str(body.season),
            "sportId": "1",
            "playerPool": "ALL",
            "limit": "200",
            "sortStat": "inningsPitched",
            "offset": str(offset),
        }
        if team_id is not None:
            params["teamId"] = str(team_id)
        url = f"{MLB_STATS_API}/stats?{urllib.parse.urlencode(params)}"
        payload = _http_get_json(url)
        stats_blk = (payload.get("stats") or [])
        if not stats_blk:
            break
        blk = stats_blk[0]
        splits = blk.get("splits") or []
        total_splits = int(blk.get("totalSplits") or 0)
        collected.extend(splits)
        offset += len(splits)
        if not splits:
            break
        if team_id is not None:
            break

    rows_out: list[dict[str, Any]] = []
    for sp in collected:
        row = _split_to_pitching_metric_row(sp, list(body.metrics), abbr_by_tid)
        ip_val = _parse_innings_mlb(row.get("IP"))
        if ip_val is None or ip_val < min_ip_f:
            continue
        rows_out.append(row)

    if body.name_contains:
        nc = body.name_contains.strip().lower()
        rows_out = [r for r in rows_out if r.get("Name") and nc in str(r["Name"]).lower()]

    rows_out = rows_out[:need]

    if not rows_out:
        # Early season / relievers: default min_ip (e.g. 20) can exclude every pitcher on a team.
        if float(body.min_ip) > 0 and not _relaxed_min_ip:
            return _pitching_season_from_mlb_api(
                body.model_copy(update={"min_ip": 0.0}),
                fg_error,
                _relaxed_min_ip=True,
            )
        raise HTTPException(
            502,
            detail=(
                "MLB Stats API pitching fallback returned no rows after filters "
                f"(min_ip={body.min_ip}, team_abbr={body.team_abbr!r}, "
                f"name_contains={body.name_contains!r}). "
                f"FanGraphs error was: {fg_error}"
            ),
        )

    df = pd.DataFrame(rows_out)
    table = _df_to_payload(df, body.row_cap)
    note_parts = [
        "Pitching data from **MLB Stats API** (official), not FanGraphs — works when FanGraphs blocks requests.",
        "**WAR** is not in this MLB season endpoint; it is blank here.",
    ]
    if fg_error:
        note_parts.append(f"FanGraphs fetch failed: {fg_error}")
    if _relaxed_min_ip:
        note_parts.append(
            "Note: **min_ip was auto-relaxed to 0** because the requested IP floor returned no rows "
            "(common early in the season or for reliever-heavy questions)."
        )
    return {**table, "source": "mlb_stats_api_pitching_season", "note": " ".join(note_parts)}


def _split_to_fielding_metric_row(
    split: dict[str, Any], metrics: list[str], abbr_by_tid: dict[int, str]
) -> dict[str, Any]:
    player = split.get("player") or {}
    team = split.get("team") or {}
    pos = split.get("position") or {}
    st = split.get("stat") or {}
    tid = team.get("id")
    team_lbl: str | None = None
    if tid is not None:
        try:
            team_lbl = abbr_by_tid.get(int(tid))
        except (TypeError, ValueError):
            team_lbl = None
    if not team_lbl:
        team_lbl = team.get("abbreviation") or team.get("name")
    out: dict[str, Any] = {}
    for m in metrics:
        if m == "Name":
            out[m] = player.get("fullName")
        elif m == "Team":
            out[m] = team_lbl
        elif m == "Pos":
            out[m] = pos.get("abbreviation") or pos.get("name")
        elif m == "G":
            out[m] = _safe_int_mlb(st.get("gamesPlayed"))
        elif m == "GS":
            out[m] = _safe_int_mlb(st.get("gamesStarted"))
        elif m == "Inn":
            out[m] = st.get("innings")
        elif m == "PO":
            out[m] = _safe_int_mlb(st.get("putOuts"))
        elif m == "A":
            out[m] = _safe_int_mlb(st.get("assists"))
        elif m == "E":
            out[m] = _safe_int_mlb(st.get("errors"))
        elif m == "DP":
            out[m] = _safe_int_mlb(st.get("doublePlays"))
        elif m == "FP":
            out[m] = _mlb_parse_rate(st.get("fielding"))
        elif m == "RF9":
            out[m] = _mlb_parse_rate(st.get("rangeFactorPer9Inn"))
        elif m in ("DRS", "UZR", "DEF"):
            out[m] = None
        else:
            out[m] = None
    return out


def _fielding_season_from_mlb_api(body: FieldingSeasonBody, fg_error: str | None) -> dict[str, Any]:
    team_id: int | None = None
    if body.team_abbr:
        team_id = _mlb_resolve_team_id(body.team_abbr, body.season)
        if team_id is None:
            raise HTTPException(
                400,
                detail=f"Unknown team_abbr {body.team_abbr!r} for MLB lookup (season {body.season}).",
            )

    need = int(body.row_cap)
    min_inn_f = float(body.min_inn)
    abbr_by_tid = _mlb_team_abbr_by_id(body.season)
    collected: list[dict[str, Any]] = []
    offset = 0
    total_splits = 10**9
    pages = 0
    max_pages = 12 if team_id is None else 3
    target_splits = max(need * 4, 300)
    if body.name_contains and team_id is None:
        max_pages = 35
        target_splits = min(8000, total_splits)

    while len(collected) < target_splits and offset < total_splits and pages < max_pages:
        pages += 1
        params: dict[str, str] = {
            "stats": "season",
            "group": "fielding",
            "season": str(body.season),
            "sportId": "1",
            "playerPool": "ALL",
            "limit": "200",
            "sortStat": "innings",
            "offset": str(offset),
        }
        if team_id is not None:
            params["teamId"] = str(team_id)
        url = f"{MLB_STATS_API}/stats?{urllib.parse.urlencode(params)}"
        payload = _http_get_json(url)
        stats_blk = (payload.get("stats") or [])
        if not stats_blk:
            break
        blk = stats_blk[0]
        splits = blk.get("splits") or []
        total_splits = int(blk.get("totalSplits") or 0)
        collected.extend(splits)
        offset += len(splits)
        if not splits:
            break
        if team_id is not None:
            break

    rows_out: list[dict[str, Any]] = []
    for sp in collected:
        row = _split_to_fielding_metric_row(sp, list(body.metrics), abbr_by_tid)
        inn_val = _parse_innings_mlb(row.get("Inn"))
        if inn_val is None or inn_val < min_inn_f:
            continue
        rows_out.append(row)

    if body.name_contains:
        nc = body.name_contains.strip().lower()
        rows_out = [r for r in rows_out if r.get("Name") and nc in str(r["Name"]).lower()]

    rows_out = rows_out[:need]

    if not rows_out:
        raise HTTPException(
            502,
            detail=(
                "MLB Stats API fielding fallback returned no rows after filters "
                f"(min_inn={body.min_inn}, team_abbr={body.team_abbr!r}, "
                f"name_contains={body.name_contains!r}). "
                f"FanGraphs error was: {fg_error}"
            ),
        )

    df = pd.DataFrame(rows_out)
    table = _df_to_payload(df, body.row_cap)
    note_parts = [
        "Fielding data from **MLB Stats API** (official), not FanGraphs — works when FanGraphs blocks requests.",
        "**DRS**, **UZR**, and **DEF** are FanGraphs-style columns; they are blank in this MLB endpoint.",
    ]
    if fg_error:
        note_parts.append(f"FanGraphs fetch failed: {fg_error}")
    return {**table, "source": "mlb_stats_api_fielding_season", "note": " ".join(note_parts)}


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


app = FastAPI(title="Statcast Query Data", version="0.1.0")


@app.get("/health")
def health() -> dict[str, str]:
    ok = (
        pitching_stats is not None
        and batting_stats is not None
        and statcast_pitcher_pitch_arsenal is not None
    )
    return {"status": "ok" if ok else "degraded", "pybaseball": "ok" if ok else _IMPORT_ERROR}


class PitcherPitchArsenalBody(BaseModel):
    year: int = Field(ge=2015, le=2030)
    min_pitches: int = Field(ge=0, le=5000)
    arsenal_type: Literal["avg_spin", "avg_speed"]
    row_cap: int = Field(ge=1, le=200, default=50)
    pitch_type_filter: str | None = None
    pitcher_id: int | None = Field(
        default=None,
        ge=1,
        description="MLBAM pitcher id — when set, return only this pitcher's row(s).",
    )


@app.post("/v1/pitcher_pitch_arsenal")
def pitcher_pitch_arsenal(body: PitcherPitchArsenalBody) -> dict[str, Any]:
    if statcast_pitcher_pitch_arsenal is None:
        raise HTTPException(500, detail=f"pybaseball unavailable: {_IMPORT_ERROR}")
    min_p = 250 if body.min_pitches == 0 else int(body.min_pitches)
    cache_key = f"ppa:{body.year}:{min_p}:{body.arsenal_type}"
    cached = _cache_get(cache_key)
    if cached is None:
        try:
            df = statcast_pitcher_pitch_arsenal(
                body.year, minP=min_p, arsenal_type=body.arsenal_type
            )
        except Exception as e:
            raise HTTPException(502, detail=f"Savant/pyb error: {e}") from e
        _cache_set(cache_key, df)
    else:
        df = cached
    if not isinstance(df, pd.DataFrame):
        raise HTTPException(502, detail="Unexpected non-DataFrame from pybaseball")
    if body.pitcher_id is not None:
        pid = int(body.pitcher_id)
        id_col = next(
            (c for c in df.columns if str(c).lower() in ("pitcher", "player_id", "player_id_mlbam", "mlbam")),
            None,
        )
        if id_col is None:
            raise HTTPException(502, detail=f"No pitcher id column in arsenal frame: {list(df.columns)[:25]}")
        df = df[pd.to_numeric(df[id_col], errors="coerce") == pid]
        if df.empty:
            return {
                "columns": [],
                "rows": [],
                "source": "baseball_savant_pitch_arsenal",
                "note": (
                    f"No pitch-arsenal row for pitcher_id={pid} in {body.year} "
                    f"with min_pitches={min_p}. Try a lower min_pitches or a different year."
                ),
            }
    if body.pitch_type_filter:
        col = next(
            (c for c in df.columns if str(c).lower() in ("pitch_type", "pitchtype", "pitch")),
            None,
        )
        if col:
            df = df[df[col].astype(str).str.upper() == body.pitch_type_filter.upper()]
    table = _df_to_payload(df, body.row_cap)
    return {**table, "source": "baseball_savant_pitch_arsenal"}


class PitchingSeasonBody(BaseModel):
    season: int = Field(ge=2000, le=2030)
    min_ip: float = Field(
        ge=0,
        le=300,
        default=DEFAULT_MIN_IP_SEASON_PITCHING,
        description="IP floor before returning a row. Use 0 for full rosters / relievers / early season.",
    )
    team_abbr: str | None = Field(
        default=None,
        min_length=2,
        max_length=4,
        description="Optional MLB team code for filtering / MLB API fallback (BOS, WSH, LAD, …).",
    )
    name_contains: str | None = Field(
        default=None,
        max_length=80,
        description="Substring match on Name (case-insensitive); use with team_abbr or low min_ip.",
    )
    metrics: list[
        Literal[
            "Name",
            "Team",
            "IP",
            "SO",
            "BB",
            "ERA",
            "WAR",
            "G",
            "SV",
            "H",
            "HR",
            "WHIP",
        ]
    ]
    row_cap: int = Field(ge=1, le=SEASON_STATS_ROW_CAP_MAX, default=80)


@app.post("/v1/pitching_season_stats")
def pitching_season_stats(body: PitchingSeasonBody) -> dict[str, Any]:
    df: pd.DataFrame | None = None
    fg_error: str | None = None

    if pitching_stats is not None:
        cache_key = f"pss:{body.season}:q0"
        cached = _cache_get(cache_key)
        if cached is not None:
            df = cached if isinstance(cached, pd.DataFrame) else None
        else:
            try:
                try:
                    raw = pitching_stats(body.season, qual=0)
                except TypeError:
                    raw = pitching_stats(body.season)
                if isinstance(raw, pd.DataFrame):
                    df = raw
                    _cache_set(cache_key, raw)
            except Exception as e:
                fg_error = str(e)

    if df is not None:
        ip_col = next((c for c in df.columns if str(c).upper() == "IP"), None)
        if ip_col is None:
            fg_error = fg_error or "No IP column in FanGraphs pitching frame"
        else:
            work = df.copy()
            work[ip_col] = pd.to_numeric(work[ip_col], errors="coerce")
            work = work[work[ip_col] >= float(body.min_ip)]
            missing = [m for m in body.metrics if m not in work.columns]
            if missing:
                fg_error = fg_error or f"FanGraphs missing columns: {missing}"
            else:
                if body.team_abbr:
                    ta = body.team_abbr.strip().upper()
                    tm = next((c for c in work.columns if str(c) == "Team"), None)
                    if tm:
                        work = work[work[tm].astype(str).str.upper().str.strip() == ta]
                if body.name_contains:
                    nc = body.name_contains.strip().lower()
                    nm = next((c for c in work.columns if str(c) == "Name"), None)
                    if nm:
                        work = work[
                            work[nm].astype(str).str.lower().str.contains(nc, na=False, regex=False)
                        ]
                slim = work[list(body.metrics)].head(int(body.row_cap))
                table = _df_to_payload(slim, body.row_cap)
                return {**table, "source": "fangraphs_pitching_stats"}

    if fg_error is None:
        fg_error = (
            "pybaseball pitching_stats unavailable"
            if pitching_stats is None
            else "FanGraphs pitching table unusable"
        )

    return _pitching_season_from_mlb_api(body, fg_error)


class MlbStatLeadersBody(BaseModel):
    season: int = Field(ge=2000, le=2030)
    leader_category: str = Field(
        min_length=2,
        max_length=80,
        description="MLB Stats API leaderCategories value, camelCase, e.g. strikeOuts, homeRuns, wins.",
    )
    stat_group: Literal["pitching", "hitting"] | None = Field(
        default=None,
        description="Required for ambiguous categories; strikeOuts defaults to pitching if omitted.",
    )
    limit: int = Field(ge=1, le=50, default=25)
    leader_game_types: Literal["R", "P", "F", "D", "L", "W"] = Field(
        default="R",
        description="Regular season R; postseason P.",
    )


@app.post("/v1/mlb_stat_leaders")
def mlb_stat_leaders(body: MlbStatLeadersBody) -> dict[str, Any]:
    """
    Official MLB leaderboards via statsapi.mlb.com (no FanGraphs).
    Use for league leaders (e.g. strikeouts, home runs) when pybaseball FanGraphs tables fail.
    """
    cat = body.leader_category.strip()
    if cat not in ALLOWED_MLB_LEADER_CATEGORIES:
        raise HTTPException(
            400,
            detail=(
                f"Unknown leader_category {cat!r}. Allowed: "
                f"{sorted(ALLOWED_MLB_LEADER_CATEGORIES)}"
            ),
        )
    sg = body.stat_group or _default_stat_group_for_leader_category(cat)
    params = {
        "sportId": "1",
        "season": str(body.season),
        "leaderCategories": cat,
        "leaderGameTypes": body.leader_game_types,
        "limit": str(body.limit),
        "statGroup": sg,
    }
    url = f"{MLB_STATS_API}/stats/leaders?{urllib.parse.urlencode(params)}"
    cache_key = f"mlbldr:{body.season}:{cat}:{sg}:{body.leader_game_types}:{body.limit}"
    cached = _cache_get(cache_key)
    if cached is None:
        payload = _http_get_json(url)
        if not isinstance(payload, dict):
            raise HTTPException(502, detail="Unexpected MLB Stats API response")
        table = _mlb_leaders_table(payload)
        out = {
            **table,
            "source": "mlb_stats_api_leaders",
            "note": (
                "Official MLB Stats API leaders (same backend as MLB.com). "
                "Values are regular-season (or chosen game type) through MLB’s update cadence."
            ),
        }
        _cache_set(cache_key, out)
        return out
    return cached


class BattingSeasonBody(BaseModel):
    season: int = Field(ge=2000, le=2030)
    min_pa: float = Field(ge=0, le=900, default=0)
    team_abbr: str | None = Field(
        default=None,
        min_length=2,
        max_length=4,
        description="Optional MLB team code (e.g. BOS, WSH, TB). Speeds MLB API fallback for one club.",
    )
    name_contains: str | None = Field(
        default=None,
        max_length=80,
        description="If set, keep rows whose Name contains this substring (case-insensitive).",
    )
    metrics: list[
        Literal[
            "Name",
            "Team",
            "G",
            "PA",
            "HR",
            "R",
            "RBI",
            "SB",
            "BB",
            "SO",
            "AVG",
            "OBP",
            "SLG",
            "OPS",
            "ISO",
            "BABIP",
            "wOBA",
            "wRC+",
            "BB%",
            "K%",
            "WAR",
        ]
    ]
    row_cap: int = Field(ge=1, le=SEASON_STATS_ROW_CAP_MAX, default=80)


class FieldingSeasonBody(BaseModel):
    season: int = Field(ge=2000, le=2030)
    min_inn: float = Field(ge=0, le=5000, default=0)
    team_abbr: str | None = Field(
        default=None,
        min_length=2,
        max_length=4,
        description="Optional MLB team code for one club (BOS, WSH, LAD, …).",
    )
    name_contains: str | None = Field(
        default=None,
        max_length=80,
        description="Substring match on player Name (case-insensitive).",
    )
    metrics: list[
        Literal[
            "Name",
            "Team",
            "Pos",
            "G",
            "GS",
            "Inn",
            "PO",
            "A",
            "E",
            "DP",
            "FP",
            "RF9",
            "DRS",
            "UZR",
            "DEF",
        ]
    ]
    row_cap: int = Field(ge=1, le=300, default=80)


@app.post("/v1/batting_season_stats")
def batting_season_stats(body: BattingSeasonBody) -> dict[str, Any]:
    df: pd.DataFrame | None = None
    fg_error: str | None = None

    if batting_stats is not None:
        cache_key = f"bss:{body.season}:q0"
        cached = _cache_get(cache_key)
        if cached is not None:
            df = cached if isinstance(cached, pd.DataFrame) else None
        else:
            try:
                try:
                    raw = batting_stats(body.season, qual=0)
                except TypeError:
                    raw = batting_stats(body.season)
                if isinstance(raw, pd.DataFrame):
                    df = raw
                    _cache_set(cache_key, raw)
            except Exception as e:
                fg_error = str(e)

    if df is not None:
        pa_col = next((c for c in df.columns if str(c).upper() == "PA"), None)
        if pa_col is None:
            fg_error = fg_error or "No PA column in FanGraphs batting frame"
        else:
            work = df.copy()
            work[pa_col] = pd.to_numeric(work[pa_col], errors="coerce")
            work = work[work[pa_col] >= float(body.min_pa)]
            missing = [m for m in body.metrics if m not in work.columns]
            if missing:
                fg_error = fg_error or f"FanGraphs missing columns: {missing}"
            else:
                if body.name_contains:
                    nc = body.name_contains.strip().lower()
                    nm = next((c for c in work.columns if str(c) == "Name"), None)
                    if nm:
                        work = work[
                            work[nm].astype(str).str.lower().str.contains(nc, na=False, regex=False)
                        ]
                slim = work[list(body.metrics)].head(int(body.row_cap))
                table = _df_to_payload(slim, body.row_cap)
                return {**table, "source": "fangraphs_batting_stats"}

    if fg_error is None:
        fg_error = (
            "pybaseball batting_stats unavailable"
            if batting_stats is None
            else "FanGraphs batting table unusable"
        )

    return _batting_season_from_mlb_api(body, fg_error)


@app.post("/v1/fielding_season_stats")
def fielding_season_stats(body: FieldingSeasonBody) -> dict[str, Any]:
    df: pd.DataFrame | None = None
    fg_error: str | None = None

    if fielding_stats is not None:
        cache_key = f"fss:{body.season}:q0"
        cached = _cache_get(cache_key)
        if cached is not None:
            df = cached if isinstance(cached, pd.DataFrame) else None
        else:
            try:
                try:
                    raw = fielding_stats(body.season, qual=0)
                except TypeError:
                    raw = fielding_stats(body.season)
                if isinstance(raw, pd.DataFrame):
                    df = raw
                    _cache_set(cache_key, raw)
            except Exception as e:
                fg_error = str(e)

    if df is not None:
        inn_col = next(
            (c for c in df.columns if str(c) in ("Inn", "INN", "IP")),
            None,
        )
        if inn_col is None:
            fg_error = fg_error or "No Inn column in FanGraphs fielding frame"
        else:
            work = df.copy()

            def _fg_inn_qual(v: Any) -> float | None:
                p = _parse_innings_mlb(v)
                if p is not None:
                    return p
                try:
                    return float(v)
                except (TypeError, ValueError):
                    return None

            work["_inn_f"] = work[inn_col].map(_fg_inn_qual)
            work = work[work["_inn_f"].fillna(0) >= float(body.min_inn)]
            missing = [m for m in body.metrics if m not in work.columns]
            if missing:
                fg_error = fg_error or f"FanGraphs missing columns: {missing}"
            else:
                if body.team_abbr:
                    ta = body.team_abbr.strip().upper()
                    tm = next((c for c in work.columns if str(c) == "Team"), None)
                    if tm:
                        work = work[work[tm].astype(str).str.upper().str.strip() == ta]
                if body.name_contains:
                    nc = body.name_contains.strip().lower()
                    nm = next((c for c in work.columns if str(c) == "Name"), None)
                    if nm:
                        work = work[
                            work[nm].astype(str).str.lower().str.contains(nc, na=False, regex=False)
                        ]
                slim = work[list(body.metrics)].head(int(body.row_cap))
                table = _df_to_payload(slim, body.row_cap)
                return {**table, "source": "fangraphs_fielding_stats"}

    if fg_error is None:
        fg_error = (
            "pybaseball fielding_stats unavailable"
            if fielding_stats is None
            else "FanGraphs fielding table unusable"
        )

    return _fielding_season_from_mlb_api(body, fg_error)


_DATE_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _split_fielding_game_log_row(
    split: dict[str, Any], metrics: list[str], abbr_by_tid: dict[int, str]
) -> dict[str, Any]:
    """One MLB /people/{id}/stats gameLog split → table row (fielding)."""
    team = split.get("team") or {}
    opp = split.get("opponent") or {}
    pos = split.get("position") or {}
    st = split.get("stat") or {}
    game = split.get("game") or {}

    def _abbr(t: dict[str, Any]) -> str | None:
        tid = t.get("id")
        if tid is not None:
            try:
                hit = abbr_by_tid.get(int(tid))
                if hit:
                    return hit
            except (TypeError, ValueError):
                pass
        return t.get("abbreviation") or t.get("name")

    out: dict[str, Any] = {}
    for m in metrics:
        if m == "Date":
            out[m] = split.get("date")
        elif m == "GamePk":
            out[m] = game.get("gamePk")
        elif m == "Team":
            out[m] = _abbr(team)
        elif m == "Opp":
            out[m] = _abbr(opp)
        elif m == "Home":
            out[m] = split.get("isHome")
        elif m == "Win":
            out[m] = split.get("isWin")
        elif m == "Pos":
            out[m] = pos.get("abbreviation") or pos.get("name")
        elif m == "Inn":
            out[m] = st.get("innings")
        elif m == "PO":
            out[m] = _safe_int_mlb(st.get("putOuts"))
        elif m == "A":
            out[m] = _safe_int_mlb(st.get("assists"))
        elif m == "E":
            out[m] = _safe_int_mlb(st.get("errors"))
        elif m == "DP":
            out[m] = _safe_int_mlb(st.get("doublePlays"))
        elif m == "FP":
            out[m] = _mlb_parse_rate(st.get("fielding"))
        elif m == "RF9":
            out[m] = _mlb_parse_rate(st.get("rangeFactorPer9Inn"))
        else:
            out[m] = None
    return out


def _mlb_fetch_game_log_splits(
    path: str,
    season: int,
    group: Literal["pitching", "hitting", "fielding"],
    start_date: str | None,
    end_date: str | None,
) -> list[dict[str, Any]]:
    """
    Paged gameLog from /teams/{id}/stats or /people/{id}/stats.
    `path` is e.g. 'teams/111' or 'people/660271' (no leading slash).
    """
    collected: list[dict[str, Any]] = []
    offset = 0
    page = 0
    page_size = 200
    max_pages = 60

    while page < max_pages:
        page += 1
        params: dict[str, str] = {
            "stats": "gameLog",
            "group": group,
            "season": str(season),
            "limit": str(page_size),
            "offset": str(offset),
        }
        if start_date:
            params["startDate"] = start_date
        if end_date:
            params["endDate"] = end_date
        url = f"{MLB_STATS_API}/{path}/stats?{urllib.parse.urlencode(params)}"
        payload = _http_get_json(url)
        stats_blk = payload.get("stats") or []
        if not stats_blk:
            break
        splits = stats_blk[0].get("splits") or []
        collected.extend(splits)
        if len(splits) < page_size:
            break
        offset += len(splits)

    return collected


def _mlb_fetch_fielding_game_log_splits(
    player_id: int,
    season: int,
    start_date: str | None,
    end_date: str | None,
) -> list[dict[str, Any]]:
    return _mlb_fetch_game_log_splits(
        f"people/{int(player_id)}",
        season,
        "fielding",
        start_date,
        end_date,
    )


TEAM_PLAYER_GAME_LOG_PITCHING_METRICS = frozenset(
    {
        "Date",
        "GamePk",
        "Team",
        "Opp",
        "Home",
        "Win",
        "Name",
        "R",
        "ER",
        "H",
        "BB",
        "SO",
        "IP",
        "HR",
        "NP",
        "SV",
    }
)

TEAM_PLAYER_GAME_LOG_HITTING_METRICS = frozenset(
    {
        "Date",
        "GamePk",
        "Team",
        "Opp",
        "Home",
        "Win",
        "Name",
        "R",
        "RBI",
        "H",
        "Doubles",
        "Triples",
        "HR",
        "BB",
        "SO",
        "PA",
        "AB",
        "SB",
        "CS",
    }
)


def _split_pitch_hit_game_log_row(
    split: dict[str, Any],
    metrics: list[str],
    abbr_by_tid: dict[int, str],
    *,
    include_name: bool,
) -> dict[str, Any]:
    """gameLog row for group=pitching or hitting (team or player)."""
    team = split.get("team") or {}
    opp = split.get("opponent") or {}
    st = split.get("stat") or {}
    game = split.get("game") or {}
    player = split.get("player") or {}

    def _abbr(t: dict[str, Any]) -> str | None:
        tid = t.get("id")
        if tid is not None:
            try:
                hit = abbr_by_tid.get(int(tid))
                if hit:
                    return hit
            except (TypeError, ValueError):
                pass
        return t.get("abbreviation") or t.get("name")

    out: dict[str, Any] = {}
    for m in metrics:
        if m == "Date":
            out[m] = split.get("date")
        elif m == "GamePk":
            out[m] = game.get("gamePk")
        elif m == "Team":
            out[m] = _abbr(team)
        elif m == "Opp":
            out[m] = _abbr(opp)
        elif m == "Home":
            out[m] = split.get("isHome")
        elif m == "Win":
            out[m] = split.get("isWin")
        elif m == "Name":
            out[m] = player.get("fullName") if include_name else None
        elif m == "R":
            out[m] = _safe_int_mlb(st.get("runs"))
        elif m == "ER":
            out[m] = _safe_int_mlb(st.get("earnedRuns"))
        elif m == "RBI":
            out[m] = _safe_int_mlb(st.get("rbi"))
        elif m == "H":
            out[m] = _safe_int_mlb(st.get("hits"))
        elif m == "Doubles":
            out[m] = _safe_int_mlb(st.get("doubles"))
        elif m == "Triples":
            out[m] = _safe_int_mlb(st.get("triples"))
        elif m == "HR":
            out[m] = _safe_int_mlb(st.get("homeRuns"))
        elif m == "BB":
            out[m] = _safe_int_mlb(st.get("baseOnBalls"))
        elif m == "SO":
            out[m] = _safe_int_mlb(st.get("strikeOuts"))
        elif m == "IP":
            out[m] = st.get("inningsPitched")
        elif m == "NP":
            out[m] = _safe_int_mlb(st.get("numberOfPitches"))
        elif m == "SV":
            out[m] = _safe_int_mlb(st.get("saves"))
        elif m == "PA":
            out[m] = _safe_int_mlb(st.get("plateAppearances"))
        elif m == "AB":
            out[m] = _safe_int_mlb(st.get("atBats"))
        elif m == "SB":
            out[m] = _safe_int_mlb(st.get("stolenBases"))
        elif m == "CS":
            out[m] = _safe_int_mlb(st.get("caughtStealing"))
        else:
            out[m] = None
    return out


def _validate_game_log_metrics(stat_group: Literal["pitching", "hitting"], metrics: list[str]) -> None:
    allowed = (
        TEAM_PLAYER_GAME_LOG_PITCHING_METRICS
        if stat_group == "pitching"
        else TEAM_PLAYER_GAME_LOG_HITTING_METRICS
    )
    bad = [m for m in metrics if m not in allowed]
    if bad:
        raise HTTPException(
            400,
            detail=f"Unknown metric(s) for {stat_group} game log: {bad}. Allowed: {sorted(allowed)}",
        )


def _game_log_sort_key(split: dict[str, Any]) -> tuple[str, int]:
    d = str(split.get("date") or "")
    g = split.get("game") or {}
    pk = int(g.get("gamePk") or 0)
    return (d, pk)


def _run_team_or_player_game_log(
    *,
    stat_group: Literal["pitching", "hitting"],
    season: int,
    path: str,
    include_name: bool,
    start_date: str | None,
    end_date: str | None,
    max_games: int | None,
    metrics: list[str],
    row_cap: int,
    empty_detail: str,
) -> dict[str, Any]:
    _validate_game_log_metrics(stat_group, list(metrics))
    splits = _mlb_fetch_game_log_splits(path, season, stat_group, start_date, end_date)
    abbr_by_tid = _mlb_team_abbr_by_id(season)
    splits.sort(key=_game_log_sort_key)
    if max_games is not None:
        splits = splits[: int(max_games)]
    rows_out = [
        _split_pitch_hit_game_log_row(sp, list(metrics), abbr_by_tid, include_name=include_name)
        for sp in splits
    ]
    if not rows_out:
        return {
            "columns": list(metrics),
            "rows": [],
            "source": f"mlb_stats_api_{stat_group}_game_log",
            "note": empty_detail,
        }
    df = pd.DataFrame(rows_out)
    table = _df_to_payload(df, row_cap)
    scope = "team" if path.startswith("teams/") else "player"
    note = (
        f"Per-game **{stat_group}** lines ({scope}) from **MLB Stats API** (gameLog). "
        "Each row is the **full game** line for that club or player (all innings in the stat block), "
        "not a partial such as ‘through the 6th’—use live/box score for in-progress inning splits."
    )
    return {**table, "source": f"mlb_stats_api_{stat_group}_game_log", "note": note}


class TeamGameLogBody(BaseModel):
    team_abbr: str = Field(min_length=2, max_length=4, description="MLB team code, e.g. BOS, WSH.")
    season: int = Field(ge=2000, le=2030)
    stat_group: Literal["pitching", "hitting"] = Field(
        description="pitching: runs/IP/etc. allowed by the team that game; hitting: team offense per game.",
    )
    start_date: str | None = Field(default=None, min_length=10, max_length=10)
    end_date: str | None = Field(default=None, min_length=10, max_length=10)
    max_games: int | None = Field(default=None, ge=1, le=200)
    metrics: list[str] = Field(min_length=1, max_length=20)
    row_cap: int = Field(ge=1, le=250, default=180)


@app.post("/v1/team_game_log")
def team_game_log(body: TeamGameLogBody) -> dict[str, Any]:
    """Team-level gameLog (pitching or hitting) — one row per team game."""
    if body.start_date and not _DATE_ISO.match(body.start_date):
        raise HTTPException(400, detail="start_date must be YYYY-MM-DD")
    if body.end_date and not _DATE_ISO.match(body.end_date):
        raise HTTPException(400, detail="end_date must be YYYY-MM-DD")
    if body.start_date and body.end_date and body.start_date > body.end_date:
        raise HTTPException(400, detail="start_date must be on or before end_date")

    tid = _mlb_resolve_team_id(body.team_abbr, body.season)
    if tid is None:
        raise HTTPException(
            400,
            detail=f"Unknown team_abbr {body.team_abbr!r} for MLB lookup (season {body.season}).",
        )

    return _run_team_or_player_game_log(
        stat_group=body.stat_group,
        season=body.season,
        path=f"teams/{tid}",
        include_name=False,
        start_date=body.start_date,
        end_date=body.end_date,
        max_games=body.max_games,
        metrics=list(body.metrics),
        row_cap=body.row_cap,
        empty_detail="No game-log rows for this team/season/range.",
    )


class PlayerGameLogBody(BaseModel):
    player_id: int = Field(ge=1, description="MLBAM player id.")
    season: int = Field(ge=2000, le=2030)
    stat_group: Literal["pitching", "hitting"]
    start_date: str | None = Field(default=None, min_length=10, max_length=10)
    end_date: str | None = Field(default=None, min_length=10, max_length=10)
    max_games: int | None = Field(default=None, ge=1, le=200)
    metrics: list[str] = Field(min_length=1, max_length=20)
    row_cap: int = Field(ge=1, le=250, default=180)


@app.post("/v1/player_game_log")
def player_game_log(body: PlayerGameLogBody) -> dict[str, Any]:
    """Player-level gameLog (pitching or hitting) — one row per game appearance."""
    if body.start_date and not _DATE_ISO.match(body.start_date):
        raise HTTPException(400, detail="start_date must be YYYY-MM-DD")
    if body.end_date and not _DATE_ISO.match(body.end_date):
        raise HTTPException(400, detail="end_date must be YYYY-MM-DD")
    if body.start_date and body.end_date and body.start_date > body.end_date:
        raise HTTPException(400, detail="start_date must be on or before end_date")

    return _run_team_or_player_game_log(
        stat_group=body.stat_group,
        season=body.season,
        path=f"people/{int(body.player_id)}",
        include_name=True,
        start_date=body.start_date,
        end_date=body.end_date,
        max_games=body.max_games,
        metrics=list(body.metrics),
        row_cap=body.row_cap,
        empty_detail="No game-log rows for this player/season/range.",
    )


def _ip_value_to_outs(v: Any) -> int:
    """Sum-safe: MLB innings → third-innings (outs)."""
    f = _parse_innings_mlb(v)
    if f is None or f < 0:
        return 0
    whole = int(f)
    frac = f - float(whole)
    thirds = int(round(frac * 3))
    if thirds not in (0, 1, 2):
        thirds = min(max(thirds, 0), 2)
    return whole * 3 + thirds


def _outs_to_ip_string(outs: int) -> str:
    w, r = divmod(max(outs, 0), 3)
    return f"{w}.{r}" if r else str(w)


def _game_log_opponent_abbr(
    split: dict[str, Any], abbr_by_tid: dict[int, str]
) -> str | None:
    opp = split.get("opponent") or {}
    tid = opp.get("id")
    if tid is not None:
        try:
            hit = abbr_by_tid.get(int(tid))
            if hit:
                return str(hit).strip().upper()
        except (TypeError, ValueError):
            pass
    ab = opp.get("abbreviation")
    if isinstance(ab, str) and ab.strip():
        return ab.strip().upper()
    return None


class PlayerGameLogVsOpponentBody(BaseModel):
    player_id: int = Field(ge=1, description="MLBAM player id (use resolve_player when unknown).")
    opponent_abbr: str = Field(
        min_length=2,
        max_length=4,
        description="Opponent team code, e.g. MIL, BOS, WSH (the club faced in that game).",
    )
    stat_group: Literal["pitching", "hitting"] = Field(
        default="pitching",
        description="pitching: pitcher’s game lines vs this opponent; hitting: batter’s game lines vs this opponent.",
    )
    start_season: int = Field(default=2008, ge=1995, le=2030)
    end_season: int = Field(default=2030, ge=1995, le=2030)
    aggregate: bool = Field(
        default=True,
        description="If true, return one summary row across all seasons; if false, per-game rows (see metrics).",
    )
    metrics: list[str] | None = Field(
        default=None,
        description="Required when aggregate is false: same metric names as player_game_log.",
    )
    row_cap: int = Field(default=280, ge=1, le=350)


@app.post("/v1/player_game_log_vs_opponent")
def player_game_log_vs_opponent(body: PlayerGameLogVsOpponentBody) -> dict[str, Any]:
    """
    Career (multi-season) game-log slice: games where this player’s opponent matches **opponent_abbr**.
    Uses MLB Stats API gameLog per season — suitable for “career stats vs Milwaukee,” etc.
    """
    if body.start_season > body.end_season:
        raise HTTPException(400, detail="start_season must be <= end_season")

    raw_opp = body.opponent_abbr.strip().upper()
    target = _SYNONYM_ABBR.get(raw_opp, raw_opp)
    if _mlb_resolve_team_id(target, body.end_season) is None:
        raise HTTPException(
            400,
            detail=f"Unknown opponent_abbr {body.opponent_abbr!r} for MLB lookup (season {body.end_season}).",
        )

    if not body.aggregate:
        if not body.metrics:
            raise HTTPException(
                400,
                detail="metrics is required when aggregate is false (same as /v1/player_game_log).",
            )
        _validate_game_log_metrics(body.stat_group, list(body.metrics))

    matched: list[tuple[int, dict[str, Any]]] = []
    for season in range(int(body.start_season), int(body.end_season) + 1):
        abbr_by_tid = _mlb_team_abbr_by_id(season)
        splits = _mlb_fetch_game_log_splits(
            f"people/{int(body.player_id)}",
            season,
            body.stat_group,
            None,
            None,
        )
        for sp in splits:
            oa = _game_log_opponent_abbr(sp, abbr_by_tid)
            if oa is None:
                continue
            oa_n = _SYNONYM_ABBR.get(oa, oa)
            if oa_n != target:
                continue
            matched.append((season, sp))

    if not matched:
        return {
            "columns": [],
            "rows": [],
            "source": "mlb_stats_api_player_game_log_vs_opponent",
            "note": (
                f"No game-log appearances found for player {body.player_id} vs opponent **{target}** "
                f"from {body.start_season} through {body.end_season} ({body.stat_group}). "
                "Try widening seasons or check the opponent abbreviation (e.g. MIL for Brewers)."
            ),
        }

    note_common = (
        f"**vs {target}** from **{body.start_season}**–**{body.end_season}** (MLB Stats API gameLog, "
        f"{body.stat_group}). "
        "Rows are **games this player appeared in** against that opponent; "
        "for pitchers, IP/R/ER are that game’s pitching line (not a partial inning split)."
    )

    if not body.aggregate:
        mets = list(body.metrics or [])
        rows_out: list[dict[str, Any]] = []
        for season, sp in matched:
            abbr_by_tid = _mlb_team_abbr_by_id(season)
            rows_out.append(
                _split_pitch_hit_game_log_row(
                    sp, mets, abbr_by_tid, include_name=True
                )
            )
        rows_out.sort(key=lambda r: (str(r.get("Date") or ""), int(r.get("GamePk") or 0)), reverse=True)
        rows_out = rows_out[: int(body.row_cap)]
        df = pd.DataFrame(rows_out)
        table = _df_to_payload(df, int(body.row_cap))
        return {
            **table,
            "source": "mlb_stats_api_player_game_log_vs_opponent",
            "note": note_common + f" Showing up to {len(rows_out)} games (newest first).",
        }

    # --- aggregate ---
    if body.stat_group == "pitching":
        total_outs = 0
        th = tr = ter = tbb = tso = thr = tnp = 0
        team_wins = 0
        for _, sp in matched:
            st = sp.get("stat") or {}
            total_outs += _ip_value_to_outs(st.get("inningsPitched"))
            th += _safe_int_mlb(st.get("hits")) or 0
            tr += _safe_int_mlb(st.get("runs")) or 0
            ter += _safe_int_mlb(st.get("earnedRuns")) or 0
            tbb += _safe_int_mlb(st.get("baseOnBalls")) or 0
            tso += _safe_int_mlb(st.get("strikeOuts")) or 0
            thr += _safe_int_mlb(st.get("homeRuns")) or 0
            tnp += _safe_int_mlb(st.get("numberOfPitches")) or 0
            if sp.get("isWin") is True:
                team_wins += 1
        games = len(matched)
        ipf = total_outs / 3.0
        era = round(9.0 * ter / ipf, 2) if ipf > 0 else None
        whip = round((tbb + th) / ipf, 3) if ipf > 0 else None
        k9 = round(9.0 * tso / ipf, 2) if ipf > 0 else None
        bb9 = round(9.0 * tbb / ipf, 2) if ipf > 0 else None
        agg = {
            "Opp": target,
            "Games": games,
            "IP": _outs_to_ip_string(total_outs),
            "H": th,
            "R": tr,
            "ER": ter,
            "BB": tbb,
            "SO": tso,
            "HR": thr,
            "NP": tnp if tnp else None,
            "ERA": era,
            "WHIP": whip,
            "K9": k9,
            "BB9": bb9,
            "TeamWins": team_wins,
            "Seasons": f"{body.start_season}–{body.end_season}",
        }
        df = pd.DataFrame([agg])
        table = _df_to_payload(df, 5)
        return {
            **table,
            "source": "mlb_stats_api_player_game_log_vs_opponent",
            "note": note_common
            + " **TeamWins** = games where this player’s **team** won (MLB isWin on the gameLog row), "
            "not necessarily this pitcher’s official W/L decision.",
        }

    # hitting aggregate
    tpa = tab = th = t2b = t3b = thr = tr = trbi = tbb = tso = 0
    team_wins = 0
    for _, sp in matched:
        st = sp.get("stat") or {}
        pa = _safe_int_mlb(st.get("plateAppearances")) or 0
        ab = _safe_int_mlb(st.get("atBats")) or 0
        pa = pa or 0
        ab = ab or 0
        tpa += pa
        tab += ab
        hi = _safe_int_mlb(st.get("hits")) or 0
        th += hi
        d2 = _safe_int_mlb(st.get("doubles")) or 0
        d3 = _safe_int_mlb(st.get("triples")) or 0
        hr = _safe_int_mlb(st.get("homeRuns")) or 0
        t2b += d2
        t3b += d3
        thr += hr
        tr += _safe_int_mlb(st.get("runs")) or 0
        trbi += _safe_int_mlb(st.get("rbi")) or 0
        tbb += _safe_int_mlb(st.get("baseOnBalls")) or 0
        tso += _safe_int_mlb(st.get("strikeOuts")) or 0
        if sp.get("isWin") is True:
            team_wins += 1

    s1 = max(th - t2b - t3b - thr, 0)
    tb = s1 + 2 * t2b + 3 * t3b + 4 * thr
    hbp = 0
    sf = 0
    avg = round(th / tab, 3) if tab else None
    obp_d = (th + tbb + hbp) / tpa if tpa else None
    slg_d = tb / tab if tab else None
    obp = round(obp_d, 3) if obp_d is not None else None
    slg = round(slg_d, 3) if slg_d is not None else None
    ops = round(obp_d + slg_d, 3) if obp_d is not None and slg_d is not None else None

    agg_h = {
        "Opp": target,
        "Games": len(matched),
        "PA": tpa,
        "AB": tab,
        "H": th,
        "2B": t2b,
        "3B": t3b,
        "HR": thr,
        "R": tr,
        "RBI": trbi,
        "BB": tbb,
        "SO": tso,
        "AVG": avg,
        "OBP": obp,
        "SLG": slg,
        "OPS": ops,
        "TeamWins": team_wins,
        "Seasons": f"{body.start_season}–{body.end_season}",
    }
    df = pd.DataFrame([agg_h])
    table = _df_to_payload(df, 5)
    return {
        **table,
        "source": "mlb_stats_api_player_game_log_vs_opponent",
        "note": note_common
        + " **OBP/SLG/OPS** use H/BB/PA and total bases from summed counting stats (HBP/SF may be incomplete). "
        "**TeamWins** = player’s team won that game.",
    }


class FieldingGameLogBody(BaseModel):
    player_id: int = Field(ge=1, description="MLBAM player id (use resolve_player when unknown).")
    season: int = Field(ge=2000, le=2030)
    start_date: str | None = Field(
        default=None,
        min_length=10,
        max_length=10,
        description="Inclusive YYYY-MM-DD; passed to MLB Stats API.",
    )
    end_date: str | None = Field(
        default=None,
        min_length=10,
        max_length=10,
        description="Inclusive YYYY-MM-DD; passed to MLB Stats API.",
    )
    max_games: int | None = Field(
        default=None,
        ge=1,
        le=200,
        description="After sorting by date, keep only the first N games (e.g. 11 for first 11 games).",
    )
    metrics: list[
        Literal[
            "Date",
            "GamePk",
            "Team",
            "Opp",
            "Home",
            "Win",
            "Pos",
            "Inn",
            "PO",
            "A",
            "E",
            "DP",
            "FP",
            "RF9",
        ]
    ]
    row_cap: int = Field(ge=1, le=250, default=120)


@app.post("/v1/fielding_game_log")
def fielding_game_log(body: FieldingGameLogBody) -> dict[str, Any]:
    """
    Per-game fielding lines from MLB Stats API (stats=gameLog, group=fielding).
    Use for date windows or first-N-games slices; season aggregates stay on fielding_season_stats.
    """
    if body.start_date and not _DATE_ISO.match(body.start_date):
        raise HTTPException(400, detail="start_date must be YYYY-MM-DD")
    if body.end_date and not _DATE_ISO.match(body.end_date):
        raise HTTPException(400, detail="end_date must be YYYY-MM-DD")
    if body.start_date and body.end_date and body.start_date > body.end_date:
        raise HTTPException(400, detail="start_date must be on or before end_date")

    splits = _mlb_fetch_fielding_game_log_splits(
        body.player_id,
        body.season,
        body.start_date,
        body.end_date,
    )
    abbr_by_tid = _mlb_team_abbr_by_id(body.season)

    def _sort_key(sp: dict[str, Any]) -> tuple[str, int, str]:
        d = str(sp.get("date") or "")
        g = sp.get("game") or {}
        pk = int(g.get("gamePk") or 0)
        pos = (sp.get("position") or {}).get("abbreviation") or ""
        return (d, pk, pos)

    splits.sort(key=_sort_key)

    if body.max_games is not None:
        splits = splits[: int(body.max_games)]

    rows_out: list[dict[str, Any]] = []
    for sp in splits:
        rows_out.append(_split_fielding_game_log_row(sp, list(body.metrics), abbr_by_tid))

    if not rows_out:
        return {
            "columns": list(body.metrics),
            "rows": [],
            "source": "mlb_stats_api_fielding_game_log",
            "note": (
                "No fielding game-log rows for this player/season/range. "
                "Check player_id, season, and dates; DH-only games often show 0.0 innings."
            ),
        }

    df = pd.DataFrame(rows_out)
    table = _df_to_payload(df, body.row_cap)
    note = (
        "Per-game **fielding** lines from **MLB Stats API** (gameLog). "
        "Sum PO/A/E or innings client-side for a window; use **max_games** for first-N-games slices."
    )
    return {**table, "source": "mlb_stats_api_fielding_game_log", "note": note}


class ResolvePlayerBody(BaseModel):
    first_name: str = Field(min_length=1, max_length=80)
    last_name: str = Field(min_length=1, max_length=80)


@app.post("/v1/resolve_player")
def resolve_player(body: ResolvePlayerBody) -> dict[str, Any]:
    if playerid_lookup is None:
        raise HTTPException(500, detail=f"pybaseball unavailable: {_IMPORT_ERROR}")
    key = f"pl:{body.last_name.lower()}:{body.first_name.lower()}"
    cached = _cache_get(key)
    if cached is None:
        try:
            df = playerid_lookup(body.last_name, body.first_name)
        except Exception as e:
            raise HTTPException(502, detail=f"Lookup error: {e}") from e
        _cache_set(key, df)
    else:
        df = cached
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        return {"columns": [], "rows": [], "source": "chadwick_register"}
    if not isinstance(df, pd.DataFrame):
        raise HTTPException(502, detail="Unexpected lookup result")
    table = _df_to_payload(df, 25)
    return {**table, "source": "chadwick_register"}


class StatcastPitchesBody(BaseModel):
    start_date: str = Field(description="YYYY-MM-DD")
    end_date: str = Field(description="YYYY-MM-DD")
    pitcher_id: int | None = Field(default=None, ge=1)
    batter_id: int | None = Field(default=None, ge=1)
    pitch_type: str | None = None
    columns: list[str] = Field(
        default_factory=lambda: [
            "game_date",
            "player_name",
            "pitch_type",
            "release_spin_rate",
            "release_speed",
            "events",
            "description",
        ]
    )
    row_cap: int = Field(ge=1, le=5000, default=500)


@app.post("/v1/statcast_pitches")
def statcast_pitches(body: StatcastPitchesBody) -> dict[str, Any]:
    if statcast is None:
        raise HTTPException(500, detail=f"pybaseball unavailable: {_IMPORT_ERROR}")
    cache_key = (
        f"scp:{body.start_date}:{body.end_date}:{body.pitcher_id}:{body.batter_id}:{body.pitch_type}"
    )
    cached = _cache_get(cache_key)
    if cached is None:
        try:
            if body.pitcher_id is not None and statcast_pitcher is not None:
                df = statcast_pitcher(
                    body.start_date, body.end_date, int(body.pitcher_id)
                )
            elif body.batter_id is not None and statcast_batter is not None:
                df = statcast_batter(
                    body.start_date, body.end_date, int(body.batter_id)
                )
            else:
                df = statcast(
                    start_dt=body.start_date,
                    end_dt=body.end_date,
                    team=None,
                    verbose=False,
                    parallel=False,
                )
        except Exception as e:
            raise HTTPException(502, detail=f"Statcast pull error: {e}") from e
        _cache_set(cache_key, df)
    else:
        df = cached
    if not isinstance(df, pd.DataFrame):
        raise HTTPException(502, detail="Unexpected non-DataFrame from statcast")

    work = df.copy()
    if body.pitcher_id is not None and "pitcher" in work.columns:
        work = work[pd.to_numeric(work["pitcher"], errors="coerce") == body.pitcher_id]
    if body.batter_id is not None and "batter" in work.columns:
        work = work[pd.to_numeric(work["batter"], errors="coerce") == body.batter_id]
    if body.pitch_type and "pitch_type" in work.columns:
        work = work[work["pitch_type"].astype(str).str.upper() == body.pitch_type.upper()]

    missing = [c for c in body.columns if c not in work.columns]
    if missing:
        raise HTTPException(
            400,
            detail=f"Unknown column(s): {missing}. Available sample: {list(work.columns)[:60]}",
        )
    slim = work[body.columns].head(int(body.row_cap))
    table = _df_to_payload(slim, body.row_cap)
    return {**table, "source": "baseball_savant_statcast_search"}


class BatterVsPitcherStatcastBody(BaseModel):
    batter_id: int = Field(ge=1, description="MLBAM batter id (Savant `batter`).")
    pitcher_id: int = Field(ge=1, description="MLBAM pitcher id (Savant `pitcher`).")
    start_date: str = Field(
        default="2015-03-01",
        description="Savant pitch-detail era (~2015+). Narrow if the request is a recent window only.",
    )
    end_date: str = Field(description="YYYY-MM-DD inclusive (e.g. yesterday for career-to-date).")
    min_pa: int = Field(
        default=1,
        ge=0,
        le=900,
        description="Minimum plate appearances in this matchup to return the rate-stat row.",
    )


@app.post("/v1/batter_vs_pitcher_statcast")
def batter_vs_pitcher_statcast(body: BatterVsPitcherStatcastBody) -> dict[str, Any]:
    """
    Batting line for one batter vs one pitcher from Statcast: pitcher-scoped Savant pull, then batter filter.
    Covers career-style questions when paired with a wide date range.
    """
    if statcast_pitcher is None:
        raise HTTPException(500, detail=f"pybaseball statcast_pitcher unavailable: {_IMPORT_ERROR}")
    if not _DATE_ISO.match(body.start_date) or not _DATE_ISO.match(body.end_date):
        raise HTTPException(400, detail="start_date and end_date must be YYYY-MM-DD")
    if body.start_date > body.end_date:
        raise HTTPException(400, detail="start_date must be on or before end_date")

    pid = int(body.pitcher_id)
    bid = int(body.batter_id)
    cache_key = f"bvsp:{body.start_date}:{body.end_date}:{bid}:{pid}"
    cached = _cache_get(cache_key)
    if cached is None:
        try:
            df = statcast_pitcher(body.start_date, body.end_date, pid)
        except Exception as e:
            raise HTTPException(502, detail=f"Statcast pitcher pull error: {e}") from e
        _cache_set(cache_key, df)
    else:
        df = cached
    if not isinstance(df, pd.DataFrame):
        raise HTTPException(502, detail="Unexpected non-DataFrame from statcast_pitcher")
    if "batter" not in df.columns:
        raise HTTPException(502, detail="Statcast frame missing batter column")

    work = df[pd.to_numeric(df["batter"], errors="coerce") == bid].copy()
    if work.empty:
        return {
            "columns": [],
            "rows": [],
            "source": "baseball_savant_batter_vs_pitcher",
            "note": (
                f"No Statcast rows where pitcher_id={pid} faced batter_id={bid} in "
                f"{body.start_date}–{body.end_date}. They may not have met in this window, "
                "or sample is pre-Savant (use game logs for pre-2015 if needed)."
            ),
        }

    ev = work["events"].astype(str).str.strip()
    terminal = work[ev.ne("") & ev.ne("nan") & work["events"].notna()].copy()
    if terminal.empty:
        return {
            "columns": [],
            "rows": [],
            "source": "baseball_savant_batter_vs_pitcher",
            "note": "No PA-terminal event rows after filtering to this batter–pitcher pair.",
        }

    terminal = _dedupe_statcast_pa_terminal(terminal)
    agg = _aggregate_one_batter_rates_from_evt(terminal)

    if agg is None or agg["pa"] < int(body.min_pa):
        return {
            "columns": ["batter_id", "pitcher_id", "start_date", "end_date", "pa", "note"],
            "rows": [
                {
                    "batter_id": bid,
                    "pitcher_id": pid,
                    "start_date": body.start_date,
                    "end_date": body.end_date,
                    "pa": agg["pa"] if agg else 0,
                    "note": (
                        f"Below min_pa={body.min_pa} for this matchup "
                        f"(try widening dates or min_pa=0 to see counts)."
                    ),
                }
            ],
            "source": "baseball_savant_batter_vs_pitcher",
            "note": (
                "Statcast **batter vs pitcher** line from pitcher-scoped pull + PA-terminal events. "
                "Pre-2015 plate appearances are not in Savant detail; use **get_player_game_log** / "
                "**get_player_game_log_vs_opponent** for MLB game logs."
            ),
        }

    bname = None
    if "player_name" in terminal.columns and not terminal.empty:
        try:
            bname = str(terminal["player_name"].iloc[-1])
        except Exception:
            bname = None
    pstr = None
    if "pitcher_name" in work.columns and not work.empty:
        try:
            pstr = str(work["pitcher_name"].iloc[0])
        except Exception:
            pstr = None

    row = {
        "batter_id": bid,
        "pitcher_id": pid,
        "batter_name": bname,
        "pitcher_name": pstr,
        "start_date": body.start_date,
        "end_date": body.end_date,
        **agg,
    }
    df_out = pd.DataFrame([row])
    table = _df_to_payload(df_out, 1)
    note = (
        "**Batter vs pitcher** from Statcast: all pitches thrown by this **pitcher** to this **batter** "
        "in the date range; rates use PA-terminal **events** (same classification as other Statcast "
        "batting tools). Does **not** include pre-Savant seasons; for team-opponent career lines use "
        "**get_player_game_log_vs_opponent** with **opponent_abbr**."
    )
    return {**table, "source": "baseball_savant_batter_vs_pitcher", "note": note}


def _statcast_half_sort_key(tb: Any) -> int:
    c = _classify_half_inning(tb)
    if c == "top":
        return 0
    if c == "bottom":
        return 1
    return 2


def _pitching_team_abbr_statcast_row(row: pd.Series) -> str | None:
    """Team on the mound for this pitch (fielding team during this half-inning)."""
    ht = _abbr_cell(row.get("home_team"))
    at = _abbr_cell(row.get("away_team"))
    half = _classify_half_inning(row.get("inning_topbot"))
    if half == "top":
        return ht
    if half == "bottom":
        return at
    return None


def _margin_for_team(
    hs: float, ash: float, ht: str | None, at: str | None, team: str | None
) -> int | None:
    """Signed run margin for `team` (positive = team leading)."""
    if team is None or ht is None or at is None:
        return None
    tu = _SYNONYM_ABBR.get(team, team)
    htu = _SYNONYM_ABBR.get(ht, ht)
    atu = _SYNONYM_ABBR.get(at, at)
    hi = int(hs)
    ai = int(ash)
    if tu == htu:
        return hi - ai
    if tu == atu:
        return ai - hi
    return None


class PitcherEnteringInningLeadStatcastBody(BaseModel):
    pitcher_id: int = Field(ge=1, description="MLBAM pitcher id.")
    start_date: str = Field(description="YYYY-MM-DD (Statcast era ~2015+).")
    end_date: str = Field(description="YYYY-MM-DD inclusive.")
    entering_inning: int = Field(
        default=4,
        ge=1,
        le=15,
        description="Inning whose **first pitch** defines the score snapshot (e.g. 4 = entering the 4th).",
    )
    min_lead_runs: float = Field(
        default=3.0,
        ge=0.0,
        le=20.0,
        description="Keep games where this pitcher’s team **led by at least** this many runs at that snapshot.",
    )
    max_games: int = Field(
        default=400,
        ge=1,
        le=800,
        description="Cap distinct games scanned (each needs a full-game Statcast pull for the score snapshot).",
    )


@app.post("/v1/pitcher_entering_inning_lead_statcast")
def pitcher_entering_inning_lead_statcast(
    body: PitcherEnteringInningLeadStatcastBody,
) -> dict[str, Any]:
    """
    Find games where the pitcher’s team had a sufficient **lead at the first pitch of inning N** (Statcast
    scoreboard columns), then sum **full-game** pitching lines from MLB gameLog for those games.
    """
    if statcast_pitcher is None:
        raise HTTPException(500, detail=f"pybaseball statcast_pitcher unavailable: {_IMPORT_ERROR}")
    if statcast_single_game is None:
        raise HTTPException(500, detail=f"pybaseball statcast_single_game unavailable: {_IMPORT_ERROR}")
    if not _DATE_ISO.match(body.start_date) or not _DATE_ISO.match(body.end_date):
        raise HTTPException(400, detail="start_date and end_date must be YYYY-MM-DD")
    if body.start_date > body.end_date:
        raise HTTPException(400, detail="start_date must be on or before end_date")
    try:
        d0 = time.strptime(body.start_date, "%Y-%m-%d")
        d1 = time.strptime(body.end_date, "%Y-%m-%d")
        span_days = (time.mktime(d1) - time.mktime(d0)) / 86400.0
    except ValueError:
        raise HTTPException(400, detail="Invalid calendar date") from None
    if span_days > 4000:
        raise HTTPException(
            400,
            detail="Date span exceeds 4000 days (~11 years); narrow the window for this Statcast scan.",
        )

    pid = int(body.pitcher_id)
    inn_target = int(body.entering_inning)
    cache_key = f"pil:{body.start_date}:{body.end_date}:{pid}:{inn_target}:{body.min_lead_runs}"
    cached = _cache_get(cache_key)
    if cached is None:
        try:
            df = statcast_pitcher(body.start_date, body.end_date, pid)
        except Exception as e:
            raise HTTPException(502, detail=f"Statcast pitcher pull error: {e}") from e
        _cache_set(cache_key, df)
    else:
        df = cached
    if not isinstance(df, pd.DataFrame) or df.empty:
        return {
            "columns": [],
            "rows": [],
            "source": "pitcher_entering_inning_lead_statcast",
            "note": "No Statcast pitch rows for this pitcher in range.",
        }

    need = ("game_pk", "inning", "inning_topbot", "home_team", "away_team", "home_score", "away_score")
    miss = [c for c in need if c not in df.columns]
    if miss:
        raise HTTPException(
            502,
            detail=f"Statcast frame missing columns for score snapshot: {miss}",
        )
    for col in ("at_bat_number", "pitch_number", "game_date"):
        if col not in df.columns:
            raise HTTPException(
                502,
                detail=f"Statcast frame missing {col!r} (need row ordering within inning).",
            )

    df = df.copy()
    df["game_pk"] = pd.to_numeric(df["game_pk"], errors="coerce")
    df = df[df["game_pk"].notna()]
    df["pitcher"] = pd.to_numeric(df["pitcher"], errors="coerce")
    work = df[df["pitcher"] == pid].copy()
    if work.empty:
        return {
            "columns": [],
            "rows": [],
            "source": "pitcher_entering_inning_lead_statcast",
            "note": f"No pitches in range for pitcher_id={pid}.",
        }

    uq = [int(x) for x in work["game_pk"].dropna().unique()]
    if len(uq) > int(body.max_games):
        raise HTTPException(
            400,
            detail=(
                f"Too many distinct games ({len(uq)}) in this pitcher Statcast pull; "
                f"narrow start_date/end_date or raise max_games (cap {body.max_games})."
            ),
        )

    def _full_game_cached(gpk: int) -> pd.DataFrame | None:
        ck = f"scsg:{gpk}"
        hit = _cache_get(ck)
        if hit is not None:
            return hit  # type: ignore[return-value]
        try:
            out = statcast_single_game(int(gpk))
        except Exception:
            return None
        if out is None or (isinstance(out, pd.DataFrame) and out.empty):
            return None
        if not isinstance(out, pd.DataFrame):
            return None
        _cache_set(ck, out)
        return out

    eligible_pks: set[int] = set()
    snap_rows: list[dict[str, Any]] = []

    for gpk in uq:
        pg = work[work["game_pk"] == gpk].copy()
        pg = pg.assign(_hk=pg["inning_topbot"].map(_statcast_half_sort_key))
        g0 = pg.sort_values(
            ["inning", "_hk", "at_bat_number", "pitch_number"],
            ascending=[True, True, True, True],
        )
        pit_row = g0.iloc[0]
        gray_team = _pitching_team_abbr_statcast_row(pit_row)
        if gray_team is None:
            continue

        full = _full_game_cached(gpk)
        if full is None:
            continue
        need2 = ("inning", "inning_topbot", "home_score", "away_score", "home_team", "away_team")
        if any(c not in full.columns for c in need2):
            continue
        if "at_bat_number" not in full.columns or "pitch_number" not in full.columns:
            continue
        inn_sub = full[full["inning"] == inn_target].copy()
        if inn_sub.empty:
            continue
        inn_sub = inn_sub.assign(_hk=inn_sub["inning_topbot"].map(_statcast_half_sort_key))
        inn_sub = inn_sub.sort_values(
            ["_hk", "at_bat_number", "pitch_number"],
            ascending=[True, True, True],
        )
        snap = inn_sub.iloc[0]
        hs = pd.to_numeric(snap.get("home_score"), errors="coerce")
        ash = pd.to_numeric(snap.get("away_score"), errors="coerce")
        if pd.isna(hs) or pd.isna(ash):
            continue
        ht = _abbr_cell(snap.get("home_team"))
        at = _abbr_cell(snap.get("away_team"))
        margin = _margin_for_team(float(hs), float(ash), ht, at, gray_team)
        if margin is None:
            continue
        if float(margin) < float(body.min_lead_runs) - 1e-9:
            continue
        eligible_pks.add(gpk)
        snap_rows.append(
            {
                "GamePk": gpk,
                "game_date": snap.get("game_date"),
                "home_score_in": int(hs),
                "away_score_in": int(ash),
                "pitcher_team": gray_team,
                "margin": int(margin),
            }
        )

    if not eligible_pks:
        return {
            "columns": [],
            "rows": [],
            "source": "pitcher_entering_inning_lead_statcast",
            "note": (
                f"No games where pitcher_id={pid}’s team led by ≥{body.min_lead_runs} at the first pitch "
                f"of inning {inn_target} in {body.start_date}–{body.end_date}. "
                "Check date range or try min_lead_runs=1."
            ),
        }

    # --- Sum full-game pitching lines from MLB gameLog for eligible game_pk ---
    by_season: dict[int, list[dict[str, Any]]] = {}
    for sr in snap_rows:
        if int(sr["GamePk"]) not in eligible_pks:
            continue
        gd = sr.get("game_date")
        if gd is None or (isinstance(gd, float) and pd.isna(gd)):
            continue
        try:
            y = int(str(gd)[:4])
        except (TypeError, ValueError):
            continue
        by_season.setdefault(y, []).append(sr)

    matched_splits: list[dict[str, Any]] = []
    for season, snaps in sorted(by_season.items()):
        splits = _mlb_fetch_game_log_splits(
            f"people/{pid}",
            int(season),
            "pitching",
            None,
            None,
        )
        pk_set = {int(sr["GamePk"]) for sr in snaps}
        for sp in splits:
            g = sp.get("game") or {}
            pk = g.get("gamePk")
            if pk is None:
                continue
            try:
                ipk = int(pk)
            except (TypeError, ValueError):
                continue
            if ipk in pk_set:
                matched_splits.append(sp)

    if not matched_splits:
        return {
            "columns": ["eligible_games_statcast", "games_matched_game_log"],
            "rows": [
                {
                    "eligible_games_statcast": len(eligible_pks),
                    "games_matched_game_log": 0,
                }
            ],
            "source": "pitcher_entering_inning_lead_statcast",
            "note": (
                f"Found {len(eligible_pks)} Statcast games meeting the lead rule, but no matching "
                "MLB gameLog rows (season boundary or API mismatch). Try a narrower date range."
            ),
        }

    total_outs = 0
    th = tr = ter = tbb = tso = thr = tnp = 0
    team_wins = 0
    for sp in matched_splits:
        st = sp.get("stat") or {}
        total_outs += _ip_value_to_outs(st.get("inningsPitched"))
        th += _safe_int_mlb(st.get("hits")) or 0
        tr += _safe_int_mlb(st.get("runs")) or 0
        ter += _safe_int_mlb(st.get("earnedRuns")) or 0
        tbb += _safe_int_mlb(st.get("baseOnBalls")) or 0
        tso += _safe_int_mlb(st.get("strikeOuts")) or 0
        thr += _safe_int_mlb(st.get("homeRuns")) or 0
        tnp += _safe_int_mlb(st.get("numberOfPitches")) or 0
        if sp.get("isWin") is True:
            team_wins += 1

    games = len(matched_splits)
    ipf = total_outs / 3.0
    era = round(9.0 * ter / ipf, 2) if ipf > 0 else None
    whip = round((tbb + th) / ipf, 3) if ipf > 0 else None
    k9 = round(9.0 * tso / ipf, 2) if ipf > 0 else None
    bb9 = round(9.0 * tbb / ipf, 2) if ipf > 0 else None

    agg = {
        "pitcher_id": pid,
        "entering_inning": inn_target,
        "min_lead_runs": body.min_lead_runs,
        "Games": games,
        "IP": _outs_to_ip_string(int(total_outs)),
        "H": th,
        "R": tr,
        "ER": ter,
        "BB": tbb,
        "SO": tso,
        "HR": thr,
        "NP": tnp if tnp else None,
        "ERA": era,
        "WHIP": whip,
        "K9": k9,
        "BB9": bb9,
        "TeamWins": team_wins,
    }
    out_df = pd.DataFrame([agg])
    table = _df_to_payload(out_df, 3)
    note = (
        f"**Lead rule:** pitcher’s team led by **≥{body.min_lead_runs}** runs at the **first pitch of inning "
        f"{inn_target}** (Statcast **home_score/away_score** on that pitch). "
        "**Pitching totals** are **full-game** MLB gameLog lines for those games only—not innings after the snapshot. "
        "Rare scoreboard edge cases can differ slightly from TV graphics."
    )
    return {**table, "source": "pitcher_entering_inning_lead_statcast", "note": note}


class StatcastSpinVarianceBody(BaseModel):
    start_date: str = Field(description="YYYY-MM-DD")
    end_date: str = Field(description="YYYY-MM-DD")
    pitch_type: str = Field(default="FF")
    group_by: Literal["pitcher", "batter"] = "pitcher"
    min_pitches: int = Field(ge=1, le=10000, default=50)
    row_cap: int = Field(ge=1, le=500, default=100)


@app.post("/v1/statcast_spin_variance")
def statcast_spin_variance(body: StatcastSpinVarianceBody) -> dict[str, Any]:
    if statcast is None:
        raise HTTPException(500, detail=f"pybaseball unavailable: {_IMPORT_ERROR}")
    cache_key = f"ssv:{body.start_date}:{body.end_date}:{body.pitch_type}"
    cached = _cache_get(cache_key)
    if cached is None:
        try:
            df = statcast(
                start_dt=body.start_date,
                end_dt=body.end_date,
                team=None,
                verbose=False,
                parallel=False,
            )
        except Exception as e:
            raise HTTPException(502, detail=f"Statcast pull error: {e}") from e
        _cache_set(cache_key, df)
    else:
        df = cached
    if not isinstance(df, pd.DataFrame):
        raise HTTPException(502, detail="Unexpected non-DataFrame from statcast")
    if "release_spin_rate" not in df.columns:
        raise HTTPException(502, detail="release_spin_rate not found in statcast frame")

    work = df.copy()
    if "pitch_type" in work.columns:
        work = work[work["pitch_type"].astype(str).str.upper() == body.pitch_type.upper()]
    work["release_spin_rate"] = pd.to_numeric(work["release_spin_rate"], errors="coerce")
    work = work.dropna(subset=["release_spin_rate"])

    id_col = "pitcher" if body.group_by == "pitcher" else "batter"
    if id_col not in work.columns:
        raise HTTPException(502, detail=f"{id_col} not found in statcast frame")
    name_col = (
        "player_name"
        if body.group_by == "pitcher"
        else ("batter_name" if "batter_name" in work.columns else None)
    )

    group = work.groupby(id_col)["release_spin_rate"].agg(["count", "mean", "std", "var"]).reset_index()
    group = group[group["count"] >= int(body.min_pitches)]
    if name_col and name_col in work.columns:
        names = work.groupby(id_col)[name_col].first().reset_index()
        group = group.merge(names, on=id_col, how="left")
    group = group.sort_values("var", ascending=False)
    out_cols = [id_col]
    if name_col and name_col in group.columns:
        out_cols.append(name_col)
    out_cols.extend(["count", "mean", "std", "var"])
    slim = group[out_cols].head(int(body.row_cap))
    table = _df_to_payload(slim, body.row_cap)
    return {**table, "source": "baseball_savant_statcast_search"}


HIT_EVENTS = frozenset({"single", "double", "triple", "home_run"})


class BatterHitDistanceByParkBody(BaseModel):
    batter_id: int = Field(ge=1)
    start_date: str = Field(description="YYYY-MM-DD")
    end_date: str = Field(description="YYYY-MM-DD")
    min_hits: int = Field(ge=1, le=500, default=1)
    row_cap: int = Field(ge=1, le=50, default=35)


@app.post("/v1/batter_hit_distance_by_park")
def batter_hit_distance_by_park(body: BatterHitDistanceByParkBody) -> dict[str, Any]:
    """Avg hit_distance_sc on base hits, grouped by home_team (stadium proxy)."""
    if statcast is None:
        raise HTTPException(500, detail=f"pybaseball unavailable: {_IMPORT_ERROR}")
    cache_key = f"sc_raw:{body.start_date}:{body.end_date}"
    cached = _cache_get(cache_key)
    if cached is None:
        try:
            df = statcast(
                start_dt=body.start_date,
                end_dt=body.end_date,
                team=None,
                verbose=False,
                parallel=False,
            )
        except Exception as e:
            raise HTTPException(502, detail=f"Statcast pull error: {e}") from e
        _cache_set(cache_key, df)
    else:
        df = cached
    if not isinstance(df, pd.DataFrame):
        raise HTTPException(502, detail="Unexpected non-DataFrame from statcast")
    if "batter" not in df.columns:
        raise HTTPException(502, detail="batter column missing")
    if "hit_distance_sc" not in df.columns:
        raise HTTPException(502, detail="hit_distance_sc missing from Statcast frame")
    if "home_team" not in df.columns:
        raise HTTPException(502, detail="home_team missing from Statcast frame")

    work = df.copy()
    work["batter"] = pd.to_numeric(work["batter"], errors="coerce")
    work = work[work["batter"] == int(body.batter_id)]
    if work.empty:
        return {
            "columns": ["home_team_abbr", "hits", "avg_hit_distance_ft", "median_hit_distance_ft"],
            "rows": [],
            "source": "baseball_savant_statcast_search",
            "note": "No rows for this batter in range.",
        }

    if "events" in work.columns:
        ev = work["events"].astype(str).str.strip().str.lower()
        work = work[ev.isin({e.lower() for e in HIT_EVENTS})]
    work["hit_distance_sc"] = pd.to_numeric(work["hit_distance_sc"], errors="coerce")
    work = work.dropna(subset=["hit_distance_sc", "home_team"])
    if work.empty:
        return {
            "columns": ["home_team_abbr", "hits", "avg_hit_distance_ft", "median_hit_distance_ft"],
            "rows": [],
            "source": "baseball_savant_statcast_search",
            "note": "No batted-ball hits with distance in range after filters.",
        }

    g = (
        work.groupby("home_team", dropna=True)["hit_distance_sc"]
        .agg(
            hits="count",
            avg_hit_distance_ft="mean",
            median_hit_distance_ft="median",
        )
        .reset_index()
        .rename(columns={"home_team": "home_team_abbr"})
    )
    g = g[g["hits"] >= int(body.min_hits)]
    g = g.sort_values("avg_hit_distance_ft", ascending=False)
    slim = g.head(int(body.row_cap))
    table = _df_to_payload(slim, body.row_cap)
    note = (
        "home_team_abbr is the game's home club (proxy for which stadium the PA occurred in). "
        "Shared/neutral sites are not distinguished."
    )
    return {**table, "source": "baseball_savant_statcast_search", "note": note}


def _statcast_runner_on_base(df: pd.DataFrame, col: str) -> pd.Series:
    """True if Savant shows a pre-pitch runner id on this base (numeric MLBAM)."""
    if col not in df.columns:
        return pd.Series(False, index=df.index, dtype=bool)
    s = df[col]
    n = pd.to_numeric(s, errors="coerce")
    has_num = n.notna() & (n > 0)
    st = s.astype(str).str.strip()
    has_digits = st.str.match(r"^\d+$", na=False)
    return has_num | has_digits


def _statcast_situation_mask(terminal: pd.DataFrame, situation: str) -> pd.Series:
    on1 = _statcast_runner_on_base(terminal, "on_1b")
    on2 = _statcast_runner_on_base(terminal, "on_2b")
    on3 = _statcast_runner_on_base(terminal, "on_3b")
    if situation == "risp":
        return on2 | on3
    if situation == "men_on":
        return on1 | on2 | on3
    if situation == "bases_empty":
        return ~(on1 | on2 | on3)
    if situation == "any":
        return pd.Series(True, index=terminal.index, dtype=bool)
    raise HTTPException(400, detail=f"Unknown situation {situation!r}")


def _dedupe_statcast_pa_terminal(terminal: pd.DataFrame) -> pd.DataFrame:
    keys = [c for c in ("game_pk", "at_bat_number") if c in terminal.columns]
    if len(keys) < 2:
        return terminal
    return terminal.sort_values(keys).drop_duplicates(subset=keys, keep="last")


def _aggregate_one_batter_rates_from_evt(terminal: pd.DataFrame) -> dict[str, Any] | None:
    """Reuses team-batting counting logic for one batter's terminal rows."""
    if terminal.empty:
        return None
    terminal = terminal.copy()
    terminal["evt_cat"] = terminal["events"].map(_classify_statcast_pa_event)
    terminal = terminal[terminal["evt_cat"].notna()]
    if terminal.empty:
        return None
    vc = terminal["evt_cat"].value_counts()
    get = lambda k: int(vc.get(k, 0))
    c1b, c2b, c3b, chr_ = get("1b"), get("2b"), get("3b"), get("hr")
    bb, hbp, sf, sh, ci = get("bb"), get("hbp"), get("sf"), get("sh"), get("ci")
    k = get("k")
    abo = get("ab_other")
    pa = int(terminal.shape[0])
    h = c1b + c2b + c3b + chr_
    ab = pa - bb - hbp - sf - sh - ci
    if ab <= 0 and pa == 0:
        return None
    tb = c1b + 2 * c2b + 3 * c3b + 4 * chr_
    avg = round(float(h) / float(ab), 3) if ab > 0 else None
    obp = round(float(h + bb + hbp) / float(pa), 3) if pa > 0 else None
    slg = round(float(tb) / float(ab), 3) if ab > 0 else None
    ops = round(float(obp) + float(slg), 3) if obp is not None and slg is not None else None
    return {
        "pa": pa,
        "ab": ab,
        "h": h,
        "doubles": c2b,
        "triples": c3b,
        "hr": chr_,
        "bb": bb,
        "hbp": hbp,
        "sf": sf,
        "sh": sh,
        "so": k,
        "avg": avg,
        "obp": obp,
        "slg": slg,
        "ops": ops,
        "ab_other": abo,
    }


class BatterSituationalStatcastBody(BaseModel):
    batter_id: int = Field(ge=1, description="MLBAM batter id (Savant `batter` column).")
    start_date: str = Field(description="YYYY-MM-DD")
    end_date: str = Field(description="YYYY-MM-DD")
    situation: Literal["risp", "men_on", "bases_empty", "any"] = Field(
        description=(
            "risp = runner on 2B or 3B; men_on = any runner; bases_empty = none; "
            "any = full sample in date range (no runner filter)."
        ),
    )
    min_pa: int = Field(
        ge=1,
        le=900,
        default=1,
        description="Minimum plate appearances in this split to return stats (raise noise if too low).",
    )


@app.post("/v1/batter_situational_statcast")
def batter_situational_statcast(body: BatterSituationalStatcastBody) -> dict[str, Any]:
    """
    Situational batting (RISP, men on, bases empty) from Statcast using on_1b/on_2b/on_3b on PA-terminal pitches.
    Official MLB statSplits feed is often empty on the public API; this path uses Savant pitch rows.
    """
    if statcast is None:
        raise HTTPException(500, detail=f"pybaseball unavailable: {_IMPORT_ERROR}")
    if not _DATE_ISO.match(body.start_date) or not _DATE_ISO.match(body.end_date):
        raise HTTPException(400, detail="start_date and end_date must be YYYY-MM-DD")
    if body.start_date > body.end_date:
        raise HTTPException(400, detail="start_date must be on or before end_date")
    try:
        d0 = time.strptime(body.start_date, "%Y-%m-%d")
        d1 = time.strptime(body.end_date, "%Y-%m-%d")
        span = (time.mktime(d1) - time.mktime(d0)) / 86400.0
    except ValueError:
        raise HTTPException(400, detail="Invalid calendar date") from None
    if span > 400:
        raise HTTPException(400, detail="Date span exceeds 400 days; narrow the window.")

    bid = int(body.batter_id)
    cache_key = f"bssit:{body.start_date}:{body.end_date}:{bid}"
    cached = _cache_get(cache_key)
    if cached is None:
        try:
            df = statcast(
                start_dt=body.start_date,
                end_dt=body.end_date,
                hitter=str(bid),
                verbose=False,
                parallel=False,
            )
        except TypeError:
            try:
                df = statcast(body.start_date, body.end_date, hitter=str(bid))
            except Exception as e:
                raise HTTPException(502, detail=f"Statcast pull error: {e}") from e
        except Exception as e:
            raise HTTPException(502, detail=f"Statcast pull error: {e}") from e
        _cache_set(cache_key, df)
    else:
        df = cached
    if not isinstance(df, pd.DataFrame):
        raise HTTPException(502, detail="Unexpected non-DataFrame from statcast")

    if "batter" in df.columns:
        work = df[pd.to_numeric(df["batter"], errors="coerce") == bid].copy()
    else:
        work = df.copy()

    if work.empty:
        return {
            "columns": [],
            "rows": [],
            "source": "baseball_savant_statcast_search",
            "note": f"No Statcast rows for batter_id={bid} in date range.",
        }

    need = ("events",)
    missing = [c for c in need if c not in work.columns]
    if missing:
        raise HTTPException(502, detail=f"Statcast frame missing columns: {missing}")

    ev = work["events"].astype(str).str.strip()
    terminal = work[ev.ne("") & ev.ne("nan") & work["events"].notna()].copy()
    if terminal.empty:
        return {
            "columns": [],
            "rows": [],
            "source": "baseball_savant_statcast_search",
            "note": "No PA-terminal event rows for this batter in range.",
        }

    terminal = _dedupe_statcast_pa_terminal(terminal)

    if not {"on_1b", "on_2b", "on_3b"}.issubset(terminal.columns):
        raise HTTPException(
            502,
            detail="Statcast export missing on_1b/on_2b/on_3b; cannot compute runner situations.",
        )

    mask = _statcast_situation_mask(terminal, body.situation)
    filt = terminal.loc[mask]
    agg = _aggregate_one_batter_rates_from_evt(filt)

    if agg is None or agg["pa"] < int(body.min_pa):
        return {
            "columns": [
                "batter_id",
                "situation",
                "start_date",
                "end_date",
                "pa",
                "note",
            ],
            "rows": [
                {
                    "batter_id": bid,
                    "situation": body.situation,
                    "start_date": body.start_date,
                    "end_date": body.end_date,
                    "pa": agg["pa"] if agg else 0,
                    "note": (
                        f"Below min_pa={body.min_pa} after situational filter "
                        f"(try wider dates or situation=any)."
                    ),
                }
            ],
            "source": "baseball_savant_statcast_search",
            "note": (
                "Situational line from Statcast **on_1b/on_2b/on_3b** on the pitch that records the PA result. "
                "Matches **RISP** = men on 2nd or 3rd only. Rare tagging edge cases can differ slightly from "
                "MLB.com display. For season-long lines without splits use **batting_season_stats**."
            ),
        }

    name = None
    if "player_name" in filt.columns and not filt.empty:
        try:
            name = str(filt["player_name"].iloc[-1])
        except Exception:
            name = None

    row = {
        "batter_id": bid,
        "batter_name": name,
        "situation": body.situation,
        "start_date": body.start_date,
        "end_date": body.end_date,
        **agg,
    }
    df_out = pd.DataFrame([row])
    table = _df_to_payload(df_out, 1)
    note = (
        "Situational batting from **Statcast** using pre-pitch runners on base "
        "(**on_2b/on_3b** for RISP). One row; **avg/obp/slg/ops** use official-style PA/AB rules on classified events. "
        "Not the same as MLB’s empty public **statSplits** JSON—this is computed from Savant."
    )
    return {**table, "source": "baseball_savant_statcast_search", "note": note}


def _classify_statcast_pa_event(ev: Any) -> str | None:
    """Bucket Statcast `events` on PA-terminal rows into counting-stat categories."""
    if ev is None or (isinstance(ev, float) and np.isnan(ev)):
        return None
    e = str(ev).strip().lower()
    if not e or e == "nan":
        return None
    if e in ("single",):
        return "1b"
    if e in ("double",):
        return "2b"
    if e in ("triple",):
        return "3b"
    if e in ("home_run",):
        return "hr"
    if e in ("walk", "intent_walk"):
        return "bb"
    if e in ("hit_by_pitch",):
        return "hbp"
    if "strikeout" in e:
        return "k"
    if e in ("sac_fly",):
        return "sf"
    if e in ("sac_bunt",):
        return "sh"
    if e in ("catcher_interf", "catcher_interference"):
        return "ci"
    return "ab_other"


# Statcast home_team / away_team are usually 3-letter codes (BOS) but sometimes full
# club names. Taking the first 3 alnum chars breaks "Red Sox" -> "RED" and drops all BOS rows.
_SYNONYM_ABBR = {
    "TB": "TBR",
    "TBA": "TBR",
    "WAS": "WSN",
    "WSH": "WSN",
    "SFG": "SFG",
    "SF": "SFG",
    "SDP": "SDP",
    "SD": "SDP",
    "KCR": "KCR",
    "KC": "KCR",
}

# Compact (letters+digits only, upper) -> canonical Savant-style abbr
_TEAM_COMPACT: dict[str, str] = {
    "BOS": "BOS",
    "BOSTONREDSOX": "BOS",
    "REDSOX": "BOS",
    "NYY": "NYY",
    "NEWYORKYANKEES": "NYY",
    "TBR": "TBR",
    "TAMPABAYRAYS": "TBR",
    "TOR": "TOR",
    "TORONTOBLUEJAYS": "TOR",
    "BAL": "BAL",
    "BALTIMOREORIOLES": "BAL",
    "TEX": "TEX",
    "TEXASRANGERS": "TEX",
    "HOU": "HOU",
    "HOUSTONASTROS": "HOU",
    "SEA": "SEA",
    "SEAMARINERS": "SEA",
    "LAA": "LAA",
    "LOSANGELESANGELS": "LAA",
    "OAK": "OAK",
    "OAKLANDATHLETICS": "OAK",
    "MIN": "MIN",
    "MINNESOTATWINS": "MIN",
    "CLE": "CLE",
    "CLEVELANDGUARDIANS": "CLE",
    "CLEVELANDINDIANS": "CLE",
    "DET": "DET",
    "DETROITTIGERS": "DET",
    "CHW": "CHW",
    "CHICAGOWHITESOX": "CHW",
    "KCR": "KCR",
    "KANSASCITYROYALS": "KCR",
    "ATL": "ATL",
    "ATLANTABRAVES": "ATL",
    "MIA": "MIA",
    "MIAMIMARLINS": "MIA",
    "NYM": "NYM",
    "NEWYORKMETS": "NYM",
    "PHI": "PHI",
    "PHILADELPHIAPHILLIES": "PHI",
    "WSN": "WSN",
    "WASHINGTONNATIONALS": "WSN",
    "CHC": "CHC",
    "CHICAGOCUBS": "CHC",
    "CIN": "CIN",
    "CINCINNATIREDS": "CIN",
    "MIL": "MIL",
    "MILWAUKEEBREWERS": "MIL",
    "PIT": "PIT",
    "PITTSBURGHPIRATES": "PIT",
    "STL": "STL",
    "STLOUISCARDINALS": "STL",
    "ARI": "ARI",
    "ARIZONADIAMONDBACKS": "ARI",
    "COL": "COL",
    "COLORADOROCKIES": "COL",
    "LAD": "LAD",
    "LOSANGELESDODGERS": "LAD",
    "SDP": "SDP",
    "SANDIEGOPADRES": "SDP",
    "SFG": "SFG",
    "SANFRANCISCOGIANTS": "SFG",
}

_VALID_MLB_ABBRS = frozenset(_TEAM_COMPACT.values()) | frozenset(_SYNONYM_ABBR.keys()) | frozenset(
    _SYNONYM_ABBR.values()
)


def _abbr_cell(x: Any) -> str | None:
    """Normalize Savant home_team / away_team to a 3-letter MLB code."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip().upper()
    if not s or s == "NAN":
        return None

    # Already a standard 3-letter code
    if len(s) == 3 and s.isalpha():
        return _SYNONYM_ABBR.get(s, s)

    compact = "".join(ch for ch in s if ch.isalnum())
    if compact in _TEAM_COMPACT:
        return _TEAM_COMPACT[compact]

    # Substring heuristics (full names without reliable compact keys)
    if "WHITE" in s and "SOX" in s:
        return "CHW"
    if "RED" in s and "SOX" in s:
        return "BOS"
    if "YANKEE" in s:
        return "NYY"
    if "RAY" in s and "TAMPA" in s:
        return "TBR"
    if "BLUE JAY" in s or "BLUEJAY" in compact:
        return "TOR"
    if "ORIOLE" in s:
        return "BAL"
    if "GUARDIAN" in s or "INDIAN" in s:
        return "CLE"
    if "TIGER" in s and "DETROIT" in s:
        return "DET"
    if "TWIN" in s:
        return "MIN"
    if "ROYAL" in s:
        return "KCR"
    if "ANGEL" in s and "LOS ANGELES" in s.replace("  ", " ") and "DODGER" not in s:
        return "LAA"
    if "DODGER" in s:
        return "LAD"
    if "PADRE" in s:
        return "SDP"
    if "GIANT" in s and "SAN FRANCISCO" in s:
        return "SFG"
    if "NATIONAL" in s and "WASHINGTON" in s:
        return "WSN"
    if "MET" in s and "NEW YORK" in s and "YANKEE" not in s:
        return "NYM"
    if "PHILL" in s:
        return "PHI"
    if "BRAVE" in s:
        return "ATL"
    if "MARLIN" in s:
        return "MIA"
    if "CUB" in s and "CHICAGO" in s:
        return "CHC"
    if "REDS" in s and "CINCINNATI" in s:
        return "CIN"
    if "BREWER" in s:
        return "MIL"
    if "PIRATE" in s:
        return "PIT"
    if "CARDINAL" in s:
        return "STL"
    if "DIAMONDBACK" in s or "D-BACK" in s:
        return "ARI"
    if "ROCKIE" in s:
        return "COL"
    if "ASTRO" in s:
        return "HOU"
    if "RANGER" in s and "TEXAS" in s:
        return "TEX"
    if "MARINER" in s:
        return "SEA"
    if "ATHLETIC" in s and "OAK" in s:
        return "OAK"

    # Last resort: first 3 letters only if they are a known MLB abbreviation (never "RED")
    if len(compact) >= 3 and compact[:3].isalpha():
        cand = compact[:3]
        if cand in _SYNONYM_ABBR:
            return _SYNONYM_ABBR[cand]
        if cand in _VALID_MLB_ABBRS:
            return cand

    return None


def _classify_half_inning(v: Any) -> str | None:
    """Return 'top', 'bottom', or None. Handles strings, ints (1/2), and NaN."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, (bool, np.bool_)):
        return None
    if isinstance(v, (int, np.integer)) and not isinstance(v, bool):
        if int(v) == 1:
            return "top"
        if int(v) == 2:
            return "bottom"
    if isinstance(v, float) and not isinstance(v, bool):
        if np.isnan(v):
            return None
        if float(v) == 1.0:
            return "top"
        if float(v) == 2.0:
            return "bottom"
    s = str(v).strip().lower()
    if not s or s == "nan":
        return None
    if s.startswith("top") or s in ("t", "1"):
        return "top"
    if s.startswith("bot") or s.startswith("bottom") or s in ("b", "2"):
        return "bottom"
    return None


def _batting_team_series(df: pd.DataFrame) -> tuple[pd.Series, str]:
    """
    Which team is batting for each pitch row (vectorized).
    Primary: inning half + home/away. Fallback: bat_score vs home/away score when not tied.
    Returns (series of 3-letter codes or NA, note suffix for transparency).
    """
    ht = df["home_team"].map(_abbr_cell) if "home_team" in df.columns else None
    at = df["away_team"].map(_abbr_cell) if "away_team" in df.columns else None
    if ht is None or at is None:
        raise HTTPException(502, detail="Statcast frame missing home_team/away_team")

    half = (
        df["inning_topbot"].map(_classify_half_inning)
        if "inning_topbot" in df.columns
        else pd.Series(pd.NA, index=df.index, dtype=object)
    )
    bt = pd.Series(pd.NA, index=df.index, dtype=object)
    top_m = half == "top"
    bot_m = half == "bottom"
    bt.loc[top_m] = at.loc[top_m].values
    bt.loc[bot_m] = ht.loc[bot_m].values

    note_extra = ""
    if {"bat_score", "home_score", "away_score"}.issubset(df.columns):
        bs = pd.to_numeric(df["bat_score"], errors="coerce")
        hs = pd.to_numeric(df["home_score"], errors="coerce")
        ascr = pd.to_numeric(df["away_score"], errors="coerce")
        bsv = bs.to_numpy(dtype=float, copy=False)
        hsv = hs.to_numpy(dtype=float, copy=False)
        asv = ascr.to_numpy(dtype=float, copy=False)
        scr_ok = np.isfinite(bsv) & np.isfinite(hsv) & np.isfinite(asv)
        tied = scr_ok & np.isclose(hsv, asv, rtol=0, atol=1e-6)
        away_bat = scr_ok & (~tied) & np.isclose(bsv, asv, rtol=0, atol=1e-6)
        home_bat = scr_ok & (~tied) & np.isclose(bsv, hsv, rtol=0, atol=1e-6)
        fill = pd.Series(
            np.where(away_bat, at.to_numpy(), np.where(home_bat, ht.to_numpy(), pd.NA)),
            index=df.index,
            dtype=object,
        )
        bt = bt.combine_first(fill)
        if bool(np.any(away_bat | home_bat)):
            note_extra = " Used bat_score/home_score/away_score to infer batting team when half-inning was ambiguous."

    return bt.astype(object), note_extra


class TeamBattingStatcastBody(BaseModel):
    start_date: str = Field(description="YYYY-MM-DD")
    end_date: str = Field(description="YYYY-MM-DD")
    team_abbr: str = Field(
        min_length=2,
        max_length=4,
        description="Savant 3-letter team code, e.g. BOS, NYY, WSH, MIL",
    )
    min_pa: int = Field(ge=1, le=900, default=3)
    row_cap: int = Field(ge=1, le=80, default=40)


@app.post("/v1/team_batting_statcast")
def team_batting_statcast(body: TeamBattingStatcastBody) -> dict[str, Any]:
    """
    PA-level Statcast aggregates for hitters on one team (when that team is batting).
    Use when FanGraphs batting lines fail or as a Savant-based cross-check.
    """
    if statcast is None:
        raise HTTPException(500, detail=f"pybaseball unavailable: {_IMPORT_ERROR}")
    team_u = body.team_abbr.strip().upper()
    cache_key = f"tbsc:{body.start_date}:{body.end_date}:{team_u}"
    cached = _cache_get(cache_key)
    if cached is None:
        try:
            df = statcast(
                start_dt=body.start_date,
                end_dt=body.end_date,
                team=team_u,
                verbose=False,
                parallel=False,
            )
        except Exception as e:
            raise HTTPException(502, detail=f"Statcast pull error: {e}") from e
        _cache_set(cache_key, df)
    else:
        df = cached
    if not isinstance(df, pd.DataFrame):
        raise HTTPException(502, detail="Unexpected non-DataFrame from statcast")
    need = ("batter", "events", "home_team", "away_team")
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise HTTPException(502, detail=f"Statcast frame missing columns: {missing}")
    if "inning_topbot" not in df.columns and not {"bat_score", "home_score", "away_score"}.issubset(
        df.columns
    ):
        raise HTTPException(
            502,
            detail=(
                "Statcast frame needs inning_topbot or bat_score/home_score/away_score "
                "to infer which team is batting."
            ),
        )

    work = df.copy()
    bt_series, half_note = _batting_team_series(work)
    work["_bat_team"] = bt_series
    work = work[work["_bat_team"].eq(team_u)]
    if work.empty:
        return {
            "columns": [],
            "rows": [],
            "source": "baseball_savant_statcast_search",
            "note": (
                f"No rows for team {team_u} in range after inferring batting team "
                f"(check Savant abbreviation and dates).{half_note}"
            ),
        }

    ev = work["events"].astype(str).str.strip()
    terminal = work[ev.ne("") & ev.ne("nan") & work["events"].notna()].copy()
    if terminal.empty:
        return {
            "columns": [],
            "rows": [],
            "source": "baseball_savant_statcast_search",
            "note": "No PA-terminal rows (events) in filtered data.",
        }

    terminal["evt_cat"] = terminal["events"].map(_classify_statcast_pa_event)
    terminal = terminal[terminal["evt_cat"].notna()]
    if terminal.empty:
        return {
            "columns": [],
            "rows": [],
            "source": "baseball_savant_statcast_search",
            "note": "Could not classify PA events in range.",
        }

    name_col = "batter_name" if "batter_name" in terminal.columns else None
    counts = (
        terminal.groupby(["batter", "evt_cat"]).size().unstack(fill_value=0).astype(int)
    )
    for col in ("1b", "2b", "3b", "hr", "bb", "hbp", "k", "sf", "sh", "ci", "ab_other"):
        if col not in counts.columns:
            counts[col] = 0

    counts["pa"] = counts.sum(axis=1)
    counts = counts[counts["pa"] >= int(body.min_pa)]
    if counts.empty:
        return {
            "columns": [],
            "rows": [],
            "source": "baseball_savant_statcast_search",
            "note": (
                f"No batters with at least {body.min_pa} PA in range for {team_u} "
                "(try a lower min_pa or wider dates)."
            ),
        }

    c1b, c2b, c3b, chr_ = counts["1b"], counts["2b"], counts["3b"], counts["hr"]
    h = c1b + c2b + c3b + chr_
    bb, hbp, sf, sh, ci = counts["bb"], counts["hbp"], counts["sf"], counts["sh"], counts["ci"]
    ab = counts["pa"] - bb - hbp - sf - sh - ci
    safe_ab = ab.replace(0, np.nan)
    pa_safe = counts["pa"].replace(0, np.nan)
    tb = c1b + 2 * c2b + 3 * c3b + 4 * chr_
    obp_raw = (h + bb + hbp) / pa_safe
    slg_raw = tb / safe_ab
    avg = (h / safe_ab).round(3)
    obp = obp_raw.round(3)
    slg = slg_raw.round(3)
    ops = (obp_raw + slg_raw).round(3)

    out = pd.DataFrame(
        {
            "batter_id": counts.index.astype(int),
            "pa": counts["pa"].values,
            "bb": bb.values,
            "k": counts["k"].values,
            "h": h.values,
            "hr": chr_.values,
            "avg": avg.values,
            "obp": obp.values,
            "slg": slg.values,
            "ops": ops.values,
        }
    )
    if name_col:
        names = terminal.drop_duplicates(subset=["batter"], keep="last").set_index("batter")[name_col]
        name_map = {int(k): v for k, v in names.items()}
        out["batter_name"] = out["batter_id"].map(name_map).fillna("")

    if "launch_speed" in terminal.columns:
        terminal["launch_speed"] = pd.to_numeric(terminal["launch_speed"], errors="coerce")
        ev_mean = terminal.groupby("batter")["launch_speed"].mean()
        ev_map = {int(k): v for k, v in ev_mean.items()}
        out["avg_ev_mph"] = out["batter_id"].map(ev_map).round(1)

    out = out.sort_values("pa", ascending=False).head(int(body.row_cap))
    table = _df_to_payload(out, int(body.row_cap))
    note = (
        "Derived from Statcast PA-terminal rows (not FanGraphs). "
        "Team filter uses Savant games involving this club; rows kept only when that team is batting. "
        "avg/obp/slg match official formulas on classified events; rare mis-tagged events can add noise."
        f"{half_note}"
    )
    return {**table, "source": "baseball_savant_statcast_search", "note": note}


class PybaseballSandboxBody(BaseModel):
    """RestrictedPython + pybaseball in a subprocess; off unless ENABLE_PYBASEBALL_SANDBOX is set."""

    code: str = Field(
        min_length=1,
        max_length=12000,
        description="Assign to RESULT. No import statements; pd, np, statcast*, batting_stats, etc. are pre-bound.",
    )
    row_cap: int = Field(ge=1, le=500, default=200)
    timeout_sec: int = Field(ge=15, le=120, default=90)


@app.post("/v1/pybaseball_sandbox")
def pybaseball_sandbox(body: PybaseballSandboxBody) -> dict[str, Any]:
    """
    Escape hatch when no named endpoint fits: run a short snippet with pybaseball + pandas
    under RestrictedPython, subprocess isolation, and a hard timeout.
    """
    try:
        from pybaseball_sandbox import run_sandbox_in_subprocess, sandbox_feature_enabled
    except ImportError as e:
        raise HTTPException(
            503,
            detail=(
                "Sandbox needs RestrictedPython. From services/data run: "
                f"pip install -r requirements.txt (or: pip install 'RestrictedPython>=7.0'). Import error: {e}"
            ),
        ) from e
    if not sandbox_feature_enabled():
        raise HTTPException(
            403,
            detail="pybaseball sandbox is disabled. Set ENABLE_PYBASEBALL_SANDBOX=1 in the data service environment.",
        )
    if statcast is None:
        raise HTTPException(500, detail=f"pybaseball unavailable: {_IMPORT_ERROR}")
    return run_sandbox_in_subprocess(body.code, body.row_cap, body.timeout_sec)
