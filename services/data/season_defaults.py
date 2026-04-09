"""
Shared caps and defaults for season hitting/pitching endpoints.

Keep tool schemas (apps/web) and FastAPI models aligned with these values so
early-season / reliever questions do not silently drop whole rosters.
"""

# Matches Pydantic Field(le=...) on BattingSeasonBody / PitchingSeasonBody.row_cap
SEASON_STATS_ROW_CAP_MAX = 300

# Full-roster friendly defaults (early April, closers with low IP).
DEFAULT_MIN_PA_SEASON_BATTING = 0.0
DEFAULT_MIN_IP_SEASON_PITCHING = 0.0
