/**
 * Human-readable one-liners for progress UI while tools run.
 */
export function toolStatusDetail(name: string, rawInput: unknown): string {
  let parsed: Record<string, unknown> = {};
  if (rawInput && typeof rawInput === "object" && !Array.isArray(rawInput)) {
    parsed = rawInput as Record<string, unknown>;
  } else if (typeof rawInput === "string") {
    try {
      const j = JSON.parse(rawInput) as unknown;
      if (j && typeof j === "object" && !Array.isArray(j)) parsed = j as Record<string, unknown>;
    } catch {
      /* ignore */
    }
  }

  switch (name) {
    case "get_pitcher_pitch_arsenal": {
      const pid = parsed.pitcher_id;
      const y = parsed.year;
      return pid
        ? `Pitch arsenal (${y}, pitcher ${pid})`
        : `Pitch arsenal leaderboard (${y})`;
    }
    case "get_pitching_season_stats": {
      const ta = parsed.team_abbr ? ` ${parsed.team_abbr}` : "";
      return `Pitching season ${parsed.season ?? ""}${ta}`.trim();
    }
    case "get_fielding_season_stats": {
      const ta = parsed.team_abbr ? ` ${parsed.team_abbr}` : "";
      return `Fielding season ${parsed.season ?? ""}${ta}`.trim();
    }
    case "get_fielding_game_log":
      return `Fielding game log (${parsed.season ?? "?"}, player ${parsed.player_id ?? "?"})`.trim();
    case "get_team_game_log":
      return `Team game log ${parsed.team_abbr ?? "?"} (${parsed.season ?? "?"}, ${parsed.stat_group ?? "?"})`.trim();
    case "get_player_game_log":
      return `Player game log (${parsed.season ?? "?"}, ${parsed.stat_group ?? "?"}, id ${parsed.player_id ?? "?"})`.trim();
    case "get_player_game_log_vs_opponent":
      return `Career vs ${parsed.opponent_abbr ?? "?"} (${parsed.stat_group ?? "pitching"}, id ${parsed.player_id ?? "?"})`.trim();
    case "get_mlb_stat_leaders":
      return `MLB leaders ${parsed.season ?? ""} (${parsed.leader_category ?? "?"})`.trim();
    case "get_batting_season_stats": {
      const ta = parsed.team_abbr ? ` ${parsed.team_abbr}` : "";
      return `Batting season ${parsed.season ?? ""}${ta}`.trim();
    }
    case "get_team_batting_statcast":
      return `Team batting Statcast (${parsed.team_abbr ?? "?"}, ${parsed.start_date ?? "?"}–${parsed.end_date ?? "?"})`;
    case "resolve_player":
      return `Player lookup: ${parsed.first_name ?? ""} ${parsed.last_name ?? ""}`.trim();
    case "get_statcast_pitches": {
      const b = parsed.batter_id;
      const p = parsed.pitcher_id;
      const who = b ? `batter ${b}` : p ? `pitcher ${p}` : "league";
      return `Statcast pitches (${who}, ${parsed.start_date ?? "?"}–${parsed.end_date ?? "?"})`;
    }
    case "get_batter_vs_pitcher_statcast":
      return `Batter ${parsed.batter_id ?? "?"} vs pitcher ${parsed.pitcher_id ?? "?"} (${parsed.start_date ?? "2015-03-01"}–${parsed.end_date ?? "?"})`.trim();
    case "get_pitcher_entering_inning_lead_statcast":
      return `Pitcher ${parsed.pitcher_id ?? "?"} (lead ≥${parsed.min_lead_runs ?? "?"} entering ${parsed.entering_inning ?? "?"})`.trim();
    case "get_statcast_spin_variance":
      return `Spin variance (${parsed.pitch_type ?? "?"}, ${parsed.group_by ?? "?"})`;
    case "get_batter_hit_distance_by_park":
      return `Hit distance by park (batter ${parsed.batter_id ?? "?"})`;
    case "get_batter_situational_statcast":
      return `Batter situational ${parsed.situation ?? "?"} (batter ${parsed.batter_id ?? "?"})`;
    case "run_pybaseball_sandbox": {
      const c = typeof parsed.code === "string" ? parsed.code : "";
      const preview = c.length > 80 ? `${c.slice(0, 80)}…` : c;
      return `Pybaseball sandbox (${c.length} chars): ${preview || "…"}`;
    }
    default:
      return name.replace(/^get_/, "").replace(/_/g, " ");
  }
}
