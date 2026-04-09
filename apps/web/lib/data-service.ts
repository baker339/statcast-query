const DEFAULT_BASE = "http://127.0.0.1:8765";

export function dataServiceBase(): string {
  return (process.env.DATA_SERVICE_URL || DEFAULT_BASE).replace(/\/$/, "");
}

export type PitchArsenalArgs = {
  year: number;
  min_pitches: number;
  arsenal_type: "avg_spin" | "avg_speed";
  row_cap: number;
  pitch_type_filter?: string;
  /** MLBAM pitcher id — server filters leaderboard to this pitcher when set. */
  pitcher_id?: number;
};

export type PitchingSeasonArgs = {
  season: number;
  min_ip: number;
  metrics: string[];
  row_cap: number;
  team_abbr?: string;
  name_contains?: string;
};

export type FieldingSeasonArgs = {
  season: number;
  min_inn: number;
  metrics: string[];
  row_cap: number;
  team_abbr?: string;
  name_contains?: string;
};

export type FieldingGameLogArgs = {
  player_id: number;
  season: number;
  metrics: string[];
  row_cap: number;
  start_date?: string;
  end_date?: string;
  max_games?: number;
};

export type TeamGameLogArgs = {
  team_abbr: string;
  season: number;
  stat_group: "pitching" | "hitting";
  metrics: string[];
  row_cap: number;
  start_date?: string;
  end_date?: string;
  max_games?: number;
};

export type PlayerGameLogArgs = {
  player_id: number;
  season: number;
  stat_group: "pitching" | "hitting";
  metrics: string[];
  row_cap: number;
  start_date?: string;
  end_date?: string;
  max_games?: number;
};

/** Multi-season game logs filtered to one opponent (career “vs MIL”, etc.). */
export type PlayerGameLogVsOpponentArgs = {
  player_id: number;
  opponent_abbr: string;
  stat_group?: "pitching" | "hitting";
  start_season?: number;
  end_season?: number;
  aggregate?: boolean;
  metrics?: string[];
  row_cap?: number;
};

export type MlbStatLeadersArgs = {
  season: number;
  leader_category: string;
  stat_group?: "pitching" | "hitting";
  limit: number;
  leader_game_types: "R" | "P" | "F" | "D" | "L" | "W";
};

export type BattingSeasonArgs = {
  season: number;
  min_pa: number;
  metrics: string[];
  row_cap: number;
  team_abbr?: string;
  name_contains?: string;
};

export type ResolvePlayerArgs = {
  first_name: string;
  last_name: string;
};

export type StatcastPitchesArgs = {
  start_date: string;
  end_date: string;
  pitcher_id?: number;
  batter_id?: number;
  pitch_type?: string;
  columns: string[];
  row_cap: number;
};

export type StatcastSpinVarianceArgs = {
  start_date: string;
  end_date: string;
  pitch_type: string;
  group_by: "pitcher" | "batter";
  min_pitches: number;
  row_cap: number;
};

export type BatterHitDistanceByParkArgs = {
  batter_id: number;
  start_date: string;
  end_date: string;
  min_hits: number;
  row_cap: number;
};

export type TeamBattingStatcastArgs = {
  start_date: string;
  end_date: string;
  team_abbr: string;
  min_pa: number;
  row_cap: number;
};

export type BatterSituationalStatcastArgs = {
  batter_id: number;
  start_date: string;
  end_date: string;
  situation: "risp" | "men_on" | "bases_empty" | "any";
  min_pa: number;
};

export type BatterVsPitcherStatcastArgs = {
  batter_id: number;
  pitcher_id: number;
  start_date?: string;
  end_date: string;
  min_pa?: number;
};

export type PitcherEnteringInningLeadStatcastArgs = {
  pitcher_id: number;
  start_date: string;
  end_date: string;
  entering_inning?: number;
  min_lead_runs?: number;
  max_games?: number;
};

export type PybaseballSandboxArgs = {
  code: string;
  row_cap?: number;
  timeout_sec?: number;
};

export async function callPitcherPitchArsenal(
  args: PitchArsenalArgs,
): Promise<{ ok: true; data: unknown } | { ok: false; error: string }> {
  try {
    const res = await fetch(`${dataServiceBase()}/v1/pitcher_pitch_arsenal`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(args),
      signal: AbortSignal.timeout(120_000),
    });
    const body = (await res.json()) as { detail?: string; error?: string };
    if (!res.ok) {
      return { ok: false, error: body.detail || body.error || res.statusText };
    }
    return { ok: true, data: body };
  } catch (e) {
    const msg = e instanceof Error ? e.message : "Data service error";
    return { ok: false, error: msg };
  }
}

export async function callPitchingSeasonStats(
  args: PitchingSeasonArgs,
): Promise<{ ok: true; data: unknown } | { ok: false; error: string }> {
  try {
    const res = await fetch(`${dataServiceBase()}/v1/pitching_season_stats`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(args),
      signal: AbortSignal.timeout(120_000),
    });
    const body = (await res.json()) as { detail?: string; error?: string };
    if (!res.ok) {
      return { ok: false, error: body.detail || body.error || res.statusText };
    }
    return { ok: true, data: body };
  } catch (e) {
    const msg = e instanceof Error ? e.message : "Data service error";
    return { ok: false, error: msg };
  }
}

export async function callFieldingSeasonStats(
  args: FieldingSeasonArgs,
): Promise<{ ok: true; data: unknown } | { ok: false; error: string }> {
  try {
    const res = await fetch(`${dataServiceBase()}/v1/fielding_season_stats`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(args),
      signal: AbortSignal.timeout(120_000),
    });
    const body = (await res.json()) as { detail?: string; error?: string };
    if (!res.ok) {
      return { ok: false, error: body.detail || body.error || res.statusText };
    }
    return { ok: true, data: body };
  } catch (e) {
    const msg = e instanceof Error ? e.message : "Data service error";
    return { ok: false, error: msg };
  }
}

export async function callFieldingGameLog(
  args: FieldingGameLogArgs,
): Promise<{ ok: true; data: unknown } | { ok: false; error: string }> {
  try {
    const res = await fetch(`${dataServiceBase()}/v1/fielding_game_log`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(args),
      signal: AbortSignal.timeout(120_000),
    });
    const body = (await res.json()) as { detail?: string; error?: string };
    if (!res.ok) {
      return { ok: false, error: body.detail || body.error || res.statusText };
    }
    return { ok: true, data: body };
  } catch (e) {
    const msg = e instanceof Error ? e.message : "Data service error";
    return { ok: false, error: msg };
  }
}

export async function callTeamGameLog(
  args: TeamGameLogArgs,
): Promise<{ ok: true; data: unknown } | { ok: false; error: string }> {
  try {
    const res = await fetch(`${dataServiceBase()}/v1/team_game_log`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(args),
      signal: AbortSignal.timeout(120_000),
    });
    const body = (await res.json()) as { detail?: string; error?: string };
    if (!res.ok) {
      return { ok: false, error: body.detail || body.error || res.statusText };
    }
    return { ok: true, data: body };
  } catch (e) {
    const msg = e instanceof Error ? e.message : "Data service error";
    return { ok: false, error: msg };
  }
}

export async function callPlayerGameLog(
  args: PlayerGameLogArgs,
): Promise<{ ok: true; data: unknown } | { ok: false; error: string }> {
  try {
    const res = await fetch(`${dataServiceBase()}/v1/player_game_log`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(args),
      signal: AbortSignal.timeout(120_000),
    });
    const body = (await res.json()) as { detail?: string; error?: string };
    if (!res.ok) {
      return { ok: false, error: body.detail || body.error || res.statusText };
    }
    return { ok: true, data: body };
  } catch (e) {
    const msg = e instanceof Error ? e.message : "Data service error";
    return { ok: false, error: msg };
  }
}

export async function callPlayerGameLogVsOpponent(
  args: PlayerGameLogVsOpponentArgs,
): Promise<{ ok: true; data: unknown } | { ok: false; error: string }> {
  try {
    const res = await fetch(`${dataServiceBase()}/v1/player_game_log_vs_opponent`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(args),
      signal: AbortSignal.timeout(180_000),
    });
    const body = (await res.json()) as { detail?: string; error?: string };
    if (!res.ok) {
      return { ok: false, error: body.detail || body.error || res.statusText };
    }
    return { ok: true, data: body };
  } catch (e) {
    const msg = e instanceof Error ? e.message : "Data service error";
    return { ok: false, error: msg };
  }
}

export async function callMlbStatLeaders(
  args: MlbStatLeadersArgs,
): Promise<{ ok: true; data: unknown } | { ok: false; error: string }> {
  try {
    const res = await fetch(`${dataServiceBase()}/v1/mlb_stat_leaders`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(args),
      signal: AbortSignal.timeout(60_000),
    });
    const body = (await res.json()) as { detail?: string; error?: string };
    if (!res.ok) {
      return { ok: false, error: body.detail || body.error || res.statusText };
    }
    return { ok: true, data: body };
  } catch (e) {
    const msg = e instanceof Error ? e.message : "Data service error";
    return { ok: false, error: msg };
  }
}

export async function callBattingSeasonStats(
  args: BattingSeasonArgs,
): Promise<{ ok: true; data: unknown } | { ok: false; error: string }> {
  try {
    const res = await fetch(`${dataServiceBase()}/v1/batting_season_stats`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(args),
      signal: AbortSignal.timeout(120_000),
    });
    const body = (await res.json()) as { detail?: string; error?: string };
    if (!res.ok) {
      return { ok: false, error: body.detail || body.error || res.statusText };
    }
    return { ok: true, data: body };
  } catch (e) {
    const msg = e instanceof Error ? e.message : "Data service error";
    return { ok: false, error: msg };
  }
}

export async function callResolvePlayer(
  args: ResolvePlayerArgs,
): Promise<{ ok: true; data: unknown } | { ok: false; error: string }> {
  try {
    const res = await fetch(`${dataServiceBase()}/v1/resolve_player`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(args),
      signal: AbortSignal.timeout(30_000),
    });
    const body = (await res.json()) as { detail?: string; error?: string };
    if (!res.ok) {
      return { ok: false, error: body.detail || body.error || res.statusText };
    }
    return { ok: true, data: body };
  } catch (e) {
    const msg = e instanceof Error ? e.message : "Data service error";
    return { ok: false, error: msg };
  }
}

export async function callStatcastPitches(
  args: StatcastPitchesArgs,
): Promise<{ ok: true; data: unknown } | { ok: false; error: string }> {
  try {
    const res = await fetch(`${dataServiceBase()}/v1/statcast_pitches`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(args),
      signal: AbortSignal.timeout(180_000),
    });
    const body = (await res.json()) as { detail?: string; error?: string };
    if (!res.ok) {
      return { ok: false, error: body.detail || body.error || res.statusText };
    }
    return { ok: true, data: body };
  } catch (e) {
    const msg = e instanceof Error ? e.message : "Data service error";
    return { ok: false, error: msg };
  }
}

export async function callPitcherEnteringInningLeadStatcast(
  args: PitcherEnteringInningLeadStatcastArgs,
): Promise<{ ok: true; data: unknown } | { ok: false; error: string }> {
  try {
    const res = await fetch(`${dataServiceBase()}/v1/pitcher_entering_inning_lead_statcast`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        pitcher_id: args.pitcher_id,
        start_date: args.start_date,
        end_date: args.end_date,
        entering_inning: args.entering_inning ?? 4,
        min_lead_runs: args.min_lead_runs ?? 3,
        max_games: args.max_games ?? 400,
      }),
      signal: AbortSignal.timeout(300_000),
    });
    const body = (await res.json()) as { detail?: string; error?: string };
    if (!res.ok) {
      return { ok: false, error: body.detail || body.error || res.statusText };
    }
    return { ok: true, data: body };
  } catch (e) {
    const msg = e instanceof Error ? e.message : "Data service error";
    return { ok: false, error: msg };
  }
}

export async function callBatterVsPitcherStatcast(
  args: BatterVsPitcherStatcastArgs,
): Promise<{ ok: true; data: unknown } | { ok: false; error: string }> {
  try {
    const res = await fetch(`${dataServiceBase()}/v1/batter_vs_pitcher_statcast`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        batter_id: args.batter_id,
        pitcher_id: args.pitcher_id,
        start_date: args.start_date ?? "2015-03-01",
        end_date: args.end_date,
        min_pa: args.min_pa ?? 1,
      }),
      signal: AbortSignal.timeout(240_000),
    });
    const body = (await res.json()) as { detail?: string; error?: string };
    if (!res.ok) {
      return { ok: false, error: body.detail || body.error || res.statusText };
    }
    return { ok: true, data: body };
  } catch (e) {
    const msg = e instanceof Error ? e.message : "Data service error";
    return { ok: false, error: msg };
  }
}

export async function callBatterHitDistanceByPark(
  args: BatterHitDistanceByParkArgs,
): Promise<{ ok: true; data: unknown } | { ok: false; error: string }> {
  try {
    const res = await fetch(`${dataServiceBase()}/v1/batter_hit_distance_by_park`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(args),
      signal: AbortSignal.timeout(300_000),
    });
    const body = (await res.json()) as { detail?: string; error?: string };
    if (!res.ok) {
      return { ok: false, error: body.detail || body.error || res.statusText };
    }
    return { ok: true, data: body };
  } catch (e) {
    const msg = e instanceof Error ? e.message : "Data service error";
    return { ok: false, error: msg };
  }
}

export async function callStatcastSpinVariance(
  args: StatcastSpinVarianceArgs,
): Promise<{ ok: true; data: unknown } | { ok: false; error: string }> {
  try {
    const res = await fetch(`${dataServiceBase()}/v1/statcast_spin_variance`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(args),
      signal: AbortSignal.timeout(180_000),
    });
    const body = (await res.json()) as { detail?: string; error?: string };
    if (!res.ok) {
      return { ok: false, error: body.detail || body.error || res.statusText };
    }
    return { ok: true, data: body };
  } catch (e) {
    const msg = e instanceof Error ? e.message : "Data service error";
    return { ok: false, error: msg };
  }
}

export async function callTeamBattingStatcast(
  args: TeamBattingStatcastArgs,
): Promise<{ ok: true; data: unknown } | { ok: false; error: string }> {
  try {
    const res = await fetch(`${dataServiceBase()}/v1/team_batting_statcast`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(args),
      signal: AbortSignal.timeout(300_000),
    });
    const body = (await res.json()) as { detail?: string; error?: string };
    if (!res.ok) {
      return { ok: false, error: body.detail || body.error || res.statusText };
    }
    return { ok: true, data: body };
  } catch (e) {
    const msg = e instanceof Error ? e.message : "Data service error";
    return { ok: false, error: msg };
  }
}

export async function callBatterSituationalStatcast(
  args: BatterSituationalStatcastArgs,
): Promise<{ ok: true; data: unknown } | { ok: false; error: string }> {
  try {
    const res = await fetch(`${dataServiceBase()}/v1/batter_situational_statcast`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(args),
      signal: AbortSignal.timeout(300_000),
    });
    const body = (await res.json()) as { detail?: string; error?: string };
    if (!res.ok) {
      return { ok: false, error: body.detail || body.error || res.statusText };
    }
    return { ok: true, data: body };
  } catch (e) {
    const msg = e instanceof Error ? e.message : "Data service error";
    return { ok: false, error: msg };
  }
}

export async function callPybaseballSandbox(
  args: PybaseballSandboxArgs,
): Promise<{ ok: true; data: unknown } | { ok: false; error: string }> {
  const timeoutSec = args.timeout_sec ?? 90;
  const clientMs = Math.min(180_000, (timeoutSec + 30) * 1000);
  try {
    const res = await fetch(`${dataServiceBase()}/v1/pybaseball_sandbox`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        code: args.code,
        row_cap: args.row_cap ?? 200,
        timeout_sec: timeoutSec,
      }),
      signal: AbortSignal.timeout(clientMs),
    });
    const body = (await res.json()) as {
      detail?: unknown;
      error?: string;
      ok?: boolean;
    };
    if (!res.ok) {
      const d = body.detail;
      const detailStr =
        typeof d === "string" ? d : d !== undefined && d !== null ? JSON.stringify(d) : "";
      return {
        ok: false,
        error: detailStr || body.error || res.statusText,
      };
    }
    if (body && typeof body === "object" && body.ok === false && typeof body.error === "string") {
      return { ok: false, error: body.error };
    }
    return { ok: true, data: body };
  } catch (e) {
    const msg = e instanceof Error ? e.message : "Data service error";
    return { ok: false, error: msg };
  }
}

