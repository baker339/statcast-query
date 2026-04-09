import Anthropic from "@anthropic-ai/sdk";
import { NextRequest, NextResponse } from "next/server";
import {
  callBattingSeasonStats,
  callBatterHitDistanceByPark,
  callBatterSituationalStatcast,
  callFieldingGameLog,
  callFieldingSeasonStats,
  callPlayerGameLog,
  callPlayerGameLogVsOpponent,
  callTeamGameLog,
  callMlbStatLeaders,
  callPitcherPitchArsenal,
  callPitchingSeasonStats,
  callResolvePlayer,
  callBatterVsPitcherStatcast,
  callPitcherEnteringInningLeadStatcast,
  callStatcastPitches,
  callStatcastSpinVariance,
  callTeamBattingStatcast,
  callPybaseballSandbox,
} from "@/lib/data-service";
import { SYSTEM_PROMPT } from "@/lib/prompts";
import { rateLimit } from "@/lib/rate-limit";
import { teamBattingToolExtras } from "@/lib/team-batting-tool-meta";
import { toolStatusDetail } from "@/lib/tool-status";
import {
  getBatterHitDistanceByParkSchema,
  getBatterSituationalStatcastSchema,
  getBattingSeasonStatsSchema,
  getFieldingGameLogSchema,
  getFieldingSeasonStatsSchema,
  getPlayerGameLogSchema,
  getPlayerGameLogVsOpponentSchema,
  getTeamGameLogSchema,
  getMlbStatLeadersSchema,
  getPitcherPitchArsenalSchema,
  getPitchingSeasonStatsSchema,
  getBatterVsPitcherStatcastSchema,
  getPitcherEnteringInningLeadStatcastSchema,
  getStatcastPitchesSchema,
  getStatcastSpinVarianceSchema,
  getTeamBattingStatcastSchema,
  OPENAI_TOOLS,
  resolvePlayerSchema,
  runPybaseballSandboxSchema,
} from "@/lib/tools";

export const maxDuration = 300;

/** US/Eastern matches MLB schedule framing; injected each request so the model need not ask the user for "today". */
function serverContextClock(): string {
  const tz = "America/New_York";
  const now = new Date();
  const isoDate = now.toLocaleDateString("en-CA", { timeZone: tz });
  const weekday = now.toLocaleDateString("en-US", { timeZone: tz, weekday: "long" });
  return `Today is ${weekday}, ${isoDate} (${tz}). Use this for "yesterday", season-to-date end dates, and Statcast windows. Do not ask the user what today's date is.`;
}

/** Extract plain text from the latest user turn (multi-turn safe). */
function lastUserMessageText(msgs: Anthropic.Messages.MessageParam[]): string {
  for (let i = msgs.length - 1; i >= 0; i--) {
    if (msgs[i].role !== "user") continue;
    const c = msgs[i].content;
    if (typeof c === "string") return c;
    if (Array.isArray(c)) {
      return c
        .map((b) =>
          "type" in b && b.type === "text" && "text" in b ? String((b as { text: string }).text) : "",
        )
        .join("\n");
    }
  }
  return "";
}

/**
 * Lineup / team batting / matchup questions must not be answered as pure prose without tools.
 * First API turn: require at least one tool call so the model cannot "essay" a refusal.
 */
function shouldForceDataToolsFirstTurn(userText: string): boolean {
  const t = userText.toLowerCase();
  const lineupContext =
    /\blineup\b/.test(t) &&
    (/\b(game|today|season|against|pitch|mound|dh\b|red sox|brewers|sox|milwaukee)\b/.test(t) ||
      /\b(propose|batting order|starting nine)\b/.test(t));
  const teamBatting =
    /\b(batting stats?|stats and events|season.{0,25}to.{0,25}date|so far this|2026 season|team batting|how they have performed|how (they|has) performed)\b/.test(
      t,
    );
  const matchup =
    /\b(on the mound|who'?s pitching|opposing pitcher|vs\. the|against the brewers|against the red sox)\b/.test(
      t,
    );
  const teamNamed =
    /\b(red sox|brewers|milwaukee)\b/.test(t) &&
    /\b(batting|lineup|stats|propose|season)\b/.test(t);
  const careerVsOpp =
    /\b(career|lifetime|whole career)\b/.test(t) &&
    /\b(vs\.?|versus|against)\b/.test(t);
  return lineupContext || teamBatting || matchup || teamNamed || careerVsOpp;
}

type TablePayload = {
  title: string;
  columns: string[];
  rows: Record<string, string | number | null>[];
};

type ConversationMessage = {
  role: "user" | "assistant";
  content: string;
};

function clientIp(req: NextRequest): string {
  return (
    req.headers.get("x-forwarded-for")?.split(",")[0]?.trim() ||
    req.headers.get("x-real-ip") ||
    "local"
  );
}

function splitAssumptions(text: string): { reply: string; assumptions?: string } {
  const nl = text.lastIndexOf("\nAssumptions:");
  if (nl !== -1) {
    return {
      reply: text.slice(0, nl).trim(),
      assumptions: text.slice(nl + "\nAssumptions:".length).trim() || undefined,
    };
  }
  const idx = text.indexOf("Assumptions:");
  if (idx !== -1) {
    return {
      reply: text.slice(0, idx).trim(),
      assumptions: text.slice(idx + "Assumptions:".length).trim() || undefined,
    };
  }
  return { reply: text.trim() };
}

function normalizeRows(
  columns: string[],
  rows: Record<string, unknown>[],
): Record<string, string | number | null>[] {
  return rows.map((row) => {
    const out: Record<string, string | number | null> = {};
    for (const c of columns) {
      const v = row[c];
      if (v === null || v === undefined) out[c] = null;
      else if (typeof v === "number" && Number.isNaN(v)) out[c] = null;
      else if (typeof v === "number" || typeof v === "string") out[c] = v;
      else if (typeof v === "boolean") out[c] = v ? "true" : "false";
      else out[c] = JSON.stringify(v);
    }
    return out;
  });
}

async function runTool(
  name: string,
  rawArgs: string,
): Promise<{ text: string; tables: TablePayload[]; error?: string }> {
  let args: unknown;
  try {
    args = JSON.parse(rawArgs || "{}");
  } catch {
    return { text: JSON.stringify({ error: "Invalid JSON arguments" }), tables: [] };
  }

  if (name === "get_pitcher_pitch_arsenal") {
    const parsed = getPitcherPitchArsenalSchema.safeParse(args);
    if (!parsed.success) {
      return {
        text: JSON.stringify({ error: parsed.error.flatten() }),
        tables: [],
        error: "Validation failed",
      };
    }
    const minPitches =
      parsed.data.min_pitches === 0 ? 250 : Math.max(1, parsed.data.min_pitches);
    const res = await callPitcherPitchArsenal({
      year: parsed.data.year,
      min_pitches: minPitches,
      arsenal_type: parsed.data.arsenal_type,
      row_cap: parsed.data.row_cap,
      pitch_type_filter: parsed.data.pitch_type_filter,
      pitcher_id: parsed.data.pitcher_id,
    });
    if (!res.ok) {
      return { text: JSON.stringify({ error: res.error }), tables: [], error: res.error };
    }
    const body = res.data as {
      columns?: string[];
      rows?: Record<string, unknown>[];
      source?: string;
      note?: string;
    };
    const columns = body.columns ?? [];
    const rows = body.rows ?? [];
    const pid = parsed.data.pitcher_id;
    const table: TablePayload = {
      title: pid
        ? `Pitch arsenal (${parsed.data.year}, ${parsed.data.arsenal_type}, pitcher_id=${pid})`
        : `Pitch arsenal (${parsed.data.year}, ${parsed.data.arsenal_type})`,
      columns,
      rows: normalizeRows(columns, rows),
    };
    return {
      text: JSON.stringify({
        source: body.source ?? "savant_pitch_arsenal",
        note: body.note,
        row_count: rows.length,
        preview: rows.slice(0, 5),
      }),
      tables: [table],
    };
  }

  if (name === "get_pitching_season_stats") {
    const parsed = getPitchingSeasonStatsSchema.safeParse(args);
    if (!parsed.success) {
      return {
        text: JSON.stringify({ error: parsed.error.flatten() }),
        tables: [],
        error: "Validation failed",
      };
    }
    const res = await callPitchingSeasonStats({
      season: parsed.data.season,
      min_ip: parsed.data.min_ip,
      metrics: parsed.data.metrics,
      row_cap: parsed.data.row_cap,
      team_abbr: parsed.data.team_abbr,
      name_contains: parsed.data.name_contains,
    });
    if (!res.ok) {
      return { text: JSON.stringify({ error: res.error }), tables: [], error: res.error };
    }
    const body = res.data as {
      columns?: string[];
      rows?: Record<string, unknown>[];
      source?: string;
      note?: string;
    };
    const columns = body.columns ?? [];
    const rows = body.rows ?? [];
    const src = body.source ?? "fangraphs_pitching";
    const table: TablePayload = {
      title:
        src === "mlb_stats_api_pitching_season"
          ? `Pitching season ${parsed.data.season} (MLB Stats API${parsed.data.team_abbr ? `, ${parsed.data.team_abbr}` : ""})`
          : `Pitching season ${parsed.data.season} (FanGraphs)`,
      columns,
      rows: normalizeRows(columns, rows),
    };
    return {
      text: JSON.stringify({
        source: src,
        note: body.note,
        row_count: rows.length,
        preview: rows.slice(0, 5),
      }),
      tables: [table],
    };
  }

  if (name === "get_fielding_season_stats") {
    const parsed = getFieldingSeasonStatsSchema.safeParse(args);
    if (!parsed.success) {
      return {
        text: JSON.stringify({ error: parsed.error.flatten() }),
        tables: [],
        error: "Validation failed",
      };
    }
    const res = await callFieldingSeasonStats({
      season: parsed.data.season,
      min_inn: parsed.data.min_inn,
      metrics: parsed.data.metrics,
      row_cap: parsed.data.row_cap,
      team_abbr: parsed.data.team_abbr,
      name_contains: parsed.data.name_contains,
    });
    if (!res.ok) {
      return { text: JSON.stringify({ error: res.error }), tables: [], error: res.error };
    }
    const body = res.data as {
      columns?: string[];
      rows?: Record<string, unknown>[];
      source?: string;
      note?: string;
    };
    const columns = body.columns ?? [];
    const rows = body.rows ?? [];
    const src = body.source ?? "fangraphs_fielding";
    const table: TablePayload = {
      title:
        src === "mlb_stats_api_fielding_season"
          ? `Fielding season ${parsed.data.season} (MLB Stats API${parsed.data.team_abbr ? `, ${parsed.data.team_abbr}` : ""})`
          : `Fielding season ${parsed.data.season} (FanGraphs)`,
      columns,
      rows: normalizeRows(columns, rows),
    };
    return {
      text: JSON.stringify({
        source: src,
        note: body.note,
        row_count: rows.length,
        preview: rows.slice(0, 5),
      }),
      tables: [table],
    };
  }

  if (name === "get_fielding_game_log") {
    const parsed = getFieldingGameLogSchema.safeParse(args);
    if (!parsed.success) {
      return {
        text: JSON.stringify({ error: parsed.error.flatten() }),
        tables: [],
        error: "Validation failed",
      };
    }
    const res = await callFieldingGameLog({
      player_id: parsed.data.player_id,
      season: parsed.data.season,
      start_date: parsed.data.start_date,
      end_date: parsed.data.end_date,
      max_games: parsed.data.max_games,
      metrics: parsed.data.metrics,
      row_cap: parsed.data.row_cap,
    });
    if (!res.ok) {
      return { text: JSON.stringify({ error: res.error }), tables: [], error: res.error };
    }
    const body = res.data as {
      columns?: string[];
      rows?: Record<string, unknown>[];
      source?: string;
      note?: string;
    };
    const columns = body.columns ?? [];
    const rows = body.rows ?? [];
    const range =
      parsed.data.start_date && parsed.data.end_date
        ? `${parsed.data.start_date}–${parsed.data.end_date}`
        : parsed.data.max_games
          ? `first ${parsed.data.max_games} games`
          : "full log";
    const table: TablePayload = {
      title: `Fielding game log ${parsed.data.season} (player ${parsed.data.player_id}, ${range})`,
      columns,
      rows: normalizeRows(columns, rows),
    };
    return {
      text: JSON.stringify({
        source: body.source ?? "mlb_fielding_game_log",
        note: body.note,
        row_count: rows.length,
        preview: rows.slice(0, 6),
      }),
      tables: [table],
    };
  }

  if (name === "get_team_game_log") {
    const parsed = getTeamGameLogSchema.safeParse(args);
    if (!parsed.success) {
      return {
        text: JSON.stringify({ error: parsed.error.flatten() }),
        tables: [],
        error: "Validation failed",
      };
    }
    const res = await callTeamGameLog({
      team_abbr: parsed.data.team_abbr,
      season: parsed.data.season,
      stat_group: parsed.data.stat_group,
      start_date: parsed.data.start_date,
      end_date: parsed.data.end_date,
      max_games: parsed.data.max_games,
      metrics: parsed.data.metrics,
      row_cap: parsed.data.row_cap,
    });
    if (!res.ok) {
      return { text: JSON.stringify({ error: res.error }), tables: [], error: res.error };
    }
    const body = res.data as {
      columns?: string[];
      rows?: Record<string, unknown>[];
      source?: string;
      note?: string;
    };
    const columns = body.columns ?? [];
    const rows = body.rows ?? [];
    const table: TablePayload = {
      title: `Team game log ${parsed.data.team_abbr} ${parsed.data.season} (${parsed.data.stat_group})`,
      columns,
      rows: normalizeRows(columns, rows),
    };
    return {
      text: JSON.stringify({
        source: body.source ?? "mlb_team_game_log",
        note: body.note,
        row_count: rows.length,
        preview: rows.slice(0, 6),
      }),
      tables: [table],
    };
  }

  if (name === "get_player_game_log") {
    const parsed = getPlayerGameLogSchema.safeParse(args);
    if (!parsed.success) {
      return {
        text: JSON.stringify({ error: parsed.error.flatten() }),
        tables: [],
        error: "Validation failed",
      };
    }
    const res = await callPlayerGameLog({
      player_id: parsed.data.player_id,
      season: parsed.data.season,
      stat_group: parsed.data.stat_group,
      start_date: parsed.data.start_date,
      end_date: parsed.data.end_date,
      max_games: parsed.data.max_games,
      metrics: parsed.data.metrics,
      row_cap: parsed.data.row_cap,
    });
    if (!res.ok) {
      return { text: JSON.stringify({ error: res.error }), tables: [], error: res.error };
    }
    const body = res.data as {
      columns?: string[];
      rows?: Record<string, unknown>[];
      source?: string;
      note?: string;
    };
    const columns = body.columns ?? [];
    const rows = body.rows ?? [];
    const table: TablePayload = {
      title: `Player game log ${parsed.data.player_id} ${parsed.data.season} (${parsed.data.stat_group})`,
      columns,
      rows: normalizeRows(columns, rows),
    };
    return {
      text: JSON.stringify({
        source: body.source ?? "mlb_player_game_log",
        note: body.note,
        row_count: rows.length,
        preview: rows.slice(0, 6),
      }),
      tables: [table],
    };
  }

  if (name === "get_player_game_log_vs_opponent") {
    const parsed = getPlayerGameLogVsOpponentSchema.safeParse(args);
    if (!parsed.success) {
      return {
        text: JSON.stringify({ error: parsed.error.flatten() }),
        tables: [],
        error: "Validation failed",
      };
    }
    const res = await callPlayerGameLogVsOpponent({
      player_id: parsed.data.player_id,
      opponent_abbr: parsed.data.opponent_abbr,
      stat_group: parsed.data.stat_group,
      start_season: parsed.data.start_season,
      end_season: parsed.data.end_season,
      aggregate: parsed.data.aggregate,
      metrics: parsed.data.metrics,
      row_cap: parsed.data.row_cap,
    });
    if (!res.ok) {
      return { text: JSON.stringify({ error: res.error }), tables: [], error: res.error };
    }
    const body = res.data as {
      columns?: string[];
      rows?: Record<string, unknown>[];
      source?: string;
      note?: string;
    };
    const columns = body.columns ?? [];
    const rows = body.rows ?? [];
    const table: TablePayload = {
      title: `vs ${parsed.data.opponent_abbr} (${parsed.data.stat_group}, ${parsed.data.aggregate ? "career agg." : "games"})`,
      columns,
      rows: normalizeRows(columns, rows),
    };
    return {
      text: JSON.stringify({
        source: body.source ?? "mlb_player_game_log_vs_opponent",
        note: body.note,
        row_count: rows.length,
        preview: rows.slice(0, 6),
      }),
      tables: [table],
    };
  }

  if (name === "get_mlb_stat_leaders") {
    const parsed = getMlbStatLeadersSchema.safeParse(args);
    if (!parsed.success) {
      return {
        text: JSON.stringify({ error: parsed.error.flatten() }),
        tables: [],
        error: "Validation failed",
      };
    }
    const res = await callMlbStatLeaders({
      season: parsed.data.season,
      leader_category: parsed.data.leader_category,
      stat_group: parsed.data.stat_group,
      limit: parsed.data.limit,
      leader_game_types: parsed.data.leader_game_types,
    });
    if (!res.ok) {
      return { text: JSON.stringify({ error: res.error }), tables: [], error: res.error };
    }
    const body = res.data as {
      columns?: string[];
      rows?: Record<string, unknown>[];
      source?: string;
      note?: string;
    };
    const columns = body.columns ?? [];
    const rows = body.rows ?? [];
    const table: TablePayload = {
      title: `MLB leaders ${parsed.data.season} (${parsed.data.leader_category})`,
      columns,
      rows: normalizeRows(columns, rows),
    };
    return {
      text: JSON.stringify({
        source: body.source ?? "mlb_stats_api",
        note: body.note,
        row_count: rows.length,
        preview: rows.slice(0, 8),
      }),
      tables: [table],
    };
  }

  if (name === "get_batting_season_stats") {
    const parsed = getBattingSeasonStatsSchema.safeParse(args);
    if (!parsed.success) {
      return {
        text: JSON.stringify({ error: parsed.error.flatten() }),
        tables: [],
        error: "Validation failed",
      };
    }
    const res = await callBattingSeasonStats({
      season: parsed.data.season,
      min_pa: parsed.data.min_pa,
      metrics: parsed.data.metrics,
      row_cap: parsed.data.row_cap,
      team_abbr: parsed.data.team_abbr,
      name_contains: parsed.data.name_contains,
    });
    if (!res.ok) {
      return {
        text: JSON.stringify({
          error: res.error,
          hint: "Backend should try MLB Stats API after FanGraphs; if you still see this, narrow min_pa, pass team_abbr for one club, try get_mlb_stat_leaders for leaders, or get_team_batting_statcast with dates.",
        }),
        tables: [],
        error: res.error,
      };
    }
    const body = res.data as {
      columns?: string[];
      rows?: Record<string, unknown>[];
      source?: string;
      note?: string;
    };
    const columns = body.columns ?? [];
    const rows = body.rows ?? [];
    const src = body.source ?? "fangraphs_batting";
    const table: TablePayload = {
      title:
        src === "mlb_stats_api_hitting_season"
          ? `Batting season ${parsed.data.season} (MLB Stats API${parsed.data.team_abbr ? `, ${parsed.data.team_abbr}` : ""})`
          : `Batting season ${parsed.data.season} (FanGraphs)`,
      columns,
      rows: normalizeRows(columns, rows),
    };
    return {
      text: JSON.stringify({
        source: src,
        note: body.note,
        row_count: rows.length,
        preview: rows.slice(0, 5),
      }),
      tables: [table],
    };
  }

  if (name === "get_team_batting_statcast") {
    const parsed = getTeamBattingStatcastSchema.safeParse(args);
    if (!parsed.success) {
      return {
        text: JSON.stringify({ error: parsed.error.flatten() }),
        tables: [],
        error: "Validation failed",
      };
    }
    const res = await callTeamBattingStatcast({
      start_date: parsed.data.start_date,
      end_date: parsed.data.end_date,
      team_abbr: parsed.data.team_abbr,
      min_pa: parsed.data.min_pa,
      row_cap: parsed.data.row_cap,
    });
    if (!res.ok) {
      return { text: JSON.stringify({ error: res.error }), tables: [], error: res.error };
    }
    const body = res.data as {
      columns?: string[];
      rows?: Record<string, unknown>[];
      source?: string;
      note?: string;
    };
    const columns = body.columns ?? [];
    const rows = body.rows ?? [];
    const extras = teamBattingToolExtras(rows);
    const table: TablePayload = {
      title: `Team batting Statcast (${parsed.data.team_abbr}, ${parsed.data.start_date}–${parsed.data.end_date})`,
      columns,
      rows: normalizeRows(columns, rows),
    };
    return {
      text: JSON.stringify({
        source: body.source ?? "statcast_team_batting",
        note: body.note,
        row_count: rows.length,
        preview: rows.slice(0, 8),
        ...extras,
      }),
      tables: [table],
    };
  }

  if (name === "resolve_player") {
    const parsed = resolvePlayerSchema.safeParse(args);
    if (!parsed.success) {
      return {
        text: JSON.stringify({ error: parsed.error.flatten() }),
        tables: [],
        error: "Validation failed",
      };
    }
    const res = await callResolvePlayer({
      first_name: parsed.data.first_name,
      last_name: parsed.data.last_name,
    });
    if (!res.ok) {
      return { text: JSON.stringify({ error: res.error }), tables: [], error: res.error };
    }
    const body = res.data as Record<string, unknown>;
    return {
      text: JSON.stringify({
        ...body,
        source: "player_id_lookup",
        note:
          (typeof body.note === "string" ? `${body.note} ` : "") +
          "Use key_mlbam (or mlbam) for Statcast filters when present. Never tell the user someone is retired, inactive, or 'last played' in a season based on this lookup alone.",
      }),
      tables: [],
    };
  }

  if (name === "get_batter_vs_pitcher_statcast") {
    const parsed = getBatterVsPitcherStatcastSchema.safeParse(args);
    if (!parsed.success) {
      return {
        text: JSON.stringify({ error: parsed.error.flatten() }),
        tables: [],
        error: "Validation failed",
      };
    }
    const res = await callBatterVsPitcherStatcast({
      batter_id: parsed.data.batter_id,
      pitcher_id: parsed.data.pitcher_id,
      start_date: parsed.data.start_date,
      end_date: parsed.data.end_date,
      min_pa: parsed.data.min_pa,
    });
    if (!res.ok) {
      return { text: JSON.stringify({ error: res.error }), tables: [], error: res.error };
    }
    const body = res.data as {
      columns?: string[];
      rows?: Record<string, unknown>[];
      source?: string;
      note?: string;
    };
    const columns = body.columns ?? [];
    const rows = body.rows ?? [];
    const table: TablePayload = {
      title: `Batter vs pitcher (Statcast ${parsed.data.start_date ?? "2015-03-01"}–${parsed.data.end_date})`,
      columns,
      rows: normalizeRows(columns, rows),
    };
    return {
      text: JSON.stringify({
        source: body.source ?? "batter_vs_pitcher_statcast",
        note: body.note,
        row_count: rows.length,
        preview: rows.slice(0, 3),
      }),
      tables: [table],
    };
  }

  if (name === "get_pitcher_entering_inning_lead_statcast") {
    const parsed = getPitcherEnteringInningLeadStatcastSchema.safeParse(args);
    if (!parsed.success) {
      return {
        text: JSON.stringify({ error: parsed.error.flatten() }),
        tables: [],
        error: "Validation failed",
      };
    }
    const res = await callPitcherEnteringInningLeadStatcast({
      pitcher_id: parsed.data.pitcher_id,
      start_date: parsed.data.start_date,
      end_date: parsed.data.end_date,
      entering_inning: parsed.data.entering_inning,
      min_lead_runs: parsed.data.min_lead_runs,
      max_games: parsed.data.max_games,
    });
    if (!res.ok) {
      return { text: JSON.stringify({ error: res.error }), tables: [], error: res.error };
    }
    const body = res.data as {
      columns?: string[];
      rows?: Record<string, unknown>[];
      source?: string;
      note?: string;
    };
    const columns = body.columns ?? [];
    const rows = body.rows ?? [];
    const table: TablePayload = {
      title: `Pitcher lead entering inn. ${parsed.data.entering_inning} (≥${parsed.data.min_lead_runs} runs)`,
      columns,
      rows: normalizeRows(columns, rows),
    };
    return {
      text: JSON.stringify({
        source: body.source ?? "pitcher_entering_inning_lead_statcast",
        note: body.note,
        row_count: rows.length,
        preview: rows.slice(0, 4),
      }),
      tables: [table],
    };
  }

  if (name === "get_statcast_pitches") {
    const parsed = getStatcastPitchesSchema.safeParse(args);
    if (!parsed.success) {
      return {
        text: JSON.stringify({ error: parsed.error.flatten() }),
        tables: [],
        error: "Validation failed",
      };
    }
    const res = await callStatcastPitches(parsed.data);
    if (!res.ok) {
      return { text: JSON.stringify({ error: res.error }), tables: [], error: res.error };
    }
    const body = res.data as {
      columns?: string[];
      rows?: Record<string, unknown>[];
      source?: string;
    };
    const columns = body.columns ?? [];
    const rows = body.rows ?? [];
    return {
      text: JSON.stringify({
        source: body.source ?? "statcast_pitches",
        row_count: rows.length,
        preview: rows.slice(0, 5),
      }),
      tables: [
        {
          title: `Statcast pitches (${parsed.data.start_date} to ${parsed.data.end_date})`,
          columns,
          rows: normalizeRows(columns, rows),
        },
      ],
    };
  }

  if (name === "get_statcast_spin_variance") {
    const parsed = getStatcastSpinVarianceSchema.safeParse(args);
    if (!parsed.success) {
      return {
        text: JSON.stringify({ error: parsed.error.flatten() }),
        tables: [],
        error: "Validation failed",
      };
    }
    const res = await callStatcastSpinVariance(parsed.data);
    if (!res.ok) {
      return { text: JSON.stringify({ error: res.error }), tables: [], error: res.error };
    }
    const body = res.data as {
      columns?: string[];
      rows?: Record<string, unknown>[];
      source?: string;
      note?: string;
    };
    const columns = body.columns ?? [];
    const rows = body.rows ?? [];
    return {
      text: JSON.stringify({
        source: body.source ?? "statcast_spin_variance",
        note: body.note,
        row_count: rows.length,
        preview: rows.slice(0, 5),
      }),
      tables: [
        {
          title: `Spin variance (${parsed.data.pitch_type}, ${parsed.data.group_by})`,
          columns,
          rows: normalizeRows(columns, rows),
        },
      ],
    };
  }

  if (name === "get_batter_situational_statcast") {
    const parsed = getBatterSituationalStatcastSchema.safeParse(args);
    if (!parsed.success) {
      return {
        text: JSON.stringify({ error: parsed.error.flatten() }),
        tables: [],
        error: "Validation failed",
      };
    }
    const res = await callBatterSituationalStatcast(parsed.data);
    if (!res.ok) {
      return { text: JSON.stringify({ error: res.error }), tables: [], error: res.error };
    }
    const body = res.data as {
      columns?: string[];
      rows?: Record<string, unknown>[];
      source?: string;
      note?: string;
    };
    const columns = body.columns ?? [];
    const rows = body.rows ?? [];
    const sit = parsed.data.situation;
    return {
      text: JSON.stringify({
        source: body.source ?? "batter_situational_statcast",
        note: body.note,
        row_count: rows.length,
        preview: rows.slice(0, 3),
      }),
      tables: [
        {
          title: `Batter situational (${sit}, id ${parsed.data.batter_id}, ${parsed.data.start_date}–${parsed.data.end_date})`,
          columns,
          rows: normalizeRows(columns, rows),
        },
      ],
    };
  }

  if (name === "get_batter_hit_distance_by_park") {
    const parsed = getBatterHitDistanceByParkSchema.safeParse(args);
    if (!parsed.success) {
      return {
        text: JSON.stringify({ error: parsed.error.flatten() }),
        tables: [],
        error: "Validation failed",
      };
    }
    const res = await callBatterHitDistanceByPark(parsed.data);
    if (!res.ok) {
      return { text: JSON.stringify({ error: res.error }), tables: [], error: res.error };
    }
    const body = res.data as {
      columns?: string[];
      rows?: Record<string, unknown>[];
      source?: string;
      note?: string;
    };
    const columns = body.columns ?? [];
    const rows = body.rows ?? [];
    return {
      text: JSON.stringify({
        source: body.source ?? "batter_hit_distance_by_park",
        note: body.note,
        row_count: rows.length,
        preview: rows.slice(0, 5),
      }),
      tables: [
        {
          title: `Avg hit distance by home team (batter ${parsed.data.batter_id}, ${parsed.data.start_date}–${parsed.data.end_date})`,
          columns,
          rows: normalizeRows(columns, rows),
        },
      ],
    };
  }

  if (name === "run_pybaseball_sandbox") {
    const parsed = runPybaseballSandboxSchema.safeParse(args);
    if (!parsed.success) {
      return {
        text: JSON.stringify({ error: parsed.error.flatten() }),
        tables: [],
        error: "Validation failed",
      };
    }
    const res = await callPybaseballSandbox({
      code: parsed.data.code,
      row_cap: parsed.data.row_cap,
      timeout_sec: parsed.data.timeout_sec,
    });
    if (!res.ok) {
      return { text: JSON.stringify({ error: res.error }), tables: [], error: res.error };
    }
    const body = res.data as {
      ok?: boolean;
      result_kind?: string;
      columns?: string[];
      rows?: Record<string, unknown>[];
      value?: unknown;
      error?: string;
      printed?: string;
      note?: string;
      source?: string;
      traceback?: string;
    };
    if (body.ok === false) {
      const err = body.error || "sandbox failed";
      return {
        text: JSON.stringify({
          error: err,
          traceback: body.traceback,
          printed: body.printed,
        }),
        tables: [],
        error: err,
      };
    }
    const tables: TablePayload[] = [];
    if (body.result_kind === "table" && Array.isArray(body.columns) && Array.isArray(body.rows)) {
      const columns = body.columns;
      const rows = body.rows;
      tables.push({
        title: "Pybaseball sandbox (table)",
        columns,
        rows: normalizeRows(columns, rows),
      });
    }
    return {
      text: JSON.stringify({
        source: body.source ?? "pybaseball_sandbox",
        result_kind: body.result_kind,
        note: body.note,
        printed: body.printed,
        value: body.result_kind === "json" ? body.value : undefined,
        row_count: body.rows?.length,
      }),
      tables,
    };
  }

  return { text: JSON.stringify({ error: `Unknown tool: ${name}` }), tables: [] };
}

type StatusEmit = (s: { title: string; detail: string }) => void;

type ChatAgentResult = {
  reply: string;
  assumptions?: string;
  tables: TablePayload[];
  error?: string;
};

async function runChatAgent(
  client: Anthropic,
  model: string,
  anthropicTools: Anthropic.Messages.Tool[],
  baseMessages: Anthropic.Messages.MessageParam[],
  emit: StatusEmit,
): Promise<ChatAgentResult> {
  const allTables: TablePayload[] = [];
  let lastDataError: string | undefined;
  const conversation: Anthropic.Messages.MessageParam[] = [...baseMessages];

  emit({
    title: "Thinking",
    detail: "Connecting to the model and loading tools…",
  });

  for (let round = 0; round < 8; round++) {
    const lastUser = lastUserMessageText(conversation);
    const forceDataTools = round === 0 && shouldForceDataToolsFirstTurn(lastUser);

    emit({
      title: "Thinking",
      detail:
        round === 0
          ? "Choosing whether to call data tools or answer directly…"
          : `Continuing with tool results (round ${round + 1})…`,
    });

    const completion = await client.messages.create({
      model,
      system: `${SYSTEM_PROMPT}\n\n## Server clock\n${serverContextClock()}`,
      messages: conversation,
      ...(round < 7 ? { tools: anthropicTools } : {}),
      ...(round < 7 && forceDataTools ? { tool_choice: { type: "any" as const } } : {}),
      max_tokens: 2048,
      temperature: 0.2,
    });

    const textParts = completion.content
      .filter((p) => p.type === "text")
      .map((p) => p.text)
      .join("\n")
      .trim();
    const toolUseParts = completion.content.filter((p) => p.type === "tool_use");

    if (toolUseParts.length > 0 && round < 7) {
      emit({
        title: "Fetching data",
        detail:
          toolUseParts.length === 1
            ? "Running one request against the local data service…"
            : `Running ${toolUseParts.length} requests against the data service…`,
      });

      conversation.push({
        role: "assistant",
        content: completion.content,
      });

      const toolResults: Anthropic.Messages.ToolResultBlockParam[] = [];
      for (const part of toolUseParts) {
        const line = toolStatusDetail(part.name, part.input ?? {});
        emit({ title: "Fetching data", detail: line });

        const out = await runTool(part.name, JSON.stringify(part.input ?? {}));
        if (out.error) lastDataError = out.error;
        allTables.push(...out.tables);
        toolResults.push({
          type: "tool_result",
          tool_use_id: part.id,
          content: out.text,
        });
      }

      emit({ title: "Thinking", detail: "Processing tool output and deciding next step…" });

      conversation.push({
        role: "user",
        content: toolResults,
      });
      continue;
    }

    emit({ title: "Writing", detail: "Formatting the answer…" });

    const raw = textParts;
    const { reply: replyBody, assumptions } = splitAssumptions(raw);
    let reply = replyBody || raw;
    if (!reply && allTables.length > 0) {
      reply =
        "Here are the results from the data service. Numbers come only from the tables below.";
    }
    if (!reply) {
      reply =
        "No textual answer was returned. If tools failed, check the error banner or data service logs.";
    }
    return {
      reply,
      assumptions,
      tables: allTables,
      ...(lastDataError ? { error: lastDataError } : {}),
    };
  }

  return {
    reply:
      "Stopped after the maximum number of tool rounds for one request. Try a narrower question (shorter Statcast dates, one team, or fewer metrics).",
    tables: allTables,
    error: "Tool loop limit exceeded",
  };
}

export async function POST(req: NextRequest) {
  const ip = clientIp(req);
  const limited = rateLimit(ip);
  if (!limited.ok) {
    return NextResponse.json(
      { error: "Rate limited", rateLimited: true },
      { status: 429, headers: { "Retry-After": String(limited.retryAfterSec) } },
    );
  }

  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) {
    return NextResponse.json(
      { error: "Missing ANTHROPIC_API_KEY on the server." },
      { status: 500 },
    );
  }

  let body: { messages?: ConversationMessage[] };
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ error: "Invalid JSON body" }, { status: 400 });
  }

  const incoming = body.messages;
  if (!Array.isArray(incoming) || incoming.length === 0) {
    return NextResponse.json({ error: "messages[] required" }, { status: 400 });
  }

  const baseMessages: Anthropic.Messages.MessageParam[] = incoming
    .filter((m) => m.role === "user" || m.role === "assistant")
    .map((m) => ({
      role: m.role as "user" | "assistant",
      content: m.content,
    }));

  const client = new Anthropic({ apiKey });
  const model = process.env.ANTHROPIC_MODEL || "claude-haiku-4-5";
  const sandboxToolEnabled = process.env.ENABLE_PYBASEBALL_SANDBOX === "1";
  const toolsForAgent = sandboxToolEnabled
    ? OPENAI_TOOLS
    : OPENAI_TOOLS.filter((t) => t.function.name !== "run_pybaseball_sandbox");
  const anthropicTools = toolsForAgent.map((t) => ({
    name: t.function.name,
    description: t.function.description,
    input_schema: t.function.parameters,
  })) as Anthropic.Messages.Tool[];

  const accept = req.headers.get("accept") ?? "";
  const streamNdjson = accept.includes("application/x-ndjson");

  if (streamNdjson) {
    const enc = new TextEncoder();
    const stream = new ReadableStream<Uint8Array>({
      async start(controller) {
        const send = (obj: unknown) => {
          controller.enqueue(enc.encode(`${JSON.stringify(obj)}\n`));
        };
        try {
          const result = await runChatAgent(client, model, anthropicTools, baseMessages, (s) => {
            send({ type: "status", title: s.title, detail: s.detail });
          });
          send({
            type: "done",
            reply: result.reply,
            assumptions: result.assumptions,
            tables: result.tables,
            ...(result.error ? { error: result.error } : {}),
          });
        } catch (e) {
          const msg = e instanceof Error ? e.message : "Request failed";
          send({ type: "error", message: msg });
        } finally {
          controller.close();
        }
      },
    });

    return new Response(stream, {
      headers: {
        "Content-Type": "application/x-ndjson; charset=utf-8",
        "Cache-Control": "no-store",
      },
    });
  }

  try {
    const result = await runChatAgent(client, model, anthropicTools, baseMessages, () => {});
    if (result.error && !result.reply) {
      return NextResponse.json({ error: result.error, tables: result.tables }, { status: 500 });
    }
    return NextResponse.json({
      reply: result.reply,
      assumptions: result.assumptions,
      tables: result.tables,
      ...(result.error ? { error: result.error } : {}),
    });
  } catch (e) {
    const msg = e instanceof Error ? e.message : "Anthropic request failed";
    return NextResponse.json({ error: msg }, { status: 502 });
  }
}
