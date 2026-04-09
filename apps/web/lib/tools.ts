import { z } from "zod";
import type { ChatCompletionTool } from "openai/resources/chat/completions";

export const getPitcherPitchArsenalSchema = z.object({
  year: z.number().int().min(2015).max(2030),
  min_pitches: z
    .number()
    .int()
    .min(0)
    .max(5000)
    .describe(
      "Minimum pitches thrown (Savant threshold). Use 0 to let the server default to 250; typical stable sample is 250+.",
    ),
  arsenal_type: z.enum(["avg_spin", "avg_speed"]),
  row_cap: z.number().int().min(1).max(200).default(50),
  pitch_type_filter: z
    .string()
    .optional()
    .describe('Optional pitch type code to filter rows if present, e.g. "FF".'),
  pitcher_id: z
    .number()
    .int()
    .positive()
    .optional()
    .describe(
      "MLBAM pitcher id to filter the Savant leaderboard to one pitcher (required for named pitcher questions).",
    ),
});

export const getPitchingSeasonStatsSchema = z.object({
  season: z.number().int().min(2000).max(2030),
  min_ip: z
    .number()
    .min(0)
    .max(300)
    .default(0)
    .describe(
      "Minimum IP to include a row. Use 0 for full rosters, relievers, or early season; raise (e.g. 20) for qualified-starter style lists.",
    ),
  team_abbr: z
    .string()
    .min(2)
    .max(4)
    .optional()
    .describe(
      "MLB team code for one club (BOS, WSH, LAD, …). Pass for team pitching questions so the backend can use the official MLB Stats API when FanGraphs fails.",
    ),
  name_contains: z
    .string()
    .min(1)
    .max(80)
    .optional()
    .describe(
      "Filter rows whose Name contains this substring (case-insensitive). Use with team_abbr or low min_ip for one pitcher.",
    ),
  metrics: z
    .array(
      z.enum([
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
      ]),
    )
    .min(1)
    .max(12),
  row_cap: z.number().int().min(1).max(300).default(80),
});

export const getFieldingSeasonStatsSchema = z.object({
  season: z.number().int().min(2000).max(2030),
  min_inn: z.number().min(0).max(5000).default(0),
  team_abbr: z
    .string()
    .min(2)
    .max(4)
    .optional()
    .describe(
      "MLB team code for one club so the backend can scope FanGraphs or MLB API (BOS, WSH, LAD, …).",
    ),
  name_contains: z
    .string()
    .min(1)
    .max(80)
    .optional()
    .describe("Substring match on player Name (case-insensitive)."),
  metrics: z
    .array(
      z.enum([
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
      ]),
    )
    .min(1)
    .max(14),
  row_cap: z.number().int().min(1).max(300).default(80),
});

/** MLB Stats API fielding game log (per game); use for date ranges or first N games. */
export const getFieldingGameLogSchema = z.object({
  player_id: z
    .number()
    .int()
    .positive()
    .describe("MLBAM player id — call resolve_player if the user names the player."),
  season: z.number().int().min(2000).max(2030),
  start_date: z
    .string()
    .regex(/^\d{4}-\d{2}-\d{2}$/)
    .optional()
    .describe("Inclusive YYYY-MM-DD (optional). Passed to MLB Stats API."),
  end_date: z
    .string()
    .regex(/^\d{4}-\d{2}-\d{2}$/)
    .optional()
    .describe("Inclusive YYYY-MM-DD (optional). Passed to MLB Stats API."),
  max_games: z
    .number()
    .int()
    .min(1)
    .max(200)
    .optional()
    .describe(
      "After sorting by date ascending, return only the first N games (e.g. 11 for first 11 games of the season).",
    ),
  metrics: z
    .array(
      z.enum([
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
      ]),
    )
    .min(1)
    .max(13),
  row_cap: z.number().int().min(1).max(250).default(120),
});

/** Per-game pitching/hitting lines (MLB gameLog). Server rejects metrics that do not match stat_group. */
export const mlbPitchHitGameLogMetricSchema = z.enum([
  "Date",
  "GamePk",
  "Team",
  "Opp",
  "Home",
  "Win",
  "Name",
  "R",
  "ER",
  "RBI",
  "H",
  "Doubles",
  "Triples",
  "HR",
  "BB",
  "SO",
  "IP",
  "NP",
  "SV",
  "PA",
  "AB",
  "SB",
  "CS",
]);

export const getTeamGameLogSchema = z.object({
  team_abbr: z
    .string()
    .min(2)
    .max(4)
    .describe("MLB team code (BOS, WSH, LAD, …). Aliases like WSN→WSH accepted by the server."),
  season: z.number().int().min(2000).max(2030),
  stat_group: z
    .enum(["pitching", "hitting"])
    .describe(
      "pitching = team runs/IP/H/etc. allowed that game (all pitchers combined). hitting = team offense that game.",
    ),
  start_date: z
    .string()
    .regex(/^\d{4}-\d{2}-\d{2}$/)
    .optional()
    .describe("Inclusive YYYY-MM-DD."),
  end_date: z
    .string()
    .regex(/^\d{4}-\d{2}-\d{2}$/)
    .optional()
    .describe("Inclusive YYYY-MM-DD."),
  max_games: z
    .number()
    .int()
    .min(1)
    .max(200)
    .optional()
    .describe("Keep only the first N games after sorting by date."),
  metrics: z.array(mlbPitchHitGameLogMetricSchema).min(1).max(20),
  row_cap: z.number().int().min(1).max(250).default(180),
});

export const getPlayerGameLogSchema = z.object({
  player_id: z
    .number()
    .int()
    .positive()
    .describe("MLBAM id — use resolve_player when the user names the player."),
  season: z.number().int().min(2000).max(2030),
  stat_group: z.enum(["pitching", "hitting"]),
  start_date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).optional(),
  end_date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).optional(),
  max_games: z.number().int().min(1).max(200).optional(),
  metrics: z.array(mlbPitchHitGameLogMetricSchema).min(1).max(20),
  row_cap: z.number().int().min(1).max(250).default(180),
});

/** Career / multi-season lines vs one opponent from MLB game logs (aggregated or per-game). */
export const getPlayerGameLogVsOpponentSchema = z.object({
  player_id: z
    .number()
    .int()
    .positive()
    .describe("MLBAM id — use resolve_player when the user names the player."),
  opponent_abbr: z
    .string()
    .min(2)
    .max(4)
    .describe(
      "Opponent team code (MIL, BOS, WSH, …) — the franchise faced in those games.",
    ),
  stat_group: z
    .enum(["pitching", "hitting"])
    .default("pitching")
    .describe("pitching for pitchers; hitting for batters."),
  start_season: z
    .number()
    .int()
    .min(1995)
    .max(2030)
    .default(2008)
    .describe("First season to scan (inclusive)."),
  end_season: z
    .number()
    .int()
    .min(1995)
    .max(2030)
    .default(2030)
    .describe("Last season to scan (inclusive)."),
  aggregate: z
    .boolean()
    .default(true)
    .describe(
      "If true (default), one summary row with summed stats. If false, per-game rows (requires metrics).",
    ),
  metrics: z
    .array(mlbPitchHitGameLogMetricSchema)
    .min(1)
    .max(20)
    .optional()
    .describe("Required when aggregate is false — same columns as get_player_game_log."),
  row_cap: z.number().int().min(1).max(350).default(280),
}).refine(
  (d) => d.aggregate !== false || (d.metrics !== undefined && d.metrics.length > 0),
  { message: "metrics is required when aggregate is false" },
);

/** Official MLB.com / Stats API leaderboards (no FanGraphs). */
export const getMlbStatLeadersSchema = z.object({
  season: z.number().int().min(2000).max(2030),
  leader_category: z.enum([
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
  ]),
  stat_group: z
    .enum(["pitching", "hitting"])
    .optional()
    .describe(
      "strikeOuts: omit for pitcher strikeout leaders (default); set hitting for batter strikeout totals.",
    ),
  limit: z.number().int().min(1).max(50).default(25),
  leader_game_types: z.enum(["R", "P", "F", "D", "L", "W"]).default("R"),
});

export const getBattingSeasonStatsSchema = z.object({
  season: z.number().int().min(2000).max(2030),
  min_pa: z.number().min(0).max(900).default(0),
  team_abbr: z
    .string()
    .min(2)
    .max(4)
    .optional()
    .describe(
      "MLB team code for one club (BOS, WSH, TB, MIL, …). **Always pass for team hitting questions** so the backend can use the official MLB Stats API when FanGraphs fails. Aliases like WSN→WSH and TBR→TB are accepted.",
    ),
  name_contains: z
    .string()
    .min(1)
    .max(80)
    .optional()
    .describe(
      "Filter to rows whose Name contains this text (case-insensitive). Use for one player (e.g. Wong) with team_abbr=LAD and min_pa=0 for season-to-date OPS.",
    ),
  metrics: z
    .array(
      z.enum([
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
      ]),
    )
    .min(1)
    .max(19),
  row_cap: z.number().int().min(1).max(300).default(80),
});

export const resolvePlayerSchema = z.object({
  first_name: z.string().min(1).max(80),
  last_name: z.string().min(1).max(80),
});

export const getStatcastPitchesSchema = z.object({
  start_date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  end_date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  pitcher_id: z.number().int().positive().optional(),
  batter_id: z.number().int().positive().optional(),
  pitch_type: z.string().max(6).optional(),
  columns: z
    .array(z.string().min(1))
    .min(1)
    .max(30)
    .describe(
      "Savant columns: use **player_name** (batter), **pitcher** (MLBAM id). **batter_name** is accepted as an alias for player_name. There is no **pitcher_name** in this export—omit it or expect it to be dropped with a note. When **pitcher_id** or **batter_id** is set, the data service uses Savant **player-scoped** pulls (efficient). For an **aggregated** batting line in a **batter vs pitcher** matchup, prefer **get_batter_vs_pitcher_statcast**.",
    ),
  row_cap: z.number().int().min(1).max(5000).default(500),
});

/** Pitcher’s team had ≥N run lead at first pitch of inning I; sums full-game gameLog lines for those games. */
export const getPitcherEnteringInningLeadStatcastSchema = z.object({
  pitcher_id: z.number().int().positive().describe("MLBAM pitcher id — resolve_player if needed."),
  start_date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  end_date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  entering_inning: z.number().int().min(1).max(15).default(4),
  min_lead_runs: z.number().min(0).max(20).default(3),
  max_games: z.number().int().min(1).max(800).default(400),
});

/** One batter vs one pitcher: PA-level Statcast aggregates (career or any date window). */
export const getBatterVsPitcherStatcastSchema = z.object({
  batter_id: z
    .number()
    .int()
    .positive()
    .describe("MLBAM batter id — use resolve_player for the hitter."),
  pitcher_id: z
    .number()
    .int()
    .positive()
    .describe("MLBAM pitcher id — use resolve_player for the pitcher."),
  start_date: z
    .string()
    .regex(/^\d{4}-\d{2}-\d{2}$/)
    .default("2015-03-01")
    .describe("Savant detail era (~2015+). Use a later date for a recent-only question."),
  end_date: z
    .string()
    .regex(/^\d{4}-\d{2}-\d{2}$/)
    .describe("Inclusive end date — use **yesterday** from Server clock for career-to-date."),
  min_pa: z.number().int().min(0).max(900).default(1),
});

export const getStatcastSpinVarianceSchema = z.object({
  start_date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  end_date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  pitch_type: z.string().max(6).default("FF"),
  group_by: z.enum(["pitcher", "batter"]).default("pitcher"),
  min_pitches: z.number().int().min(1).max(10000).default(50),
  row_cap: z.number().int().min(1).max(500).default(100),
});

export const getBatterHitDistanceByParkSchema = z.object({
  batter_id: z.number().int().positive(),
  start_date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  end_date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  min_hits: z.number().int().min(1).max(500).default(1),
  row_cap: z.number().int().min(1).max(50).default(35),
});

export const getTeamBattingStatcastSchema = z.object({
  start_date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  end_date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  team_abbr: z
    .string()
    .min(2)
    .max(4)
    .describe(
      "Baseball Savant 3-letter team code for the batting club, e.g. BOS, MIL, NYY, WSH (not WAS).",
    ),
  min_pa: z.number().int().min(0).max(900).default(3),
  row_cap: z.number().int().min(1).max(80).default(40),
});

/** RISP / men on / bases empty from Statcast runner columns (not MLB statSplits API). */
export const getBatterSituationalStatcastSchema = z.object({
  batter_id: z
    .number()
    .int()
    .positive()
    .describe("MLBAM batter id — use resolve_player when the user names the hitter."),
  start_date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  end_date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  situation: z
    .enum(["risp", "men_on", "bases_empty", "any"])
    .describe(
      "risp = runners in scoring position (on 2B or 3B); men_on = any runner; bases_empty; any = whole window.",
    ),
  min_pa: z.number().int().min(1).max(900).default(1),
});

/** RestrictedPython sandbox on the data service; only when ENABLE_PYBASEBALL_SANDBOX=1 on server + Next. */
export const runPybaseballSandboxSchema = z.object({
  code: z
    .string()
    .min(1)
    .max(12000)
    .describe(
      "Python only: no import. Pre-bound: pd, np, statcast, statcast_batter, statcast_pitcher, statcast_single_game, statcast_pitcher_pitch_arsenal, batting_stats, pitching_stats, fielding_stats, playerid_lookup, datetime, json. Must assign RESULT = ... (DataFrame or JSON-serializable).",
    ),
  row_cap: z.number().int().min(1).max(500).default(200),
  timeout_sec: z.number().int().min(15).max(120).default(90),
});

export type ToolName =
  | "get_pitcher_pitch_arsenal"
  | "get_pitching_season_stats"
  | "get_fielding_season_stats"
  | "get_fielding_game_log"
  | "get_team_game_log"
  | "get_player_game_log"
  | "get_player_game_log_vs_opponent"
  | "get_mlb_stat_leaders"
  | "get_batting_season_stats"
  | "get_team_batting_statcast"
  | "resolve_player"
  | "get_statcast_pitches"
  | "get_batter_vs_pitcher_statcast"
  | "get_pitcher_entering_inning_lead_statcast"
  | "get_statcast_spin_variance"
  | "get_batter_hit_distance_by_park"
  | "get_batter_situational_statcast"
  | "run_pybaseball_sandbox";

export const OPENAI_TOOLS: ChatCompletionTool[] = [
  {
    type: "function",
    function: {
      name: "get_pitcher_pitch_arsenal",
      description:
        "Savant pitch-arsenal leaderboard: average spin (rpm) or average speed by pitch type for a season. **Always pass pitcher_id (MLBAM)** when the user names a pitcher—otherwise you get a league-wide table. Use resolve_player to get the id.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          year: { type: "integer", minimum: 2015, maximum: 2030 },
          min_pitches: {
            type: "integer",
            minimum: 0,
            maximum: 5000,
            description:
              "Minimum number of pitches for inclusion. Typical: 250. Savant also supports qualified 'q' server-side when min is set appropriately — the backend maps 0 to qualified.",
          },
          arsenal_type: {
            type: "string",
            enum: ["avg_spin", "avg_speed"],
          },
          row_cap: { type: "integer", minimum: 1, maximum: 200 },
          pitch_type_filter: {
            type: "string",
            description: "Optional pitch type code, e.g. FF (four-seam fastball).",
          },
          pitcher_id: {
            type: "integer",
            minimum: 1,
            description: "MLBAM pitcher id — filters to this pitcher only.",
          },
        },
        required: ["year", "min_pitches", "arsenal_type", "row_cap"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "get_pitching_season_stats",
      description:
        "Season pitching lines: tries FanGraphs first; **automatically falls back to the official MLB Stats API** when FanGraphs blocks or errors. Use **team_abbr** for one club’s staff; **name_contains** for one pitcher. **WAR** is FanGraphs-only and is blank in MLB fallback. For simple **league leaders** (K, wins, ERA, saves), prefer **get_mlb_stat_leaders**.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          season: { type: "integer", minimum: 2000, maximum: 2030 },
          min_ip: { type: "number", minimum: 0, maximum: 300 },
          team_abbr: {
            type: "string",
            description:
              "Optional MLB team code (BOS, LAD, WSH, …) for team pitching tables and reliable MLB API fallback.",
          },
          name_contains: {
            type: "string",
            description: "Optional substring filter on pitcher Name (case-insensitive).",
          },
          metrics: {
            type: "array",
            items: {
              type: "string",
              enum: [
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
              ],
            },
            minItems: 1,
            maxItems: 12,
          },
          row_cap: { type: "integer", minimum: 1, maximum: 300 },
        },
        required: ["season", "min_ip", "metrics", "row_cap"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "get_fielding_season_stats",
      description:
        "Season **aggregate** fielding (one row per player-position for the year). Tries FanGraphs **fielding_stats** first; falls back to MLB season stats. For **per-game** fielding, date windows, or **first N games** of a season, use **get_fielding_game_log** with **player_id**. **DRS / UZR / DEF** are blank in MLB season fallback.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          season: { type: "integer", minimum: 2000, maximum: 2030 },
          min_inn: { type: "number", minimum: 0, maximum: 5000 },
          team_abbr: {
            type: "string",
            description: "Optional MLB team code for one club (BOS, LAD, WSH, …).",
          },
          name_contains: {
            type: "string",
            description: "Optional substring filter on player Name (case-insensitive).",
          },
          metrics: {
            type: "array",
            items: {
              type: "string",
              enum: [
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
              ],
            },
            minItems: 1,
            maxItems: 14,
          },
          row_cap: { type: "integer", minimum: 1, maximum: 300 },
        },
        required: ["season", "min_inn", "metrics", "row_cap"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "get_fielding_game_log",
      description:
        "**Per-game fielding** from the official MLB Stats API (gameLog). Use for a **date range**, **first N games** (**max_games** after sorting by date), or full-season game rows. Requires **player_id** (MLBAM). Columns include **Date**, **GamePk**, **Team**, **Opp**, **Pos**, **Inn**, **PO**, **A**, **E**, **FP**, etc. Aggregate PO/A/E/innings across returned rows for a window comparison.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          player_id: { type: "integer", minimum: 1 },
          season: { type: "integer", minimum: 2000, maximum: 2030 },
          start_date: {
            type: "string",
            description: "Optional inclusive YYYY-MM-DD.",
          },
          end_date: {
            type: "string",
            description: "Optional inclusive YYYY-MM-DD.",
          },
          max_games: {
            type: "integer",
            minimum: 1,
            maximum: 200,
            description: "Optional: keep only chronologically first N games (e.g. 11).",
          },
          metrics: {
            type: "array",
            items: {
              type: "string",
              enum: [
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
              ],
            },
            minItems: 1,
            maxItems: 13,
          },
          row_cap: { type: "integer", minimum: 1, maximum: 250 },
        },
        required: ["player_id", "season", "metrics", "row_cap"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "get_team_game_log",
      description:
        "**Team game-by-game** pitching or hitting from MLB Stats API (gameLog). **pitching**: runs (R), ER, H, BB, SO, IP allowed **for the full game** (all pitchers combined)—use to compare **shutouts**, fewest runs allowed, etc. **hitting**: runs scored, hits, HR, etc. per game. **Not** inning-by-inning or “through the 6th” for a live game (rows are full-game aggregates). Pass **team_abbr** (e.g. BOS). For **stat_group pitching**, typical metrics: Date, GamePk, Opp, R, ER, H, BB, SO, IP, Win.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          team_abbr: { type: "string", description: "MLB team code, e.g. BOS, WSH." },
          season: { type: "integer", minimum: 2000, maximum: 2030 },
          stat_group: {
            type: "string",
            enum: ["pitching", "hitting"],
            description: "pitching = runs allowed that game; hitting = team offense that game.",
          },
          start_date: { type: "string", description: "Optional YYYY-MM-DD." },
          end_date: { type: "string", description: "Optional YYYY-MM-DD." },
          max_games: {
            type: "integer",
            minimum: 1,
            maximum: 200,
            description: "Optional first N games chronologically.",
          },
          metrics: {
            type: "array",
            items: {
              type: "string",
              enum: [
                "Date",
                "GamePk",
                "Team",
                "Opp",
                "Home",
                "Win",
                "Name",
                "R",
                "ER",
                "RBI",
                "H",
                "Doubles",
                "Triples",
                "HR",
                "BB",
                "SO",
                "IP",
                "NP",
                "SV",
                "PA",
                "AB",
                "SB",
                "CS",
              ],
            },
            minItems: 1,
            maxItems: 20,
          },
          row_cap: { type: "integer", minimum: 1, maximum: 250 },
        },
        required: ["team_abbr", "season", "stat_group", "metrics", "row_cap"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "get_player_game_log",
      description:
        "**Player game-by-game** pitching or hitting (MLB gameLog). One row per game for that player. Use **stat_group pitching** for a pitcher’s runs allowed, IP, K per start; **hitting** for a batter’s game lines. Include **Name** in metrics if useful. Same full-game caveat as team log (not partial innings).",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          player_id: { type: "integer", minimum: 1 },
          season: { type: "integer", minimum: 2000, maximum: 2030 },
          stat_group: { type: "string", enum: ["pitching", "hitting"] },
          start_date: { type: "string" },
          end_date: { type: "string" },
          max_games: { type: "integer", minimum: 1, maximum: 200 },
          metrics: {
            type: "array",
            items: {
              type: "string",
              enum: [
                "Date",
                "GamePk",
                "Team",
                "Opp",
                "Home",
                "Win",
                "Name",
                "R",
                "ER",
                "RBI",
                "H",
                "Doubles",
                "Triples",
                "HR",
                "BB",
                "SO",
                "IP",
                "NP",
                "SV",
                "PA",
                "AB",
                "SB",
                "CS",
              ],
            },
            minItems: 1,
            maxItems: 20,
          },
          row_cap: { type: "integer", minimum: 1, maximum: 250 },
        },
        required: ["player_id", "season", "stat_group", "metrics", "row_cap"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "get_player_game_log_vs_opponent",
      description:
        "**Career or multi-season stats vs one opponent** (e.g. pitcher vs Brewers/MIL, batter vs NYY). Pulls **official MLB game logs** for each season between **start_season** and **end_season**, keeps only games where **Opp** matches **opponent_abbr**, then **aggregates** (default) into one line (IP, ERA, WHIP, K, BB, etc. for pitching; PA, AVG, OPS, etc. for hitting). Use **resolve_player** for **player_id**. This is the right tool when the user asks “career against Milwaukee,” “lifetime vs the Dodgers,” or “how has he done vs them” — do **not** refuse as “no tool exists.” For **only the current season**, you can use **get_player_game_log** with filters instead.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          player_id: { type: "integer", minimum: 1 },
          opponent_abbr: {
            type: "string",
            description: "MLB team abbreviation for the opponent (MIL, LAD, WSH, …).",
          },
          stat_group: {
            type: "string",
            enum: ["pitching", "hitting"],
            description: "Default pitching for pitchers; hitting for batters.",
          },
          start_season: { type: "integer", minimum: 1995, maximum: 2030 },
          end_season: { type: "integer", minimum: 1995, maximum: 2030 },
          aggregate: {
            type: "boolean",
            description: "Default true: one summary row. Set false for per-game lines (then pass metrics).",
          },
          metrics: {
            type: "array",
            items: { type: "string" },
            description: "When aggregate is false: same metric names as get_player_game_log.",
          },
          row_cap: { type: "integer", minimum: 1, maximum: 350 },
        },
        required: ["player_id", "opponent_abbr"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "get_mlb_stat_leaders",
      description:
        "**Primary tool for MLB league leaders** (e.g. who leads in strikeouts, home runs, steals, wins, saves, ERA). Uses the **official MLB Stats API** (same data as MLB.com)—**not FanGraphs**, so it keeps working when FanGraphs blocks requests. For pitcher strikeout leaders use leader_category **strikeOuts** (default stat_group is pitching). For **most strikeouts by a batter**, use strikeOuts with stat_group **hitting**.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          season: { type: "integer", minimum: 2000, maximum: 2030 },
          leader_category: {
            type: "string",
            enum: [
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
            ],
          },
          stat_group: {
            type: "string",
            enum: ["pitching", "hitting"],
            description:
              "Optional. strikeOuts defaults to pitching (K leaders); set hitting for batter strikeout totals.",
          },
          limit: { type: "integer", minimum: 1, maximum: 50 },
          leader_game_types: {
            type: "string",
            enum: ["R", "P", "F", "D", "L", "W"],
            description: "R = regular season (default).",
          },
        },
        required: ["season", "leader_category", "limit", "leader_game_types"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "get_batting_season_stats",
      description:
        "Season batting lines (hitters): tries FanGraphs first; **automatically falls back to the official MLB Stats API** when FanGraphs blocks (403) or errors. For **one player on a team** (e.g. Connor Wong OPS), pass **team_abbr** (e.g. LAD), **name_contains** (e.g. Wong), **min_pa** 0, and metrics including **OPS**. For **one team**, pass **team_abbr** and include **Team** in metrics. **wOBA / wRC+ / WAR** may be blank in MLB fallback. For Savant PA-level OPS use **get_team_batting_statcast** with dates.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          season: { type: "integer", minimum: 2000, maximum: 2030 },
          min_pa: { type: "number", minimum: 0, maximum: 900 },
          team_abbr: {
            type: "string",
            description:
              "3–4 letter MLB team code (BOS, NYY, WSH, TB, MIL, LAD). Use for team-scoped or player-filtered queries.",
          },
          name_contains: {
            type: "string",
            description:
              "Substring to match against player Name (case-insensitive). Use with team_abbr for one hitter’s season line.",
          },
          metrics: {
            type: "array",
            items: {
              type: "string",
              enum: [
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
              ],
            },
            minItems: 1,
            maxItems: 19,
          },
          row_cap: { type: "integer", minimum: 1, maximum: 300 },
        },
        required: ["season", "min_pa", "metrics", "row_cap"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "get_team_batting_statcast",
      description:
        "**Statcast team hitting + rate stats:** PA-level aggregates per batter including **ops**, **obp**, **slg**, **avg**, **pa**, **hr**, etc., for one club over a date range (only PAs when that team is batting). **Use this when the user asks to compute OPS / OBP / SLG from Statcast for a team window**—the server applies official-style formulas on classified PA outcomes. Also use when **get_batting_season_stats** fails or for season-to-date (opening day → yesterday per Server clock). Pass Savant team code (BOS, LAD, MIL, …).",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          start_date: { type: "string", description: "YYYY-MM-DD" },
          end_date: { type: "string", description: "YYYY-MM-DD" },
          team_abbr: {
            type: "string",
            description:
              "Savant 3-letter code: BOS, MIL, NYY, WSH, etc. (not city names).",
          },
          min_pa: {
            type: "integer",
            minimum: 0,
            maximum: 900,
            description: "Minimum plate appearances to include a batter (use 0 in April for thin samples).",
          },
          row_cap: { type: "integer", minimum: 1, maximum: 80 },
        },
        required: ["start_date", "end_date", "team_abbr", "min_pa", "row_cap"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "get_batter_situational_statcast",
      description:
        "**Situational batting for one hitter** from Statcast: **average with RISP**, men on base, bases empty, or full window. Uses Savant **on_1b/on_2b/on_3b** on each PA result (official MLB **statSplits** JSON is often empty publicly). Call **resolve_player** for **batter_id** if needed. Pass **situation**: **risp** for runners in scoring position (2B/3B only). Use opening day → yesterday for season-to-date. For plain season totals without splits use **get_batting_season_stats**.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          batter_id: { type: "integer", minimum: 1 },
          start_date: { type: "string", description: "YYYY-MM-DD" },
          end_date: { type: "string", description: "YYYY-MM-DD" },
          situation: {
            type: "string",
            enum: ["risp", "men_on", "bases_empty", "any"],
            description:
              "risp = on 2B or 3B; men_on = any runner; bases_empty; any = no runner filter.",
          },
          min_pa: {
            type: "integer",
            minimum: 1,
            maximum: 900,
            description: "Minimum PAs in the split (default 1; raise for noisy small samples).",
          },
        },
        required: ["batter_id", "start_date", "end_date", "situation", "min_pa"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "resolve_player",
      description:
        "Optional: map a spelled name to **numeric ids** for Statcast (batter_id / pitcher_id). **Skip** if the user already gave an MLBAM id or you can answer with team/season stats without it. **Output is not authoritative for rosters:** never use it to claim retirement, last season, or ‘not active.’ Never mention the underlying dataset by name in the user reply.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          first_name: { type: "string" },
          last_name: { type: "string" },
        },
        required: ["first_name", "last_name"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "get_statcast_pitches",
      description:
        "Pull pitch-level Statcast rows for a date range. Columns: **player_name** = batter on that pitch; **pitcher** = pitcher MLBAM id. **batter_name** is accepted as an alias for player_name. There is **no pitcher_name** column in Savant’s export (omit it). When **pitcher_id** or **batter_id** is set, the **data service uses Savant player-scoped CSV pulls** (not a full league download). For **career batting stats vs one pitcher** (PA/AVG/OPS), use **get_batter_vs_pitcher_statcast** instead of raw pitch rows. Keep row_cap modest.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          start_date: { type: "string", description: "YYYY-MM-DD" },
          end_date: { type: "string", description: "YYYY-MM-DD" },
          pitcher_id: { type: "integer", minimum: 1 },
          batter_id: { type: "integer", minimum: 1 },
          pitch_type: { type: "string", description: "e.g. FF, SL, CH" },
          columns: {
            type: "array",
            items: { type: "string" },
            minItems: 1,
            maxItems: 30,
            description:
              "Use player_name (batter); batter_name aliases to it. Use pitcher for pitcher id—not pitcher_name.",
          },
          row_cap: { type: "integer", minimum: 1, maximum: 5000 },
        },
        required: ["start_date", "end_date", "columns", "row_cap"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "get_batter_vs_pitcher_statcast",
      description:
        "**Batter vs pitcher matchup (Statcast):** aggregated **batting line** (PA, H, HR, BB, SO, AVG, OBP, SLG, OPS) for one **batter_id** against one **pitcher_id** over **start_date**–**end_date**. Uses Savant’s **pitcher-scoped** pull, then filters to that batter—this is the right tool for “Yelich vs Gray career,” “how does Judge do vs Cole,” etc. Call **resolve_player** twice if needed. Default **start_date** ~2015 (Savant era); pre-2015 PA are **not** included—mention that if relevant. For **batter vs a whole team** (not one pitcher), use **get_player_game_log_vs_opponent**.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          batter_id: { type: "integer", minimum: 1 },
          pitcher_id: { type: "integer", minimum: 1 },
          start_date: {
            type: "string",
            description: "YYYY-MM-DD; default Savant-era floor ~2015-03-01 for career.",
          },
          end_date: { type: "string", description: "YYYY-MM-DD inclusive (yesterday for career-to-date)." },
          min_pa: {
            type: "integer",
            minimum: 0,
            maximum: 900,
            description: "Minimum PAs to return the full stat row (default 1).",
          },
        },
        required: ["batter_id", "pitcher_id", "end_date"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "get_pitcher_entering_inning_lead_statcast",
      description:
        "**Game-state filter + pitching line:** For one pitcher, finds games where **his team’s lead at the first pitch of inning N** (default **entering the 4th**) was **≥ min_lead_runs** (default **3**), using **full-game Statcast** scoreboard columns at that pitch, then returns **summed full-game** pitching stats (IP, ER, K, BB, etc.) from **official MLB gameLog** for only those games. Use for “how does Gray pitch when his team is up 3 going into the 4th?” **Not** inning-by-inning WPA—**full-game** lines in qualifying games. Calls **statcast_single_game** per distinct appearance (can be slow); **narrow dates** or lower **max_games** if needed. Statcast ~2015+.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          pitcher_id: { type: "integer", minimum: 1 },
          start_date: { type: "string", description: "YYYY-MM-DD" },
          end_date: { type: "string", description: "YYYY-MM-DD" },
          entering_inning: {
            type: "integer",
            minimum: 1,
            maximum: 15,
            description: "Inning whose first pitch defines the score snapshot (4 = entering the 4th).",
          },
          min_lead_runs: {
            type: "number",
            minimum: 0,
            maximum: 20,
            description: "Minimum run lead for the pitcher’s team at that snapshot.",
          },
          max_games: {
            type: "integer",
            minimum: 1,
            maximum: 800,
            description: "Cap distinct games (each needs a full-game Statcast fetch).",
          },
        },
        required: ["pitcher_id", "start_date", "end_date"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "get_statcast_spin_variance",
      description:
        "Compute variance/stddev of release_spin_rate from pitch-level Statcast rows, grouped by pitcher or batter.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          start_date: { type: "string", description: "YYYY-MM-DD" },
          end_date: { type: "string", description: "YYYY-MM-DD" },
          pitch_type: { type: "string", description: "Pitch type like FF" },
          group_by: { type: "string", enum: ["pitcher", "batter"] },
          min_pitches: { type: "integer", minimum: 1, maximum: 10000 },
          row_cap: { type: "integer", minimum: 1, maximum: 500 },
        },
        required: [
          "start_date",
          "end_date",
          "pitch_type",
          "group_by",
          "min_pitches",
          "row_cap",
        ],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "get_batter_hit_distance_by_park",
      description:
        "For one batter (MLBAM batter_id), aggregate Statcast base hits (single/double/triple/home_run) and average hit_distance_sc by home_team abbreviation — a practical ballpark proxy (each MLB club maps to its home stadium). Use after resolve_player. For full-season asks, pass the season date range (may be slow first time).",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          batter_id: { type: "integer", minimum: 1 },
          start_date: { type: "string", description: "YYYY-MM-DD" },
          end_date: { type: "string", description: "YYYY-MM-DD" },
          min_hits: {
            type: "integer",
            minimum: 1,
            maximum: 500,
            description: "Minimum hits at a home_team bucket to include",
          },
          row_cap: { type: "integer", minimum: 1, maximum: 50 },
        },
        required: ["batter_id", "start_date", "end_date", "min_hits", "row_cap"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "run_pybaseball_sandbox",
      description:
        "**Escape hatch only.** Runs short Python on the data service in a RestrictedPython subprocess (pybaseball + pandas + numpy already available — **no import** in code). You **must** set **RESULT** to a DataFrame or JSON-serializable value (e.g. list of dicts). **Prefer the named HTTP tools first** (season stats, game logs, Statcast endpoints). Use this for custom pandas/pybaseball logic that has no dedicated tool. **Requires** ENABLE_PYBASEBALL_SANDBOX=1 on the data service and on the Next server (tool is hidden otherwise). Keep **statcast()** date ranges short; respect **row_cap**.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          code: {
            type: "string",
            description: "Python snippet ending with RESULT = ...",
          },
          row_cap: { type: "integer", minimum: 1, maximum: 500 },
          timeout_sec: { type: "integer", minimum: 15, maximum: 120 },
        },
        required: ["code"],
      },
    },
  },
];
