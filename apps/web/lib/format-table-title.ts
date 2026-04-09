/** MLB abbreviations → readable names for table captions (Savant / Stats API style). */
const MLB_TEAM_DISPLAY: Record<string, string> = {
  AZ: "Arizona Diamondbacks",
  ATL: "Atlanta Braves",
  BAL: "Baltimore Orioles",
  BOS: "Boston Red Sox",
  CHC: "Chicago Cubs",
  CWS: "Chicago White Sox",
  CIN: "Cincinnati Reds",
  CLE: "Cleveland Guardians",
  COL: "Colorado Rockies",
  DET: "Detroit Tigers",
  HOU: "Houston Astros",
  KC: "Kansas City Royals",
  LAA: "Los Angeles Angels",
  LAD: "Los Angeles Dodgers",
  MIA: "Miami Marlins",
  MIL: "Milwaukee Brewers",
  MIN: "Minnesota Twins",
  NYM: "New York Mets",
  NYY: "New York Yankees",
  ATH: "Athletics",
  OAK: "Athletics",
  PHI: "Philadelphia Phillies",
  PIT: "Pittsburgh Pirates",
  SD: "San Diego Padres",
  SF: "San Francisco Giants",
  SEA: "Seattle Mariners",
  STL: "St. Louis Cardinals",
  TB: "Tampa Bay Rays",
  TEX: "Texas Rangers",
  TOR: "Toronto Blue Jays",
  WSH: "Washington Nationals",
  WSN: "Washington Nationals",
  SDP: "San Diego Padres",
  SFG: "San Francisco Giants",
  KCR: "Kansas City Royals",
  TBR: "Tampa Bay Rays",
};

function parseISODate(ymd: string): Date | null {
  const m = ymd.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return null;
  const y = Number(m[1]);
  const mo = Number(m[2]) - 1;
  const d = Number(m[3]);
  const dt = new Date(y, mo, d);
  return Number.isNaN(dt.getTime()) ? null : dt;
}

function prettifyCamelCase(key: string): string {
  const spaced = key.replace(/([a-z])([A-Z])/g, "$1 $2");
  return spaced.charAt(0).toUpperCase() + spaced.slice(1);
}

function formatDateRange(startYmd: string, endYmd: string): string {
  const s = parseISODate(startYmd);
  const e = parseISODate(endYmd);
  if (!s || !e) return `${startYmd} – ${endYmd}`;

  const sameYear = s.getFullYear() === e.getFullYear();
  if (sameYear) {
    const startStr = s.toLocaleDateString("en-US", { month: "short", day: "numeric" });
    const endStr = e.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
    return `${startStr} – ${endStr}`;
  }
  const a = s.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
  const b = e.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
  return `${a} – ${b}`;
}

export type FormattedTableTitle = {
  /** Main line — plain language */
  headline: string;
  /** Second line — dates, source detail, etc. */
  subline?: string;
};

/**
 * Turn compact API-style table titles into short, readable captions for the UI.
 * Raw title strings from the chat route stay unchanged for CSV filenames / debugging.
 */
export function formatTableTitleDisplay(raw: string): FormattedTableTitle {
  const t = raw.trim();

  const teamBatting = t.match(
    /^Team batting Statcast \(([A-Z0-9]{2,4}),\s*(\d{4}-\d{2}-\d{2})\s*[–-]\s*(\d{4}-\d{2}-\d{2})\)\s*$/i,
  );
  if (teamBatting) {
    const abbr = teamBatting[1].toUpperCase();
    const d1 = teamBatting[2];
    const d2 = teamBatting[3];
    const name = MLB_TEAM_DISPLAY[abbr] ?? `${abbr}`;
    return {
      headline: `${name} — team batting`,
      subline: `${formatDateRange(d1, d2)} · Statcast`,
    };
  }

  const battingSeasonMlb = t.match(
    /^Batting season (\d{4}) \(MLB Stats API(?:,\s*([A-Z0-9]{2,4}))?\)\s*$/i,
  );
  if (battingSeasonMlb) {
    const season = battingSeasonMlb[1];
    const abbr = battingSeasonMlb[2]?.toUpperCase();
    const teamPart = abbr
      ? `${MLB_TEAM_DISPLAY[abbr] ?? abbr} — `
      : "";
    return {
      headline: `${teamPart}${season} batting`,
      subline: "MLB official stats",
    };
  }

  const battingSeasonFg = t.match(/^Batting season (\d{4}) \(FanGraphs\)\s*$/i);
  if (battingSeasonFg) {
    return {
      headline: `${battingSeasonFg[1]} batting`,
      subline: "FanGraphs",
    };
  }

  const pitchingSeason = t.match(/^Pitching season (\d{4}) \(FanGraphs\)\s*$/i);
  if (pitchingSeason) {
    return {
      headline: `${pitchingSeason[1]} pitching`,
      subline: "FanGraphs",
    };
  }

  const mlbLeaders = t.match(/^MLB leaders (\d{4}) \((.+)\)\s*$/i);
  if (mlbLeaders) {
    return {
      headline: `${mlbLeaders[1]} league leaders`,
      subline: prettifyCamelCase(mlbLeaders[2]),
    };
  }

  const statcastPitches = t.match(
    /^Statcast pitches \((\d{4}-\d{2}-\d{2}) to (\d{4}-\d{2}-\d{2})\)\s*$/i,
  );
  if (statcastPitches) {
    return {
      headline: "Pitch-level Statcast",
      subline: formatDateRange(statcastPitches[1], statcastPitches[2]),
    };
  }

  const pitchArsenalLeague = t.match(
    /^Pitch arsenal \((\d{4}), (avg_spin|avg_speed)\)\s*$/i,
  );
  if (pitchArsenalLeague) {
    const metric = pitchArsenalLeague[2] === "avg_spin" ? "Average spin rate" : "Average pitch speed";
    return {
      headline: `${pitchArsenalLeague[1]} pitch mix (Savant)`,
      subline: `${metric} · league`,
    };
  }

  const pitchArsenalPitcher = t.match(
    /^Pitch arsenal \((\d{4}), (avg_spin|avg_speed), pitcher_id=(\d+)\)\s*$/i,
  );
  if (pitchArsenalPitcher) {
    const metric = pitchArsenalPitcher[2] === "avg_spin" ? "Average spin rate" : "Average pitch speed";
    return {
      headline: `${pitchArsenalPitcher[1]} pitch mix (Savant)`,
      subline: `${metric} · pitcher #${pitchArsenalPitcher[3]}`,
    };
  }

  return { headline: t };
}
