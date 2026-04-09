/**
 * Extra JSON fields for get_team_batting_statcast tool results so the model
 * must treat non-empty Statcast tables as authoritative (even when FanGraphs errors).
 */

function num(v: unknown): number | null {
  if (v === null || v === undefined) return null;
  if (typeof v === "number" && !Number.isNaN(v)) return v;
  if (typeof v === "string") {
    const n = parseFloat(v);
    return Number.isNaN(n) ? null : n;
  }
  return null;
}

export function teamBattingToolExtras(rows: Record<string, unknown>[]): {
  has_team_hitting_data: boolean;
  instruction: string;
  top_hitters_summary: string;
} {
  const n = rows.length;
  if (n === 0) {
    return {
      has_team_hitting_data: false,
      instruction:
        "No Statcast batting rows for this team/date window. Try wider dates, lower min_pa, or check team_abbr (Savant code, e.g. BOS).",
      top_hitters_summary: "",
    };
  }

  const enriched = rows.map((r) => ({
    r,
    ops: num(r.ops) ?? -1,
    pa: num(r.pa) ?? 0,
  }));
  enriched.sort((a, b) => {
    if (b.ops !== a.ops) return b.ops - a.ops;
    return b.pa - a.pa;
  });

  const top = enriched.slice(0, 12);
  const top_hitters_summary = top
    .map(({ r }) => {
      const name = String(r.batter_name ?? "").trim() || `batter_id ${r.batter_id ?? "?"}`;
      const pa = num(r.pa);
      const ops = num(r.ops);
      const avg = num(r.avg);
      const parts = [
        name,
        pa != null ? `PA ${pa}` : null,
        ops != null ? `OPS ${ops}` : null,
        ops == null && avg != null ? `AVG ${avg}` : null,
      ].filter(Boolean);
      return parts.join(", ");
    })
    .join(" | ");

  return {
    has_team_hitting_data: true,
    instruction:
      "This response includes REAL Statcast season-to-date hitting lines for the requested team. " +
      "You MUST name these players in the batting order (use batter_name from the table). " +
      "Do NOT say Statcast or Savant returned no data, and do NOT blame 'pipelines' or FanGraphs failures if this object has has_team_hitting_data true. " +
      "A FanGraphs (502) error does not erase Statcast success.",
    top_hitters_summary,
  };
}
