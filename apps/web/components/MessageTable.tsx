"use client";

import { useMemo } from "react";

import { formatTableTitleDisplay } from "@/lib/format-table-title";

export type TablePayload = {
  title: string;
  columns: string[];
  rows: Record<string, string | number | null>[];
};

function escapeCsvCell(v: string | number | null): string {
  if (v === null || v === undefined) return "";
  const s = String(v);
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

export function MessageTable({ table }: { table: TablePayload }) {
  const titleDisplay = useMemo(
    () => formatTableTitleDisplay(table.title),
    [table.title],
  );

  const csv = useMemo(() => {
    const lines = [
      table.columns.map(escapeCsvCell).join(","),
      ...table.rows.map((row) =>
        table.columns.map((c) => escapeCsvCell(row[c] ?? null)).join(","),
      ),
    ];
    return lines.join("\n");
  }, [table]);

  const download = () => {
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${table.title.replace(/[^a-z0-9]+/gi, "-").toLowerCase() || "table"}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div
      className="mt-3 overflow-hidden rounded-xl border border-white/10 bg-ballpark-panel/80 shadow-panel backdrop-blur-sm"
      title={table.title}
    >
      <div className="flex items-center justify-between gap-2 border-b border-white/5 bg-gradient-to-r from-ballpark-accent/10 to-transparent px-3 py-2.5">
        <div className="flex min-w-0 flex-1 items-start gap-2">
          <span
            className="mt-0.5 hidden h-6 w-1 shrink-0 rounded-full bg-ballpark-accent/80 sm:block"
            aria-hidden
          />
          <div className="min-w-0 flex-1">
            <p className="text-xs font-medium leading-snug text-ballpark-chalk/95">
              {titleDisplay.headline}
            </p>
            {titleDisplay.subline ? (
              <p className="mt-0.5 text-[11px] leading-snug text-ballpark-chalk/55">
                {titleDisplay.subline}
              </p>
            ) : null}
          </div>
        </div>
        <button
          type="button"
          onClick={download}
          className="shrink-0 rounded-lg border border-white/10 bg-ballpark-navy/60 px-2.5 py-1.5 text-[11px] font-medium text-ballpark-chalk/70 transition hover:border-ballpark-accent/40 hover:bg-ballpark-accent/10 hover:text-ballpark-chalk"
        >
          Export CSV
        </button>
      </div>
      <div className="max-h-80 overflow-auto">
        <table className="w-full border-collapse text-left text-xs font-mono">
          <thead className="sticky top-0 z-[1] bg-[#1a222c] shadow-sm">
            <tr>
              {table.columns.map((c) => (
                <th
                  key={c}
                  className="border-b border-white/10 px-3 py-2.5 text-[10px] font-semibold uppercase tracking-wide text-ballpark-chalk/45"
                >
                  {c}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {table.rows.length === 0 ? (
              <tr>
                <td
                  colSpan={Math.max(1, table.columns.length)}
                  className="px-3 py-6 text-center text-ballpark-chalk/40"
                >
                  No rows
                </td>
              </tr>
            ) : (
              table.rows.map((row, i) => (
                <tr
                  key={i}
                  className={`border-b border-white/[0.04] last:border-0 ${
                    i % 2 === 0 ? "bg-white/[0.02]" : "bg-transparent"
                  }`}
                >
                  {table.columns.map((c) => (
                    <td key={c} className="px-3 py-2 text-ballpark-chalk/90">
                      {row[c] === null || row[c] === undefined ? "—" : String(row[c])}
                    </td>
                  ))}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
