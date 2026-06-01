"use client";

import { useMemo, useState } from "react";
import { api, type Injury, type Sport } from "@/lib/api";
import { useApi } from "@/lib/useApi";
import { SportTabs } from "@/components/SportTabs";
import { formatDate } from "@/lib/format";

function statusStyle(status: string): string {
  const s = status.toLowerCase();
  if (s.includes("out") || s.startsWith("il"))
    return "bg-rose-500/15 text-rose-300 ring-rose-500/30";
  if (s.includes("doubtful"))
    return "bg-orange-500/15 text-orange-300 ring-orange-500/30";
  if (s.includes("questionable"))
    return "bg-amber-500/15 text-amber-300 ring-amber-500/30";
  if (s.includes("probable"))
    return "bg-emerald-500/15 text-emerald-300 ring-emerald-500/30";
  return "bg-slate-500/15 text-slate-300 ring-slate-500/30";
}

export default function InjuriesPage() {
  const sportsState = useApi<Sport[]>(() => api.sports(), []);
  const [sport, setSport] = useState("baseball_mlb");
  const [query, setQuery] = useState("");

  const injState = useApi<Injury[]>(() => api.injuries(sport), [sport]);
  const sports = sportsState.data ?? [];
  const injuries = injState.data ?? [];

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return injuries;
    return injuries.filter(
      (i) =>
        i.player.toLowerCase().includes(q) ||
        i.team_abbr.toLowerCase().includes(q) ||
        i.injury.toLowerCase().includes(q)
    );
  }, [injuries, query]);

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-1">
        <h1 className="text-2xl font-bold tracking-tight">Injury Report</h1>
        <p className="text-sm text-muted">
          Availability and status across the league. Factor these into your
          picks.
        </p>
      </div>

      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        {sportsState.loading ? (
          <div className="h-9 w-72 animate-pulse rounded-lg bg-surface-2" />
        ) : (
          <SportTabs sports={sports} active={sport} onSelect={setSport} />
        )}
        <input
          type="text"
          value={query}
          placeholder="Search player, team, injury…"
          onChange={(e) => setQuery(e.target.value)}
          className="w-full rounded-lg border border-border bg-surface px-3 py-2 text-sm text-foreground outline-none placeholder:text-muted focus:border-slate-600 sm:w-64"
        />
      </div>

      {injState.loading ? (
        <div className="h-96 animate-pulse rounded-xl bg-surface" />
      ) : injState.error ? (
        <div className="rounded-xl border border-rose-500/30 bg-rose-500/5 px-4 py-6 text-sm text-rose-300">
          Couldn&apos;t load injuries. {injState.error}
        </div>
      ) : (
        <>
        {/* Mobile: stacked cards */}
        <div className="space-y-3 sm:hidden">
          {filtered.length === 0 ? (
            <div className="rounded-xl border border-dashed border-border bg-surface/50 px-4 py-10 text-center text-sm text-muted">
              No injuries match “{query}”.
            </div>
          ) : (
            filtered.map((inj, i) => (
              <div
                key={`m-${inj.player}-${i}`}
                className="rounded-xl border border-border bg-surface p-4"
              >
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <div className="truncate font-medium text-foreground">
                      {inj.player}
                    </div>
                    <div className="mt-0.5 text-xs text-muted">
                      {inj.team_abbr} · {inj.position}
                    </div>
                  </div>
                  <span
                    className={`shrink-0 rounded-md px-2 py-1 text-xs font-semibold ring-1 ${statusStyle(
                      inj.status
                    )}`}
                  >
                    {inj.status}
                  </span>
                </div>
                <div className="mt-2 text-sm text-foreground">{inj.injury}</div>
                {inj.note && (
                  <div className="mt-1 text-xs text-muted">{inj.note}</div>
                )}
                <div className="mt-2 text-[11px] text-muted">
                  Updated {formatDate(inj.updated)}
                </div>
              </div>
            ))
          )}
        </div>

        {/* Desktop: table */}
        <div className="hidden overflow-x-auto rounded-xl border border-border bg-surface sm:block">
          <table className="w-full min-w-[720px] text-sm">
            <thead>
              <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-muted">
                <th className="px-4 py-3 font-medium">Player</th>
                <th className="px-4 py-3 font-medium">Team</th>
                <th className="px-4 py-3 font-medium">Pos</th>
                <th className="px-4 py-3 font-medium">Status</th>
                <th className="px-4 py-3 font-medium">Injury</th>
                <th className="px-4 py-3 font-medium">Note</th>
                <th className="px-4 py-3 text-right font-medium">Updated</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((inj, i) => (
                <tr
                  key={`${inj.player}-${i}`}
                  className="border-b border-border/60 last:border-0 hover:bg-surface-2"
                >
                  <td className="px-4 py-3 font-medium text-foreground">
                    {inj.player}
                  </td>
                  <td className="px-4 py-3 text-muted">{inj.team_abbr}</td>
                  <td className="px-4 py-3 text-muted">{inj.position}</td>
                  <td className="px-4 py-3">
                    <span
                      className={`rounded-md px-2 py-1 text-xs font-semibold ring-1 ${statusStyle(
                        inj.status
                      )}`}
                    >
                      {inj.status}
                    </span>
                  </td>
                  <td className="px-4 py-3">{inj.injury}</td>
                  <td className="px-4 py-3 max-w-xs text-muted">{inj.note}</td>
                  <td className="px-4 py-3 text-right text-muted">
                    {formatDate(inj.updated)}
                  </td>
                </tr>
              ))}
              {filtered.length === 0 && (
                <tr>
                  <td
                    colSpan={7}
                    className="px-4 py-10 text-center text-sm text-muted"
                  >
                    No injuries match “{query}”.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
        </>
      )}
    </div>
  );
}
