"use client";

import { useMemo, useState } from "react";
import Link from "next/link";
import { api, API_BASE, type Pick, type Sport } from "@/lib/api";
import { useApi } from "@/lib/useApi";
import { SportTabs } from "@/components/SportTabs";
import { PickCard } from "@/components/PickCard";
import { SkeletonCard } from "@/components/SkeletonCard";
import { SkeletonTable } from "@/components/SkeletonTable";
import { pct, signed, confidenceTier } from "@/lib/format";

const DEFAULT_DATE = "2026-05-31";

export default function Dashboard() {
  const sportsState = useApi<Sport[]>(() => api.sports(), []);
  const [sport, setSport] = useState("baseball_mlb");
  const [date, setDate] = useState(DEFAULT_DATE);

  const slateState = useApi(() => api.slate(sport, date), [sport, date]);

  const sports = sportsState.data ?? [];
  const picks = slateState.data?.picks ?? [];

  const groups = useMemo(() => bucketize(picks), [picks]);

  return (
    <div className="space-y-6">
      <section className="flex flex-col gap-4">
        <div className="flex flex-col gap-1">
          <h1 className="text-2xl font-bold tracking-tight">Today&apos;s Board</h1>
          <p className="text-sm text-muted">
            Confidence-scored prop projections. Click any pick for the full
            player breakdown.
          </p>
        </div>

        <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          {sportsState.loading ? (
            <TabsSkeleton />
          ) : (
            <SportTabs sports={sports} active={sport} onSelect={setSport} />
          )}

          <label className="flex items-center gap-2 self-start rounded-lg border border-border bg-surface px-3 py-2 text-sm sm:self-auto">
            <span className="text-muted">Date</span>
            <input
              type="date"
              value={date}
              onChange={(e) => setDate(e.target.value)}
              className="bg-transparent text-foreground outline-none [color-scheme:dark]"
            />
          </label>
        </div>
      </section>

      {slateState.loading ? (
        <BoardSkeleton />
      ) : slateState.error ? (
        <ErrorState message={slateState.error} />
      ) : picks.length === 0 ? (
        <EmptyState />
      ) : (
        <>
          <SummaryBar picks={picks} groups={groups} />

          <div className="grid grid-cols-1 gap-5 lg:grid-cols-3">
            <Section
              title="Strong Overs"
              hint="High-confidence overs"
              dot="bg-emerald-400"
              picks={groups.strongOver}
              sport={sport}
            />
            <Section
              title="Trending Overs"
              hint="Momentum building"
              dot="bg-amber-400"
              picks={groups.trendingOver}
              sport={sport}
            />
            <Section
              title="Strong Unders"
              hint="High-confidence unders"
              dot="bg-rose-400"
              picks={groups.strongUnder}
              sport={sport}
            />
          </div>

          <AllPicksTable picks={picks} sport={sport} />
        </>
      )}
    </div>
  );
}

interface Groups {
  strongOver: Pick[];
  trendingOver: Pick[];
  strongUnder: Pick[];
  trendingUnder: Pick[];
}

function bucketize(picks: Pick[]): Groups {
  return {
    strongOver: picks.filter((p) => p.recommendation.bucket === "strong_over"),
    trendingOver: picks.filter(
      (p) => p.recommendation.bucket === "trending_over"
    ),
    strongUnder: picks.filter(
      (p) => p.recommendation.bucket === "strong_under"
    ),
    trendingUnder: picks.filter(
      (p) => p.recommendation.bucket === "trending_under"
    ),
  };
}

function SummaryBar({ picks, groups }: { picks: Pick[]; groups: Groups }) {
  const avgConf = Math.round(
    picks.reduce((s, p) => s + p.confidence, 0) / Math.max(picks.length, 1)
  );
  const items = [
    { label: "Total Picks", value: picks.length, className: "text-foreground" },
    {
      label: "Strong Overs",
      value: groups.strongOver.length,
      className: "text-emerald-400",
    },
    {
      label: "Strong Unders",
      value: groups.strongUnder.length,
      className: "text-rose-400",
    },
    { label: "Avg Confidence", value: avgConf, className: "text-sky-400" },
  ];
  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
      {items.map((it) => (
        <div
          key={it.label}
          className="rounded-xl border border-border bg-surface px-4 py-3 transition-all duration-200 hover:border-emerald-500/30"
        >
          <div className="text-xs uppercase tracking-wide text-muted">
            {it.label}
          </div>
          <div className={`mt-1 text-2xl font-bold tabular-nums ${it.className}`}>
            {it.value}
          </div>
        </div>
      ))}
    </div>
  );
}

function Section({
  title,
  hint,
  dot,
  picks,
  sport,
}: {
  title: string;
  hint: string;
  dot: string;
  picks: Pick[];
  sport: string;
}) {
  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span className={`inline-block h-2 w-2 rounded-full ${dot}`} />
          <h2 className="text-sm font-semibold uppercase tracking-wide">
            {title}
          </h2>
          <span className="rounded-full bg-surface-2 px-2 py-0.5 text-xs text-muted">
            {picks.length}
          </span>
        </div>
      </div>
      {picks.length === 0 ? (
        <div className="rounded-xl border border-dashed border-border bg-surface/50 px-4 py-8 text-center text-sm text-muted">
          No {title.toLowerCase()} — {hint.toLowerCase()}.
        </div>
      ) : (
        <div className="space-y-3">
          {picks.map((p) => (
            <PickCard key={p.id} pick={p} sport={sport} />
          ))}
        </div>
      )}
    </div>
  );
}

function AllPicksTable({ picks, sport }: { picks: Pick[]; sport: string }) {
  return (
    <div className="space-y-3">
      <h2 className="text-sm font-semibold uppercase tracking-wide">All Picks</h2>
      <div className="overflow-x-auto rounded-xl border border-border bg-surface">
        <table className="w-full min-w-[760px] text-sm">
          <thead>
            <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-muted">
              <th className="px-4 py-3 font-medium">Player</th>
              <th className="px-4 py-3 font-medium">Matchup</th>
              <th className="px-4 py-3 font-medium">Stat</th>
              <th className="px-4 py-3 text-right font-medium">Line</th>
              <th className="px-4 py-3 text-right font-medium">Proj</th>
              <th className="px-4 py-3 text-right font-medium">Edge</th>
              <th className="px-4 py-3 text-right font-medium">L10</th>
              <th className="px-4 py-3 text-right font-medium">Conf</th>
              <th className="px-4 py-3 text-right font-medium">Pick</th>
            </tr>
          </thead>
          <tbody>
            {picks.map((p) => {
              const tier = confidenceTier(p.confidence);
              const dirHr =
                p.recommendation.side === "under"
                  ? 1 - p.hit_rate_l10
                  : p.hit_rate_l10;
              const over = p.recommendation.bucket.endsWith("over");
              const under = p.recommendation.bucket.endsWith("under");
              return (
                <tr
                  key={p.id}
                  className="border-b border-border/60 last:border-0 hover:bg-surface-2"
                >
                  <td className="px-4 py-3">
                    <Link
                      href={`/player/${encodeURIComponent(
                        p.player
                      )}?sport=${sport}`}
                      className="font-medium text-foreground hover:text-emerald-300"
                    >
                      {p.player}
                    </Link>
                    <div className="text-xs text-muted">{p.team_abbr}</div>
                  </td>
                  <td className="px-4 py-3 text-muted">
                    {p.team_abbr} vs {p.opponent_abbr}
                  </td>
                  <td className="px-4 py-3">{p.stat_type}</td>
                  <td className="px-4 py-3 text-right tabular-nums">{p.line}</td>
                  <td className="px-4 py-3 text-right tabular-nums">
                    {p.projection}
                  </td>
                  <td
                    className={`px-4 py-3 text-right font-medium tabular-nums ${
                      p.edge >= 0 ? "text-emerald-400" : "text-rose-400"
                    }`}
                  >
                    {signed(p.edge)}
                  </td>
                  <td className="px-4 py-3 text-right tabular-nums">
                    {pct(dirHr)}
                  </td>
                  <td
                    className={`px-4 py-3 text-right font-semibold tabular-nums ${tier.className}`}
                  >
                    {p.confidence}
                  </td>
                  <td className="px-4 py-3 text-right">
                    <span
                      className={`rounded-md px-2 py-1 text-xs font-semibold ${
                        over
                          ? "bg-emerald-500/15 text-emerald-300"
                          : under
                          ? "bg-rose-500/15 text-rose-300"
                          : "bg-slate-500/15 text-slate-300"
                      }`}
                    >
                      {p.recommendation.label}
                    </span>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function TabsSkeleton() {
  return (
    <div className="flex gap-2">
      {Array.from({ length: 5 }).map((_, i) => (
        <div
          key={i}
          className="h-9 w-24 animate-pulse rounded-lg bg-surface-2"
        />
      ))}
    </div>
  );
}

function BoardSkeleton() {
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <div key={i} className="skeleton h-[72px] rounded-xl" />
        ))}
      </div>
      <div className="grid grid-cols-1 gap-5 lg:grid-cols-3">
        {Array.from({ length: 3 }).map((_, col) => (
          <div key={col} className="space-y-3">
            <div className="skeleton h-5 w-32" />
            {Array.from({ length: 2 }).map((_, i) => (
              <SkeletonCard key={i} />
            ))}
          </div>
        ))}
      </div>
      <SkeletonTable rows={5} />
    </div>
  );
}

function ErrorState({ message }: { message: string }) {
  return (
    <div className="rounded-xl border border-rose-500/30 bg-rose-500/5 px-4 py-6 text-sm text-rose-300">
      <div className="font-semibold">Couldn&apos;t load the slate.</div>
      <div className="mt-1 text-rose-300/80">{message}</div>
      <div className="mt-2 text-rose-300/60">
        Is the API running at <code>{API_BASE}</code>?
      </div>
    </div>
  );
}

function EmptyState() {
  return (
    <div className="rounded-xl border border-dashed border-border bg-surface/50 px-4 py-12 text-center text-sm text-muted">
      No picks available for this sport / date.
    </div>
  );
}
