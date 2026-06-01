"use client";

import Link from "next/link";
import { useParams, useSearchParams } from "next/navigation";
import { api, type PlayerDetail } from "@/lib/api";
import { useApi } from "@/lib/useApi";
import { TrendChart } from "@/components/TrendChart";
import { TeamLogo } from "@/components/TeamLogo";
import { pct, formatDate } from "@/lib/format";

export default function PlayerPage() {
  const params = useParams<{ name: string }>();
  const search = useSearchParams();
  const sport = search.get("sport") ?? "baseball_mlb";
  const name = decodeURIComponent(params.name);

  const { data, loading, error } = useApi<PlayerDetail>(
    () => api.player(name, sport),
    [name, sport]
  );

  return (
    <div className="space-y-6">
      <Link
        href="/"
        className="inline-flex items-center gap-1.5 text-sm text-muted hover:text-foreground"
      >
        ← Back to board
      </Link>

      {loading ? (
        <Skeleton />
      ) : error ? (
        <div className="rounded-xl border border-rose-500/30 bg-rose-500/5 px-4 py-6 text-sm text-rose-300">
          Couldn&apos;t load {name}. {error}
        </div>
      ) : !data ? null : (
        <PlayerView data={data} />
      )}
    </div>
  );
}

function PlayerView({ data }: { data: PlayerDetail }) {
  const primaryGames = data.last_games.slice(0, 10);
  const primaryRate = data.hit_rates.find(
    (h) => h.stat_type === data.primary_stat
  );

  return (
    <div className="space-y-6">
      {/* header */}
      <div className="flex flex-col gap-4 rounded-2xl border border-border bg-surface p-5 sm:flex-row sm:items-center sm:justify-between animate-fade-up transition-all duration-200 hover:border-emerald-500/30">
        <div className="flex items-center gap-4">
          <TeamLogo
            sport={data.sport}
            abbr={data.team_abbr}
            size={56}
            className="rounded-xl bg-surface-2 p-1.5 ring-1 ring-border"
          />
          <div>
            <h1 className="text-2xl font-bold tracking-tight">{data.name}</h1>
            <div className="mt-1 text-sm text-muted">
              {data.position} · {data.team}
            </div>
          </div>
        </div>
        {primaryRate && (
          <div className="flex gap-6 sm:gap-8">
            <HeaderStat
              label={`${data.primary_stat} L10`}
              value={pct(primaryRate.hit_rate_l10)}
            />
            <HeaderStat
              label="Avg L10"
              value={fmt(primaryRate.avg_l10)}
            />
            <HeaderStat
              label="Line"
              value={fmt(primaryRate.line)}
            />
          </div>
        )}
      </div>

      {/* season averages */}
      <section className="space-y-3">
        <h2 className="text-sm font-semibold uppercase tracking-wide">
          Season Averages
        </h2>
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
          {Object.entries(data.season_averages).map(([stat, avg]) => (
            <div
              key={stat}
              className="rounded-xl border border-border bg-surface px-4 py-3 transition-all duration-200 hover:border-emerald-500/30"
            >
              <div className="truncate text-xs uppercase tracking-wide text-muted">
                {stat}
              </div>
              <div className="mt-1 text-xl font-bold tabular-nums">
                {fmt(avg)}
              </div>
            </div>
          ))}
        </div>
      </section>

      {/* trend chart */}
      <section className="space-y-3">
        <h2 className="text-sm font-semibold uppercase tracking-wide">
          {data.primary_stat} Trend
        </h2>
        <div className="rounded-2xl border border-border bg-surface p-5 transition-all duration-200 hover:border-emerald-500/30">
          <TrendChart
            games={primaryGames}
            line={primaryRate?.line ?? 0}
            label={data.primary_stat}
          />
        </div>
      </section>

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
        {/* hit rates by stat */}
        <section className="space-y-3">
          <h2 className="text-sm font-semibold uppercase tracking-wide">
            Hit Rate by Stat
          </h2>
          <div className="space-y-3 rounded-2xl border border-border bg-surface p-5">
            {data.hit_rates.map((h) => (
              <div key={h.stat_type}>
                <div className="mb-1 flex items-center justify-between text-sm">
                  <span className="text-foreground">
                    {h.stat_type}{" "}
                    <span className="text-muted">o{fmt(h.line)}</span>
                  </span>
                  <span className="tabular-nums text-muted">
                    L10{" "}
                    <span className="font-semibold text-foreground">
                      {pct(h.hit_rate_l10)}
                    </span>{" "}
                    · Szn {pct(h.hit_rate_season)}
                  </span>
                </div>
                <div className="flex h-2 gap-1">
                  <div className="relative h-2 flex-1 overflow-hidden rounded-full bg-surface-2 ring-1 ring-border">
                    <div
                      className={`h-full rounded-full ${
                        h.hit_rate_l10 >= 0.6
                          ? "bg-emerald-400"
                          : h.hit_rate_l10 >= 0.45
                          ? "bg-amber-400"
                          : "bg-rose-400"
                      }`}
                      style={{ width: `${h.hit_rate_l10 * 100}%` }}
                    />
                  </div>
                </div>
              </div>
            ))}
          </div>
        </section>

        {/* last games table */}
        <section className="space-y-3">
          <h2 className="text-sm font-semibold uppercase tracking-wide">
            Last {data.last_games.length} Games — {data.primary_stat}
          </h2>
          <div className="overflow-x-auto rounded-2xl border border-border bg-surface transition-all duration-200 hover:border-emerald-500/30">
            <table className="w-full min-w-[560px] text-sm">
              <thead>
                <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-muted">
                  <th className="px-4 py-2.5 font-medium">Date</th>
                  <th className="px-4 py-2.5 font-medium">Opp</th>
                  <th className="px-4 py-2.5 text-right font-medium">Value</th>
                  <th className="px-4 py-2.5 text-right font-medium">Line</th>
                  <th className="px-4 py-2.5 text-right font-medium">Result</th>
                </tr>
              </thead>
              <tbody>
                {data.last_games.map((g, i) => (
                  <tr
                    key={i}
                    className="border-b border-border/60 last:border-0"
                  >
                    <td className="px-4 py-2.5 text-muted">
                      {formatDate(g.date)}
                    </td>
                    <td className="px-4 py-2.5">
                      <span className="text-muted">
                        {g.home ? "vs" : "@"}
                      </span>{" "}
                      {g.opponent_abbr}
                    </td>
                    <td className="px-4 py-2.5 text-right font-semibold tabular-nums">
                      {fmt(g.value)}
                    </td>
                    <td className="px-4 py-2.5 text-right tabular-nums text-muted">
                      {fmt(g.line)}
                    </td>
                    <td className="px-4 py-2.5 text-right">
                      <span
                        className={`inline-block rounded px-2 py-0.5 text-xs font-semibold ${
                          g.hit
                            ? "bg-emerald-500/15 text-emerald-300"
                            : "bg-rose-500/15 text-rose-300"
                        }`}
                      >
                        {g.hit ? "OVER" : "UNDER"}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      </div>
    </div>
  );
}

function HeaderStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="text-right">
      <div className="text-xs uppercase tracking-wide text-muted">{label}</div>
      <div className="mt-0.5 text-xl font-bold tabular-nums">{value}</div>
    </div>
  );
}

function fmt(v: number): string {
  return v % 1 === 0 ? String(v) : v.toFixed(1);
}

function Skeleton() {
  return (
    <div className="space-y-6">
      <div className="h-24 animate-pulse rounded-2xl bg-surface" />
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-6">
        {Array.from({ length: 6 }).map((_, i) => (
          <div key={i} className="h-20 animate-pulse rounded-xl bg-surface" />
        ))}
      </div>
      <div className="h-64 animate-pulse rounded-2xl bg-surface" />
    </div>
  );
}
