"use client";

import Link from "next/link";
import type { Pick } from "@/lib/api";
import { confidenceTier, pct, signed, trendGlyph } from "@/lib/format";

const accent: Record<
  string,
  { badge: string; bar: string; edge: string; ring: string }
> = {
  emerald: {
    badge: "bg-emerald-500/15 text-emerald-300 ring-emerald-500/30",
    bar: "bg-emerald-400",
    edge: "text-emerald-400",
    ring: "hover:border-emerald-500/40",
  },
  rose: {
    badge: "bg-rose-500/15 text-rose-300 ring-rose-500/30",
    bar: "bg-rose-400",
    edge: "text-rose-400",
    ring: "hover:border-rose-500/40",
  },
  slate: {
    badge: "bg-slate-500/15 text-slate-300 ring-slate-500/30",
    bar: "bg-slate-400",
    edge: "text-slate-300",
    ring: "hover:border-slate-500/50",
  },
};

function accentFor(bucket: string) {
  if (bucket.endsWith("over")) return accent.emerald;
  if (bucket.endsWith("under")) return accent.rose;
  return accent.slate;
}

export function PickCard({ pick, sport }: { pick: Pick; sport: string }) {
  const a = accentFor(pick.recommendation.bucket);
  const tier = confidenceTier(pick.confidence);
  const dirHr =
    pick.recommendation.side === "under"
      ? 1 - pick.hit_rate_l10
      : pick.hit_rate_l10;

  return (
    <Link
      href={`/player/${encodeURIComponent(pick.player)}?sport=${sport}`}
      className={`group block rounded-xl border border-border bg-surface p-4 transition-all hover:bg-surface-2 ${a.ring} animate-fade-up`}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="truncate text-[15px] font-semibold text-foreground group-hover:text-white">
            {pick.player}
          </div>
          <div className="mt-0.5 text-xs text-muted">
            {pick.team_abbr} <span className="text-slate-600">vs</span>{" "}
            {pick.opponent_abbr} · {pick.game_time}
          </div>
        </div>
        <span
          className={`shrink-0 rounded-md px-2 py-1 text-[11px] font-semibold uppercase tracking-wide ring-1 ${a.badge}`}
        >
          {pick.recommendation.label}
        </span>
      </div>

      <div className="mt-3 flex items-end justify-between">
        <div>
          <div className="text-xs uppercase tracking-wide text-muted">
            {pick.stat_type}
          </div>
          <div className="mt-0.5 flex items-baseline gap-2">
            <span className="text-2xl font-bold tabular-nums text-foreground">
              {pick.line}
            </span>
            <span className="text-xs text-muted">line</span>
          </div>
        </div>
        <div className="text-right">
          <div className="text-xs uppercase tracking-wide text-muted">Proj</div>
          <div className="mt-0.5 flex items-baseline justify-end gap-2">
            <span className="text-2xl font-bold tabular-nums text-foreground">
              {pick.projection}
            </span>
            <span className={`text-xs font-semibold tabular-nums ${a.edge}`}>
              {signed(pick.edge)}
            </span>
          </div>
        </div>
      </div>

      {/* confidence meter */}
      <div className="mt-3">
        <div className="mb-1 flex items-center justify-between text-[11px]">
          <span className="text-muted">Confidence</span>
          <span className={`font-semibold ${tier.className}`}>
            {pick.confidence} · {tier.label}
          </span>
        </div>
        <div className="h-1.5 w-full overflow-hidden rounded-full bg-surface-2 ring-1 ring-border">
          <div
            className={`h-full rounded-full ${a.bar}`}
            style={{ width: `${pick.confidence}%` }}
          />
        </div>
      </div>

      <div className="mt-3 flex items-center justify-between border-t border-border/70 pt-3 text-xs">
        <div className="flex items-center gap-3">
          <span className="text-muted">
            L10{" "}
            <span className="font-semibold text-foreground tabular-nums">
              {pct(dirHr)}
            </span>
          </span>
          <span className="text-muted">
            Szn{" "}
            <span className="font-semibold text-foreground tabular-nums">
              {pct(
                pick.recommendation.side === "under"
                  ? 1 - pick.hit_rate_season
                  : pick.hit_rate_season
              )}
            </span>
          </span>
        </div>
        <span
          className={`tabular-nums ${
            pick.trend === "up"
              ? "text-emerald-400"
              : pick.trend === "down"
              ? "text-rose-400"
              : "text-slate-500"
          }`}
        >
          {trendGlyph(pick.trend)} {signed(pick.trend_pct)}%
        </span>
      </div>
    </Link>
  );
}
