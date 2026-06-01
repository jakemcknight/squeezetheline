"use client";

import Link from "next/link";
import type { Pick } from "@/lib/api";
import { confidenceTier, pct, signed, trendGlyph } from "@/lib/format";
import { SavePickButton } from "@/components/SavePickButton";
import { TeamLogo } from "@/components/TeamLogo";
import { useParlay } from "@/lib/parlay";

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

// Hit-rate bar color: strong = emerald, coinflip = amber, weak = rose.
function hitColor(rate: number): string {
  if (rate >= 0.6) return "bg-emerald-400";
  if (rate >= 0.45) return "bg-amber-400";
  return "bg-rose-400";
}

export function PickCard({ pick, sport }: { pick: Pick; sport: string }) {
  const a = accentFor(pick.recommendation.bucket);
  const tier = confidenceTier(pick.confidence);
  const dirHr =
    pick.recommendation.side === "under"
      ? 1 - pick.hit_rate_l10
      : pick.hit_rate_l10;
  const dirSeason =
    pick.recommendation.side === "under"
      ? 1 - pick.hit_rate_season
      : pick.hit_rate_season;

  const { has, toggle } = useParlay();
  const inParlay = has(pick.id);

  function onParlay(e: React.MouseEvent) {
    e.preventDefault();
    e.stopPropagation();
    toggle(pick, sport);
  }

  return (
    <Link
      href={`/player/${encodeURIComponent(pick.player)}?sport=${sport}`}
      className={`group block rounded-xl border border-border bg-surface p-4 transition-all hover:bg-surface-2 ${a.ring} animate-fade-up`}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="flex min-w-0 items-center gap-2.5">
          <TeamLogo sport={sport} abbr={pick.team_abbr} size={34} />
          <div className="min-w-0">
            <div className="truncate text-[15px] font-semibold text-foreground group-hover:text-white">
              {pick.player}
            </div>
            <div className="mt-0.5 flex flex-wrap items-center gap-1 text-xs text-muted">
              <span>{pick.team_abbr}</span>
              <span className="text-slate-600">vs</span>
              <TeamLogo sport={sport} abbr={pick.opponent_abbr} size={14} />
              <span>{pick.opponent_abbr}</span>
              <span className="text-slate-600">·</span>
              <span>{pick.game_time}</span>
            </div>
          </div>
        </div>
        <div className="flex shrink-0 flex-col items-end gap-1.5">
          <span
            className={`rounded-md px-2 py-1 text-[11px] font-semibold uppercase tracking-wide ring-1 ${a.badge}`}
          >
            {pick.recommendation.label}
          </span>
          <SavePickButton pick={pick} sport={sport} />
        </div>
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

      {/* hit-rate bar + trend */}
      <div className="mt-3 border-t border-border/70 pt-3">
        <div className="mb-1 flex items-center justify-between text-[11px]">
          <span className="text-muted">L10 hit rate</span>
          <span className="flex items-center gap-2">
            <span className="font-semibold tabular-nums text-foreground">
              {pct(dirHr)}
            </span>
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
          </span>
        </div>
        <div className="h-1.5 w-full overflow-hidden rounded-full bg-surface-2 ring-1 ring-border">
          <div
            className={`h-full rounded-full ${hitColor(dirHr)} transition-all`}
            style={{ width: `${Math.round(dirHr * 100)}%` }}
          />
        </div>
        <div className="mt-1 text-[11px] text-muted">
          Season{" "}
          <span className="font-semibold tabular-nums text-foreground">
            {pct(dirSeason)}
          </span>
        </div>
      </div>

      {/* add to parlay */}
      <button
        type="button"
        onClick={onParlay}
        aria-pressed={inParlay}
        className={`mt-3 flex h-10 w-full items-center justify-center gap-2 rounded-lg text-sm font-semibold transition-colors ${
          inParlay
            ? "bg-emerald-500/15 text-emerald-300 ring-1 ring-emerald-500/40 hover:bg-emerald-500/25"
            : "bg-surface-2 text-foreground ring-1 ring-border hover:bg-slate-700/40"
        }`}
      >
        {inParlay ? (
          <>
            <svg width="15" height="15" viewBox="0 0 24 24" fill="none" aria-hidden>
              <path
                d="M5 12l5 5 9-11"
                stroke="currentColor"
                strokeWidth="2.4"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </svg>
            In Parlay
          </>
        ) : (
          <>
            <svg width="15" height="15" viewBox="0 0 24 24" fill="none" aria-hidden>
              <path
                d="M12 5v14M5 12h14"
                stroke="currentColor"
                strokeWidth="2.4"
                strokeLinecap="round"
              />
            </svg>
            Add to Parlay
          </>
        )}
      </button>
    </Link>
  );
}
