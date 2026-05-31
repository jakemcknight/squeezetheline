"use client";

import { useCallback, useEffect, useState } from "react";
import Link from "next/link";
import { useAuth } from "@/components/AuthProvider";
import { userApi, type PickHistory, type SavedPick } from "@/lib/api";
import { pct, signed } from "@/lib/format";

export default function MyPicksPage() {
  const { configured, loading: authLoading, user, getToken, openAuth } =
    useAuth();
  const [data, setData] = useState<PickHistory | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    const token = getToken();
    if (!token) return;
    setLoading(true);
    setError(null);
    try {
      const history = await userApi.history(token);
      setData(history);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load picks.");
    } finally {
      setLoading(false);
    }
  }, [getToken]);

  useEffect(() => {
    if (!authLoading && user) load();
    else if (!authLoading) setLoading(false);
  }, [authLoading, user, load]);

  if (!configured) {
    return (
      <Notice title="Sign-in isn't available">
        User accounts aren&apos;t configured for this deployment.
      </Notice>
    );
  }

  if (authLoading) {
    return <Skeleton />;
  }

  if (!user) {
    return (
      <Notice title="Sign in to see your picks">
        Save picks from the board and track how they settle over time.
        <div className="mt-4">
          <button
            onClick={openAuth}
            className="rounded-lg bg-emerald-500 px-4 py-2 text-sm font-semibold text-black transition-colors hover:bg-emerald-400"
          >
            Sign in
          </button>
        </div>
      </Notice>
    );
  }

  if (loading) return <Skeleton />;

  if (error) {
    return (
      <Notice title="Couldn't load your picks" tone="error">
        {error}
      </Notice>
    );
  }

  const stats = data?.stats;
  const picks = data?.picks ?? [];

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-1">
        <h1 className="text-2xl font-bold tracking-tight">My Picks</h1>
        <p className="text-sm text-muted">
          Picks you&apos;re tracking and how they&apos;ve settled.
        </p>
      </div>

      {stats && <StatsBar stats={stats} />}

      {picks.length === 0 ? (
        <Notice title="No saved picks yet">
          Head to the{" "}
          <Link href="/" className="text-emerald-400 hover:text-emerald-300">
            board
          </Link>{" "}
          and hit <span className="font-semibold">＋ Save</span> on any pick.
        </Notice>
      ) : (
        <div className="overflow-x-auto rounded-xl border border-border bg-surface">
          <table className="w-full min-w-[820px] text-sm">
            <thead>
              <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-muted">
                <th className="px-4 py-3 font-medium">Player</th>
                <th className="px-4 py-3 font-medium">Matchup</th>
                <th className="px-4 py-3 font-medium">Stat</th>
                <th className="px-4 py-3 text-right font-medium">Line</th>
                <th className="px-4 py-3 text-right font-medium">Proj</th>
                <th className="px-4 py-3 font-medium">Pick</th>
                <th className="px-4 py-3 text-right font-medium">Actual</th>
                <th className="px-4 py-3 text-right font-medium">Result</th>
              </tr>
            </thead>
            <tbody>
              {picks.map((p) => (
                <PickRow key={p.id} pick={p} />
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function StatsBar({
  stats,
}: {
  stats: NonNullable<PickHistory["stats"]>;
}) {
  const items = [
    { label: "Tracked", value: stats.total, className: "text-foreground" },
    { label: "Wins", value: stats.wins, className: "text-emerald-400" },
    { label: "Losses", value: stats.losses, className: "text-rose-400" },
    { label: "Pending", value: stats.pending, className: "text-slate-400" },
    {
      label: "Win Rate",
      value: pct(stats.win_rate),
      className: "text-sky-400",
    },
  ];
  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-5">
      {items.map((it) => (
        <div
          key={it.label}
          className="rounded-xl border border-border bg-surface px-4 py-3"
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

function PickRow({ pick }: { pick: SavedPick }) {
  return (
    <tr className="border-b border-border/60 last:border-0 hover:bg-surface-2">
      <td className="px-4 py-3">
        <Link
          href={`/player/${encodeURIComponent(pick.player)}?sport=${
            pick.sport
          }`}
          className="font-medium text-foreground hover:text-emerald-300"
        >
          {pick.player}
        </Link>
        <div className="text-xs text-muted">{pick.team_abbr}</div>
      </td>
      <td className="px-4 py-3 text-muted">
        {pick.team_abbr} vs {pick.opponent_abbr}
      </td>
      <td className="px-4 py-3">{pick.stat_type}</td>
      <td className="px-4 py-3 text-right tabular-nums">{pick.line}</td>
      <td className="px-4 py-3 text-right tabular-nums">{pick.projection}</td>
      <td className="px-4 py-3">
        <span className="rounded-md bg-surface-2 px-2 py-1 text-xs font-semibold text-foreground">
          {pick.recommendation}
        </span>
      </td>
      <td className="px-4 py-3 text-right tabular-nums">
        {pick.actual_value ?? "—"}
      </td>
      <td className="px-4 py-3 text-right">
        <ResultBadge result={pick.result} />
      </td>
    </tr>
  );
}

function ResultBadge({ result }: { result: SavedPick["result"] }) {
  const map: Record<SavedPick["result"], string> = {
    win: "bg-emerald-500/15 text-emerald-300",
    loss: "bg-rose-500/15 text-rose-300",
    push: "bg-amber-500/15 text-amber-300",
    pending: "bg-slate-500/15 text-slate-300",
  };
  return (
    <span
      className={`rounded-md px-2 py-1 text-xs font-semibold capitalize ${map[result]}`}
    >
      {result}
    </span>
  );
}

function Notice({
  title,
  tone = "default",
  children,
}: {
  title: string;
  tone?: "default" | "error";
  children: React.ReactNode;
}) {
  const border =
    tone === "error" ? "border-rose-500/30 bg-rose-500/5" : "border-border bg-surface/50";
  return (
    <div className={`rounded-xl border ${border} px-6 py-10 text-center`}>
      <div className="text-base font-semibold text-foreground">{title}</div>
      <div className="mt-1 text-sm text-muted">{children}</div>
    </div>
  );
}

function Skeleton() {
  return (
    <div className="space-y-6">
      <div className="h-8 w-40 animate-pulse rounded bg-surface-2" />
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-5">
        {Array.from({ length: 5 }).map((_, i) => (
          <div key={i} className="h-20 animate-pulse rounded-xl bg-surface" />
        ))}
      </div>
      <div className="h-64 animate-pulse rounded-xl bg-surface" />
    </div>
  );
}
