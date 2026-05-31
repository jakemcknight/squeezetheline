"use client";

import { useState } from "react";
import { useAuth } from "@/components/AuthProvider";
import { userApi, type Pick } from "@/lib/api";

/**
 * "Save Pick" control shown on a pick card. Hidden when auth isn't configured.
 * When the user is signed out it opens the auth modal; signed in, it POSTs the
 * pick to the backend. Sits inside a <Link>, so it stops click propagation to
 * avoid navigating to the player page.
 */
export function SavePickButton({
  pick,
  sport,
}: {
  pick: Pick;
  sport: string;
}) {
  const { configured, user, getToken, openAuth } = useAuth();
  const [state, setState] = useState<"idle" | "saving" | "saved" | "error">(
    "idle"
  );

  if (!configured) return null;

  async function handleClick(e: React.MouseEvent) {
    // We're nested in the card's <Link> — don't navigate.
    e.preventDefault();
    e.stopPropagation();

    if (!user) {
      openAuth();
      return;
    }
    const token = getToken();
    if (!token) {
      openAuth();
      return;
    }

    setState("saving");
    try {
      await userApi.savePick(token, {
        pick_id: pick.id,
        player: pick.player,
        team_abbr: pick.team_abbr,
        opponent_abbr: pick.opponent_abbr,
        sport,
        stat_type: pick.stat_type,
        line: pick.line,
        projection: pick.projection,
        edge: pick.edge,
        confidence: pick.confidence,
        side: pick.recommendation.side,
        recommendation: pick.recommendation.label,
        game_time: pick.game_time,
      });
      setState("saved");
    } catch {
      setState("error");
    }
  }

  const label =
    state === "saving"
      ? "Saving…"
      : state === "saved"
      ? "✓ Saved"
      : state === "error"
      ? "Retry"
      : "＋ Save";

  return (
    <button
      type="button"
      onClick={handleClick}
      disabled={state === "saving"}
      title={user ? "Track this pick" : "Sign in to save this pick"}
      className={`shrink-0 rounded-md px-2 py-1 text-[11px] font-semibold ring-1 transition-colors disabled:opacity-60 ${
        state === "saved"
          ? "bg-emerald-500/15 text-emerald-300 ring-emerald-500/30"
          : state === "error"
          ? "bg-rose-500/15 text-rose-300 ring-rose-500/30"
          : "bg-surface-2 text-muted ring-border hover:text-foreground"
      }`}
    >
      {label}
    </button>
  );
}
