"use client";

import { useState } from "react";
import { useParlay } from "@/lib/parlay";
import { signed } from "@/lib/format";

export function ParlaySlip() {
  const { legs, remove, clear, count, combinedEdge, avgConfidence } = useParlay();
  const [open, setOpen] = useState(false);

  if (count === 0) return null;

  return (
    <div className="pointer-events-none fixed inset-x-0 bottom-0 z-50 flex justify-center px-3 pb-3 sm:px-6 sm:pb-4">
      <div className="pointer-events-auto w-full max-w-2xl overflow-hidden rounded-2xl border border-border bg-surface/95 shadow-2xl shadow-black/50 backdrop-blur-md">
        {/* Toggle header */}
        <button
          type="button"
          onClick={() => setOpen((o) => !o)}
          className="flex w-full items-center justify-between gap-3 px-4 py-3 text-left"
        >
          <div className="flex items-center gap-3">
            <span className="grid h-9 w-9 place-items-center rounded-full bg-emerald-500 text-sm font-bold text-slate-950">
              {count}
            </span>
            <div>
              <div className="text-sm font-semibold text-foreground">Parlay slip</div>
              <div className="text-xs text-muted">
                {count} {count === 1 ? "leg" : "legs"} · avg conf{" "}
                {Math.round(avgConfidence)}
              </div>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <div className="text-right">
              <div className="text-base font-bold tabular-nums text-emerald-400">
                {signed(combinedEdge)}%
              </div>
              <div className="text-[11px] text-muted">combined edge</div>
            </div>
            <svg
              width="18"
              height="18"
              viewBox="0 0 24 24"
              fill="none"
              aria-hidden
              className={`text-muted transition-transform ${open ? "rotate-180" : ""}`}
            >
              <path
                d="M6 9l6 6 6-6"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </svg>
          </div>
        </button>

        {open && (
          <div className="border-t border-border">
            <ul className="max-h-64 divide-y divide-border overflow-y-auto">
              {legs.map((leg) => (
                <li
                  key={leg.id}
                  className="flex items-center justify-between gap-3 px-4 py-2.5"
                >
                  <div className="min-w-0">
                    <div className="truncate text-sm font-medium text-foreground">
                      {leg.player}
                    </div>
                    <div className="text-xs text-muted">
                      {leg.recommendation} {leg.line} {leg.stat_type} ·{" "}
                      <span
                        className={
                          leg.edge >= 0 ? "text-emerald-400" : "text-rose-400"
                        }
                      >
                        {signed(leg.edge)}
                      </span>
                    </div>
                  </div>
                  <button
                    type="button"
                    onClick={() => remove(leg.id)}
                    aria-label={`Remove ${leg.player}`}
                    className="grid h-9 w-9 shrink-0 place-items-center rounded-lg text-muted transition-colors hover:bg-surface-2 hover:text-rose-400"
                  >
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" aria-hidden>
                      <path
                        d="M6 6l12 12M18 6L6 18"
                        stroke="currentColor"
                        strokeWidth="2"
                        strokeLinecap="round"
                      />
                    </svg>
                  </button>
                </li>
              ))}
            </ul>
            <div className="flex items-center justify-between gap-3 border-t border-border px-4 py-3">
              <div className="text-xs text-muted">
                Hit potential builds with each leg
              </div>
              <button
                type="button"
                onClick={clear}
                className="rounded-lg px-3 py-2 text-sm font-medium text-muted transition-colors hover:bg-surface-2 hover:text-foreground"
              >
                Clear all
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
