"use client";

import type { GameLog } from "@/lib/api";

/**
 * Compact SVG bar chart of recent game values with the prop line overlaid.
 * Bars are colored by whether they cleared the line (hit). Oldest → newest.
 */
export function TrendChart({
  games,
  line,
  label,
}: {
  games: GameLog[];
  line: number;
  label: string;
}) {
  const ordered = [...games].reverse(); // oldest first
  const values = ordered.map((g) => g.value);
  const max = Math.max(line, ...values) * 1.15 || 1;

  const W = 560;
  const H = 200;
  const padX = 8;
  const padTop = 14;
  const padBottom = 24;
  const plotH = H - padTop - padBottom;
  const n = ordered.length || 1;
  const slot = (W - padX * 2) / n;
  const barW = Math.min(34, slot * 0.6);
  const lineY = padTop + plotH - (line / max) * plotH;

  return (
    <div className="w-full overflow-hidden">
      <div className="mb-2 flex items-center justify-between text-xs text-muted">
        <span className="uppercase tracking-wide">{label} — last {ordered.length}</span>
        <span className="flex items-center gap-3">
          <span className="flex items-center gap-1">
            <span className="inline-block h-2 w-2 rounded-sm bg-emerald-400" /> Over
          </span>
          <span className="flex items-center gap-1">
            <span className="inline-block h-2 w-2 rounded-sm bg-rose-400/80" /> Under
          </span>
          <span className="flex items-center gap-1">
            <span className="inline-block h-0.5 w-4 bg-sky-400" /> Line {line}
          </span>
        </span>
      </div>
      <svg
        viewBox={`0 0 ${W} ${H}`}
        className="h-48 w-full"
        preserveAspectRatio="none"
      >
        {/* baseline */}
        <line
          x1={padX}
          y1={padTop + plotH}
          x2={W - padX}
          y2={padTop + plotH}
          stroke="#1e2735"
          strokeWidth={1}
        />
        {ordered.map((g, i) => {
          const h = Math.max(2, (g.value / max) * plotH);
          const x = padX + i * slot + (slot - barW) / 2;
          const y = padTop + plotH - h;
          return (
            <g key={i}>
              <rect
                x={x}
                y={y}
                width={barW}
                height={h}
                rx={3}
                className={g.hit ? "fill-emerald-400/80" : "fill-rose-400/70"}
              />
              <text
                x={x + barW / 2}
                y={y - 4}
                textAnchor="middle"
                className="fill-slate-400"
                style={{ fontSize: 10 }}
              >
                {g.value % 1 === 0 ? g.value : g.value.toFixed(1)}
              </text>
              <text
                x={x + barW / 2}
                y={H - 8}
                textAnchor="middle"
                className="fill-slate-500"
                style={{ fontSize: 9 }}
              >
                {g.opponent_abbr}
              </text>
            </g>
          );
        })}
        {/* prop line */}
        <line
          x1={padX}
          y1={lineY}
          x2={W - padX}
          y2={lineY}
          stroke="#38bdf8"
          strokeWidth={1.5}
          strokeDasharray="5 4"
        />
      </svg>
    </div>
  );
}
