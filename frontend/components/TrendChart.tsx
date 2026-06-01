"use client";

import {
  Bar,
  BarChart,
  Cell,
  LabelList,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { GameLog } from "@/lib/api";

const HIT = "#34d399"; // emerald-400 — cleared the line
const MISS = "#fb7185"; // rose-400 — under the line
const LINE = "#38bdf8"; // sky-400 — the prop line

interface Datum {
  idx: number;
  opp: string;
  value: number;
  line: number;
  hit: boolean;
  home: boolean;
  date: string;
}

function ChartTooltip({
  active,
  payload,
  label,
}: {
  active?: boolean;
  payload?: { payload: Datum }[];
  label?: string;
}) {
  if (!active || !payload?.length) return null;
  const d = payload[0].payload;
  return (
    <div className="rounded-lg border border-border bg-surface-2 px-3 py-2 text-xs shadow-xl">
      <div className="font-medium text-foreground">
        {d.home ? "vs" : "@"} {d.opp}
      </div>
      <div className="mt-1 text-muted">
        {label}:{" "}
        <span
          className={`font-semibold ${d.hit ? "text-emerald-300" : "text-rose-300"}`}
        >
          {fmt(d.value)}
        </span>{" "}
        <span className="text-slate-500">· line {fmt(d.line)}</span>
      </div>
      <div className={`mt-0.5 font-semibold ${d.hit ? "text-emerald-400" : "text-rose-400"}`}>
        {d.hit ? "OVER" : "UNDER"}
      </div>
    </div>
  );
}

/**
 * Bar chart of recent game values with the prop line overlaid as a reference
 * line. Bars are colored by whether the value cleared the line (hit). Oldest →
 * newest, left to right.
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
  const data: Datum[] = [...games]
    .reverse() // oldest first
    .map((g, i) => ({
      idx: i,
      opp: g.opponent_abbr,
      value: g.value,
      line: g.line,
      hit: g.hit,
      home: g.home,
      date: g.date,
    }));

  return (
    <div className="w-full">
      <div className="mb-3 flex items-center justify-between text-xs text-muted">
        <span className="uppercase tracking-wide">
          {label} — last {data.length}
        </span>
        <span className="flex items-center gap-3">
          <span className="flex items-center gap-1.5">
            <span className="inline-block h-2 w-2 rounded-sm bg-emerald-400" /> Over
          </span>
          <span className="flex items-center gap-1.5">
            <span className="inline-block h-2 w-2 rounded-sm bg-rose-400" /> Under
          </span>
          <span className="flex items-center gap-1.5">
            <span className="inline-block h-0.5 w-4 bg-sky-400" /> Line {fmt(line)}
          </span>
        </span>
      </div>

      <div className="w-full">
        <ResponsiveContainer width="100%" height={224} minHeight={224} debounce={1}>
          <BarChart data={data} margin={{ top: 18, right: 6, bottom: 0, left: -18 }}>
            <XAxis
              dataKey="opp"
              tick={{ fill: "#64748b", fontSize: 10 }}
              tickLine={false}
              axisLine={{ stroke: "#1e2735" }}
              interval={0}
            />
            <YAxis
              tick={{ fill: "#64748b", fontSize: 10 }}
              tickLine={false}
              axisLine={false}
              width={38}
            />
            <Tooltip
              cursor={{ fill: "rgba(148,163,184,0.06)" }}
              content={<ChartTooltip label={label} />}
            />
            <ReferenceLine
              y={line}
              stroke={LINE}
              strokeWidth={1.5}
              strokeDasharray="5 4"
              ifOverflow="extendDomain"
            />
            <Bar dataKey="value" radius={[3, 3, 0, 0]} maxBarSize={38}>
              <LabelList
                dataKey="value"
                position="top"
                fill="#94a3b8"
                fontSize={10}
                formatter={(v) => fmt(Number(v))}
              />
              {data.map((d) => (
                <Cell key={d.idx} fill={d.hit ? HIT : MISS} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

function fmt(v: number): string {
  return v % 1 === 0 ? String(v) : v.toFixed(1);
}
