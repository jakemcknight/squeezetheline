"use client";

import type { Sport } from "@/lib/api";

export function SportTabs({
  sports,
  active,
  onSelect,
}: {
  sports: Sport[];
  active: string;
  onSelect: (key: string) => void;
}) {
  return (
    <div className="flex flex-wrap gap-2">
      {sports.map((s) => {
        const isActive = s.key === active;
        return (
          <button
            key={s.key}
            onClick={() => onSelect(s.key)}
            className={`group flex items-center gap-2 rounded-lg border px-3.5 py-2 text-sm font-medium transition-all ${
              isActive
                ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-300 shadow-[0_0_0_1px_rgba(16,185,129,0.15)]"
                : "border-border bg-surface text-muted hover:border-slate-600 hover:text-foreground"
            }`}
          >
            <span className="text-base leading-none">{s.icon}</span>
            <span>{s.name}</span>
            {s.active ? (
              <span className="ml-0.5 inline-block h-1.5 w-1.5 rounded-full bg-emerald-400" title="In season" />
            ) : (
              <span className="ml-0.5 inline-block h-1.5 w-1.5 rounded-full bg-slate-600" title="Offseason" />
            )}
          </button>
        );
      })}
    </div>
  );
}
