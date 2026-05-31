// Small presentation helpers shared across pages.

export function pct(value: number): string {
  return `${Math.round(value * 100)}%`;
}

export function signed(value: number): string {
  const v = Math.round(value * 10) / 10;
  return v > 0 ? `+${v}` : `${v}`;
}

export function confidenceTier(confidence: number): {
  label: string;
  className: string;
} {
  if (confidence >= 72)
    return { label: "High", className: "text-emerald-400" };
  if (confidence >= 60)
    return { label: "Medium", className: "text-amber-400" };
  return { label: "Low", className: "text-slate-400" };
}

// Color helpers keyed off the recommendation bucket.
export function bucketAccent(bucket: string): string {
  if (bucket.endsWith("over")) return "emerald";
  if (bucket.endsWith("under")) return "rose";
  return "slate";
}

export function trendGlyph(trend: string): string {
  if (trend === "up") return "▲";
  if (trend === "down") return "▼";
  return "�—";
}

export function formatDate(iso: string): string {
  // iso like "2026-05-31"
  const [y, m, d] = iso.split("-").map(Number);
  if (!y || !m || !d) return iso;
  const dt = new Date(Date.UTC(y, m - 1, d));
  return dt.toLocaleDateString("en-US", {
    weekday: "short",
    month: "short",
    day: "numeric",
    timeZone: "UTC",
  });
}

export function playerSlug(name: string): string {
  return encodeURIComponent(name);
}
