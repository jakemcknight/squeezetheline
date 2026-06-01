"use client";

import { useState } from "react";
import { teamLogoUrl } from "@/lib/logos";

/**
 * Team logo from the ESPN CDN with a graceful fallback to a text badge when the
 * asset 404s or the sport/abbr isn't mappable.
 */
export function TeamLogo({
  sport,
  abbr,
  size = 28,
  className = "",
}: {
  sport: string;
  abbr: string;
  size?: number;
  className?: string;
}) {
  const src = teamLogoUrl(sport, abbr);
  const [failed, setFailed] = useState(false);

  if (!src || failed) {
    return (
      <span
        className={`inline-flex shrink-0 items-center justify-center rounded-full bg-surface-2 text-[10px] font-bold text-muted ring-1 ring-border ${className}`}
        style={{ width: size, height: size }}
        aria-label={abbr}
      >
        {abbr?.slice(0, 3).toUpperCase()}
      </span>
    );
  }

  return (
    // eslint-disable-next-line @next/next/no-img-element
    <img
      src={src}
      alt={abbr}
      width={size}
      height={size}
      loading="lazy"
      onError={() => setFailed(true)}
      className={`shrink-0 object-contain ${className}`}
      style={{ width: size, height: size }}
    />
  );
}
