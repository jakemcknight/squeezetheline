// Typed client for the Squeeze the Line FastAPI backend.
// Types mirror backend/models.py. Point at a deployed backend by setting
// NEXT_PUBLIC_API_URL (falls back to the legacy NEXT_PUBLIC_API_BASE, then
// the local dev server).

export const API_BASE =
  process.env.NEXT_PUBLIC_API_URL ??
  process.env.NEXT_PUBLIC_API_BASE ??
  "http://localhost:8000";

export interface Sport {
  key: string;
  name: string;
  full_name: string;
  icon: string;
  season: string;
  active: boolean;
  stat_types: string[];
}

export interface Recommendation {
  bucket:
    | "strong_over"
    | "trending_over"
    | "strong_under"
    | "trending_under"
    | "neutral";
  label: string;
  side: "over" | "under" | "none";
}

export interface Pick {
  id: string;
  player: string;
  team: string;
  team_abbr: string;
  opponent: string;
  opponent_abbr: string;
  game_time: string;
  stat_type: string;
  line: number;
  projection: number;
  edge: number;
  confidence: number;
  hit_rate_l10: number;
  hit_rate_season: number;
  trend: "up" | "down" | "flat";
  trend_pct: number;
  recommendation: Recommendation;
}

export interface Slate {
  sport: string;
  date: string;
  generated_at: string;
  picks: Pick[];
}

export interface GameLog {
  date: string;
  opponent_abbr: string;
  home: boolean;
  stat_type: string;
  value: number;
  line: number;
  hit: boolean;
}

export interface StatHitRate {
  stat_type: string;
  line: number;
  hit_rate_l10: number;
  hit_rate_season: number;
  avg_l10: number;
  avg_season: number;
}

export interface PlayerDetail {
  name: string;
  team: string;
  team_abbr: string;
  position: string;
  sport: string;
  headshot: string | null;
  season_averages: Record<string, number>;
  primary_stat: string;
  last_games: GameLog[];
  hit_rates: StatHitRate[];
}

export interface Injury {
  player: string;
  team: string;
  team_abbr: string;
  position: string;
  status: string;
  injury: string;
  updated: string;
  note: string;
}

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`API ${path} failed: ${res.status} ${res.statusText}`);
  }
  return (await res.json()) as T;
}

export const api = {
  sports: () => get<Sport[]>("/api/sports"),
  slate: (sport: string, date?: string) =>
    get<Slate>(
      `/api/slate?sport=${encodeURIComponent(sport)}${
        date ? `&date=${encodeURIComponent(date)}` : ""
      }`
    ),
  player: (name: string, sport: string) =>
    get<PlayerDetail>(
      `/api/player/${encodeURIComponent(name)}?sport=${encodeURIComponent(
        sport
      )}`
    ),
  injuries: (sport: string) =>
    get<Injury[]>(`/api/injuries?sport=${encodeURIComponent(sport)}`),
};
