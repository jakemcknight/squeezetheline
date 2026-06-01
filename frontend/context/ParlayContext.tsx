"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";
import type { Pick } from "@/lib/api";

export interface ParlayLeg {
  id: string;
  player: string;
  sport: string;
  team_abbr: string;
  opponent_abbr: string;
  stat_type: string;
  line: number;
  edge: number;
  confidence: number;
  recommendation: string;
  side: string;
}

interface ParlayContextValue {
  legs: ParlayLeg[];
  add: (pick: Pick, sport: string) => void;
  remove: (id: string) => void;
  toggle: (pick: Pick, sport: string) => void;
  clear: () => void;
  has: (id: string) => boolean;
  count: number;
  combinedEdge: number;
  avgConfidence: number;
}

const ParlayContext = createContext<ParlayContextValue | null>(null);
const STORAGE_KEY = "stl_parlay";

function legFromPick(pick: Pick, sport: string): ParlayLeg {
  return {
    id: pick.id,
    player: pick.player,
    sport,
    team_abbr: pick.team_abbr,
    opponent_abbr: pick.opponent_abbr,
    stat_type: pick.stat_type,
    line: pick.line,
    edge: pick.edge,
    confidence: pick.confidence,
    recommendation: pick.recommendation.label,
    side: pick.recommendation.side,
  };
}

export function ParlayProvider({ children }: { children: React.ReactNode }) {
  const [legs, setLegs] = useState<ParlayLeg[]>([]);
  const [hydrated, setHydrated] = useState(false);

  useEffect(() => {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (raw) setLegs(JSON.parse(raw));
    } catch {
      /* ignore malformed storage */
    }
    setHydrated(true);
  }, []);

  useEffect(() => {
    if (!hydrated) return;
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(legs));
    } catch {
      /* ignore quota errors */
    }
  }, [legs, hydrated]);

  const add = useCallback((pick: Pick, sport: string) => {
    setLegs((prev) =>
      prev.some((l) => l.id === pick.id) ? prev : [...prev, legFromPick(pick, sport)],
    );
  }, []);

  const remove = useCallback((id: string) => {
    setLegs((prev) => prev.filter((l) => l.id !== id));
  }, []);

  const toggle = useCallback((pick: Pick, sport: string) => {
    setLegs((prev) =>
      prev.some((l) => l.id === pick.id)
        ? prev.filter((l) => l.id !== pick.id)
        : [...prev, legFromPick(pick, sport)],
    );
  }, []);

  const clear = useCallback(() => setLegs([]), []);
  const has = useCallback((id: string) => legs.some((l) => l.id === id), [legs]);

  const { combinedEdge, avgConfidence } = useMemo(() => {
    if (legs.length === 0) return { combinedEdge: 0, avgConfidence: 0 };
    const product = legs.reduce((acc, l) => acc * (1 + Math.abs(l.edge) / 100), 1);
    const conf = legs.reduce((acc, l) => acc + l.confidence, 0) / legs.length;
    return { combinedEdge: (product - 1) * 100, avgConfidence: conf };
  }, [legs]);

  const value: ParlayContextValue = {
    legs,
    add,
    remove,
    toggle,
    clear,
    has,
    count: legs.length,
    combinedEdge,
    avgConfidence,
  };

  return <ParlayContext.Provider value={value}>{children}</ParlayContext.Provider>;
}

export function useParlay(): ParlayContextValue {
  const ctx = useContext(ParlayContext);
  if (!ctx) throw new Error("useParlay must be used within a ParlayProvider");
  return ctx;
}
