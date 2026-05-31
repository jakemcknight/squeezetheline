"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";
import type { Session, User } from "@supabase/supabase-js";
import { getSupabase, isSupabaseConfigured } from "@/lib/supabase";
import { AuthModal } from "@/components/AuthModal";

interface AuthContextValue {
  /** Whether Supabase is configured at all. When false, hide auth UI. */
  configured: boolean;
  /** Still resolving the initial session. */
  loading: boolean;
  user: User | null;
  session: Session | null;
  /** Current access token (for authed API calls), or null. */
  getToken: () => string | null;
  signInWithEmail: (email: string, password: string) => Promise<void>;
  signUpWithEmail: (email: string, password: string) => Promise<void>;
  signInWithGoogle: () => Promise<void>;
  signOut: () => Promise<void>;
  /** Open the sign-in/up modal. */
  openAuth: () => void;
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const configured = isSupabaseConfigured;
  const [session, setSession] = useState<Session | null>(null);
  const [loading, setLoading] = useState(configured);
  const [modalOpen, setModalOpen] = useState(false);

  useEffect(() => {
    const supabase = getSupabase();
    if (!supabase) {
      setLoading(false);
      return;
    }
    supabase.auth.getSession().then(({ data }) => {
      setSession(data.session);
      setLoading(false);
    });
    const { data: sub } = supabase.auth.onAuthStateChange((_event, s) => {
      setSession(s);
    });
    return () => sub.subscription.unsubscribe();
  }, []);

  // Close the modal once a session arrives (e.g. after sign-in / OAuth return).
  useEffect(() => {
    if (session) setModalOpen(false);
  }, [session]);

  const signInWithEmail = useCallback(
    async (email: string, password: string) => {
      const supabase = getSupabase();
      if (!supabase) throw new Error("Auth not configured.");
      const { error } = await supabase.auth.signInWithPassword({
        email,
        password,
      });
      if (error) throw error;
    },
    []
  );

  const signUpWithEmail = useCallback(
    async (email: string, password: string) => {
      const supabase = getSupabase();
      if (!supabase) throw new Error("Auth not configured.");
      const { error } = await supabase.auth.signUp({ email, password });
      if (error) throw error;
    },
    []
  );

  const signInWithGoogle = useCallback(async () => {
    const supabase = getSupabase();
    if (!supabase) throw new Error("Auth not configured.");
    const { error } = await supabase.auth.signInWithOAuth({
      provider: "google",
      options: {
        redirectTo:
          typeof window !== "undefined" ? window.location.origin : undefined,
      },
    });
    if (error) throw error;
  }, []);

  const signOut = useCallback(async () => {
    const supabase = getSupabase();
    if (!supabase) return;
    await supabase.auth.signOut();
    setSession(null);
  }, []);

  const getToken = useCallback(
    () => session?.access_token ?? null,
    [session]
  );

  const value = useMemo<AuthContextValue>(
    () => ({
      configured,
      loading,
      user: session?.user ?? null,
      session,
      getToken,
      signInWithEmail,
      signUpWithEmail,
      signInWithGoogle,
      signOut,
      openAuth: () => setModalOpen(true),
    }),
    [
      configured,
      loading,
      session,
      getToken,
      signInWithEmail,
      signUpWithEmail,
      signInWithGoogle,
      signOut,
    ]
  );

  return (
    <AuthContext.Provider value={value}>
      {children}
      {configured && (
        <AuthModal open={modalOpen} onClose={() => setModalOpen(false)} />
      )}
    </AuthContext.Provider>
  );
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within an AuthProvider");
  return ctx;
}
