import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  env: {
    // Base URL of the FastAPI backend. Override per-environment to point the
    // frontend at a deployed backend (e.g. your Railway/Render URL) instead of
    // the local dev server. Defaults to localhost for `npm run dev`.
    NEXT_PUBLIC_API_URL:
      process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000",
    // Supabase auth. Left empty when not configured — the app hides auth
    // features rather than failing (see lib/supabase.ts).
    NEXT_PUBLIC_SUPABASE_URL: process.env.NEXT_PUBLIC_SUPABASE_URL ?? "",
    NEXT_PUBLIC_SUPABASE_ANON_KEY:
      process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "",
  },
};

export default nextConfig;
