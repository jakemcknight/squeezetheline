import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  env: {
    // Base URL of the FastAPI backend. Override per-environment to point the
    // frontend at a deployed backend (e.g. your Railway/Render URL) instead of
    // the local dev server. Defaults to localhost for `npm run dev`.
    NEXT_PUBLIC_API_URL:
      process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000",
  },
};

export default nextConfig;
