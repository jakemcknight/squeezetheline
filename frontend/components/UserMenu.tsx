"use client";

import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { useAuth } from "@/components/AuthProvider";

export function UserMenu() {
  const { configured, loading, user, openAuth, signOut } = useAuth();
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  // Close the dropdown on outside click.
  useEffect(() => {
    const onClick = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", onClick);
    return () => document.removeEventListener("mousedown", onClick);
  }, []);

  // Hidden entirely when auth isn't configured — public board, no auth UI.
  if (!configured) return null;

  if (loading) {
    return <div className="h-8 w-8 animate-pulse rounded-full bg-surface-2" />;
  }

  if (!user) {
    return (
      <button
        onClick={openAuth}
        className="rounded-md bg-emerald-500 px-3 py-1.5 text-sm font-semibold text-black transition-colors hover:bg-emerald-400"
      >
        Sign in
      </button>
    );
  }

  const email = user.email ?? "Account";
  const initial = email.charAt(0).toUpperCase();
  const avatarUrl =
    (user.user_metadata?.avatar_url as string | undefined) ?? null;

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen((o) => !o)}
        className="flex items-center gap-2 rounded-full border border-border bg-surface p-0.5 pr-2 transition-colors hover:bg-surface-2"
        aria-label="User menu"
      >
        {avatarUrl ? (
          // eslint-disable-next-line @next/next/no-img-element
          <img
            src={avatarUrl}
            alt=""
            className="h-7 w-7 rounded-full object-cover"
          />
        ) : (
          <span className="grid h-7 w-7 place-items-center rounded-full bg-emerald-500/20 text-sm font-semibold text-emerald-300">
            {initial}
          </span>
        )}
        <span className="hidden max-w-[120px] truncate text-sm text-muted sm:block">
          {email}
        </span>
      </button>

      {open && (
        <div className="absolute right-0 mt-2 w-56 overflow-hidden rounded-xl border border-border bg-surface shadow-xl animate-fade-up">
          <div className="border-b border-border px-4 py-3">
            <div className="text-xs text-muted">Signed in as</div>
            <div className="truncate text-sm font-medium text-foreground">
              {email}
            </div>
          </div>
          <Link
            href="/my-picks"
            onClick={() => setOpen(false)}
            className="block px-4 py-2.5 text-sm text-foreground transition-colors hover:bg-surface-2"
          >
            My Picks
          </Link>
          <button
            onClick={async () => {
              setOpen(false);
              await signOut();
            }}
            className="block w-full px-4 py-2.5 text-left text-sm text-rose-300 transition-colors hover:bg-surface-2"
          >
            Sign out
          </button>
        </div>
      )}
    </div>
  );
}
