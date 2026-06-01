"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { useAuth } from "@/components/AuthProvider";
import { UserMenu } from "@/components/UserMenu";

const baseLinks = [
  { href: "/", label: "Dashboard" },
  { href: "/injuries", label: "Injuries" },
];

function isActive(href: string, pathname: string): boolean {
  return href === "/"
    ? pathname === "/" || pathname.startsWith("/player")
    : pathname.startsWith(href);
}

export function NavBar() {
  const pathname = usePathname();
  const { configured, user } = useAuth();
  const [open, setOpen] = useState(false);

  // "My Picks" is only relevant for signed-in users.
  const links =
    configured && user
      ? [...baseLinks, { href: "/my-picks", label: "My Picks" }]
      : baseLinks;

  // Collapse the mobile menu whenever the route changes.
  useEffect(() => {
    setOpen(false);
  }, [pathname]);

  return (
    <header className="sticky top-0 z-40 border-b border-border/80 bg-background/80 backdrop-blur-md">
      <div className="mx-auto flex h-14 w-full max-w-7xl items-center gap-4 px-4 sm:gap-6 sm:px-6">
        {/* Hamburger — mobile only */}
        <button
          type="button"
          onClick={() => setOpen((o) => !o)}
          aria-label={open ? "Close menu" : "Open menu"}
          aria-expanded={open}
          className="-ml-2 grid h-11 w-11 shrink-0 place-items-center rounded-lg text-muted transition-colors hover:bg-surface-2 hover:text-foreground sm:hidden"
        >
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" aria-hidden>
            {open ? (
              <path
                d="M6 6l12 12M18 6L6 18"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
              />
            ) : (
              <path
                d="M4 7h16M4 12h16M4 17h16"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
              />
            )}
          </svg>
        </button>

        <Link
          href="/"
          className="flex items-center gap-2 font-semibold tracking-tight"
        >
          <span className="grid h-7 w-7 place-items-center rounded-md bg-emerald-500/15 text-emerald-400 ring-1 ring-emerald-500/30">
            ↕
          </span>
          <span className="text-[15px]">
            Squeeze<span className="text-emerald-400">theLine</span>
          </span>
        </Link>

        {/* Inline nav — desktop only */}
        <nav className="hidden items-center gap-1 text-sm sm:flex">
          {links.map((l) => {
            const active = isActive(l.href, pathname);
            return (
              <Link
                key={l.href}
                href={l.href}
                className={`rounded-md px-3 py-1.5 transition-colors ${
                  active
                    ? "bg-surface-2 text-foreground"
                    : "text-muted hover:text-foreground"
                }`}
              >
                {l.label}
              </Link>
            );
          })}
        </nav>

        <div className="ml-auto">
          <UserMenu />
        </div>
      </div>

      {/* Mobile dropdown panel */}
      {open && (
        <nav className="border-t border-border/80 bg-background/95 px-4 py-2 sm:hidden">
          {links.map((l) => {
            const active = isActive(l.href, pathname);
            return (
              <Link
                key={l.href}
                href={l.href}
                onClick={() => setOpen(false)}
                className={`flex min-h-11 items-center rounded-lg px-3 text-sm transition-colors ${
                  active
                    ? "bg-surface-2 text-foreground"
                    : "text-muted hover:bg-surface-2 hover:text-foreground"
                }`}
              >
                {l.label}
              </Link>
            );
          })}
        </nav>
      )}
    </header>
  );
}
