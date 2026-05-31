"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const links = [
  { href: "/", label: "Dashboard" },
  { href: "/injuries", label: "Injuries" },
];

export function NavBar() {
  const pathname = usePathname();
  return (
    <header className="sticky top-0 z-40 border-b border-border/80 bg-background/80 backdrop-blur-md">
      <div className="mx-auto flex h-14 w-full max-w-7xl items-center gap-6 px-4 sm:px-6">
        <Link href="/" className="flex items-center gap-2 font-semibold tracking-tight">
          <span className="grid h-7 w-7 place-items-center rounded-md bg-emerald-500/15 text-emerald-400 ring-1 ring-emerald-500/30">
            ↕
          </span>
          <span className="text-[15px]">
            Squeeze<span className="text-emerald-400">theLine</span>
          </span>
        </Link>

        <nav className="flex items-center gap-1 text-sm">
          {links.map((l) => {
            const active =
              l.href === "/"
                ? pathname === "/" || pathname.startsWith("/player")
                : pathname.startsWith(l.href);
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

        <div className="ml-auto hidden items-center gap-2 text-xs text-muted sm:flex">
          <span className="inline-block h-1.5 w-1.5 rounded-full bg-emerald-400" />
          Mock data
        </div>
      </div>
    </header>
  );
}
