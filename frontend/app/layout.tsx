import type { Metadata } from "next";
import "./globals.css";
import { NavBar } from "@/components/NavBar";
import { AuthProvider } from "@/components/AuthProvider";
import { ParlayProvider } from "@/context/ParlayContext";
import { ParlaySlip } from "@/components/ParlaySlip";

export const metadata: Metadata = {
  title: "Squeeze the Line — Player Prop Projections",
  description:
    "Daily player-prop projections, confidence-scored picks, trends and injury reports across MLB, WNBA, NFL and college.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className="antialiased">
        <AuthProvider>
          <ParlayProvider>
            <NavBar />
            <main className="mx-auto w-full max-w-7xl px-4 pb-28 pt-6 sm:px-6">
              {children}
            </main>
            <ParlaySlip />
          </ParlayProvider>
        </AuthProvider>
      </body>
    </html>
  );
}
