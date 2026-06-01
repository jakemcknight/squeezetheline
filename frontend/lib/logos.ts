// ESPN CDN team-logo URLs derived from the sport key + team abbreviation.
// The backend doesn't ship logo URLs, so we build them here and fall back to a
// text badge (see components/TeamLogo) when ESPN has no matching asset.

const ESPN_LEAGUE: Record<string, string> = {
  baseball_mlb: "mlb",
  basketball_nba: "nba",
  basketball_wnba: "wnba",
  football_nfl: "nfl",
  hockey_nhl: "nhl",
  football_ncaaf: "college-football",
  basketball_ncaab: "mens-college-basketball",
  basketball_mens_ncaa: "mens-college-basketball",
};

export function teamLogoUrl(sport: string, abbr: string): string | null {
  const league = ESPN_LEAGUE[sport];
  if (!league || !abbr) return null;
  return `https://a.espncdn.com/i/teamlogos/${league}/500/${abbr.toLowerCase()}.png`;
}
