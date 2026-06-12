# Squeeze the Line

Player-prop projections, slates, and injury reports across the FIFA World Cup,
MLB, NFL, NCAA football, NBA, WNBA, and NCAA basketball. Odds come from
[The Odds API](https://the-odds-api.com); season stats, positions, defense, and
injuries are scraped from public sources and run through a sport-aware
projection engine. Soccer adds a bespoke Poisson model (`soccer_model.py`) for
its low-scoring, position-driven counting stats — see the World Cup section
below.

## Architecture

The repo hosts three things that share one data/projection pipeline:

```
squeezetheline/
├── app.py, config.py, analysis.py, data.py, scrapers/   # core pipeline (+ legacy Streamlit app)
├── backend/        # FastAPI service — REST API over the pipeline
│   ├── main.py         # routes: /api/sports, /api/slate, /api/player, /api/injuries
│   ├── models.py       # Pydantic response models
│   └── providers/      # LiveProvider (real scraped data) | MockProvider (sample data)
└── frontend/       # Next.js app — dashboard UI that consumes the FastAPI backend
```

- **Pipeline (repo root):** `config.py`, `analysis.py`, `data.py`, and
  `scrapers/` fetch odds/stats and produce projections. Originally surfaced
  through the Streamlit app in `app.py`.
- **Backend (`backend/`):** a FastAPI wrapper. It picks a data provider at
  startup — `LiveProvider` when `ODDS_API_KEY` is set and the root modules
  import cleanly, otherwise `MockProvider` (so the API always boots and serves
  something). See `backend/providers/__init__.py`.
- **Frontend (`frontend/`):** a Next.js dashboard. It talks to the backend via
  `NEXT_PUBLIC_API_URL` (`frontend/lib/api.ts`).

> **Deploy note:** the backend's `LiveProvider` imports repo-root modules
> (`config`, `analysis`, `data`, `scrapers`). The backend service must therefore
> be deployed from the **repo root**, not from `backend/` — the start command
> loads the app via `--app-dir backend`. Deploying `backend/` alone makes the
> live provider fall back to mock data.

## FIFA World Cup 2026

Soccer is wired as a first-class sport (`soccer_fifa_world_cup`) alongside the US
leagues, but the data flow and model differ — see `soccer_model.py` and
`scrapers/soccer.py` for the full rationale.

**Odds (The Odds API, key `soccer_fifa_world_cup`).** Supported player-prop
markets and how they map onto the projection pipeline:

| Market (Odds API key)            | Shown as            | Supported | How it's modeled |
|----------------------------------|---------------------|-----------|------------------|
| `player_goal_scorer_anytime`     | Anytime Goalscorer  | ✅ full   | Yes/No → Over-0.5 goals; `P=1-e^(-λ)` |
| `player_shots`                   | Shots               | ✅ full   | over/under, Poisson |
| `player_shots_on_target`         | Shots on Target     | ✅ full   | over/under, Poisson |
| `player_assists`                 | Assists             | ✅ full   | over/under, Poisson |
| `player_to_receive_card`         | To Receive a Card   | ✅ full   | Yes/No → Over-0.5 cards |
| `player_first_goal_scorer` / `player_last_goal_scorer` | — | ❌ unsupported | goal *ordering*, not groundable in per-player rates |
| `player_to_receive_red_card`     | —                   | ❌ unsupported | too rare/noisy to model |

The Yes/No markets carry no numeric line, so the odds layer rewrites them to a
synthetic Over-0.5 line on the matching stat (scoring ≥1 goal == anytime
goalscorer). They're treated as plus-money "does it happen" bets: only the Yes
side is surfaced, and only for credible scorers/booking candidates.

**Cost / tier note.** World Cup player props are returned on the project's
current Odds API plan (a 20,000-credit tier — verified live: a request for the
day's match returned the props with ~19.9k credits remaining). Props are billed
per event at `markets × regions` credits, so a full slate day costs roughly
`(# matches) × 5` credits. The free 500-credit tier would not sustain a full
tournament; the current plan comfortably does.

**Player stats (ESPN, free).** ESPN has no league-wide soccer game-log endpoint,
so each player's recent **club** match form (goals, shots, shots on target,
assists, cards) comes from the athlete *overview* feed (last ~5 matches). Recent
club form is the best public per-player signal for an international tournament,
where national-team samples are tiny. ESPN soccer injuries
(`soccer/fifa.world/injuries`) and national-team logos (ESPN "countries" set)
are also wired.

**Projection model (`soccer_model.py`).** A bespoke Poisson engine, because
soccer counting stats are low and Poisson-distributed: it shrinks the (small)
recent sample toward a position prior (forwards score ~9× defenders), scales
scoring by a documented national-team opponent-strength factor, and reads prop
probabilities straight off the Poisson.

**Known limitations (graceful, documented).** Slate depth tracks what the book
posts — many group-stage matches list only the goalscorer market. Per-match
*minutes* aren't in the ESPN feed, so a start ≈ 90′ / sub ≈ 30′ is estimated.
Player headshots aren't available for soccer (logos are). Odds-API↔ESPN name
differences (e.g. "Heung-Min Son" vs "Son Heung-Min") are reconciled by
exact token-set identity in the backend live provider; the legacy Streamlit
`app.py` path doesn't yet apply that reconciliation.

## Running locally

### Backend (FastAPI)

```bash
cd backend
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # then add your ODDS_API_KEY
uvicorn main:app --reload --port 8000
```

Without `ODDS_API_KEY`, the API serves mock data. API docs at
http://localhost:8000/docs, health check at `/api/health`.

### Frontend (Next.js)

```bash
cd frontend
npm install
cp .env.example .env.local     # NEXT_PUBLIC_API_URL defaults to localhost:8000
npm run dev                    # http://localhost:3000
```

## Deployment

### Frontend → Vercel

1. Import the repo into Vercel and set the **Root Directory** to `frontend`.
2. Build command `next build` / output handled automatically by the Next preset.
3. Set env var **`NEXT_PUBLIC_API_URL`** to your deployed backend URL
   (e.g. `https://squeeze-the-line-api.onrender.com`).

### Backend → Railway or Render

The repo ships ready-to-use configs at the repo root:

- `render.yaml` — Render Blueprint
- `railway.json` — Railway config
- `Procfile` — Heroku/Railway-style fallback

All three use the same start command, run from the repo root:

```
uvicorn main:app --app-dir backend --host 0.0.0.0 --port $PORT
```

**Render:** create a new Blueprint pointed at this repo (root directory = repo
root). It installs `backend/requirements.txt` and starts the service. Add
`ODDS_API_KEY` (and any Supabase vars) in the dashboard.

**Railway:** create a service from this repo with the **Root Directory left at
the repo root**. Railway reads `railway.json`. Add `ODDS_API_KEY` in the
Variables tab.

## Environment variables

### Backend (`backend/.env.example`)

| Var | Required | Purpose |
| --- | --- | --- |
| `ODDS_API_KEY` | **Yes** (for live data) | The Odds API key. Without it the backend serves mock data. |
| `SUPABASE_URL` | Optional | Supabase project URL — used by legacy auth/picks/digest modules, not the projection endpoints. |
| `SUPABASE_ANON_KEY` | Optional | Public anon key. |
| `SUPABASE_SERVICE_ROLE_KEY` | Optional | Service-role key for server-side writes. Keep secret. |

### Frontend (`frontend/.env.example`)

| Var | Required | Purpose |
| --- | --- | --- |
| `NEXT_PUBLIC_API_URL` | Recommended | Base URL of the backend. Defaults to `http://localhost:8000`. |
