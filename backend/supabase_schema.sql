-- Squeeze the Line — Supabase schema for user features.
--
-- Run this in the Supabase SQL editor (Dashboard → SQL Editor). It creates the
-- tables backing the auth-protected API endpoints:
--   POST /api/picks/save     -> saved_picks
--   GET  /api/picks/saved    -> saved_picks (+ pick_results)
--   GET  /api/picks/history  -> saved_picks (+ pick_results)
--   POST /api/alerts         -> alerts
--
-- The API authenticates users via their Supabase JWT and talks to these tables
-- with the service-role key. Row Level Security policies below additionally
-- restrict access to a user's own rows for any direct (anon-key) access.

-- ---------------------------------------------------------------------------
-- saved_picks: a pick a user has chosen to track.
-- ---------------------------------------------------------------------------
create table if not exists public.saved_picks (
    id              uuid primary key default gen_random_uuid(),
    user_id         uuid not null references auth.users (id) on delete cascade,
    pick_id         text not null,
    player          text not null,
    team_abbr       text not null default '',
    opponent_abbr   text not null default '',
    sport           text not null,
    stat_type       text not null,
    line            double precision not null,
    projection      double precision not null,
    edge            double precision not null,
    confidence      integer not null,
    side            text not null default 'none',   -- over | under | none
    recommendation  text not null default '',       -- human label, e.g. "Strong Over"
    game_time       text,
    saved_at        timestamptz not null default now(),
    -- A user tracks a given pick at most once.
    unique (user_id, pick_id)
);

create index if not exists saved_picks_user_idx on public.saved_picks (user_id, saved_at desc);

-- ---------------------------------------------------------------------------
-- pick_results: the settled outcome of a tracked pick (one-to-one).
-- ---------------------------------------------------------------------------
create table if not exists public.pick_results (
    id              uuid primary key default gen_random_uuid(),
    saved_pick_id   uuid not null references public.saved_picks (id) on delete cascade,
    user_id         uuid not null references auth.users (id) on delete cascade,
    result          text not null default 'pending', -- pending | win | loss | push
    actual_value    double precision,
    settled_at      timestamptz,
    unique (saved_pick_id)
);

create index if not exists pick_results_user_idx on public.pick_results (user_id);

-- ---------------------------------------------------------------------------
-- alerts: line-movement alerts a user has set up.
-- ---------------------------------------------------------------------------
create table if not exists public.alerts (
    id          uuid primary key default gen_random_uuid(),
    user_id     uuid not null references auth.users (id) on delete cascade,
    pick_id     text not null,
    player      text not null,
    stat_type   text not null,
    sport       text not null,
    direction   text not null,                -- over | under
    threshold   double precision not null,    -- notify when the line crosses this
    note        text,
    active      boolean not null default true,
    created_at  timestamptz not null default now()
);

create index if not exists alerts_user_idx on public.alerts (user_id, created_at desc);

-- ---------------------------------------------------------------------------
-- user_activity: analytics event log (login, page view, AI query, etc.).
-- Written with the service-role key (activity.log). Read two ways:
--   * admin analytics view  -> service-role key (cross-user, bypasses RLS)
--   * any direct anon access -> restricted to the user's own rows by RLS
-- user_id is the Supabase auth user id stored as text.
-- ---------------------------------------------------------------------------
create table if not exists public.user_activity (
    id          uuid primary key default gen_random_uuid(),
    user_id     text not null,
    user_email  text not null default '',
    action      text not null,
    details     jsonb not null default '{}'::jsonb,
    created_at  timestamptz not null default now()
);

create index if not exists user_activity_created_idx on public.user_activity (created_at desc);
create index if not exists user_activity_user_idx on public.user_activity (user_id, created_at desc);

-- ---------------------------------------------------------------------------
-- parlays: multi-leg parlay tickets a user has saved for ROI tracking.
-- Written/deleted with the service-role key; read with the anon key scoped to
-- the signed-in user. Keyed by user_email (matches parlays.py).
-- ---------------------------------------------------------------------------
create table if not exists public.parlays (
    id                      uuid primary key default gen_random_uuid(),
    user_email              text not null,
    name                    text not null default '',
    legs                    jsonb not null default '[]'::jsonb,
    combined_odds_american  integer not null default 0,
    combined_odds_decimal   double precision not null default 1.0,
    implied_pct             double precision not null default 0.0,
    estimated_hit_pct       double precision not null default 0.0,
    stake                   double precision not null default 0.0,
    status                  text not null default 'open',  -- open | won | lost | void
    created_at              timestamptz not null default now()
);

create index if not exists parlays_user_idx on public.parlays (user_email, created_at desc);

-- ---------------------------------------------------------------------------
-- Row Level Security: each user may only see/modify their own rows.
-- (The API uses the service-role key, which bypasses RLS; these policies guard
-- any direct client access with the anon key.)
-- ---------------------------------------------------------------------------
alter table public.saved_picks   enable row level security;
alter table public.pick_results  enable row level security;
alter table public.alerts        enable row level security;
alter table public.user_activity enable row level security;
alter table public.parlays       enable row level security;

create policy "saved_picks are owned by the user"
    on public.saved_picks
    for all
    using (auth.uid() = user_id)
    with check (auth.uid() = user_id);

create policy "pick_results are owned by the user"
    on public.pick_results
    for all
    using (auth.uid() = user_id)
    with check (auth.uid() = user_id);

create policy "alerts are owned by the user"
    on public.alerts
    for all
    using (auth.uid() = user_id)
    with check (auth.uid() = user_id);

-- user_activity: anon-key reads are limited to the signed-in user's own rows.
-- (Writes and the cross-user admin analytics view use the service-role key,
-- which bypasses RLS, so only a SELECT policy is needed here. user_id is text,
-- so we cast auth.uid() to text to compare.)
create policy "user_activity is readable by its owner"
    on public.user_activity
    for select
    using (auth.uid()::text = user_id);

-- parlays: anon-key reads are limited to the signed-in user's own rows, matched
-- by the email in their JWT. (Writes/deletes use the service-role key.)
create policy "parlays are readable by their owner"
    on public.parlays
    for select
    using ((auth.jwt() ->> 'email') = user_email);
