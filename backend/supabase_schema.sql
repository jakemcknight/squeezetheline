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
-- Row Level Security: each user may only see/modify their own rows.
-- (The API uses the service-role key, which bypasses RLS; these policies guard
-- any direct client access with the anon key.)
-- ---------------------------------------------------------------------------
alter table public.saved_picks  enable row level security;
alter table public.pick_results enable row level security;
alter table public.alerts       enable row level security;

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
