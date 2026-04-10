create extension if not exists pgcrypto;

create table if not exists public.artists_registry (
  id uuid primary key default gen_random_uuid(),
  artist_name text not null,
  spotify_url text not null,
  youtube_url text,
  spotify_artist_id text not null unique,
  cm_artist_id bigint,
  updated_at timestamptz not null default now()
);

create index if not exists artists_registry_updated_at_idx
  on public.artists_registry (updated_at desc);

alter table public.artists_registry enable row level security;

-- The backend uses SUPABASE_SERVICE_ROLE_KEY, so no public policies are required.
