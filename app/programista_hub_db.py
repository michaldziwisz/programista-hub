from __future__ import annotations

import os

import psycopg
from psycopg.rows import dict_row

DB_DSN = os.environ.get(
    "PROGRAMISTA_HUB_DB_DSN",
    "host=127.0.0.1 port=15432 dbname=programista_hub connect_timeout=2",
)

_SCHEMA_STATEMENTS: list[str] = [
    "CREATE EXTENSION IF NOT EXISTS pg_trgm",
    "CREATE EXTENSION IF NOT EXISTS unaccent",
    # unaccent is STABLE, so we wrap it for index expressions.
    """
    CREATE OR REPLACE FUNCTION programista_unaccent(text)
    RETURNS text
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    AS $$ SELECT public.unaccent($1) $$
    """.strip(),
    """
    CREATE TABLE IF NOT EXISTS provider (
      id text PRIMARY KEY,
      kind text NOT NULL,
      display_name text NOT NULL,
      updated_at timestamptz NOT NULL DEFAULT now()
    )
    """.strip(),
    """
    CREATE TABLE IF NOT EXISTS source (
      provider_id text NOT NULL REFERENCES provider(id) ON DELETE CASCADE,
      id text NOT NULL,
      name text NOT NULL,
      updated_at timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (provider_id, id)
    )
    """.strip(),
    """
    CREATE TABLE IF NOT EXISTS schedule_item (
      id bigserial PRIMARY KEY,
      provider_id text NOT NULL REFERENCES provider(id) ON DELETE CASCADE,
      source_id text NOT NULL,
      day date NOT NULL,
      start_time time NOT NULL,
      title text NOT NULL,
      subtitle text,
      details_ref text,
      details_summary text,
      accessibility jsonb,
      created_at timestamptz NOT NULL DEFAULT now(),
      FOREIGN KEY (provider_id, source_id) REFERENCES source(provider_id, id) ON DELETE CASCADE,
      UNIQUE (provider_id, source_id, day, start_time, title)
    )
    """.strip(),
    """
    CREATE TABLE IF NOT EXISTS item_details (
      provider_id text NOT NULL REFERENCES provider(id) ON DELETE CASCADE,
      details_ref text NOT NULL,
      details_text text NOT NULL,
      fetched_at timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (provider_id, details_ref)
    )
    """.strip(),
    """
    CREATE TABLE IF NOT EXISTS provider_page (
      provider_id text NOT NULL REFERENCES provider(id) ON DELETE CASCADE,
      page_title text NOT NULL,
      page_id bigint,
      rev_id bigint,
      day date,
      updated_at timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (provider_id, page_title)
    )
    """.strip(),
    """
    CREATE TABLE IF NOT EXISTS fetch_state (
      key text PRIMARY KEY,
      updated_at timestamptz NOT NULL DEFAULT now(),
      value text
    )
    """.strip(),
    "ALTER TABLE fetch_state ADD COLUMN IF NOT EXISTS value text",
    """
    CREATE TABLE IF NOT EXISTS api_key (
      key_hash text PRIMARY KEY,
      label text NOT NULL,
      created_at timestamptz NOT NULL DEFAULT now(),
      revoked_at timestamptz
    )
    """.strip(),
    "CREATE INDEX IF NOT EXISTS schedule_item_lookup_idx ON schedule_item (provider_id, source_id, day, start_time)",
    "CREATE INDEX IF NOT EXISTS schedule_item_details_ref_idx ON schedule_item (provider_id, details_ref) WHERE details_ref IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS provider_page_day_idx ON provider_page (provider_id, day)",
    """
    CREATE INDEX IF NOT EXISTS schedule_item_title_trgm_idx
      ON schedule_item USING gin (programista_unaccent(lower(title)) gin_trgm_ops)
    """.strip(),
    """
    CREATE INDEX IF NOT EXISTS schedule_item_subtitle_trgm_idx
      ON schedule_item USING gin (programista_unaccent(lower(subtitle)) gin_trgm_ops)
    """.strip(),
    """
    CREATE INDEX IF NOT EXISTS schedule_item_summary_trgm_idx
      ON schedule_item USING gin (programista_unaccent(lower(details_summary)) gin_trgm_ops)
    """.strip(),
    """
    CREATE INDEX IF NOT EXISTS item_details_text_trgm_idx
      ON item_details USING gin (programista_unaccent(lower(details_text)) gin_trgm_ops)
    """.strip(),
]


def connect() -> psycopg.Connection:
    return psycopg.connect(DB_DSN, row_factory=dict_row)


def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        for stmt in _SCHEMA_STATEMENTS:
            cur.execute(stmt)
    conn.commit()
