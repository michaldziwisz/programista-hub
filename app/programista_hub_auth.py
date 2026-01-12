from __future__ import annotations

import hashlib

import psycopg


def hash_api_key(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def is_api_key_valid(conn: psycopg.Connection, key: str) -> bool:
    key = (key or "").strip()
    if not key:
        return False
    key_hash = hash_api_key(key)
    row = conn.execute(
        "SELECT 1 AS ok FROM api_key WHERE key_hash=%s AND revoked_at IS NULL",
        (key_hash,),
    ).fetchone()
    return bool(row["ok"]) if row else False

