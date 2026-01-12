from __future__ import annotations

import argparse
import secrets
import sys

from programista_hub_auth import hash_api_key
from programista_hub_db import connect, ensure_schema


def _cmd_create_key(args: argparse.Namespace) -> int:
    label: str = args.label
    key = secrets.token_urlsafe(32)
    key_hash = hash_api_key(key)

    with connect() as conn:
        ensure_schema(conn)
        conn.execute(
            """
            INSERT INTO api_key (key_hash, label, created_at, revoked_at)
            VALUES (%s, %s, now(), NULL)
            ON CONFLICT (key_hash) DO UPDATE
              SET label = excluded.label,
                  revoked_at = NULL
            """,
            (key_hash, label),
        )
        conn.commit()

    sys.stdout.write(key + "\n")
    return 0


def _cmd_revoke_key(args: argparse.Namespace) -> int:
    key_or_hash: str = args.key_or_hash.strip()
    if not key_or_hash:
        raise SystemExit("Empty key")
    key_hash = key_or_hash if len(key_or_hash) == 64 else hash_api_key(key_or_hash)

    with connect() as conn:
        ensure_schema(conn)
        conn.execute("UPDATE api_key SET revoked_at = now() WHERE key_hash=%s", (key_hash,))
        conn.commit()

    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="programista-hub-admin")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_create = sub.add_parser("create-key", help="Create a new API key")
    p_create.add_argument("label")
    p_create.set_defaults(func=_cmd_create_key)

    p_revoke = sub.add_parser("revoke-key", help="Revoke an existing API key (key or sha256 hash)")
    p_revoke.add_argument("key_or_hash")
    p_revoke.set_defaults(func=_cmd_revoke_key)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())

