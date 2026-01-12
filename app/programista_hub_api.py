from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets

from datetime import UTC, datetime

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from programista_hub_auth import hash_api_key, is_api_key_valid
from programista_hub_db import connect, ensure_schema

app = FastAPI(title="Programista Hub")

_REQUIRE_API_KEY = os.environ.get("PROGRAMISTA_HUB_REQUIRE_API_KEY", "0").strip() == "1"
_API_KEY_HEADER = os.environ.get("PROGRAMISTA_HUB_API_KEY_HEADER", "X-Programista-Key")
_GITHUB_WEBHOOK_SECRET = os.environ.get("PROGRAMISTA_HUB_GITHUB_WEBHOOK_SECRET", "")
_PROVIDERS_REPO_FULL_NAME = os.environ.get(
    "PROGRAMISTA_HUB_PROVIDERS_REPO_FULL_NAME",
    "michaldziwisz/programista-providers",
).strip()


@app.on_event("startup")
def _startup() -> None:
    with connect() as conn:
        ensure_schema(conn)


def _db_check() -> bool:
    with connect() as conn:
        row = conn.execute("SELECT 1 AS ok").fetchone()
        return bool(row["ok"]) if row else False


@app.get("/health")
def health() -> dict:
    now = datetime.now(UTC).isoformat()
    try:
        db_ok = _db_check()
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "db_ok": False, "time": now, "error": str(e)}

    return {"ok": True, "db_ok": db_ok, "time": now}


@app.middleware("http")
async def _auth(request: Request, call_next):  # type: ignore[no-untyped-def]
    if not _REQUIRE_API_KEY:
        return await call_next(request)

    if request.url.path in {"/health", "/register", "/webhook/providers"}:
        return await call_next(request)

    api_key = request.headers.get(_API_KEY_HEADER)
    if not api_key:
        return JSONResponse(status_code=401, content={"detail": "Missing API key"})

    try:
        with connect() as conn:
            if not is_api_key_valid(conn, api_key):
                return JSONResponse(status_code=401, content={"detail": "Invalid API key"})
    except Exception as e:  # noqa: BLE001
        return JSONResponse(status_code=503, content={"detail": "Auth unavailable", "error": str(e)})

    return await call_next(request)


def _set_fetch_state(conn, key: str, value: str | None) -> None:
    conn.execute(
        """
        INSERT INTO fetch_state (key, updated_at, value)
        VALUES (%s, now(), %s)
        ON CONFLICT (key) DO UPDATE
          SET updated_at = excluded.updated_at,
              value = excluded.value
        """,
        (key, value),
    )


def _verify_github_hmac(body: bytes, signature_header: str, *, secret: str) -> bool:
    if not signature_header or not signature_header.startswith("sha256="):
        return False
    expected = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    provided = signature_header.split("=", 1)[-1].strip()
    return secrets.compare_digest(expected, provided)


@app.post("/webhook/providers")
async def webhook_providers(request: Request) -> dict:
    if not _GITHUB_WEBHOOK_SECRET:
        return JSONResponse(status_code=503, content={"detail": "Webhook secret not configured"})

    body = await request.body()
    sig = request.headers.get("X-Hub-Signature-256") or ""
    if not _verify_github_hmac(body, sig, secret=_GITHUB_WEBHOOK_SECRET):
        return JSONResponse(status_code=401, content={"detail": "Invalid webhook signature"})

    event = (request.headers.get("X-GitHub-Event") or "").strip().lower()
    if event == "ping":
        return {"ok": True, "event": "ping"}

    try:
        payload = json.loads(body.decode("utf-8", errors="replace"))
    except json.JSONDecodeError:
        return JSONResponse(status_code=400, content={"detail": "Invalid JSON"})

    if event == "release":
        action = str(payload.get("action") or "").strip().lower()
        repo_full_name = str(payload.get("repository", {}).get("full_name") or "").strip()
        if repo_full_name != _PROVIDERS_REPO_FULL_NAME:
            return {"ok": True, "ignored": True, "reason": "repo_mismatch"}

        if action != "published":
            return {"ok": True, "ignored": True, "reason": "action_mismatch", "action": action}

        tag = str(payload.get("release", {}).get("tag_name") or "").strip() or "published"
        with connect() as conn:
            ensure_schema(conn)
            _set_fetch_state(conn, "providers:update_requested", tag)
            conn.commit()
        return {"ok": True, "queued": True, "tag": tag}

    return {"ok": True, "ignored": True, "event": event}


class RegisterRequest(BaseModel):
    install_id: str = Field(min_length=8, max_length=100)
    label: str | None = Field(default=None, max_length=200)
    app_version: str | None = Field(default=None, max_length=50)
    platform: str | None = Field(default=None, max_length=80)


@app.post("/register")
def register(req: RegisterRequest) -> dict:
    install_id = (req.install_id or "").strip()
    if not install_id:
        return JSONResponse(status_code=400, content={"detail": "Missing install_id"})

    label_parts: list[str] = ["programista", install_id]
    if req.app_version:
        label_parts.append(str(req.app_version).strip())
    if req.platform:
        label_parts.append(str(req.platform).strip())
    label = " ".join([p for p in label_parts if p])[:200]
    if req.label and req.label.strip():
        label = (req.label.strip() + " | " + label)[:200]

    api_key = secrets.token_urlsafe(32)
    key_hash = hash_api_key(api_key)

    with connect() as conn:
        ensure_schema(conn)
        conn.execute(
            """
            INSERT INTO api_key (key_hash, label, created_at, revoked_at)
            VALUES (%s, %s, now(), NULL)
            ON CONFLICT (key_hash) DO NOTHING
            """,
            (key_hash, label),
        )
        conn.commit()

    return {"api_key": api_key, "header": _API_KEY_HEADER}


@app.get("/providers")
def list_providers() -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT id, kind, display_name, updated_at FROM provider ORDER BY kind, display_name"
        ).fetchall()
    return rows


@app.get("/sources")
def list_sources(
    *,
    kind: str | None = Query(default=None),
    provider_id: str | None = Query(default=None),
) -> list[dict]:
    sql = (
        "SELECT s.provider_id, p.kind, p.display_name AS provider_name, s.id, s.name "
        "FROM source s JOIN provider p ON p.id = s.provider_id"
    )
    where = []
    params: list[object] = []
    if kind:
        where.append("p.kind = %s")
        params.append(kind)
    if provider_id:
        where.append("s.provider_id = %s")
        params.append(provider_id)

    if where:
        sql += " WHERE " + " AND ".join(where)

    sql += " ORDER BY p.kind, p.display_name, s.name"

    with connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return rows


class SearchRequest(BaseModel):
    query: str = Field(min_length=1, max_length=200)
    kinds: list[str] | None = None
    limit: int = Field(default=50, ge=1, le=200)


@app.post("/search")
def search(req: SearchRequest) -> list[dict]:
    q = req.query.strip()
    if not q:
        return []

    sql = (
        "SELECT "
        "  p.kind, "
        "  si.provider_id, "
        "  p.display_name AS provider_name, "
        "  si.source_id, "
        "  s.name AS source_name, "
        "  si.day, "
        "  si.start_time, "
        "  si.title, "
        "  si.subtitle, "
        "  si.details_ref, "
        "  si.details_summary, "
        "  COALESCE(si.accessibility, '[]'::jsonb) AS accessibility "
        "FROM schedule_item si "
        "JOIN provider p ON p.id = si.provider_id "
        "JOIN source s ON s.provider_id = si.provider_id AND s.id = si.source_id "
        "LEFT JOIN item_details d ON d.provider_id = si.provider_id AND d.details_ref = si.details_ref "
        "WHERE "
        "  (programista_unaccent(lower(si.title)) LIKE programista_unaccent(lower(%s)) "
        "   OR (si.subtitle IS NOT NULL AND programista_unaccent(lower(si.subtitle)) LIKE programista_unaccent(lower(%s))) "
        "   OR (si.details_summary IS NOT NULL AND programista_unaccent(lower(si.details_summary)) LIKE programista_unaccent(lower(%s))) "
        "   OR (d.details_text IS NOT NULL AND programista_unaccent(lower(d.details_text)) LIKE programista_unaccent(lower(%s))))"
    )

    pattern = f"%{q}%"
    params: list[object] = [pattern, pattern, pattern, pattern]

    if req.kinds:
        sql += " AND p.kind = ANY(%s)"
        params.append(req.kinds)

    # Intentionally avoid ORDER BY here: with a large archive this would force a full scan+sort
    # for common queries. Clients can sort the limited result set locally if needed.
    sql += " LIMIT %s"
    params.append(req.limit)

    with connect() as conn:
        rows = conn.execute(sql, params).fetchall()

    return rows
