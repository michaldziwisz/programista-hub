from __future__ import annotations

import logging
import os
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from psycopg.types.json import Jsonb

from programista_hub_db import connect, ensure_schema
from programista_hub_fandom_archive import (
    ARCHIVE_PARSER_VERSION,
    FANDOM_PROVIDER_ID,
    FandomBlockedError,
    ensure_archive_provider,
    ingest_pending_fandom_pages,
    scan_fandom_allpages,
)
from programista_hub_radio import (
    ensure_radio_providers,
    refresh_nowyswiat_day,
    refresh_polskieradio_day,
    refresh_radio357_week,
    refresh_radioolsztyn_day,
    refresh_radioolsztyn_index,
    refresh_radiokierowcow_day,
    refresh_radiopoznan_day,
    refresh_radiowroclaw_weekdays,
    refresh_tokfm_week,
    upsert_radiowroclaw_days,
    upsert_tokfm_days,
)
from programista_hub_teleman import ensure_provider as ensure_teleman_provider
from programista_hub_teleman import fetch_teleman_details_text as teleman_fetch_details_text
from programista_hub_teleman import refresh_schedule as teleman_refresh_schedule
from programista_hub_teleman import refresh_sources as teleman_refresh_sources
from programista_hub_tv_accessibility import (
    ensure_tv_accessibility_providers,
    purge_tv_accessibility,
    refresh_polsat_accessibility_day,
    refresh_puls_accessibility,
    refresh_tvp_accessibility_day,
)

from tvguide_app.core.cache import SqliteCache
from tvguide_app.core.http import HttpClient
from tvguide_app.core.models import ProviderId, ScheduleItem, Source, SourceId
from tvguide_app.core.provider_packs.loader import PackLoader, PackStore
from tvguide_app.core.provider_packs.schema import PackFormatError, ProviderKind
from tvguide_app.core.provider_packs.updater import ProviderPackUpdater
from tvguide_app.core.providers.archive_base import ArchiveProvider
from tvguide_app.core.providers.base import ScheduleProvider

log = logging.getLogger("programista_hub_worker")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    raw = raw.strip().lower()
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return default


TELEMAN_DAYS = min(_env_int("PROGRAMISTA_HUB_TELEMAN_DAYS", 14), 14)
TELEMAN_MAX_TASKS_PER_CYCLE = _env_int("PROGRAMISTA_HUB_TELEMAN_MAX_TASKS", 25)
TELEMAN_SLEEP_SECONDS = _env_int("PROGRAMISTA_HUB_TELEMAN_SLEEP_SECONDS", 60)
TELEMAN_REQUEST_DELAY_SECONDS = _env_float("PROGRAMISTA_HUB_TELEMAN_REQUEST_DELAY_SECONDS", 0.15)
TELEMAN_SOURCES_TTL_SECONDS = _env_int("PROGRAMISTA_HUB_TELEMAN_SOURCES_TTL_SECONDS", 7 * 24 * 3600)
TELEMAN_DETAILS_MAX_PER_CYCLE = _env_int("PROGRAMISTA_HUB_TELEMAN_DETAILS_MAX", 10)
TELEMAN_DETAILS_REQUEST_DELAY_SECONDS = _env_float(
    "PROGRAMISTA_HUB_TELEMAN_DETAILS_REQUEST_DELAY_SECONDS", 0.2
)

RADIO_MAX_TASKS_PER_CYCLE = _env_int("PROGRAMISTA_HUB_RADIO_MAX_TASKS", 10)
RADIO_REQUEST_DELAY_SECONDS = _env_float("PROGRAMISTA_HUB_RADIO_REQUEST_DELAY_SECONDS", 0.15)

ARCHIVE_ALLPAGES_BATCH_SIZE = _env_int("PROGRAMISTA_HUB_ARCHIVE_ALLPAGES_BATCH_SIZE", 200)
ARCHIVE_INGEST_PAGES_PER_CYCLE = _env_int("PROGRAMISTA_HUB_ARCHIVE_INGEST_PAGES", 1)
ARCHIVE_REQUEST_DELAY_SECONDS = max(_env_float("PROGRAMISTA_HUB_ARCHIVE_REQUEST_DELAY_SECONDS", 0.2), 0.2)

TV_ACCESS_DAYS = _env_int("PROGRAMISTA_HUB_TV_ACCESS_DAYS", 14)
TV_ACCESS_MAX_TASKS_PER_CYCLE = _env_int("PROGRAMISTA_HUB_TV_ACCESS_MAX_TASKS", 5)
TV_ACCESS_REQUEST_DELAY_SECONDS = _env_float("PROGRAMISTA_HUB_TV_ACCESS_REQUEST_DELAY_SECONDS", 0.2)

TV_PACK_DAYS = min(_env_int("PROGRAMISTA_HUB_TV_PACK_DAYS", 14), 14)
TV_PACK_MAX_TASKS_PER_CYCLE = _env_int("PROGRAMISTA_HUB_TV_PACK_MAX_TASKS", 5)
TV_PACK_REQUEST_DELAY_SECONDS = _env_float("PROGRAMISTA_HUB_TV_PACK_REQUEST_DELAY_SECONDS", 0.2)

PROVIDERS_AUTO_UPDATE = _env_bool("PROGRAMISTA_HUB_PROVIDERS_AUTO_UPDATE", True)
PROVIDERS_CHECK_TTL_SECONDS = _env_int("PROGRAMISTA_HUB_PROVIDERS_CHECK_TTL_SECONDS", 6 * 3600)
PROVIDERS_BASE_URL = os.environ.get(
    "PROGRAMISTA_HUB_PROVIDERS_BASE_URL",
    "https://github.com/michaldziwisz/programista-providers/releases/latest/download/",
).strip()
PROVIDERS_DIR = Path(
    os.environ.get(
        "PROGRAMISTA_HUB_PROVIDERS_DIR",
        str(Path(__file__).resolve().parent / ".providers"),
    )
).expanduser()
PROVIDERS_HTTP_CACHE_PATH = Path(
    os.environ.get(
        "PROGRAMISTA_HUB_PROVIDERS_HTTP_CACHE_PATH",
        str(Path(__file__).resolve().parent / ".cache" / "providers-http.sqlite3"),
    )
).expanduser()
PROVIDERS_APP_VERSION = os.environ.get("PROGRAMISTA_HUB_APP_VERSION", "0.1.0").strip()
PROVIDERS_USER_AGENT = os.environ.get(
    "PROGRAMISTA_HUB_PROVIDERS_USER_AGENT",
    "programista-hub/0.1 (+https://tyflo.eu.org/programista/)",
).strip()


def _upsert_fetch_state(conn, key: str, value: str | None) -> None:
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


def _get_fetch_state_value(conn, key: str) -> str | None:
    row = conn.execute("SELECT value FROM fetch_state WHERE key=%s", (key,)).fetchone()
    if not row:
        return None
    val = row["value"]
    if val is None:
        return None
    return str(val)


def _maybe_update_provider_packs(conn, updater: ProviderPackUpdater) -> bool:
    if not PROVIDERS_AUTO_UPDATE:
        return False

    requested = _get_fetch_state_value(conn, "providers:update_requested")
    should_check = bool(requested) or _is_key_stale(conn, "providers:last_check", PROVIDERS_CHECK_TTL_SECONDS)
    if not should_check:
        return False

    try:
        result = updater.update_if_needed(force_check=True)
        _upsert_fetch_state(conn, "providers:last_check", "1")
        _upsert_fetch_state(conn, "providers:last_result", result.message)
        if requested:
            _upsert_fetch_state(conn, "providers:update_requested", None)
        conn.commit()
        updated = bool(result.updated)
        if updated:
            # If providers were updated, invalidate per-day schedule freshness markers so that
            # schedules that previously failed (or were parsed differently) are refreshed quickly.
            with conn.transaction():
                conn.execute("DELETE FROM fetch_state WHERE key LIKE %s", ("providers:schedule:%",))
            conn.commit()
        return updated
    except Exception as e:  # noqa: BLE001
        log.warning("Providers update failed: %s", e)
        with conn.transaction():
            _upsert_fetch_state(conn, "providers:last_check", "1")
            _upsert_fetch_state(conn, "providers:last_error", str(e))
        conn.commit()
        return False


def _load_pack_schedule_providers(
    loader: PackLoader,
    http: HttpClient,
    *,
    kind: ProviderKind,
) -> list[ScheduleProvider]:
    try:
        loaded = loader.load_kind(kind, http)
    except PackFormatError as e:
        log.warning("Provider pack load failed (%s): %s", kind, e)
        return []
    except Exception as e:  # noqa: BLE001
        log.warning("Provider pack load error (%s): %s", kind, e)
        return []
    if not loaded:
        return []
    providers = loaded.providers
    if not isinstance(providers, list):
        return []
    return [p for p in providers if isinstance(p, ScheduleProvider)]


def _load_pack_archive_providers(
    loader: PackLoader,
    http: HttpClient,
    *,
    kind: ProviderKind,
) -> list[ArchiveProvider]:
    try:
        loaded = loader.load_kind(kind, http)
    except PackFormatError as e:
        log.warning("Provider pack load failed (%s): %s", kind, e)
        return []
    except Exception as e:  # noqa: BLE001
        log.warning("Provider pack load error (%s): %s", kind, e)
        return []
    if not loaded:
        return []
    providers = loaded.providers
    if not isinstance(providers, list):
        return []
    return [p for p in providers if isinstance(p, ArchiveProvider)]


def _archive_blocked_remaining_seconds(conn) -> int:
    raw = _get_fetch_state_value(conn, "fandom:blocked_until")
    if not raw:
        return 0
    try:
        blocked_until = float(raw)
    except ValueError:
        return 0
    now = time.time()
    if now >= blocked_until:
        return 0
    return int(blocked_until - now)


def _set_archive_blocked(conn, *, seconds: int) -> None:
    seconds = max(60, int(seconds))
    blocked_until = int(time.time() + seconds)
    with conn.transaction():
        _upsert_fetch_state(conn, "fandom:blocked_until", str(blocked_until))
    conn.commit()


def _archive_request_delay_seconds(conn) -> float:
    raw = _get_fetch_state_value(conn, "fandom:request_delay_seconds")
    if raw:
        try:
            return max(float(raw), ARCHIVE_REQUEST_DELAY_SECONDS)
        except ValueError:
            return ARCHIVE_REQUEST_DELAY_SECONDS
    return ARCHIVE_REQUEST_DELAY_SECONDS


def _set_archive_request_delay_seconds(conn, delay_seconds: float) -> None:
    delay_seconds = max(ARCHIVE_REQUEST_DELAY_SECONDS, float(delay_seconds))
    delay_seconds = min(delay_seconds, 30.0)
    with conn.transaction():
        _upsert_fetch_state(conn, "fandom:request_delay_seconds", f"{delay_seconds:.3f}")
    conn.commit()


def _schedule_ttl_seconds(day_offset: int) -> int:
    if day_offset <= 0:
        return 60 * 60
    if day_offset == 1:
        return 2 * 60 * 60
    return 12 * 60 * 60


def _upsert_provider_row(conn, *, provider_id: str, kind: str, display_name: str) -> None:
    conn.execute(
        """
        INSERT INTO provider (id, kind, display_name, updated_at)
        VALUES (%s, %s, %s, now())
        ON CONFLICT (id) DO UPDATE
          SET kind = excluded.kind,
              display_name = excluded.display_name,
              updated_at = excluded.updated_at
        """,
        (provider_id, kind, display_name),
    )


def _upsert_source_row(conn, *, provider_id: str, source_id: str, name: str) -> None:
    conn.execute(
        """
        INSERT INTO source (provider_id, id, name, updated_at)
        VALUES (%s, %s, %s, now())
        ON CONFLICT (provider_id, id) DO UPDATE
          SET name = excluded.name,
              updated_at = excluded.updated_at
        """,
        (provider_id, source_id, name),
    )


def _normalize_provider_source_id(value: object) -> str:
    # provider packs may use NewType wrappers, but at runtime they are plain strings.
    if value is None:
        return ""
    return str(value).strip()


def _get_sources_from_db(conn, *, provider_id: str) -> list[Source]:
    rows = conn.execute(
        "SELECT id, name FROM source WHERE provider_id=%s ORDER BY name, id",
        (provider_id,),
    ).fetchall()
    sources: list[Source] = []
    for r in rows:
        sid = _normalize_provider_source_id(r["id"])
        name = str(r["name"] or "").strip()
        if not sid or not name:
            continue
        sources.append(Source(provider_id=ProviderId(provider_id), id=SourceId(sid), name=name))
    return sources


def _ingest_schedule_for_source_day(
    conn,
    *,
    kind: str,
    provider_id: str,
    source_id: str,
    day: date,
    items: list[ScheduleItem],
    keep_only_accessible: bool = False,
) -> int:
    inserted = 0

    with conn.transaction():
        conn.execute(
            "DELETE FROM schedule_item WHERE provider_id=%s AND source_id=%s AND day=%s",
            (provider_id, source_id, day),
        )

        for it in items:
            if it.start_time is None:
                continue
            if keep_only_accessible and not it.accessibility:
                continue

            acc = Jsonb(list(it.accessibility)) if it.accessibility else None
            conn.execute(
                """
                INSERT INTO schedule_item (
                  provider_id, source_id, day, start_time,
                  title, subtitle, details_ref, details_summary, accessibility
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (provider_id, source_id, day, start_time, title) DO UPDATE
                  SET subtitle = excluded.subtitle,
                      details_ref = excluded.details_ref,
                      details_summary = excluded.details_summary,
                      accessibility = excluded.accessibility
                """,
                (
                    provider_id,
                    source_id,
                    day,
                    it.start_time,
                    it.title,
                    it.subtitle,
                    it.details_ref,
                    it.details_summary,
                    acc,
                ),
            )
            inserted += 1

        _upsert_fetch_state(
            conn,
            f"providers:schedule:{kind}:{provider_id}:{source_id}:{day.isoformat()}",
            str(inserted),
        )

    conn.commit()
    return inserted


def _is_key_stale(conn, key: str, ttl_seconds: int) -> bool:
    row = conn.execute("SELECT updated_at, value FROM fetch_state WHERE key=%s", (key,)).fetchone()
    if not row:
        return True
    updated_at: datetime = row["updated_at"]
    value = row.get("value")
    effective_ttl_seconds = ttl_seconds
    if key.startswith("providers:schedule:") and isinstance(value, str):
        try:
            inserted = int(value.strip() or "0")
        except ValueError:
            inserted = 1
        if inserted <= 0:
            effective_ttl_seconds = min(ttl_seconds, 10 * 60)
    age = datetime.now(UTC) - updated_at
    return age.total_seconds() >= effective_ttl_seconds


def _get_teleman_sources(conn) -> list[str]:
    rows = conn.execute(
        "SELECT id FROM source WHERE provider_id=%s ORDER BY id", ("teleman",)
    ).fetchall()
    return [r["id"] for r in rows]


def _purge_teleman(conn, *, min_day: date, max_day: date) -> None:
    conn.execute(
        "DELETE FROM schedule_item WHERE provider_id=%s AND (day < %s OR day > %s)",
        ("teleman", min_day, max_day),
    )
    conn.commit()


def _backfill_teleman_details(conn) -> None:
    rows = conn.execute(
        """
        SELECT
          si.details_ref AS details_ref,
          max(si.details_summary) AS summary
        FROM schedule_item si
        LEFT JOIN item_details d
          ON d.provider_id = si.provider_id
         AND d.details_ref = si.details_ref
        WHERE si.provider_id = %s
          AND si.details_ref IS NOT NULL
          AND d.details_ref IS NULL
        GROUP BY si.details_ref
        ORDER BY max(si.day) DESC
        LIMIT %s
        """,
        ("teleman", TELEMAN_DETAILS_MAX_PER_CYCLE),
    ).fetchall()

    for row in rows:
        details_ref = row["details_ref"]
        summary = row["summary"]
        if not isinstance(details_ref, str) or not details_ref.strip():
            continue
        details_ref = details_ref.strip()

        try:
            details_text = teleman_fetch_details_text(details_ref)
        except Exception as e:  # noqa: BLE001
            log.info("Teleman details fetch failed (%s): %s", details_ref, e)
            continue

        details_text = (details_text or "").strip() or (str(summary).strip() if summary else "")
        if not details_text:
            details_text = details_ref

        with conn.transaction():
            conn.execute(
                """
                INSERT INTO item_details (provider_id, details_ref, details_text, fetched_at)
                VALUES (%s, %s, %s, now())
                ON CONFLICT (provider_id, details_ref) DO UPDATE
                  SET details_text = excluded.details_text,
                      fetched_at = excluded.fetched_at
                """,
                ("teleman", details_ref, details_text),
            )
        conn.commit()

        if TELEMAN_DETAILS_REQUEST_DELAY_SECONDS > 0:
            time.sleep(TELEMAN_DETAILS_REQUEST_DELAY_SECONDS)


def _refresh_radio_from_packs(conn, providers: list[ScheduleProvider]) -> None:
    today = date.today()
    max_day = today + timedelta(days=13)

    tasks_done = 0

    def can_spend(cost: int = 1) -> bool:
        return tasks_done + cost <= RADIO_MAX_TASKS_PER_CYCLE

    for provider in providers:
        if not can_spend():
            break

        provider_id = (provider.provider_id or "").strip()
        if not provider_id:
            continue

        _upsert_provider_row(
            conn,
            provider_id=provider_id,
            kind="radio",
            display_name=(provider.display_name or provider_id).strip(),
        )
        conn.commit()

        sources_key = f"providers:sources:radio:{provider_id}"
        sources: list[Source] = []

        if _is_key_stale(conn, sources_key, 7 * 24 * 3600):
            try:
                sources = provider.list_sources(force_refresh=False)
            except Exception as e:  # noqa: BLE001
                log.info("Radio sources failed (%s): %s", provider_id, e)
                sources = []

            if sources:
                with conn.transaction():
                    for src in sources:
                        sid = _normalize_provider_source_id(src.id)
                        name = str(src.name or "").strip()
                        if sid and name:
                            _upsert_source_row(conn, provider_id=provider_id, source_id=sid, name=name)
                    _upsert_fetch_state(conn, sources_key, "1")
                conn.commit()

        if not sources:
            sources = _get_sources_from_db(conn, provider_id=provider_id)
        if not sources:
            continue

        try:
            days = provider.list_days(force_refresh=False)
        except Exception:  # noqa: BLE001
            days = [today]

        days = sorted({d for d in days if isinstance(d, date) and today <= d <= max_day})

        # Polskie Radio: the current endpoint appears to return only "today" (ignores selectedDate).
        if provider_id == "polskieradio":
            days = [today]

        for d in days:
            if not can_spend():
                break
            day_offset = (d - today).days
            ttl = _schedule_ttl_seconds(day_offset)

            for src in sources:
                if not can_spend():
                    break
                sid = _normalize_provider_source_id(src.id)
                if not sid:
                    continue

                key = f"providers:schedule:radio:{provider_id}:{sid}:{d.isoformat()}"
                if not _is_key_stale(conn, key, ttl):
                    continue

                try:
                    items = provider.get_schedule(src, d, force_refresh=False)
                except Exception as e:  # noqa: BLE001
                    log.info("Radio schedule failed (%s %s %s): %s", provider_id, sid, d.isoformat(), e)
                    continue

                inserted = _ingest_schedule_for_source_day(
                    conn,
                    kind="radio",
                    provider_id=provider_id,
                    source_id=sid,
                    day=d,
                    items=items,
                )
                log.info("Radio: %s %s %s items=%s", provider_id, sid, d.isoformat(), inserted)
                tasks_done += 1

                if RADIO_REQUEST_DELAY_SECONDS > 0:
                    time.sleep(RADIO_REQUEST_DELAY_SECONDS)

    # Keep only today's and future schedules for radio providers.
    conn.execute(
        """
        DELETE FROM schedule_item si
        USING provider p
        WHERE si.provider_id = p.id
          AND p.kind = %s
          AND si.day < %s
        """,
        ("radio", today),
    )
    conn.commit()


def _refresh_tv_accessibility_from_packs(conn, providers: list[ScheduleProvider]) -> None:
    today = date.today()
    min_day = today - timedelta(days=1)
    max_day = today + timedelta(days=max(0, TV_ACCESS_DAYS - 1))

    tasks_done = 0

    def can_spend(cost: int = 1) -> bool:
        return tasks_done + cost <= TV_ACCESS_MAX_TASKS_PER_CYCLE

    for provider in providers:
        if not can_spend():
            break

        provider_id = (provider.provider_id or "").strip()
        if not provider_id:
            continue

        _upsert_provider_row(
            conn,
            provider_id=provider_id,
            kind="tv_accessibility",
            display_name=(provider.display_name or provider_id).strip(),
        )
        conn.commit()

        sources_key = f"providers:sources:tv_accessibility:{provider_id}"
        sources: list[Source] = []

        if _is_key_stale(conn, sources_key, 7 * 24 * 3600):
            try:
                sources = provider.list_sources(force_refresh=False)
            except Exception as e:  # noqa: BLE001
                log.info("TV accessibility sources failed (%s): %s", provider_id, e)
                sources = []

            if sources:
                with conn.transaction():
                    for src in sources:
                        sid = _normalize_provider_source_id(src.id)
                        name = str(src.name or "").strip()
                        if sid and name:
                            _upsert_source_row(conn, provider_id=provider_id, source_id=sid, name=name)
                    _upsert_fetch_state(conn, sources_key, "1")
                conn.commit()

        if not sources:
            sources = _get_sources_from_db(conn, provider_id=provider_id)
        if not sources:
            continue

        try:
            days = provider.list_days(force_refresh=False)
        except Exception:  # noqa: BLE001
            days = []

        days = sorted({d for d in days if isinstance(d, date) and min_day <= d <= max_day})

        for d in days:
            if not can_spend():
                break
            day_offset = (d - today).days
            ttl = _schedule_ttl_seconds(day_offset)

            day_key = f"providers:schedule:tv_accessibility:{provider_id}:{d.isoformat()}"
            if not _is_key_stale(conn, day_key, ttl):
                continue

            inserted_total = 0
            with conn.transaction():
                for src in sources:
                    sid = _normalize_provider_source_id(src.id)
                    name = str(src.name or "").strip()
                    if sid and name:
                        _upsert_source_row(conn, provider_id=provider_id, source_id=sid, name=name)

                    try:
                        items = provider.get_schedule(src, d, force_refresh=False)
                    except Exception as e:  # noqa: BLE001
                        log.info(
                            "TV accessibility schedule failed (%s %s %s): %s",
                            provider_id,
                            sid,
                            d.isoformat(),
                            e,
                        )
                        continue

                    conn.execute(
                        "DELETE FROM schedule_item WHERE provider_id=%s AND source_id=%s AND day=%s",
                        (provider_id, sid, d),
                    )

                    for it in items:
                        if it.start_time is None or not it.accessibility:
                            continue
                        conn.execute(
                            """
                            INSERT INTO schedule_item (
                              provider_id, source_id, day, start_time,
                              title, subtitle, details_ref, details_summary, accessibility
                            )
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (provider_id, source_id, day, start_time, title) DO UPDATE
                              SET subtitle = excluded.subtitle,
                                  details_ref = excluded.details_ref,
                                  details_summary = excluded.details_summary,
                                  accessibility = excluded.accessibility
                            """,
                            (
                                provider_id,
                                sid,
                                d,
                                it.start_time,
                                it.title,
                                it.subtitle,
                                it.details_ref,
                                it.details_summary,
                                Jsonb(list(it.accessibility)),
                            ),
                        )
                        inserted_total += 1

                _upsert_fetch_state(conn, day_key, "1")

            conn.commit()
            log.info("TV accessibility: %s %s items=%s", provider_id, d.isoformat(), inserted_total)
            tasks_done += 1

            if TV_ACCESS_REQUEST_DELAY_SECONDS > 0:
                time.sleep(TV_ACCESS_REQUEST_DELAY_SECONDS)

    # Keep only the configured window for all tv_accessibility providers.
    conn.execute(
        """
        DELETE FROM schedule_item si
        USING provider p
        WHERE si.provider_id = p.id
          AND p.kind = %s
          AND (si.day < %s OR si.day > %s)
        """,
        ("tv_accessibility", min_day, max_day),
    )
    conn.commit()


def _refresh_tv_from_packs(conn, providers: list[ScheduleProvider]) -> None:
    today = date.today()
    min_day = today - timedelta(days=1)
    max_day = today + timedelta(days=max(0, TV_PACK_DAYS - 1))

    tasks_done = 0

    def can_spend(cost: int = 1) -> bool:
        return tasks_done + cost <= TV_PACK_MAX_TASKS_PER_CYCLE

    for provider in providers:
        if not can_spend():
            break

        provider_id = (provider.provider_id or "").strip()
        if not provider_id:
            continue
        # Teleman is ingested by the legacy path (details scraping etc.); packs provide only extras.
        if provider_id == "teleman":
            continue

        _upsert_provider_row(
            conn,
            provider_id=provider_id,
            kind="tv",
            display_name=(provider.display_name or provider_id).strip(),
        )
        conn.commit()

        sources_key = f"providers:sources:tv:{provider_id}"
        sources: list[Source] = []

        if _is_key_stale(conn, sources_key, 7 * 24 * 3600):
            try:
                sources = provider.list_sources(force_refresh=False)
            except Exception as e:  # noqa: BLE001
                log.info("TV sources failed (%s): %s", provider_id, e)
                sources = []

            if sources:
                with conn.transaction():
                    for src in sources:
                        sid = _normalize_provider_source_id(src.id)
                        name = str(src.name or "").strip()
                        if sid and name:
                            _upsert_source_row(conn, provider_id=provider_id, source_id=sid, name=name)
                    _upsert_fetch_state(conn, sources_key, "1")
                conn.commit()

        if not sources:
            sources = _get_sources_from_db(conn, provider_id=provider_id)
        if not sources:
            continue

        try:
            days = provider.list_days(force_refresh=False)
        except Exception:  # noqa: BLE001
            days = []

        days = sorted({d for d in days if isinstance(d, date) and min_day <= d <= max_day})

        for d in days:
            if not can_spend():
                break
            day_offset = (d - today).days
            ttl = _schedule_ttl_seconds(day_offset)

            day_key = f"providers:schedule:tv:{provider_id}:{d.isoformat()}"
            if not _is_key_stale(conn, day_key, ttl):
                continue

            inserted_total = 0

            for idx, src in enumerate(sources):
                sid = _normalize_provider_source_id(src.id)
                name = str(src.name or "").strip()
                if not sid:
                    continue
                if sid and name:
                    _upsert_source_row(conn, provider_id=provider_id, source_id=sid, name=name)

                try:
                    # Many TVP-backed providers fetch a whole day in one request; force refresh only once.
                    force = idx == 0
                    items = provider.get_schedule(src, d, force_refresh=force)
                except Exception as e:  # noqa: BLE001
                    log.info("TV schedule failed (%s %s %s): %s", provider_id, sid, d.isoformat(), e)
                    continue

                inserted_total += _ingest_schedule_for_source_day(
                    conn,
                    kind="tv",
                    provider_id=provider_id,
                    source_id=sid,
                    day=d,
                    items=items,
                    keep_only_accessible=False,
                )

            with conn.transaction():
                _upsert_fetch_state(conn, day_key, "1")
            conn.commit()
            log.info("TV(packs): %s %s items=%s", provider_id, d.isoformat(), inserted_total)
            tasks_done += 1

            if TV_PACK_REQUEST_DELAY_SECONDS > 0:
                time.sleep(TV_PACK_REQUEST_DELAY_SECONDS)

    # Keep only the configured window for pack-based TV providers (Teleman is purged separately).
    conn.execute(
        """
        DELETE FROM schedule_item si
        USING provider p
        WHERE si.provider_id = p.id
          AND p.kind = %s
          AND p.id <> %s
          AND (si.day < %s OR si.day > %s)
        """,
        ("tv", "teleman", min_day, max_day),
    )
    conn.commit()


def _refresh_radio(conn) -> None:
    ensure_radio_providers(conn)

    today = date.today()
    tasks_done = 0

    def can_spend(cost: int = 1) -> bool:
        return tasks_done + cost <= RADIO_MAX_TASKS_PER_CYCLE

    # Polskie Radio: the current endpoint appears to return only \"today\" (ignores selectedDate).
    if can_spend():
        key = f"polskieradio:multischedule:{today.isoformat()}"
        if _is_key_stale(conn, key, 30 * 60):
            log.info("Polskie Radio: refresh %s", today.isoformat())
            refresh_polskieradio_day(conn, day=today, request_delay_seconds=RADIO_REQUEST_DELAY_SECONDS)
            tasks_done += 1

        # Keep only today's schedule to avoid showing stale/incorrect days.
        conn.execute("DELETE FROM schedule_item WHERE provider_id=%s AND day <> %s", ("polskieradio", today))
        conn.commit()

    # Radio Kierowców
    for day_offset in range(7):
        if not can_spend():
            break
        day = today + timedelta(days=day_offset)
        key = f"radiokierowcow:schedule:{day.isoformat()}"
        ttl = _schedule_ttl_seconds(day_offset)
        if not _is_key_stale(conn, key, ttl):
            continue
        log.info("Radio Kierowców: refresh %s", day.isoformat())
        refresh_radiokierowcow_day(conn, day=day, request_delay_seconds=RADIO_REQUEST_DELAY_SECONDS)
        tasks_done += 1

    # Radio Nowy Świat
    for day_offset in range(7):
        if not can_spend():
            break
        day = today + timedelta(days=day_offset)
        key = f"nowyswiat:ramowka:{day.isoformat()}"
        ttl = _schedule_ttl_seconds(day_offset)
        if not _is_key_stale(conn, key, ttl):
            continue
        log.info("Radio Nowy Świat: refresh %s", day.isoformat())
        refresh_nowyswiat_day(conn, day=day, request_delay_seconds=RADIO_REQUEST_DELAY_SECONDS)
        tasks_done += 1

    # Radio Poznań (only today)
    if can_spend():
        key = f"radiopoznan:program:{today.isoformat()}"
        if _is_key_stale(conn, key, 60 * 60):
            log.info("Radio Poznań: refresh %s", today.isoformat())
            refresh_radiopoznan_day(conn, day=today, request_delay_seconds=RADIO_REQUEST_DELAY_SECONDS)
            tasks_done += 1

    # Radio 357 (one fetch for a week-ish grid)
    if can_spend():
        if _is_key_stale(conn, "radio357:ramowka", 60 * 20):
            log.info("Radio 357: refresh")
            refresh_radio357_week(conn, request_delay_seconds=RADIO_REQUEST_DELAY_SECONDS)
            tasks_done += 1

    # Radio Olsztyn: fetch index, then a few days
    ro_days: list[date] = []
    if can_spend():
        if _is_key_stale(conn, "radioolsztyn:index", 60 * 30):
            log.info("Radio Olsztyn: refresh index")
            ro_days = refresh_radioolsztyn_index(conn, request_delay_seconds=RADIO_REQUEST_DELAY_SECONDS)
            tasks_done += 1

    # If we refreshed the index, pull a couple of days. (If we didn't, keep it light.)
    for d in sorted([x for x in ro_days if today <= x <= today + timedelta(days=14)])[:5]:
        if not can_spend():
            break
        key = f"radioolsztyn:ramowka:{d.isoformat()}"
        if not _is_key_stale(conn, key, 60 * 30):
            continue
        log.info("Radio Olsztyn: refresh %s", d.isoformat())
        refresh_radioolsztyn_day(conn, day=d, request_delay_seconds=RADIO_REQUEST_DELAY_SECONDS)
        tasks_done += 1

    # Radio Wrocław (7 weekday pages)
    if can_spend(7) and _is_key_stale(conn, "radiowroclaw:week", 6 * 3600):
        log.info("Radio Wrocław: refresh weekdays")
        by_weekday = refresh_radiowroclaw_weekdays(request_delay_seconds=RADIO_REQUEST_DELAY_SECONDS)
        days = [today + timedelta(days=i) for i in range(14)]
        upsert_radiowroclaw_days(conn, by_weekday=by_weekday, days=days)
        tasks_done += 7

    # TOK FM (one weekly grid page)
    if can_spend() and _is_key_stale(conn, "tokfm:ramowka", 6 * 3600):
        log.info("TOK FM: refresh")
        by_weekday = refresh_tokfm_week(conn, request_delay_seconds=RADIO_REQUEST_DELAY_SECONDS)
        days = [today + timedelta(days=i) for i in range(7)]
        upsert_tokfm_days(conn, by_weekday=by_weekday, days=days)
        tasks_done += 1


def _refresh_archive(conn) -> None:
    ensure_archive_provider(conn)

    remaining = _archive_blocked_remaining_seconds(conn)
    if remaining > 0:
        log.info("Fandom: blocked, skipping for %ss", remaining)
        return
    request_delay_seconds = _archive_request_delay_seconds(conn)

    # If parsing logic changes, re-ingest previously cached pages (revision id stays the same,
    # but the extracted schedule items should be regenerated).
    row = conn.execute("SELECT value FROM fetch_state WHERE key=%s", ("fandom:parser_version",)).fetchone()
    current_version_s = str(ARCHIVE_PARSER_VERSION)
    previous_version_s = str(row["value"]) if row and row["value"] is not None else None
    if previous_version_s != current_version_s:
        log.info("Fandom: parser version %s -> %s, scheduling re-ingest", previous_version_s, current_version_s)
        with conn.transaction():
            conn.execute(
                "UPDATE provider_page SET rev_id = NULL WHERE provider_id = %s",
                (FANDOM_PROVIDER_ID,),
            )
            conn.execute(
                """
                INSERT INTO fetch_state (key, updated_at, value)
                VALUES (%s, now(), %s)
                ON CONFLICT (key) DO UPDATE
                  SET updated_at = excluded.updated_at,
                      value = excluded.value
                """,
                ("fandom:parser_version", current_version_s),
            )
        conn.commit()

    row = conn.execute("SELECT value FROM fetch_state WHERE key=%s", ("fandom:allpages",)).fetchone()
    token = row["value"] if row else None
    scan_complete = row is not None and (token is None or str(token).strip() == "")

    if not scan_complete or _is_key_stale(conn, "fandom:allpages", 7 * 24 * 3600):
        try:
            inserted = scan_fandom_allpages(
                conn,
                batch_size=ARCHIVE_ALLPAGES_BATCH_SIZE,
                request_delay_seconds=request_delay_seconds,
            )
        except FandomBlockedError as e:
            backoff = e.retry_after_seconds or (60 * 60 if e.status_code == 403 else 15 * 60)
            _set_archive_blocked(conn, seconds=backoff)
            _set_archive_request_delay_seconds(conn, max(request_delay_seconds * 2, 1.0))
            log.warning("Fandom: scan blocked (HTTP %s), backoff=%ss", e.status_code, backoff)
            return
        except Exception as e:  # noqa: BLE001
            log.info("Fandom: scan failed: %s", e)
            return
        if inserted:
            log.info("Fandom: scanned %s day pages", inserted)

    try:
        processed = ingest_pending_fandom_pages(
            conn,
            max_pages=ARCHIVE_INGEST_PAGES_PER_CYCLE,
            request_delay_seconds=request_delay_seconds,
        )
    except FandomBlockedError as e:
        backoff = e.retry_after_seconds or (60 * 60 if e.status_code == 403 else 15 * 60)
        _set_archive_blocked(conn, seconds=backoff)
        _set_archive_request_delay_seconds(conn, max(request_delay_seconds * 2, 1.0))
        log.warning("Fandom: ingest blocked (HTTP %s), backoff=%ss", e.status_code, backoff)
        return
    except Exception as e:  # noqa: BLE001
        log.info("Fandom: ingest failed: %s", e)
        return
    if processed:
        log.info("Fandom: ingested %s pages", processed)


def _refresh_tv_accessibility(conn) -> None:
    ensure_tv_accessibility_providers(conn)

    today = date.today()
    min_day = today - timedelta(days=1)
    max_day = today + timedelta(days=max(0, TV_ACCESS_DAYS - 1))

    tasks_done = 0

    def can_spend(cost: int = 1) -> bool:
        return tasks_done + cost <= TV_ACCESS_MAX_TASKS_PER_CYCLE

    # TVP (one page per day includes all stations)
    for day_offset in range(max(0, TV_ACCESS_DAYS)):
        if not can_spend():
            break
        day = today + timedelta(days=day_offset)
        ttl = _schedule_ttl_seconds(day_offset)
        key = f"tvp:program:{day.isoformat()}"
        if not _is_key_stale(conn, key, ttl):
            continue
        log.info("TVP accessibility: refresh %s", day.isoformat())
        refresh_tvp_accessibility_day(conn, day=day, request_delay_seconds=TV_ACCESS_REQUEST_DELAY_SECONDS)
        tasks_done += 1

    # Polsat (rolling modules; 7 days)
    for day_offset in range(min(7, max(0, TV_ACCESS_DAYS))):
        if not can_spend():
            break
        day = today + timedelta(days=day_offset)
        ttl = _schedule_ttl_seconds(day_offset)
        key = f"polsat:module:day:{day.isoformat()}"
        if not _is_key_stale(conn, key, ttl):
            continue
        log.info("Polsat accessibility: refresh %s", day.isoformat())
        refresh_polsat_accessibility_day(conn, day=day, request_delay_seconds=TV_ACCESS_REQUEST_DELAY_SECONDS)
        tasks_done += 1

    # Puls EPG (index + xml files)
    if can_spend() and _is_key_stale(conn, "puls:epg:index", 6 * 3600):
        log.info("Puls accessibility: refresh")
        refresh_puls_accessibility(
            conn,
            request_delay_seconds=TV_ACCESS_REQUEST_DELAY_SECONDS,
            keep_min_day=min_day,
            keep_max_day=max_day,
        )
        tasks_done += 1

    purge_tv_accessibility(conn, min_day=min_day, max_day=max_day)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    providers_store = PackStore(PROVIDERS_DIR)
    providers_cache = SqliteCache(PROVIDERS_HTTP_CACHE_PATH)
    providers_http = HttpClient(providers_cache, user_agent=PROVIDERS_USER_AGENT)
    providers_updater = ProviderPackUpdater(providers_http, providers_store, base_url=PROVIDERS_BASE_URL)
    providers_loader = PackLoader(providers_store, app_version=PROVIDERS_APP_VERSION or "0.0.0")

    while True:
        try:
            with connect() as conn:
                ensure_schema(conn)
                ensure_teleman_provider(conn)

                if _is_key_stale(conn, "teleman:sources", TELEMAN_SOURCES_TTL_SECONDS):
                    log.info("Teleman: refreshing sources")
                    count = teleman_refresh_sources(conn, request_delay_seconds=TELEMAN_REQUEST_DELAY_SECONDS)
                    log.info("Teleman: sources refreshed (%s)", count)

                sources = _get_teleman_sources(conn)
                if not sources:
                    log.info("Teleman: no sources in DB yet")
                    time.sleep(TELEMAN_SLEEP_SECONDS)
                    continue

                today = date.today()
                min_day = today - timedelta(days=1)
                max_day = today + timedelta(days=max(0, TELEMAN_DAYS - 1))

                tasks_done = 0

                for day_offset in range(max(0, TELEMAN_DAYS)):
                    if tasks_done >= TELEMAN_MAX_TASKS_PER_CYCLE:
                        break

                    day = today + timedelta(days=day_offset)
                    ttl = _schedule_ttl_seconds(day_offset)

                    for source_id in sources:
                        if tasks_done >= TELEMAN_MAX_TASKS_PER_CYCLE:
                            break

                        key = f"teleman:schedule:{source_id}:{day.isoformat()}"
                        if not _is_key_stale(conn, key, ttl):
                            continue

                        log.info("Teleman: refresh %s %s", source_id, day.isoformat())
                        count = teleman_refresh_schedule(
                            conn,
                            source_id=source_id,
                            day=day,
                            request_delay_seconds=TELEMAN_REQUEST_DELAY_SECONDS,
                        )
                        log.info("Teleman: %s %s items=%s", source_id, day.isoformat(), count)
                        tasks_done += 1

                _purge_teleman(conn, min_day=min_day, max_day=max_day)
                _backfill_teleman_details(conn)

                _maybe_update_provider_packs(conn, providers_updater)

                tv_access_providers = _load_pack_schedule_providers(
                    providers_loader,
                    providers_http,
                    kind="tv_accessibility",
                )
                _refresh_tv_accessibility_from_packs(conn, tv_access_providers)

                tv_providers = _load_pack_schedule_providers(
                    providers_loader,
                    providers_http,
                    kind="tv",
                )
                _refresh_tv_from_packs(conn, tv_providers)

                radio_providers = _load_pack_schedule_providers(
                    providers_loader,
                    providers_http,
                    kind="radio",
                )
                _refresh_radio_from_packs(conn, radio_providers)
                _refresh_archive(conn)

        except Exception:
            log.exception("Worker loop error")

        time.sleep(TELEMAN_SLEEP_SECONDS)


if __name__ == "__main__":
    main()
