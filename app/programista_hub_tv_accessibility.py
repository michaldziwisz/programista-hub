from __future__ import annotations

import gzip
import html
import json
import os
import re
import time as time_mod
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET

import psycopg
import requests
from bs4 import BeautifulSoup
from psycopg.types.json import Jsonb


TV_ACCESS_KIND = "tv_accessibility"

TVP_PROVIDER_ID = "tvp"
POLSAT_PROVIDER_ID = "polsat"
PULS_PROVIDER_ID = "puls"

TVP_PROGRAM_URL = "https://www.tvp.pl/program-tv"
POLSAT_MODULE_URL = "https://www.polsat.pl/tv-html/module/page{page}/"
PULS_EPG_BASE_URL = "https://tyflo.eu.org/epg/puls/"

_USER_AGENT = os.environ.get(
    "PROGRAMISTA_HUB_USER_AGENT",
    "ProgramistaHub/0.1 (+https://tyflo.eu.org/programista/)",
)

_session = requests.Session()
_session.headers.update({"User-Agent": _USER_AGENT})


def _clean_text(text: str) -> str:
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _clean_multiline(text: str) -> str:
    lines = [ln.strip() for ln in html.unescape(text or "").splitlines()]
    lines = [ln for ln in lines if ln]
    return "\n".join(lines).strip()


def _uniq_str(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for it in items:
        if it in seen:
            continue
        seen.add(it)
        out.append(it)
    return out


def ensure_tv_accessibility_providers(conn: psycopg.Connection) -> None:
    for pid, name in [
        (TVP_PROVIDER_ID, "Telewizja (TVP)"),
        (POLSAT_PROVIDER_ID, "Telewizja (Polsat)"),
        (PULS_PROVIDER_ID, "Telewizja (TV Puls)"),
    ]:
        conn.execute(
            """
            INSERT INTO provider (id, kind, display_name, updated_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (id) DO UPDATE
              SET kind = excluded.kind,
                  display_name = excluded.display_name,
                  updated_at = excluded.updated_at
            """,
            (pid, TV_ACCESS_KIND, name),
        )
    conn.commit()


# --- TVP ---


@dataclass(frozen=True)
class _TvpStation:
    slug: str
    name: str


@dataclass(frozen=True)
class _TvpItem:
    start_ms: int
    end_ms: int | None
    title: str
    description: str | None
    accessibility: list[str]


@dataclass(frozen=True)
class _TvpStationSchedule:
    station: _TvpStation
    items: list[_TvpItem]


def refresh_tvp_accessibility_day(
    conn: psycopg.Connection,
    *,
    day: date,
    request_delay_seconds: float = 0.0,
) -> int:
    day_key = day.isoformat()
    html_text = _fetch_text(f"{TVP_PROGRAM_URL}?date={day_key}", timeout_seconds=25.0)
    schedules = _parse_tvp_program_page(html_text)

    # Upsert sources
    for sch in schedules:
        conn.execute(
            """
            INSERT INTO source (provider_id, id, name, updated_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (provider_id, id) DO UPDATE
              SET name = excluded.name,
                  updated_at = excluded.updated_at
            """,
            (TVP_PROVIDER_ID, sch.station.slug, sch.station.name),
        )

    inserted = 0
    with conn.transaction():
        conn.execute("DELETE FROM schedule_item WHERE provider_id=%s AND day=%s", (TVP_PROVIDER_ID, day))
        for sch in schedules:
            for it in sch.items:
                if not it.accessibility:
                    continue
                conn.execute(
                    """
                    INSERT INTO schedule_item (
                      provider_id, source_id, day, start_time,
                      title, subtitle, details_ref, details_summary, accessibility
                    )
                    VALUES (%s,%s,%s,%s,%s,NULL,NULL,%s,%s)
                    ON CONFLICT (provider_id, source_id, day, start_time, title) DO UPDATE
                      SET details_summary = excluded.details_summary,
                          accessibility = excluded.accessibility
                    """,
                    (
                        TVP_PROVIDER_ID,
                        sch.station.slug,
                        day,
                        _ms_to_local_time(it.start_ms),
                        it.title,
                        it.description,
                        Jsonb(it.accessibility),
                    ),
                )
                inserted += 1

        conn.execute(
            """
            INSERT INTO fetch_state (key, updated_at)
            VALUES (%s, now())
            ON CONFLICT (key) DO UPDATE SET updated_at = excluded.updated_at
            """,
            (f"tvp:program:{day_key}",),
        )
    conn.commit()

    if request_delay_seconds > 0:
        time_mod.sleep(request_delay_seconds)

    return inserted


def _parse_tvp_program_page(html_text: str) -> list[_TvpStationSchedule]:
    decoder = json.JSONDecoder()
    schedules: list[_TvpStationSchedule] = []
    pos = 0
    while True:
        idx = html_text.find("window.__stationsProgram[", pos)
        if idx == -1:
            break
        eq = html_text.find("=", idx)
        if eq == -1:
            break
        brace = html_text.find("{", eq)
        if brace == -1:
            pos = eq + 1
            continue
        try:
            obj, end = decoder.raw_decode(html_text[brace:])
        except json.JSONDecodeError:
            pos = brace + 1
            continue
        pos = brace + end

        parsed = _parse_tvp_station_schedule(obj)
        if parsed:
            schedules.append(parsed)

    return schedules


def _parse_tvp_station_schedule(obj: Any) -> _TvpStationSchedule | None:
    if not isinstance(obj, dict):
        return None
    station_raw = obj.get("station")
    if not isinstance(station_raw, dict):
        return None
    url = station_raw.get("url")
    name = station_raw.get("name")
    if not isinstance(url, str) or not url.strip() or not isinstance(name, str) or not name.strip():
        return None

    slug = _station_slug_from_url(url)
    if not slug:
        return None

    items_raw = obj.get("items")
    if not isinstance(items_raw, list):
        items_raw = []

    items: list[_TvpItem] = []
    for it in items_raw:
        parsed = _parse_tvp_item(it)
        if parsed:
            items.append(parsed)

    return _TvpStationSchedule(
        station=_TvpStation(slug=slug, name=_normalize_tvp_station_name(name)),
        items=items,
    )


def _parse_tvp_item(it: Any) -> _TvpItem | None:
    if not isinstance(it, dict):
        return None
    start_ms = it.get("date_start")
    end_ms = it.get("date_end")
    title = it.get("title")
    if not isinstance(start_ms, int) or not isinstance(title, str) or not title.strip():
        return None
    if end_ms is not None and not isinstance(end_ms, int):
        end_ms = None

    accessibility: list[str] = []
    if it.get("ad") is True:
        accessibility.append("AD")
    if it.get("jm") is True:
        accessibility.append("JM")
    if it.get("nt") is True:
        accessibility.append("N")
    accessibility = _uniq_str(accessibility)

    description = None
    program = it.get("program")
    if isinstance(program, dict):
        desc = program.get("description_long") or program.get("description")
        if isinstance(desc, str) and desc.strip():
            description = _clean_multiline(desc)

    return _TvpItem(
        start_ms=start_ms,
        end_ms=end_ms,
        title=_clean_text(title),
        description=description,
        accessibility=accessibility,
    )


def _station_slug_from_url(url: str) -> str:
    try:
        path = urlparse(url).path
    except Exception:  # noqa: BLE001
        return ""
    parts = [p for p in path.split("/") if p]
    return parts[-1] if parts else ""


def _normalize_tvp_station_name(name: str) -> str:
    name = _clean_text(name)
    if name.startswith("TVP") and len(name) > 3 and name[3:].isdigit():
        return "TVP " + name[3:]
    return name


def _ms_to_local_time(ts_ms: int) -> time:
    return datetime.fromtimestamp(ts_ms / 1000).time().replace(microsecond=0)


# --- Polsat ---


@dataclass(frozen=True)
class _PolsatItem:
    start_ms: int
    start_time: time
    title: str
    description: str | None
    accessibility: list[str]


def refresh_polsat_accessibility_day(
    conn: psycopg.Connection,
    *,
    day: date,
    request_delay_seconds: float = 0.0,
) -> int:
    offset = (day - date.today()).days
    if offset < 0 or offset > 6:
        return 0

    pages: list[int] = [offset + 1]
    if offset > 0:
        pages.append(offset)

    merged: dict[str, list[_PolsatItem]] = {}
    for page in pages:
        html_text = _fetch_text(POLSAT_MODULE_URL.format(page=page), timeout_seconds=25.0)
        per_channel = _parse_polsat_day_from_module(html_text, day=day)
        for ch, items in per_channel.items():
            merged.setdefault(ch, []).extend(items)

        if request_delay_seconds > 0:
            time_mod.sleep(request_delay_seconds)

    # Stable ordering + cross-page de-duplication
    for ch, items in merged.items():
        items.sort(key=lambda it: (it.start_ms, it.title.casefold()))
        deduped: list[_PolsatItem] = []
        seen: set[tuple[int, str]] = set()
        for it in items:
            key = (it.start_ms, it.title.casefold())
            if key in seen:
                continue
            seen.add(key)
            deduped.append(it)
        merged[ch] = deduped

    # Upsert sources
    for ch in sorted(merged.keys(), key=lambda s: s.casefold()):
        conn.execute(
            """
            INSERT INTO source (provider_id, id, name, updated_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (provider_id, id) DO UPDATE
              SET name = excluded.name,
                  updated_at = excluded.updated_at
            """,
            (POLSAT_PROVIDER_ID, ch, ch),
        )

    inserted = 0
    with conn.transaction():
        conn.execute("DELETE FROM schedule_item WHERE provider_id=%s AND day=%s", (POLSAT_PROVIDER_ID, day))
        for ch, items in merged.items():
            for it in items:
                if not it.accessibility:
                    continue
                conn.execute(
                    """
                    INSERT INTO schedule_item (
                      provider_id, source_id, day, start_time,
                      title, subtitle, details_ref, details_summary, accessibility
                    )
                    VALUES (%s,%s,%s,%s,%s,NULL,NULL,%s,%s)
                    ON CONFLICT (provider_id, source_id, day, start_time, title) DO UPDATE
                      SET details_summary = excluded.details_summary,
                          accessibility = excluded.accessibility
                    """,
                    (
                        POLSAT_PROVIDER_ID,
                        ch,
                        day,
                        it.start_time,
                        it.title,
                        it.description,
                        Jsonb(it.accessibility),
                    ),
                )
                inserted += 1

        conn.execute(
            """
            INSERT INTO fetch_state (key, updated_at)
            VALUES (%s, now())
            ON CONFLICT (key) DO UPDATE SET updated_at = excluded.updated_at
            """,
            (f"polsat:module:day:{day.isoformat()}",),
        )
    conn.commit()

    return inserted


def _parse_polsat_day_from_module(html_text: str, *, day: date) -> dict[str, list[_PolsatItem]]:
    soup = BeautifulSoup(html_text, "lxml")
    out: dict[str, list[_PolsatItem]] = {}

    for row in soup.select("div.tv__row[data-channel]"):
        channel = _clean_text(row.get("data-channel") or "")
        if not channel:
            continue

        items = _parse_polsat_row_items(row, day=day)
        if items:
            out[channel] = items

    return out


def _parse_polsat_row_items(row: BeautifulSoup, *, day: date) -> list[_PolsatItem]:
    items: list[_PolsatItem] = []
    for cast in row.select("div.tvcast[data-start][data-end]"):
        start_ms_s = _clean_text(cast.get("data-start") or "")
        end_ms_s = _clean_text(cast.get("data-end") or "")
        if not start_ms_s.isdigit() or not end_ms_s.isdigit():
            continue

        start_ms = int(start_ms_s)
        try:
            start_dt = datetime.fromtimestamp(start_ms / 1000)
        except Exception:  # noqa: BLE001
            continue
        if start_dt.date() != day:
            continue

        title_el = cast.select_one(".tvcast__title")
        title = _clean_text(title_el.get_text(" ")) if title_el else ""
        if not title:
            continue

        accessibility: list[str] = []
        for icon in cast.select(".tvcast__accesibility-icon"):
            text = _clean_text(icon.get_text(" ")).upper()
            title_attr = _clean_text(icon.get("title") or "").casefold()
            if text == "AD" or "audiodeskrypcja" in title_attr:
                accessibility.append("AD")
            elif text == "JM" or "język migowy" in title_attr or "jezyk migowy" in title_attr:
                accessibility.append("JM")
            elif text == "N" or "napisy" in title_attr:
                accessibility.append("N")

        accessibility = _uniq_str(accessibility)

        items.append(
            _PolsatItem(
                start_ms=start_ms,
                start_time=start_dt.time().replace(microsecond=0),
                title=title,
                description=None,
                accessibility=accessibility,
            )
        )

    items.sort(key=lambda it: it.start_ms)
    return items


# --- Puls EPG ---


@dataclass(frozen=True)
class _PulsEpgFiles:
    tvpuls_url: str | None
    puls2_url: str | None


def refresh_puls_accessibility(
    conn: psycopg.Connection,
    *,
    request_delay_seconds: float = 0.0,
    keep_min_day: date | None = None,
    keep_max_day: date | None = None,
) -> int:
    html_text = _fetch_text(PULS_EPG_BASE_URL, timeout_seconds=20.0)
    files = _parse_puls_epg_index(html_text, base_url=PULS_EPG_BASE_URL)
    inserted = 0

    for source_id, url in [("tvpuls", files.tvpuls_url), ("puls2", files.puls2_url)]:
        if not url:
            continue
        conn.execute(
            """
            INSERT INTO source (provider_id, id, name, updated_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (provider_id, id) DO UPDATE
              SET name = excluded.name,
                  updated_at = excluded.updated_at
            """,
            (PULS_PROVIDER_ID, source_id, "TV Puls" if source_id == "tvpuls" else "Puls 2"),
        )

        xml_text = _fetch_text(url, timeout_seconds=35.0, allow_gzip=True)
        by_day = _parse_puls_epg_xml_all_days(xml_text)

        with conn.transaction():
            for day, items in by_day.items():
                if keep_min_day and day < keep_min_day:
                    continue
                if keep_max_day and day > keep_max_day:
                    continue

                # delete only this source/day
                conn.execute(
                    "DELETE FROM schedule_item WHERE provider_id=%s AND source_id=%s AND day=%s",
                    (PULS_PROVIDER_ID, source_id, day),
                )
                for it in items:
                    if not it.accessibility:
                        continue
                    conn.execute(
                        """
                        INSERT INTO schedule_item (
                          provider_id, source_id, day, start_time,
                          title, subtitle, details_ref, details_summary, accessibility
                        )
                        VALUES (%s,%s,%s,%s,%s,NULL,NULL,%s,%s)
                        ON CONFLICT (provider_id, source_id, day, start_time, title) DO UPDATE
                          SET details_summary = excluded.details_summary,
                              accessibility = excluded.accessibility
                        """,
                        (
                            PULS_PROVIDER_ID,
                            source_id,
                            day,
                            it.start_time,
                            it.title,
                            it.description,
                            Jsonb(it.accessibility),
                        ),
                    )
                    inserted += 1

            conn.execute(
                """
                INSERT INTO fetch_state (key, updated_at, value)
                VALUES (%s, now(), %s)
                ON CONFLICT (key) DO UPDATE
                  SET updated_at = excluded.updated_at,
                      value = excluded.value
                """,
                (f"puls:epg:file:{source_id}", url),
            )
        conn.commit()

        if request_delay_seconds > 0:
            time_mod.sleep(request_delay_seconds)

    conn.execute(
        """
        INSERT INTO fetch_state (key, updated_at)
        VALUES (%s, now())
        ON CONFLICT (key) DO UPDATE SET updated_at = excluded.updated_at
        """,
        ("puls:epg:index",),
    )
    conn.commit()

    return inserted


def _parse_puls_epg_index(html_text: str, *, base_url: str) -> _PulsEpgFiles:
    soup = BeautifulSoup(html_text, "lxml")
    links: list[str] = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href or href.endswith("/"):
            continue
        if not (href.lower().endswith(".xml") or href.lower().endswith(".xml.gz")):
            continue
        links.append(href)

    tvpuls: list[str] = []
    puls2: list[str] = []
    for href in links:
        name = href.casefold()
        if "puls2" in name:
            puls2.append(href)
        elif "tvpuls" in name or "puls" in name:
            tvpuls.append(href)

    def pick(candidates: list[str]) -> str | None:
        if not candidates:
            return None
        candidates_sorted = sorted(set(candidates))
        return urljoin(base_url, candidates_sorted[-1])

    return _PulsEpgFiles(tvpuls_url=pick(tvpuls), puls2_url=pick(puls2))


@dataclass(frozen=True)
class _PulsItem:
    start_time: time
    title: str
    description: str | None
    accessibility: list[str]
    sort_key: str


def _parse_puls_epg_xml_all_days(xml_text: str) -> dict[date, list[_PulsItem]]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return {}

    items_by_day: dict[date, list[_PulsItem]] = {}
    for ev in root.findall(".//event"):
        actual = (ev.get("actual_time") or "").strip()
        if len(actual) < 10:
            continue
        try:
            event_day = date.fromisoformat(actual[:10])
        except ValueError:
            continue

        start_dt = _parse_epg_datetime(actual)
        if not start_dt:
            continue

        desc_el = ev.find("description")
        title = _clean_text(desc_el.get("title") if desc_el is not None else "") or _clean_text(
            ev.get("original_title") or ""
        )
        if not title:
            continue

        long_synopsis = desc_el.get("long_synopsis") if desc_el is not None else None
        synopsis = _clean_multiline(long_synopsis or "") if long_synopsis else ""

        features, synopsis_clean = _extract_accessibility_from_synopsis(synopsis)
        features = _uniq_str(features)

        items_by_day.setdefault(event_day, []).append(
            _PulsItem(
                start_time=start_dt.time().replace(microsecond=0),
                title=title,
                description=synopsis_clean or None,
                accessibility=features,
                sort_key=actual,
            )
        )

    for day_items in items_by_day.values():
        day_items.sort(key=lambda it: it.sort_key)
    return items_by_day


def _parse_epg_datetime(value: str) -> datetime | None:
    value = value.strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _extract_accessibility_from_synopsis(synopsis: str) -> tuple[list[str], str]:
    text = synopsis.strip()
    features: list[str] = []

    while True:
        m = re.match(r"^\((AD|JM|N)\)\s*", text)
        if not m:
            break
        features.append(m.group(1))
        text = text[m.end() :].lstrip()

    return features, text


def _fetch_text(
    url: str,
    *,
    timeout_seconds: float,
    allow_gzip: bool = False,
) -> str:
    r = _session.get(url, timeout=timeout_seconds)
    r.raise_for_status()
    if allow_gzip and url.lower().endswith(".gz"):
        return gzip.decompress(r.content).decode("utf-8", errors="replace")
    return r.text


def purge_tv_accessibility(
    conn: psycopg.Connection,
    *,
    min_day: date,
    max_day: date,
) -> None:
    conn.execute(
        """
        DELETE FROM schedule_item
        WHERE provider_id = ANY(%s)
          AND (day < %s OR day > %s)
        """,
        ([TVP_PROVIDER_ID, POLSAT_PROVIDER_ID, PULS_PROVIDER_ID], min_day, max_day),
    )
    conn.commit()
