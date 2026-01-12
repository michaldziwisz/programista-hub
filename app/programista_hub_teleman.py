from __future__ import annotations

import html
import os
import re
import time as time_mod
from dataclasses import dataclass
from datetime import date, time
from urllib.parse import urljoin

import psycopg
import requests
from bs4 import BeautifulSoup


TELEMAN_BASE = "https://www.teleman.pl"
TELEMAN_PROVIDER_ID = "teleman"

_USER_AGENT = os.environ.get(
    "PROGRAMISTA_HUB_USER_AGENT",
    "ProgramistaHub/0.1 (+https://tyflo.eu.org/programista/)",
)

_session = requests.Session()
_session.headers.update({"User-Agent": _USER_AGENT})

_PAGE_DAY_RE = re.compile(r"<title>[^<]*(\d{1,2})\.(\d{2})\.(\d{4})", re.IGNORECASE)


def _clean_text(text: str) -> str:
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _parse_time_hhmm(text: str) -> time | None:
    m = re.match(r"^\s*(\d{1,2})[:.](\d{2})\s*$", text)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        return None
    return time(hour=hh, minute=mm)


def _fetch_text(url: str, *, timeout_seconds: float = 25.0) -> str:
    r = _session.get(url, timeout=timeout_seconds)
    r.raise_for_status()
    return r.text


@dataclass(frozen=True)
class ParsedItem:
    start_time: time
    title: str
    subtitle: str | None
    summary: str | None
    details_ref: str | None


def parse_teleman_stations(html_text: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html_text, "lxml")
    nav = soup.select_one("nav#stations-index")
    if not nav:
        return []

    stations: list[tuple[str, str]] = []
    for a in nav.select("a[href^='/program-tv/stacje/']"):
        href = a.get("href")
        if not href:
            continue
        slug = href.rsplit("/", 1)[-1]
        name = _clean_text(a.get_text(" "))
        if slug and name:
            stations.append((slug, name))

    seen: set[str] = set()
    out: list[tuple[str, str]] = []
    for slug, name in stations:
        if slug in seen:
            continue
        seen.add(slug)
        out.append((slug, name))
    return out


def parse_teleman_station_schedule(html_text: str) -> list[ParsedItem]:
    soup = BeautifulSoup(html_text, "lxml")
    ul = soup.select_one("ul.stationItems")
    if not ul:
        return []

    items: list[ParsedItem] = []
    for li in ul.select("li[id^='prog']"):
        em = li.find("em")
        start = _parse_time_hhmm(_clean_text(em.get_text(" "))) if em else None
        if start is None:
            continue

        detail = li.select_one("div.detail")
        if not detail:
            continue

        a = detail.find("a", href=True)
        title = _clean_text(a.get_text(" ")) if a else ""
        href = a.get("href") if a else None

        genre_p = detail.select_one("p.genre")
        subtitle = _clean_text(genre_p.get_text(" ")) if genre_p else None

        summary = None
        for p in detail.find_all("p"):
            if "genre" in (p.get("class") or []):
                continue
            summary = _clean_text(p.get_text(" "))
            if summary:
                break

        resolved_title = title or (summary or "")
        if not resolved_title:
            continue

        items.append(
            ParsedItem(
                start_time=start,
                title=resolved_title,
                subtitle=subtitle,
                summary=summary,
                details_ref=href,
            )
        )
    return items


def parse_teleman_page_day(html_text: str) -> date | None:
    m = _PAGE_DAY_RE.search(html_text)
    if not m:
        return None
    dd_s, mm_s, yy_s = m.groups()
    try:
        return date(int(yy_s), int(mm_s), int(dd_s))
    except ValueError:
        return None


def parse_teleman_show_details(html_text: str) -> str:
    soup = BeautifulSoup(html_text, "lxml")
    sections: list[str] = []
    for h2 in soup.select("div.section > h2"):
        title = _clean_text(h2.get_text(" "))
        if title not in {"Opis", "W tym odcinku"}:
            continue
        p = h2.find_next_sibling("p")
        if not p:
            continue
        body = _clean_text(p.get_text(" "))
        if body:
            sections.append(f"{title}:\n{body}")
    return "\n\n".join(sections)


def fetch_teleman_details_text(details_ref: str) -> str:
    url = urljoin(TELEMAN_BASE, details_ref)
    html_text = _fetch_text(url)
    return parse_teleman_show_details(html_text)


def ensure_provider(conn: psycopg.Connection) -> None:
    conn.execute(
        """
        INSERT INTO provider (id, kind, display_name, updated_at)
        VALUES (%s, %s, %s, now())
        ON CONFLICT (id) DO UPDATE
          SET kind = excluded.kind,
              display_name = excluded.display_name,
              updated_at = excluded.updated_at
        """,
        (TELEMAN_PROVIDER_ID, "tv", "Telewizja (Teleman)"),
    )
    conn.commit()


def refresh_sources(conn: psycopg.Connection, *, request_delay_seconds: float = 0.0) -> int:
    html_text = _fetch_text(f"{TELEMAN_BASE}/")
    stations = parse_teleman_stations(html_text)

    for slug, name in stations:
        conn.execute(
            """
            INSERT INTO source (provider_id, id, name, updated_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (provider_id, id) DO UPDATE
              SET name = excluded.name,
                  updated_at = excluded.updated_at
            """,
            (TELEMAN_PROVIDER_ID, slug, name),
        )

    conn.execute(
        """
        INSERT INTO fetch_state (key, updated_at)
        VALUES (%s, now())
        ON CONFLICT (key) DO UPDATE SET updated_at = excluded.updated_at
        """,
        ("teleman:sources",),
    )
    conn.commit()

    if request_delay_seconds > 0:
        time_mod.sleep(request_delay_seconds)

    return len(stations)


def refresh_schedule(
    conn: psycopg.Connection,
    *,
    source_id: str,
    day: date,
    request_delay_seconds: float = 0.0,
) -> int:
    url = f"{TELEMAN_BASE}/program-tv/stacje/{source_id}?date={day.isoformat()}"
    html_text = _fetch_text(url)

    # Teleman returns today's schedule when the requested date is outside its supported range.
    # Guard against ingesting the wrong day.
    actual_day = parse_teleman_page_day(html_text)
    if actual_day and actual_day != day:
        return 0

    items = parse_teleman_station_schedule(html_text)

    with conn.transaction():
        conn.execute(
            "DELETE FROM schedule_item WHERE provider_id=%s AND source_id=%s AND day=%s",
            (TELEMAN_PROVIDER_ID, source_id, day),
        )
        for it in items:
            conn.execute(
                """
                INSERT INTO schedule_item (
                  provider_id, source_id, day, start_time,
                  title, subtitle, details_ref, details_summary
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (provider_id, source_id, day, start_time, title) DO UPDATE
                  SET subtitle = excluded.subtitle,
                      details_ref = excluded.details_ref,
                      details_summary = excluded.details_summary
                """,
                (
                    TELEMAN_PROVIDER_ID,
                    source_id,
                    day,
                    it.start_time,
                    it.title,
                    it.subtitle,
                    it.details_ref,
                    it.summary,
                ),
            )

        conn.execute(
            """
            INSERT INTO fetch_state (key, updated_at)
            VALUES (%s, now())
            ON CONFLICT (key) DO UPDATE SET updated_at = excluded.updated_at
            """,
            (f"teleman:schedule:{source_id}:{day.isoformat()}",),
        )

    if request_delay_seconds > 0:
        time_mod.sleep(request_delay_seconds)

    return len(items)
