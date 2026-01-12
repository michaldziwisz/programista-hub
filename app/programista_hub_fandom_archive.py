from __future__ import annotations

import html
import os
import re
import time as time_mod
import unicodedata
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from email.utils import parsedate_to_datetime
from typing import Any

import psycopg
import requests


FANDOM_API = "https://staratelewizja.fandom.com/pl/api.php"
FANDOM_PROVIDER_ID = "fandom-archive"
FANDOM_DISPLAY_NAME = "Programy archiwalne (Fandom)"
ARCHIVE_PARSER_VERSION = 2

DEFAULT_SINGLE_CHANNEL_SOURCE_NAME = "TVP 1"

_USER_AGENT = os.environ.get(
    "PROGRAMISTA_HUB_USER_AGENT",
    "ProgramistaHub/0.1 (+https://tyflo.eu.org/programista/)",
)

_session = requests.Session()
_session.headers.update(
    {
        "User-Agent": _USER_AGENT,
        "Accept": "*/*",
        "Accept-Language": "pl,en;q=0.8",
    }
)


class FandomBlockedError(RuntimeError):
    def __init__(self, *, status_code: int, retry_after_seconds: int | None = None) -> None:
        super().__init__(f"Fandom blocked: HTTP {status_code}")
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds


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


def _fold(text: str) -> str:
    if not text:
        return ""
    text = text.casefold()
    text = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in text if not unicodedata.combining(ch))


_MONTHS_GENITIVE: dict[str, int] = {
    "stycznia": 1,
    "lutego": 2,
    "marca": 3,
    "kwietnia": 4,
    "maja": 5,
    "czerwca": 6,
    "lipca": 7,
    "sierpnia": 8,
    "wrzesnia": 9,  # września
    "pazdziernika": 10,  # października
    "listopada": 11,
    "grudnia": 12,
}


def parse_fandom_day_title_to_date(title: str) -> date | None:
    if not title:
        return None

    t = _clean_text(title.replace("_", " "))
    m = re.match(r"^(\d{1,2})\s+([^\s]+)\s+(\d{4})$", t)
    if not m:
        return None

    day_s, month_s, year_s = m.group(1), m.group(2), m.group(3)
    try:
        dd = int(day_s)
        yyyy = int(year_s)
    except ValueError:
        return None

    month_key = _fold(month_s)
    mm = _MONTHS_GENITIVE.get(month_key)
    if not mm:
        return None

    try:
        return date(yyyy, mm, dd)
    except ValueError:
        return None


def _fetch_json(params: dict[str, str], *, timeout_seconds: float = 30.0) -> dict[str, Any]:
    r = _session.get(FANDOM_API, params=params, timeout=timeout_seconds)
    if r.status_code in (403, 429):
        retry_after = None
        ra = (r.headers.get("retry-after") or "").strip()
        if ra:
            if ra.isdigit():
                retry_after = int(ra)
            else:
                try:
                    dt = parsedate_to_datetime(ra)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=UTC)
                    retry_after = max(0, int((dt - datetime.now(UTC)).total_seconds()))
                except Exception:  # noqa: BLE001
                    retry_after = None
        raise FandomBlockedError(status_code=r.status_code, retry_after_seconds=retry_after)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict):
        raise ValueError("Invalid JSON response (expected object).")
    return data


def _upsert_provider(conn: psycopg.Connection, *, provider_id: str, kind: str, display_name: str) -> None:
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


def _upsert_source(conn: psycopg.Connection, *, provider_id: str, source_id: str, name: str) -> None:
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


def ensure_archive_provider(conn: psycopg.Connection) -> None:
    with conn.transaction():
        _upsert_provider(conn, provider_id=FANDOM_PROVIDER_ID, kind="archive", display_name=FANDOM_DISPLAY_NAME)
    conn.commit()


def _get_state(conn: psycopg.Connection, key: str) -> tuple[str | None, Any | None]:
    row = conn.execute("SELECT updated_at, value FROM fetch_state WHERE key=%s", (key,)).fetchone()
    if not row:
        return None, None
    return (row["updated_at"], row["value"])


def scan_fandom_allpages(
    conn: psycopg.Connection,
    *,
    batch_size: int = 200,
    request_delay_seconds: float = 0.0,
) -> int:
    _, token = _get_state(conn, "fandom:allpages")
    apcontinue = str(token) if token else None

    params: dict[str, str] = {
        "action": "query",
        "format": "json",
        "list": "allpages",
        "apnamespace": "0",
        "aplimit": str(max(1, min(batch_size, 500))),
    }
    if apcontinue:
        params["apcontinue"] = apcontinue

    data = _fetch_json(params)
    pages = data.get("query", {}).get("allpages", [])
    if not isinstance(pages, list):
        pages = []

    next_token = None
    cont = data.get("continue")
    if isinstance(cont, dict) and isinstance(cont.get("apcontinue"), str):
        next_token = cont["apcontinue"]

    inserted = 0
    with conn.transaction():
        for p in pages:
            if not isinstance(p, dict):
                continue
            page_id = p.get("pageid")
            title = p.get("title")
            if not isinstance(page_id, int) or not isinstance(title, str):
                continue

            day = parse_fandom_day_title_to_date(title)
            if not day:
                continue

            conn.execute(
                """
                INSERT INTO provider_page (provider_id, page_title, page_id, day, updated_at)
                VALUES (%s, %s, %s, %s, now())
                ON CONFLICT (provider_id, page_title) DO UPDATE
                  SET page_id = excluded.page_id,
                      day = excluded.day,
                      updated_at = excluded.updated_at
                """,
                (FANDOM_PROVIDER_ID, title, page_id, day),
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
            ("fandom:allpages", next_token),
        )

    if request_delay_seconds > 0:
        time_mod.sleep(request_delay_seconds)

    return inserted


def _fetch_page_wikitext(page_id: int) -> tuple[str, int | None]:
    data = _fetch_json(
        {
            "action": "query",
            "format": "json",
            "formatversion": "2",
            "prop": "revisions",
            "pageids": str(page_id),
            "rvprop": "ids|content",
            "rvslots": "main",
        }
    )
    pages = data.get("query", {}).get("pages", [])
    if not isinstance(pages, list) or not pages:
        return "", None
    page = pages[0]
    if not isinstance(page, dict):
        return "", None
    revisions = page.get("revisions", [])
    if not isinstance(revisions, list) or not revisions:
        return "", None
    rev0 = revisions[0]
    if not isinstance(rev0, dict):
        return "", None
    revid = rev0.get("revid")
    rev_id_int = int(revid) if isinstance(revid, int) else None

    slots = rev0.get("slots", {})
    if not isinstance(slots, dict):
        return "", rev_id_int
    main = slots.get("main", {})
    if not isinstance(main, dict):
        return "", rev_id_int
    content = main.get("content")
    if not isinstance(content, str):
        return "", rev_id_int
    return content, rev_id_int


# ---------------------------------------------------------------------------
# Wikitext parsing (adapted from desktop provider)
# ---------------------------------------------------------------------------


def strip_wiki_markup(text: str) -> str:
    if not text:
        return ""

    text = re.sub(r"\[\[(Plik|File):[^\]]+\]\]", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\[\[[^\]|]+\|([^\]]+)\]\]", r"\1", text)
    text = re.sub(r"\[\[([^\]]+)\]\]", r"\1", text)
    text = re.sub(r"\{\{[^}]+\}\}", "", text)
    text = text.replace("'''", "").replace("''", "")
    text = re.sub(r"<[^>]+>", "", text)
    return _clean_text(text)


def _channel_key(name: str) -> str:
    compact = re.sub(r"\s+", "", (name or "").casefold())
    if compact in {"tvp1", "tp1", "program1"}:
        return "tvp1"
    if compact in {"tvp2", "tp2", "program2"}:
        return "tvp2"
    return compact


def is_default_single_channel_name(name: str) -> bool:
    return _channel_key(name) == "tvp1"


def extract_time_lines_from_wikitext(wikitext: str) -> list[str]:
    if not wikitext:
        return []

    normalized = re.sub(r"<br\s*/?>", "\n", wikitext, flags=re.IGNORECASE)
    time_start_re = re.compile(r"^\s*\d{1,2}(?:[:.]|\s)\d{2}\b")

    lines: list[str] = []
    for raw_line in normalized.splitlines():
        line = _clean_text(strip_wiki_markup(raw_line))
        if not line:
            continue
        if not time_start_re.match(line):
            continue
        lines.append(line)
    return lines


def extract_channels_from_category_links(wikitext: str) -> list[str]:
    if not wikitext:
        return []

    cat_re = re.compile(
        r"\[\[(?:Kategoria|Category):Ramówki\s+(.+?)\s+z\s+(\d{4})\s+roku(?:\|[^\]]*)?\]\]",
        re.IGNORECASE,
    )
    seen: set[str] = set()
    channels: list[str] = []
    for m in cat_re.finditer(wikitext):
        channel = strip_wiki_markup(m.group(1))
        channel_norm = channel.casefold().strip()
        if not channel_norm or channel_norm in seen:
            continue
        seen.add(channel_norm)
        channels.append(channel)
    return channels


def split_wikitext_file_blocks(wikitext: str) -> list[str]:
    if not wikitext:
        return []

    file_start_re = re.compile(r"^\s*\[\[(?:Plik|File):[^\]]+\]\]\s*(?P<rest>.*)$", re.IGNORECASE)
    time_hint_re = re.compile(r"\b\d{1,2}[:.]\d{2}\b")

    blocks: list[str] = []
    current: list[str] = []
    started = False

    for line in wikitext.splitlines():
        stripped = line.strip()
        if stripped.startswith("[[Kategoria:") or stripped.startswith("[[Category:"):
            break

        m = file_start_re.match(stripped)
        if m:
            if started:
                block = "\n".join(current).strip()
                if block and time_hint_re.search(block):
                    blocks.append(block)
            current = []
            started = True
            rest = m.group("rest").strip()
            if rest:
                current.append(rest)
            continue

        if not started:
            continue
        current.append(line)

    if started:
        block = "\n".join(current).strip()
        if block and time_hint_re.search(block):
            blocks.append(block)

    return blocks


def split_wikitext_plain_channel_sections(wikitext: str) -> list[tuple[str, str]]:
    if not wikitext:
        return []

    normalized = re.sub(r"<br\s*/?>", "\n", wikitext, flags=re.IGNORECASE)

    time_start_re = re.compile(r"^\s*\d{1,2}(?:[:.]|\s)\d{2}\b")
    date_dot_re = re.compile(r"\b\d{1,2}\.\d{1,2}\.\d{4}\b")
    weekday_re = re.compile(
        r"\b(poniedzia[łl]ek|wtorek|środa|sroda|czwartek|piątek|piatek|sobota|niedziela)\b",
        re.IGNORECASE,
    )

    raw_lines = normalized.splitlines()
    clean_lines = [_clean_text(strip_wiki_markup(x)) for x in raw_lines]

    pairs: list[tuple[str, str]] = []
    current_channel: str | None = None
    current_lines: list[str] = []

    def is_header_line(text: str) -> bool:
        if not text:
            return True
        if date_dot_re.search(text):
            return True
        if weekday_re.search(text):
            return True
        return False

    def looks_like_channel_start(index: int, line_text: str) -> bool:
        if not line_text or time_start_re.match(line_text) or is_header_line(line_text):
            return False
        # Heuristic: plain-text pages sometimes include internal section labels like
        # "Wczoraj i dziś:"; treat anything with a colon as a non-channel header.
        if ":" in line_text:
            return False
        if len(line_text) > 50:
            return False
        if any(q in line_text for q in ('"', "„", "”", "«", "»")):
            return False
        if any(sep in line_text for sep in (" - ", " – ", " — ")):
            return False
        if len(line_text.split()) > 5:
            return False
        for j in range(index + 1, len(clean_lines)):
            nxt = clean_lines[j]
            if not nxt:
                continue
            if nxt.startswith("[[Kategoria:") or nxt.startswith("[[Category:"):
                return False
            return bool(time_start_re.match(nxt))
        return False

    for i, line_text in enumerate(clean_lines):
        raw = raw_lines[i]
        stripped_raw = raw.strip()
        if stripped_raw.startswith("[[Kategoria:") or stripped_raw.startswith("[[Category:"):
            break

        if not line_text:
            continue

        if time_start_re.match(line_text):
            if current_channel is not None:
                current_lines.append(line_text)
            continue

        if not looks_like_channel_start(i, line_text):
            continue

        if current_channel is not None:
            block = "\n".join(current_lines).strip()
            if block:
                pairs.append((current_channel, block))
        current_channel = line_text
        current_lines = []

    if current_channel is not None:
        block = "\n".join(current_lines).strip()
        if block:
            pairs.append((current_channel, block))

    return pairs


def extract_channel_schedule_from_wikitext(wikitext: str, channel_name: str) -> str:
    if not wikitext:
        return ""

    target_key = _channel_key(channel_name)
    heading_re = re.compile(r"^(?P<eq>={3,6})\s*(?P<title>.*?)\s*(?P=eq)\s*$")

    collecting = False
    collected: list[str] = []

    for line in wikitext.splitlines():
        m = heading_re.match(line.strip())
        if m:
            heading_title = strip_wiki_markup(m.group("title"))
            heading_title_norm = heading_title.casefold().strip()
            if not heading_title_norm or any(
                x in heading_title_norm for x in ("plik:", "file:", ".png", ".jpg", ".svg")
            ):
                continue
            collecting = _channel_key(heading_title) == target_key
            continue

        if collecting:
            collected.append(line)

    block = "\n".join(collected).strip()
    if block:
        return block

    channels = extract_channels_from_category_links(wikitext)
    blocks = split_wikitext_file_blocks(wikitext)
    if not channels or not blocks:
        pairs = split_wikitext_plain_channel_sections(wikitext)
        for ch, b in pairs:
            if _channel_key(ch) == target_key:
                return b

        if is_default_single_channel_name(channel_name):
            time_lines = extract_time_lines_from_wikitext(wikitext)
            if time_lines:
                return "\n".join(time_lines)
        return ""

    idx = next((i for i, c in enumerate(channels) if _channel_key(c) == target_key), None)
    if idx is None or idx >= len(blocks):
        return ""
    return blocks[idx]


def extract_channels_from_wikitext(wikitext: str) -> list[str]:
    if not wikitext:
        return []

    heading_re = re.compile(r"^(?P<eq>={3,6})\s*(?P<title>.*?)\s*(?P=eq)\s*$")

    seen: set[str] = set()
    channels: list[str] = []
    for line in wikitext.splitlines():
        m = heading_re.match(line.strip())
        if not m:
            continue
        heading_title = strip_wiki_markup(m.group("title"))
        heading_title_norm = heading_title.casefold().strip()
        if not heading_title_norm:
            continue
        if any(x in heading_title_norm for x in ("plik:", "file:", ".png", ".jpg", ".svg")):
            continue
        if heading_title_norm in seen:
            continue
        seen.add(heading_title_norm)
        channels.append(heading_title)
    if channels:
        return channels

    cat_channels = extract_channels_from_category_links(wikitext)
    if cat_channels:
        return cat_channels

    pairs = split_wikitext_plain_channel_sections(wikitext)
    if not pairs:
        time_lines = extract_time_lines_from_wikitext(wikitext)
        if time_lines:
            return [DEFAULT_SINGLE_CHANNEL_SOURCE_NAME]
        return []

    seen = set()
    result: list[str] = []
    for ch, _block in pairs:
        norm = ch.casefold().strip()
        if not norm or norm in seen:
            continue
        seen.add(norm)
        result.append(ch)
    return result


def split_schedule_entries(channel_block: str) -> list[str]:
    if not channel_block:
        return []
    normalized = re.sub(r"<br\s*/?>", "\n", channel_block, flags=re.IGNORECASE)
    entries: list[str] = []
    for raw_line in normalized.splitlines():
        line = _clean_text(strip_wiki_markup(raw_line))
        if not line:
            continue
        line_norm = line.casefold()
        if line_norm.startswith("kategoria:") or line_norm.startswith("category:"):
            continue
        if line_norm.startswith("plik:") or line_norm.startswith("file:"):
            continue
        entries.append(line)
    return entries


def parse_entry_start_and_rest(entry: str) -> tuple[time | None, str]:
    m = re.match(
        r"^\s*(\d{1,2})\s*(?:[:.]|\s)\s*(\d{2})(?:\s*[-–]\s*(\d{1,2})\s*(?:[:.]|\s)\s*(\d{2}))?\s*(?:[-–]\s*)?(.*)$",
        entry,
    )
    if not m:
        return None, _clean_text(entry)
    hh, mm, rest = m.group(1), m.group(2), m.group(5)
    t = _parse_time_hhmm(f"{hh}:{mm}")
    return t, _clean_text(rest)


def split_title_subtitle(rest: str) -> tuple[str, str | None]:
    for sep in (" - ", ";"):
        if sep in rest:
            title, tail = rest.split(sep, 1)
            title = _clean_text(title)
            tail = _clean_text(tail)
            return (title or rest, tail or None)
    return _clean_text(rest), None


@dataclass(frozen=True)
class ParsedArchiveItem:
    start_time: time
    title: str
    subtitle: str | None


def _parse_channel_items(wikitext: str, channel_name: str) -> list[ParsedArchiveItem]:
    block = extract_channel_schedule_from_wikitext(wikitext, channel_name)
    entries = split_schedule_entries(block)
    out: list[ParsedArchiveItem] = []
    for entry in entries:
        start, rest = parse_entry_start_and_rest(entry)
        if start is None:
            continue
        title, subtitle = split_title_subtitle(rest)
        if not title:
            continue
        out.append(ParsedArchiveItem(start_time=start, title=title, subtitle=subtitle))

    seen: set[tuple[str, str]] = set()
    deduped: list[ParsedArchiveItem] = []
    for it in out:
        key = (it.start_time.strftime("%H:%M"), it.title.casefold())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(it)
    return deduped


def ingest_fandom_page(
    conn: psycopg.Connection,
    *,
    page_id: int,
    page_title: str,
    day: date,
    request_delay_seconds: float = 0.0,
) -> int:
    wikitext, rev_id = _fetch_page_wikitext(page_id)
    if request_delay_seconds > 0:
        time_mod.sleep(request_delay_seconds)

    rev_id_db = rev_id if rev_id is not None else 0

    inserted = 0
    with conn.transaction():
        if wikitext:
            channels = extract_channels_from_wikitext(wikitext)
            for channel in channels:
                _upsert_source(conn, provider_id=FANDOM_PROVIDER_ID, source_id=channel, name=channel)
                conn.execute(
                    "DELETE FROM schedule_item WHERE provider_id=%s AND source_id=%s AND day=%s",
                    (FANDOM_PROVIDER_ID, channel, day),
                )
                for item in _parse_channel_items(wikitext, channel):
                    conn.execute(
                        """
                        INSERT INTO schedule_item (
                          provider_id, source_id, day, start_time,
                          title, subtitle, details_ref, details_summary
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (provider_id, source_id, day, start_time, title) DO UPDATE
                          SET subtitle = excluded.subtitle
                        """,
                        (FANDOM_PROVIDER_ID, channel, day, item.start_time, item.title, item.subtitle, None, None),
                    )
                    inserted += 1

        conn.execute(
            """
            UPDATE provider_page
            SET rev_id=%s, updated_at=now()
            WHERE provider_id=%s AND page_title=%s
            """,
            (rev_id_db, FANDOM_PROVIDER_ID, page_title),
        )

    conn.commit()
    return inserted


def ingest_pending_fandom_pages(
    conn: psycopg.Connection,
    *,
    max_pages: int = 1,
    request_delay_seconds: float = 0.0,
) -> int:
    rows = conn.execute(
        """
        SELECT page_id, page_title, day, rev_id
        FROM provider_page
        WHERE provider_id=%s AND day IS NOT NULL AND page_id IS NOT NULL AND rev_id IS NULL
        ORDER BY day ASC
        LIMIT %s
        """,
        (FANDOM_PROVIDER_ID, max_pages),
    ).fetchall()

    processed = 0
    for row in rows:
        page_id = row["page_id"]
        page_title = row["page_title"]
        day = row["day"]
        if not isinstance(page_id, int) or not isinstance(page_title, str) or not isinstance(day, date):
            continue
        ingest_fandom_page(
            conn,
            page_id=page_id,
            page_title=page_title,
            day=day,
            request_delay_seconds=request_delay_seconds,
        )
        processed += 1

    return processed
