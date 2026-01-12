from __future__ import annotations

import html
import json
import os
import re
import time as time_mod
from dataclasses import dataclass
from datetime import date, time, timedelta
from urllib.parse import urljoin

import psycopg
import requests
from bs4 import BeautifulSoup


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


def _clean_multiline_text(text: str) -> str:
    if not text:
        return ""
    text = html.unescape(text)
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = re.sub(r"\s+", " ", raw_line).strip()
        if line:
            lines.append(line)
    return "\n".join(lines)


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


def _post_form_text(url: str, data: dict[str, str], *, timeout_seconds: float = 25.0) -> str:
    r = _session.post(url, data=data, timeout=timeout_seconds)
    r.raise_for_status()
    return r.text


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


# ---------------------------------------------------------------------------
# Polskie Radio
# ---------------------------------------------------------------------------

PR_MULTISCHEDULE_URL = (
    "https://www.polskieradio.pl/Portal/Schedule/AjaxPages/AjaxGetMultiScheduleView.aspx"
)
PR_DETAILS_URL = "https://www.polskieradio.pl/Portal/Schedule/AjaxPages/AjaxGetProgrammeDetails.aspx"
PR_BASE = "https://www.polskieradio.pl"

PR_CHANNELS: list[str] = ["Jedynka", "Dwójka", "Trójka", "Czwórka", "Radio Poland", "PR24"]


@dataclass(frozen=True)
class _PrItem:
    start_time: time | None
    title: str
    details_ref: str | None


def _parse_onclick_details_ref(onclick: str) -> str | None:
    m = re.search(
        r"showProgrammeDetails\(\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']+)'\s*\)",
        onclick,
    )
    if not m:
        return None
    return "|".join([m.group(1), m.group(2), m.group(3), m.group(4)])


def _extract_pr_programme_title(a) -> str:
    title_el = a.select_one("span.desc") or a.select_one("span.title") or a.select_one(".desc")
    if title_el:
        t = _clean_text(title_el.get_text(" "))
        if t:
            return t

    title_attr = _clean_text(a.get("title") or "")
    if title_attr:
        return title_attr

    return _clean_text(a.get_text(" "))


def parse_pr_multischedule_html(html_text: str, channel_order: list[str]) -> dict[str, list[_PrItem]]:
    soup = BeautifulSoup(html_text, "lxml")
    containers = soup.select("div.scheduleViewContainer")
    by_channel: dict[str, list[_PrItem]] = {}
    for idx, container in enumerate(containers):
        if idx >= len(channel_order):
            break
        channel = channel_order[idx]
        items: list[_PrItem] = []
        for li in container.select("li"):
            a = li.find("a", onclick=True)
            if not a:
                continue
            onclick = a.get("onclick") or ""
            details_ref = _parse_onclick_details_ref(onclick)
            title = _extract_pr_programme_title(a)
            start = None
            start_span = li.select_one("span.sTime") or li.select_one(".emitedNowProgrammeStartHour")
            if start_span:
                start = _parse_time_hhmm(_clean_text(start_span.get_text()))
            if not title:
                continue
            items.append(_PrItem(start_time=start, title=title, details_ref=details_ref))
        by_channel[channel] = items
    return by_channel


def _parse_pr_details_ref(details_ref: str) -> dict[str, str]:
    schedule_id, programme_id, start_time_s, selected_date = details_ref.split("|", 3)
    return {
        "scheduleId": schedule_id,
        "programmeId": programme_id,
        "startTime": start_time_s,
        "selectedDate": selected_date,
    }


def _normalize_pr_description(text: str) -> str:
    t = _clean_multiline_text(text)
    if not t:
        return ""
    lowered = t.casefold()
    if lowered in {"s", ".", "-", "—", "–"}:
        return ""
    if len(t) <= 2:
        return ""
    return t


@dataclass(frozen=True)
class _PrDetailsPopup:
    start_time: str
    title: str
    lead: str
    description: str
    programme_href: str | None


def _parse_pr_programme_details_popup_html(html_text: str) -> _PrDetailsPopup:
    soup = BeautifulSoup(html_text, "lxml")

    start_time_el = soup.select_one("#programmeDetails_lblProgrammeStartTime")
    programme_title_el = soup.select_one("#programmeDetails_lblProgrammeTitle")
    lead_el = soup.select_one("#programmeDetails_lblProgrammeLead")
    description_el = soup.select_one("#programmeDetails_lblProgrammeDescription")
    website_el = soup.select_one("#programmeDetails_hypProgrammeWebsite")

    start_time_s = _clean_text(start_time_el.get_text(" ")) if start_time_el else ""
    title_s = _clean_text(programme_title_el.get_text(" ")) if programme_title_el else ""
    lead_s = _clean_multiline_text(lead_el.get_text("\n")) if lead_el else ""

    desc_s = ""
    if description_el:
        desc_s = _clean_multiline_text(description_el.get_text("\n"))

    programme_href = None
    if website_el:
        href = website_el.get("href")
        if isinstance(href, str) and href.strip():
            programme_href = href.strip()

    return _PrDetailsPopup(
        start_time=start_time_s,
        title=title_s,
        lead=_normalize_pr_description(lead_s),
        description=_normalize_pr_description(desc_s),
        programme_href=programme_href,
    )


@dataclass(frozen=True)
class _PrProgrammePageDetails:
    lead: str
    description: str


def _parse_pr_programme_page_html(html_text: str) -> _PrProgrammePageDetails:
    soup = BeautifulSoup(html_text, "lxml")
    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        return _PrProgrammePageDetails(lead="", description="")

    try:
        data = json.loads(script.string)
    except json.JSONDecodeError:
        return _PrProgrammePageDetails(lead="", description="")

    details = data.get("props", {}).get("pageProps", {}).get("details", {})
    if not isinstance(details, dict):
        return _PrProgrammePageDetails(lead="", description="")

    lead_raw = details.get("lead", "")
    desc_html = details.get("description", "")

    lead = _clean_multiline_text(str(lead_raw)) if lead_raw else ""

    desc_text = ""
    if desc_html:
        desc_text = _clean_multiline_text(BeautifulSoup(str(desc_html), "lxml").get_text("\n"))

    return _PrProgrammePageDetails(
        lead=_normalize_pr_description(lead),
        description=_normalize_pr_description(desc_text),
    )


def _format_details(*, header: str, lead: str, description: str) -> str:
    parts = [p for p in (header, lead, description) if p]
    return "\n\n".join(parts)


def fetch_polskieradio_details_text(details_ref: str) -> str:
    html_text = _post_form_text(PR_DETAILS_URL, _parse_pr_details_ref(details_ref))
    popup = _parse_pr_programme_details_popup_html(html_text)

    lead = popup.lead
    description = popup.description

    if (not lead and not description) and popup.programme_href:
        programme_url = urljoin(PR_BASE, popup.programme_href)
        try:
            programme_html = _fetch_text(programme_url)
        except Exception:  # noqa: BLE001
            programme_html = ""

        if programme_html:
            programme = _parse_pr_programme_page_html(programme_html)
            if programme.lead:
                lead = programme.lead
            if programme.description:
                description = programme.description

    header = " ".join([p for p in (popup.start_time, popup.title) if p])
    return _format_details(header=header, lead=lead, description=description)


def refresh_polskieradio_day(
    conn: psycopg.Connection,
    *,
    day: date,
    request_delay_seconds: float = 0.0,
) -> int:
    html_text = _post_form_text(PR_MULTISCHEDULE_URL, {"selectedDate": day.isoformat()})
    by_channel = parse_pr_multischedule_html(html_text, PR_CHANNELS)

    # The endpoint sometimes ignores the requested date and returns a schedule for a different day
    # (typically \"today\"). Detect that and avoid polluting the DB with incorrect dates.
    requested_day_s = day.isoformat()
    actual_day_s: str | None = None
    for items in by_channel.values():
        for it in items:
            if not it.details_ref:
                continue
            parts = it.details_ref.split("|", 3)
            if len(parts) == 4:
                actual_day_s = parts[3]
                break
        if actual_day_s:
            break

    if actual_day_s and actual_day_s != requested_day_s:
        return 0

    inserted = 0

    with conn.transaction():
        for channel in PR_CHANNELS:
            _upsert_source(conn, provider_id="polskieradio", source_id=channel, name=channel)
            conn.execute(
                "DELETE FROM schedule_item WHERE provider_id=%s AND source_id=%s AND day=%s",
                ("polskieradio", channel, day),
            )

            for it in by_channel.get(channel, []):
                if it.start_time is None:
                    continue
                conn.execute(
                    """
                    INSERT INTO schedule_item (
                      provider_id, source_id, day, start_time,
                      title, subtitle, details_ref, details_summary
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (provider_id, source_id, day, start_time, title) DO UPDATE
                      SET details_ref = excluded.details_ref
                    """,
                    ("polskieradio", channel, day, it.start_time, it.title, None, it.details_ref, None),
                )
                inserted += 1

        conn.execute(
            """
            INSERT INTO fetch_state (key, updated_at)
            VALUES (%s, now())
            ON CONFLICT (key) DO UPDATE SET updated_at = excluded.updated_at
            """,
            (f"polskieradio:multischedule:{day.isoformat()}",),
        )

    if request_delay_seconds > 0:
        time_mod.sleep(request_delay_seconds)

    return inserted


# ---------------------------------------------------------------------------
# Radio Kierowców
# ---------------------------------------------------------------------------

RK_BASE = "https://radiokierowcow.pl"


@dataclass(frozen=True)
class _RkProgramme:
    start: time | None
    title: str
    lead: str
    description: str


def _weekday_template_date(year: int, weekday: int) -> date:
    d = date(year, 1, 1)
    delta = (-d.weekday()) % 7
    first_monday = d + timedelta(days=delta)
    return first_monday + timedelta(days=weekday)


def _parse_time_hhmmss(text: str) -> time | None:
    t = _clean_text(text)
    if len(t) >= 5 and t[2] == ":":
        t = t[:5]
    return _parse_time_hhmm(t)


def _parse_rk_schedule_json(text: str) -> list[_RkProgramme]:
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return []
    if not isinstance(data, dict):
        return []

    raw_items = data.get("data")
    if not isinstance(raw_items, list):
        return []

    programmes: list[_RkProgramme] = []
    for raw in raw_items:
        if not isinstance(raw, dict):
            continue
        title = _clean_text(str(raw.get("title") or ""))
        if not title:
            continue
        start = _parse_time_hhmmss(str(raw.get("startTime") or ""))
        lead = _clean_multiline_text(str(raw.get("lead") or ""))
        desc = _clean_multiline_text(str(raw.get("currentDescription") or ""))
        programmes.append(_RkProgramme(start=start, title=title, lead=lead, description=desc))
    return programmes


def _fetch_rk_programmes(day: date) -> list[_RkProgramme]:
    url = f"{RK_BASE}/api/Schedule/Get?date={day.isoformat()}"
    text = _fetch_text(url)
    return _parse_rk_schedule_json(text)


def refresh_radiokierowcow_day(
    conn: psycopg.Connection,
    *,
    day: date,
    request_delay_seconds: float = 0.0,
) -> int:
    programmes = _fetch_rk_programmes(day)
    if not programmes:
        weekday = day.weekday()
        for years_back in range(1, 6):
            candidate = _weekday_template_date(day.year - years_back, weekday)
            programmes = _fetch_rk_programmes(candidate)
            if programmes:
                break

    with conn.transaction():
        _upsert_source(conn, provider_id="radiokierowcow", source_id="prk", name="Polskie Radio Kierowców")
        conn.execute(
            "DELETE FROM schedule_item WHERE provider_id=%s AND source_id=%s AND day=%s",
            ("radiokierowcow", "prk", day),
        )
        inserted = 0
        for p in programmes:
            if p.start is None:
                continue
            details = "\n\n".join([x for x in (p.lead, p.description) if x])
            conn.execute(
                """
                INSERT INTO schedule_item (
                  provider_id, source_id, day, start_time,
                  title, subtitle, details_ref, details_summary
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (provider_id, source_id, day, start_time, title) DO UPDATE
                  SET details_summary = excluded.details_summary
                """,
                ("radiokierowcow", "prk", day, p.start, p.title, None, None, details or None),
            )
            inserted += 1

        conn.execute(
            """
            INSERT INTO fetch_state (key, updated_at)
            VALUES (%s, now())
            ON CONFLICT (key) DO UPDATE SET updated_at = excluded.updated_at
            """,
            (f"radiokierowcow:schedule:{day.isoformat()}",),
        )

    if request_delay_seconds > 0:
        time_mod.sleep(request_delay_seconds)

    return inserted


# ---------------------------------------------------------------------------
# Radio Nowy Świat
# ---------------------------------------------------------------------------

RNS_BASE = "https://nowyswiat.online"


@dataclass(frozen=True)
class _RnsProgramme:
    start: time | None
    title: str
    details: str


def _parse_rns_ramowka_html(html_text: str) -> list[_RnsProgramme]:
    soup = BeautifulSoup(html_text, "lxml")
    day_container = soup.select_one("li.rns-switcher-grid-element") or soup
    out: list[_RnsProgramme] = []
    for li in day_container.select("li.rns-switcher-single"):
        time_el = li.select_one(".rns-switcher-time")
        start = _parse_time_hhmm(_clean_text(time_el.get_text(" "))) if time_el else None

        title_el = li.select_one(".rns-switcher-title")
        title = _clean_text(title_el.get_text(" ")) if title_el else ""
        if not title:
            continue

        names_el = li.select_one(".rns-switcher-names")
        details = ""
        if names_el:
            raw = _clean_multiline_text(names_el.get_text("\n"))
            lines: list[str] = []
            pending_comma = False
            for ln in [x.strip() for x in raw.splitlines()]:
                if not ln or ln == "|":
                    continue
                if ln == ",":
                    pending_comma = True
                    continue
                if ln.startswith(":") and lines:
                    lines[-1] = lines[-1].rstrip() + ln
                    pending_comma = False
                    continue
                if pending_comma and lines:
                    lines[-1] = lines[-1].rstrip() + ", " + ln
                    pending_comma = False
                    continue
                pending_comma = False
                lines.append(ln)
            details = "\n".join(lines)

        out.append(_RnsProgramme(start=start, title=title, details=details))
    return out


def refresh_nowyswiat_day(
    conn: psycopg.Connection,
    *,
    day: date,
    request_delay_seconds: float = 0.0,
) -> int:
    url = f"{RNS_BASE}/ramowka?search={day.isoformat()}"
    html_text = _fetch_text(url)
    programmes = _parse_rns_ramowka_html(html_text)

    with conn.transaction():
        _upsert_source(conn, provider_id="nowyswiat", source_id="rns", name="Radio Nowy Świat")
        conn.execute(
            "DELETE FROM schedule_item WHERE provider_id=%s AND source_id=%s AND day=%s",
            ("nowyswiat", "rns", day),
        )
        inserted = 0
        for p in programmes:
            if p.start is None:
                continue
            conn.execute(
                """
                INSERT INTO schedule_item (
                  provider_id, source_id, day, start_time,
                  title, subtitle, details_ref, details_summary
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (provider_id, source_id, day, start_time, title) DO UPDATE
                  SET details_summary = excluded.details_summary
                """,
                ("nowyswiat", "rns", day, p.start, p.title, None, None, p.details or None),
            )
            inserted += 1

        conn.execute(
            """
            INSERT INTO fetch_state (key, updated_at)
            VALUES (%s, now())
            ON CONFLICT (key) DO UPDATE SET updated_at = excluded.updated_at
            """,
            (f"nowyswiat:ramowka:{day.isoformat()}",),
        )

    if request_delay_seconds > 0:
        time_mod.sleep(request_delay_seconds)

    return inserted


# ---------------------------------------------------------------------------
# Radio 357
# ---------------------------------------------------------------------------

R357_URL = "https://radio357.pl/ramowka/"


@dataclass(frozen=True)
class _R357Programme:
    start: time | None
    title: str
    details: str


def _parse_ddmm(text: str) -> tuple[int, int] | None:
    t = _clean_text(text)
    if not t:
        return None
    m = re.search(r"(\d{1,2})\.(\d{1,2})", t)
    if not m:
        return None
    day = int(m.group(1))
    month = int(m.group(2))
    if not (1 <= day <= 31 and 1 <= month <= 12):
        return None
    return (day, month)


def _closest_date_with_day_month(anchor: date, ddmm: tuple[int, int]) -> date:
    day, month = ddmm
    candidates: list[date] = []
    for year in (anchor.year - 1, anchor.year, anchor.year + 1):
        try:
            candidates.append(date(year, month, day))
        except ValueError:
            continue
    if not candidates:
        return anchor
    return min(candidates, key=lambda d: abs((d - anchor).days))


def _normalize_author_text(text: str) -> str:
    t = _clean_text(text)
    if not t:
        return ""
    t = re.sub(r"\s*,\s*", ", ", t)
    t = re.sub(r",\s*$", "", t)
    t = _clean_text(t)
    lowered = t.casefold()
    if lowered in {"s", ".", "-", "—", "–"}:
        return ""
    if len(t) <= 2:
        return ""
    return t


def _find_today_index(nav_items, today: date) -> int:
    for idx, nav in enumerate(nav_items):
        date_label_el = nav.select_one(".scheduleDate")
        date_label = _clean_text(date_label_el.get_text(" ")) if date_label_el else ""
        if date_label.casefold() in {"dzisiaj", "dziś", "today"}:
            return idx

    for idx, nav in enumerate(nav_items):
        date_label_el = nav.select_one(".scheduleDate")
        date_label = _clean_text(date_label_el.get_text(" ")) if date_label_el else ""
        ddmm = _parse_ddmm(date_label)
        if ddmm and ddmm == (today.day, today.month):
            return idx

    return len(nav_items) // 2


def parse_r357_ramowka_html(html_text: str, *, today: date | None = None) -> dict[date, list[_R357Programme]]:
    soup = BeautifulSoup(html_text, "lxml")
    nav_items = soup.select("#scheduleNav .scheduleWrap")
    slides = soup.select("#scheduleList .swiper-wrapper > .swiper-slide")

    count = min(len(nav_items), len(slides))
    if count == 0:
        return {}

    current_day = today or date.today()
    idx_today = _find_today_index(nav_items[:count], current_day)

    days_by_index: list[date] = []
    for index, nav in enumerate(nav_items[:count]):
        computed = current_day + timedelta(days=index - idx_today)
        date_label_el = nav.select_one(".scheduleDate")
        date_label = _clean_text(date_label_el.get_text(" ")) if date_label_el else ""
        ddmm = _parse_ddmm(date_label)
        if ddmm and (computed.day, computed.month) != ddmm:
            computed = _closest_date_with_day_month(computed, ddmm)
        days_by_index.append(computed)

    by_day: dict[date, list[_R357Programme]] = {}
    for index in range(count):
        day = days_by_index[index]
        slide = slides[index]
        programmes: list[_R357Programme] = []
        for el in slide.select(".podcastElement"):
            time_el = el.select_one(".podcastHour span.h2")
            start = _parse_time_hhmm(_clean_text(time_el.get_text(" "))) if time_el else None

            title_el = el.select_one("h3.podcastSubTitle")
            title = _clean_text(title_el.get_text(" ")) if title_el else ""
            if not title:
                continue

            author_el = el.select_one(".podcastAuthor")
            author = _normalize_author_text(_clean_text(author_el.get_text(" ", strip=True)) if author_el else "")

            desc_el = el.select_one(".podcastDesc")
            description = _clean_multiline_text(desc_el.get_text("\n")) if desc_el else ""

            details_parts = [part for part in (author, description) if part]
            details = "\n\n".join(details_parts)

            programmes.append(_R357Programme(start=start, title=title, details=details))

        by_day[day] = programmes
    return by_day


def refresh_radio357_week(
    conn: psycopg.Connection,
    *,
    request_delay_seconds: float = 0.0,
) -> int:
    html_text = _fetch_text(R357_URL)
    week = parse_r357_ramowka_html(html_text)

    today = date.today()
    max_day = today + timedelta(days=14)

    with conn.transaction():
        _upsert_source(conn, provider_id="radio357", source_id="r357", name="Radio 357")

        inserted = 0
        for day, programmes in week.items():
            if day < today or day > max_day:
                continue

            conn.execute(
                "DELETE FROM schedule_item WHERE provider_id=%s AND source_id=%s AND day=%s",
                ("radio357", "r357", day),
            )
            for p in programmes:
                if p.start is None:
                    continue
                conn.execute(
                    """
                    INSERT INTO schedule_item (
                      provider_id, source_id, day, start_time,
                      title, subtitle, details_ref, details_summary
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (provider_id, source_id, day, start_time, title) DO UPDATE
                      SET details_summary = excluded.details_summary
                    """,
                    ("radio357", "r357", day, p.start, p.title, None, None, p.details or None),
                )
                inserted += 1

        conn.execute(
            """
            INSERT INTO fetch_state (key, updated_at)
            VALUES (%s, now())
            ON CONFLICT (key) DO UPDATE SET updated_at = excluded.updated_at
            """,
            ("radio357:ramowka",),
        )

    if request_delay_seconds > 0:
        time_mod.sleep(request_delay_seconds)

    return inserted


# ---------------------------------------------------------------------------
# Radio Olsztyn
# ---------------------------------------------------------------------------

RO_BASE = "https://radioolsztyn.pl"
RO_SCHEDULE_INDEX_URL = f"{RO_BASE}/mvc/ramowka/date/"


@dataclass(frozen=True)
class _RoProgramme:
    start: time | None
    title: str
    details: str


def parse_ro_days_html(html_text: str) -> list[date]:
    soup = BeautifulSoup(html_text, "lxml")
    out: set[date] = set()
    for a in soup.select('a[href*="/mvc/ramowka/date/"]'):
        href = _clean_text(a.get("href") or "")
        m = re.search(r"/mvc/ramowka/date/(\d{4}-\d{2}-\d{2})\b", href)
        if not m:
            continue
        try:
            out.add(date.fromisoformat(m.group(1)))
        except ValueError:
            continue
    return sorted(out)


def parse_ro_ramowka_html(html_text: str) -> list[_RoProgramme]:
    soup = BeautifulSoup(html_text, "lxml")
    programmes: list[_RoProgramme] = []

    for inner in soup.select(".ramowkaItemInner"):
        header = inner.select_one(".ramowkaItemHeader")
        if not header:
            continue

        title_el = header.select_one(".ramowkaTitleLink, .ramowkaTitleNoLink")
        if not title_el:
            continue

        time_el = title_el.select_one("b")
        start = _parse_time_hhmm(_clean_text(time_el.get_text(" "))) if time_el else None
        start_s = start.strftime("%H:%M") if start else ""

        title_text = _clean_text(title_el.get_text(" ", strip=True))
        title = _clean_text(title_text[len(start_s) :]) if start_s and title_text.startswith(start_s) else title_text
        if not title:
            continue

        opis_el = inner.select_one(".ramowkaItemOpis")
        details = _clean_multiline_text(opis_el.get_text("\n")) if opis_el else ""

        programmes.append(_RoProgramme(start=start, title=title, details=details))

    seen: set[tuple[str, str, str]] = set()
    out: list[_RoProgramme] = []
    for p in programmes:
        key = (p.start.strftime("%H:%M") if p.start else "", p.title.casefold(), p.details.casefold())
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
    return out


def refresh_radioolsztyn_index(
    conn: psycopg.Connection,
    *,
    request_delay_seconds: float = 0.0,
) -> list[date]:
    html_text = _fetch_text(RO_SCHEDULE_INDEX_URL)
    days = parse_ro_days_html(html_text)
    with conn.transaction():
        conn.execute(
            """
            INSERT INTO fetch_state (key, updated_at)
            VALUES (%s, now())
            ON CONFLICT (key) DO UPDATE SET updated_at = excluded.updated_at
            """,
            ("radioolsztyn:index",),
        )
    if request_delay_seconds > 0:
        time_mod.sleep(request_delay_seconds)
    return days


def refresh_radioolsztyn_day(
    conn: psycopg.Connection,
    *,
    day: date,
    request_delay_seconds: float = 0.0,
) -> int:
    url = f"{RO_SCHEDULE_INDEX_URL}{day.isoformat()}"
    html_text = _fetch_text(url)
    programmes = parse_ro_ramowka_html(html_text)

    with conn.transaction():
        _upsert_source(conn, provider_id="radioolsztyn", source_id="olsztyn", name="Radio Olsztyn")
        conn.execute(
            "DELETE FROM schedule_item WHERE provider_id=%s AND source_id=%s AND day=%s",
            ("radioolsztyn", "olsztyn", day),
        )

        inserted = 0
        for p in programmes:
            if p.start is None:
                continue
            conn.execute(
                """
                INSERT INTO schedule_item (
                  provider_id, source_id, day, start_time,
                  title, subtitle, details_ref, details_summary
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (provider_id, source_id, day, start_time, title) DO UPDATE
                  SET details_summary = excluded.details_summary
                """,
                ("radioolsztyn", "olsztyn", day, p.start, p.title, None, None, p.details or None),
            )
            inserted += 1

        conn.execute(
            """
            INSERT INTO fetch_state (key, updated_at)
            VALUES (%s, now())
            ON CONFLICT (key) DO UPDATE SET updated_at = excluded.updated_at
            """,
            (f"radioolsztyn:ramowka:{day.isoformat()}",),
        )

    if request_delay_seconds > 0:
        time_mod.sleep(request_delay_seconds)

    return inserted


# ---------------------------------------------------------------------------
# Radio Poznań
# ---------------------------------------------------------------------------

RP_BASE = "https://radiopoznan.fm"


@dataclass(frozen=True)
class _RpProgramme:
    start: time | None
    title: str
    details_ref: str | None


def _parse_rp_start_time(range_text: str) -> time | None:
    t = _clean_text(range_text)
    m = re.search(r"\b(\d{1,2}:\d{2})\b", t)
    if not m:
        return None
    value = m.group(1)
    if len(value) == 4:
        value = "0" + value
    return _parse_time_hhmm(value)


def parse_rp_program_html(html_text: str) -> list[_RpProgramme]:
    soup = BeautifulSoup(html_text, "lxml")
    container = soup.select_one("#play_list") or soup

    items: list[_RpProgramme] = []
    for li in container.select("li"):
        time_el = li.select_one("span.time")
        if not time_el:
            continue
        time_text = _clean_text(time_el.get_text(" "))
        start = _parse_rp_start_time(time_text)

        a = li.select_one("a[href]")
        details_ref = _clean_text(a.get("href") or "") if a else ""
        details_ref = details_ref or None

        if a:
            title = _clean_text(a.get_text(" "))
        else:
            raw = _clean_text(li.get_text(" ", strip=True))
            if time_text and raw.startswith(time_text):
                raw = raw[len(time_text) :]
            title = _clean_text(raw)

        if not title:
            continue

        items.append(_RpProgramme(start=start, title=title, details_ref=details_ref))

    seen: set[tuple[str, str, str]] = set()
    out: list[_RpProgramme] = []
    for it in items:
        key = (it.start.strftime("%H:%M") if it.start else "", it.title.casefold(), (it.details_ref or "").casefold())
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out


def parse_rp_audycje_details_html(html_text: str) -> str:
    soup = BeautifulSoup(html_text, "lxml")

    article = soup.select_one("article.rm-broadcast") or soup.select_one("article.rm-news-item") or soup.select_one(
        "article"
    )
    if article:
        h2 = article.select_one("h2")
        title = _clean_text(h2.get_text(" ")) if h2 else ""

        p = article.select_one("p")
        body = _clean_multiline_text(p.get_text("\n")) if p else ""

        parts = [p for p in (title, body) if p]
        if parts:
            return "\n\n".join(parts)

    meta = soup.select_one('meta[name="description"]')
    if meta and meta.get("content"):
        return _clean_text(str(meta.get("content")))
    return ""


def fetch_radiopoznan_details_text(details_ref: str) -> str:
    url = urljoin(RP_BASE, details_ref)
    html_text = _fetch_text(url)
    return parse_rp_audycje_details_html(html_text)


def refresh_radiopoznan_day(
    conn: psycopg.Connection,
    *,
    day: date,
    request_delay_seconds: float = 0.0,
) -> int:
    html_text = _fetch_text(f"{RP_BASE}/program/{day.isoformat()}.html")
    programmes = parse_rp_program_html(html_text)

    with conn.transaction():
        _upsert_source(conn, provider_id="radiopoznan", source_id="poznan", name="Radio Poznań")
        conn.execute(
            "DELETE FROM schedule_item WHERE provider_id=%s AND source_id=%s AND day=%s",
            ("radiopoznan", "poznan", day),
        )

        inserted = 0
        for p in programmes:
            if p.start is None:
                continue
            conn.execute(
                """
                INSERT INTO schedule_item (
                  provider_id, source_id, day, start_time,
                  title, subtitle, details_ref, details_summary
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (provider_id, source_id, day, start_time, title) DO UPDATE
                  SET details_ref = excluded.details_ref
                """,
                ("radiopoznan", "poznan", day, p.start, p.title, None, p.details_ref, None),
            )
            inserted += 1

        conn.execute(
            """
            INSERT INTO fetch_state (key, updated_at)
            VALUES (%s, now())
            ON CONFLICT (key) DO UPDATE SET updated_at = excluded.updated_at
            """,
            (f"radiopoznan:program:{day.isoformat()}",),
        )

    if request_delay_seconds > 0:
        time_mod.sleep(request_delay_seconds)

    return inserted


# ---------------------------------------------------------------------------
# Radio Wrocław
# ---------------------------------------------------------------------------

RW_BASE = "https://www.radiowroclaw.pl"


@dataclass(frozen=True)
class _RwProgramme:
    start: time | None
    title: str
    details: str


def parse_rw_broadcasts_html(html_text: str) -> list[_RwProgramme]:
    soup = BeautifulSoup(html_text, "lxml")
    table = soup.select_one("table.broadcast") or soup

    programmes: list[_RwProgramme] = []
    for row in table.select("tr.row"):
        start_el = row.select_one("td.start")
        start = _parse_time_hhmm(_clean_text(start_el.get_text(" "))) if start_el else None

        info = row.select_one("td.info") or row
        title_el = info.select_one("strong")
        title = _clean_text(title_el.get_text(" ")) if title_el else ""
        if not title:
            continue

        raw_descs: list[str] = []
        for desc_el in info.select("div.desc"):
            desc = _clean_multiline_text(desc_el.get_text("\n"))
            if desc:
                raw_descs.append(desc)

        seen_desc: set[str] = set()
        descs: list[str] = []
        for desc in raw_descs:
            key = desc.casefold()
            if key in seen_desc:
                continue
            seen_desc.add(key)
            descs.append(desc)
        details = "\n\n".join(descs)

        programmes.append(_RwProgramme(start=start, title=title, details=details))

    seen: set[tuple[str, str, str]] = set()
    out: list[_RwProgramme] = []
    for p in programmes:
        key = (p.start.strftime("%H:%M") if p.start else "", p.title.casefold(), p.details.casefold())
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
    return out


def refresh_radiowroclaw_weekdays(
    *,
    request_delay_seconds: float = 0.0,
) -> dict[int, list[_RwProgramme]]:
    by_weekday: dict[int, list[_RwProgramme]] = {}
    for weekday in range(1, 8):
        html_text = _fetch_text(f"{RW_BASE}/broadcasts/view/{weekday}")
        by_weekday[weekday] = parse_rw_broadcasts_html(html_text)
        if request_delay_seconds > 0:
            time_mod.sleep(request_delay_seconds)
    return by_weekday


def upsert_radiowroclaw_days(
    conn: psycopg.Connection,
    *,
    by_weekday: dict[int, list[_RwProgramme]],
    days: list[date],
) -> int:
    inserted = 0
    with conn.transaction():
        _upsert_source(conn, provider_id="radiowroclaw", source_id="wroclaw", name="Radio Wrocław")
        for day in days:
            weekday = day.isoweekday()
            programmes = by_weekday.get(weekday, [])

            conn.execute(
                "DELETE FROM schedule_item WHERE provider_id=%s AND source_id=%s AND day=%s",
                ("radiowroclaw", "wroclaw", day),
            )
            for p in programmes:
                if p.start is None:
                    continue
                conn.execute(
                    """
                    INSERT INTO schedule_item (
                      provider_id, source_id, day, start_time,
                      title, subtitle, details_ref, details_summary
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (provider_id, source_id, day, start_time, title) DO UPDATE
                      SET details_summary = excluded.details_summary
                    """,
                    ("radiowroclaw", "wroclaw", day, p.start, p.title, None, None, p.details or None),
                )
                inserted += 1

        conn.execute(
            """
            INSERT INTO fetch_state (key, updated_at)
            VALUES (%s, now())
            ON CONFLICT (key) DO UPDATE SET updated_at = excluded.updated_at
            """,
            ("radiowroclaw:week",),
        )

    return inserted


# ---------------------------------------------------------------------------
# TOK FM
# ---------------------------------------------------------------------------

TOKFM_SCHEDULE_URL = "https://audycje.tokfm.pl/ramowka"


@dataclass(frozen=True)
class _TokProgramme:
    start: time | None
    title: str
    details: str
    details_ref: str | None


def _uniq_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for v in values:
        key = v.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(v)
    return out


def parse_tokfm_ramowka_html(html_text: str) -> dict[int, list[_TokProgramme]]:
    soup = BeautifulSoup(html_text, "lxml")
    out: dict[int, list[_TokProgramme]] = {}

    for weekday in range(1, 8):
        ul = soup.select_one(f"ul.tok-schedule__el_{weekday}")
        if not ul:
            continue

        programmes: list[_TokProgramme] = []
        for entry in ul.select("li.tok-schedule__entry"):
            time_el = entry.select_one(".tok-schedule__time")
            start = _parse_time_hhmm(_clean_text(time_el.get_text(" "))) if time_el else None

            h3s = entry.select("h3.tok-schedule__program--name")
            show_title = _clean_text(h3s[0].get_text(" ")) if len(h3s) >= 1 else ""
            episode_title = _clean_text(h3s[1].get_text(" ")) if len(h3s) >= 2 else ""

            show_href = ""
            if len(h3s) >= 1:
                a = h3s[0].select_one("a[href]")
                show_href = _clean_text(a.get("href") or "") if a else ""

            episode_href = ""
            if len(h3s) >= 2:
                a = h3s[1].select_one("a[href]")
                episode_href = _clean_text(a.get("href") or "") if a else ""

            title = show_title
            if episode_title and episode_title.casefold() != show_title.casefold():
                title = f"{show_title} — {episode_title}" if show_title else episode_title
            title = _clean_text(title)
            if not title:
                continue

            leaders: list[str] = []
            for a in entry.select(".tok-schedule__program--leader-name a"):
                name = _clean_text(a.get_text(" "))
                if name:
                    leaders.append(name)
            details = ", ".join(_uniq_strings(leaders))

            details_ref = episode_href or show_href
            details_ref = details_ref or None

            programmes.append(_TokProgramme(start=start, title=title, details=details, details_ref=details_ref))

        seen: set[tuple[str, str]] = set()
        deduped: list[_TokProgramme] = []
        for p in programmes:
            key = (p.start.strftime("%H:%M") if p.start else "", p.title.casefold())
            if key in seen:
                continue
            seen.add(key)
            deduped.append(p)

        out[weekday] = deduped

    return out


def parse_tokfm_details_html(html_text: str) -> str:
    soup = BeautifulSoup(html_text, "lxml")
    meta = soup.select_one('meta[name="description"]')
    if meta and meta.get("content"):
        return _clean_text(str(meta.get("content")))
    og = soup.select_one('meta[property="og:description"]')
    if og and og.get("content"):
        return _clean_text(str(og.get("content")))
    return ""


def fetch_tokfm_details_text(details_ref: str) -> str:
    html_text = _fetch_text(details_ref)
    return parse_tokfm_details_html(html_text)


def refresh_tokfm_week(
    conn: psycopg.Connection,
    *,
    request_delay_seconds: float = 0.0,
) -> dict[int, list[_TokProgramme]]:
    html_text = _fetch_text(TOKFM_SCHEDULE_URL)
    by_weekday = parse_tokfm_ramowka_html(html_text)

    with conn.transaction():
        conn.execute(
            """
            INSERT INTO fetch_state (key, updated_at)
            VALUES (%s, now())
            ON CONFLICT (key) DO UPDATE SET updated_at = excluded.updated_at
            """,
            ("tokfm:ramowka",),
        )

    if request_delay_seconds > 0:
        time_mod.sleep(request_delay_seconds)

    return by_weekday


def upsert_tokfm_days(
    conn: psycopg.Connection,
    *,
    by_weekday: dict[int, list[_TokProgramme]],
    days: list[date],
) -> int:
    inserted = 0
    with conn.transaction():
        _upsert_source(conn, provider_id="tokfm", source_id="tokfm", name="TOK FM")
        for day in days:
            weekday = day.isoweekday()
            programmes = by_weekday.get(weekday, [])

            conn.execute(
                "DELETE FROM schedule_item WHERE provider_id=%s AND source_id=%s AND day=%s",
                ("tokfm", "tokfm", day),
            )
            for p in programmes:
                if p.start is None:
                    continue
                conn.execute(
                    """
                    INSERT INTO schedule_item (
                      provider_id, source_id, day, start_time,
                      title, subtitle, details_ref, details_summary
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (provider_id, source_id, day, start_time, title) DO UPDATE
                      SET details_ref = excluded.details_ref,
                          details_summary = excluded.details_summary
                    """,
                    ("tokfm", "tokfm", day, p.start, p.title, None, p.details_ref, p.details or None),
                )
                inserted += 1

    return inserted


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def ensure_radio_providers(conn: psycopg.Connection) -> None:
    providers = [
        ("polskieradio", "radio", "Polskie Radio"),
        ("radiokierowcow", "radio", "Polskie Radio Kierowców"),
        ("nowyswiat", "radio", "Radio Nowy Świat"),
        ("radio357", "radio", "Radio 357"),
        ("radioolsztyn", "radio", "Radio Olsztyn"),
        ("radiopoznan", "radio", "Radio Poznań"),
        ("radiowroclaw", "radio", "Radio Wrocław"),
        ("tokfm", "radio", "TOK FM"),
    ]

    with conn.transaction():
        for provider_id, kind, display_name in providers:
            _upsert_provider(conn, provider_id=provider_id, kind=kind, display_name=display_name)

        for ch in PR_CHANNELS:
            _upsert_source(conn, provider_id="polskieradio", source_id=ch, name=ch)
        _upsert_source(conn, provider_id="radiokierowcow", source_id="prk", name="Polskie Radio Kierowców")
        _upsert_source(conn, provider_id="nowyswiat", source_id="rns", name="Radio Nowy Świat")
        _upsert_source(conn, provider_id="radio357", source_id="r357", name="Radio 357")
        _upsert_source(conn, provider_id="radioolsztyn", source_id="olsztyn", name="Radio Olsztyn")
        _upsert_source(conn, provider_id="radiopoznan", source_id="poznan", name="Radio Poznań")
        _upsert_source(conn, provider_id="radiowroclaw", source_id="wroclaw", name="Radio Wrocław")
        _upsert_source(conn, provider_id="tokfm", source_id="tokfm", name="TOK FM")

    conn.commit()
