from __future__ import annotations

import re
from html import unescape
from urllib.parse import quote, urljoin

import pandas as pd
import requests

BASE_URL = "https://www.cityofepa.org"
SEARCH_URL_TEMPLATE = BASE_URL + "/search/node/{query}"
USER_AGENT = "Mozilla/5.0"


def _clean_html_text(value: str) -> str:
    value = re.sub(r"<[^>]+>", " ", value)
    value = unescape(value).replace("\xa0", " ")
    return " ".join(value.split())


def fetch_search_page(query: str, *, page: int = 0, timeout: int = 60, session: requests.Session | None = None) -> str:
    client = session or requests.Session()
    url = SEARCH_URL_TEMPLATE.format(query=quote(query))
    if page:
        url = f"{url}?page={page}"
    response = client.get(url, timeout=timeout, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    return response.text


def parse_search_results(html: str, *, page: int) -> pd.DataFrame:
    rows: list[dict[str, str | int]] = []
    for chunk in re.findall(r'<li class="search-result">(.*?)</li>', html, flags=re.IGNORECASE | re.DOTALL):
        link_match = re.search(r'<a href="([^"]+)">(.*?)</a>', chunk, flags=re.IGNORECASE | re.DOTALL)
        if not link_match:
            continue
        href = urljoin(BASE_URL, unescape(link_match.group(1)))
        title = _clean_html_text(link_match.group(2))
        snippet_match = re.search(r'<p class="search-snippet">(.*?)</p>', chunk, flags=re.IGNORECASE | re.DOTALL)
        snippet = _clean_html_text(snippet_match.group(1)) if snippet_match else ""
        rows.append({"page": page, "title": title, "url": href, "snippet": snippet})
    return pd.DataFrame(rows)


def fetch_event_page(url: str, *, timeout: int = 60, session: requests.Session | None = None) -> str:
    client = session or requests.Session()
    response = client.get(url, timeout=timeout, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    return response.text


def parse_event_page(html: str, *, url: str) -> dict[str, object]:
    title_match = re.search(r"<title>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    date_match = re.search(r'<span class="date-display-single"[^>]*>(.*?)</span>', html, flags=re.IGNORECASE | re.DOTALL)
    ical_match = re.search(r'href="([^"]*calendar\.ics)"', html, flags=re.IGNORECASE)
    google_match = re.search(r'href="(https://calendar\.google\.com/[^"]+)"', html, flags=re.IGNORECASE)
    agenda_match = re.search(r'<h2><a href="([^"]+)">View Agenda Here</a></h2>', html, flags=re.IGNORECASE)
    repeat_match = re.search(r"<div class='repeat_rule_expand'><div><ul>(.*?)</ul></div></div>", html, flags=re.IGNORECASE | re.DOTALL)
    repeat_dates = []
    if repeat_match:
        repeat_dates = [_clean_html_text(item) for item in re.findall(r"<li>(.*?)</li>", repeat_match.group(1), flags=re.IGNORECASE | re.DOTALL)]
    clean_title = _clean_html_text(title_match.group(1)) if title_match else ""
    return {
        "url": url,
        "title": clean_title.replace("| City of East Palo Alto", "").strip(),
        "event_datetime": _clean_html_text(date_match.group(1)) if date_match else "",
        "ical_url": urljoin(BASE_URL, unescape(ical_match.group(1))) if ical_match else "",
        "google_url": unescape(google_match.group(1)) if google_match else "",
        "agenda_url": urljoin(BASE_URL, unescape(agenda_match.group(1))) if agenda_match else "",
        "is_canceled": "cancel" in clean_title.lower(),
        "repeat_dates_count": len(repeat_dates),
        "repeat_dates_preview": " | ".join(repeat_dates[:5]),
    }


def fetch_board_archive(
    *,
    query: str = "Rent Stabilization Board",
    max_pages: int = 5,
    timeout: int = 60,
    session: requests.Session | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    client = session or requests.Session()
    search_frames: list[pd.DataFrame] = []
    event_rows: list[dict[str, object]] = []
    seen_urls: set[str] = set()
    for page in range(max_pages):
        html = fetch_search_page(query, page=page, timeout=timeout, session=client)
        results = parse_search_results(html, page=page)
        if results.empty:
            continue
        search_frames.append(results)
        for row in results.itertuples(index=False):
            if "/rent-stabilization/page/" not in row.url:
                continue
            if row.url in seen_urls:
                continue
            seen_urls.add(row.url)
            event_html = fetch_event_page(row.url, timeout=timeout, session=client)
            event_row = parse_event_page(event_html, url=row.url)
            if not _is_board_event_row(event_row):
                continue
            event_rows.append(event_row)
    search_df = pd.concat(search_frames, ignore_index=True) if search_frames else pd.DataFrame(columns=["page", "title", "url", "snippet"])
    event_df = pd.DataFrame(event_rows)
    return search_df, event_df


def summarize_board_archive(search_df: pd.DataFrame, event_df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"metric": "search_rows", "value": int(len(search_df))},
            {"metric": "unique_search_urls", "value": int(search_df["url"].nunique()) if not search_df.empty else 0},
            {"metric": "event_pages", "value": int(len(event_df))},
            {"metric": "event_pages_with_agenda_url", "value": int(event_df["agenda_url"].astype(bool).sum()) if not event_df.empty else 0},
            {"metric": "event_pages_with_ical_url", "value": int(event_df["ical_url"].astype(bool).sum()) if not event_df.empty else 0},
            {"metric": "canceled_event_pages", "value": int(event_df["is_canceled"].sum()) if not event_df.empty else 0},
        ]
    )


def _is_board_event_row(row: dict[str, object]) -> bool:
    title = str(row.get("title") or "").lower()
    return bool(
        row.get("event_datetime")
        or row.get("ical_url")
        or row.get("agenda_url")
        or ("board" in title and "meeting" in title)
    )
