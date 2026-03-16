from __future__ import annotations

import re
from html import unescape
from typing import Iterable

import pandas as pd
import requests

MAR_URL = "https://www.smgov.net/departments/rentcontrol/mar.aspx"

PILOT_QUERIES: list[dict[str, str]] = [
    {"street_number": "624", "street_name": "Lincoln Blvd"},
    {"street_number": "2012", "street_name": "10th St"},
    {"street_number": "", "street_name": "Colorado Ave"},
]


def _extract_hidden_value(html: str, field_name: str) -> str:
    pattern = rf'name="{re.escape(field_name)}"[^>]*value="([^"]*)"'
    match = re.search(pattern, html, flags=re.IGNORECASE)
    return unescape(match.group(1)) if match else ""


def fetch_mar_page(*, timeout: int = 60, session: requests.Session | None = None) -> str:
    client = session or requests.Session()
    response = client.get(MAR_URL, timeout=timeout)
    response.raise_for_status()
    return response.text


def submit_mar_lookup(
    street_number: str,
    street_name: str,
    *,
    timeout: int = 60,
    session: requests.Session | None = None,
) -> str:
    client = session or requests.Session()
    landing = fetch_mar_page(timeout=timeout, session=client)
    payload = {
        "__VIEWSTATE": _extract_hidden_value(landing, "__VIEWSTATE"),
        "__VIEWSTATEGENERATOR": _extract_hidden_value(landing, "__VIEWSTATEGENERATOR"),
        "__EVENTVALIDATION": _extract_hidden_value(landing, "__EVENTVALIDATION"),
        "ctl00$mainContent$txtStNumber": street_number,
        "ctl00$mainContent$txtStreet": street_name,
        "ctl00$mainContent$btnSearch": "Search",
    }
    response = client.post(MAR_URL, data=payload, timeout=timeout)
    response.raise_for_status()
    return response.text


def _clean_cell(value: str) -> str:
    value = unescape(re.sub(r"<[^>]+>", " ", value))
    value = value.replace("\xa0", " ")
    return " ".join(value.split())


def parse_mar_results(html: str) -> pd.DataFrame:
    table_match = re.search(
        r'<table[^>]+id="ctl00_mainContent_gvMarData"[^>]*>(.*?)</table>',
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not table_match:
        return pd.DataFrame(
            columns=[
                "address",
                "unit",
                "mar",
                "tenancy_date",
                "bedrooms",
                "parcel",
            ]
        )

    rows: list[dict[str, str]] = []
    body = table_match.group(1)
    for row_html in re.findall(r"<tr[^>]*>(.*?)</tr>", body, flags=re.IGNORECASE | re.DOTALL):
        if "<th" in row_html.lower():
            continue
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row_html, flags=re.IGNORECASE | re.DOTALL)
        if len(cells) != 6:
            continue
        cleaned = [_clean_cell(cell) for cell in cells]
        rows.append(
            {
                "address": cleaned[0],
                "unit": cleaned[1],
                "mar": cleaned[2],
                "tenancy_date": cleaned[3],
                "bedrooms": cleaned[4],
                "parcel": cleaned[5],
            }
        )
    return pd.DataFrame(rows)


def run_mar_pilot(
    queries: Iterable[dict[str, str]] | None = None,
    *,
    timeout: int = 60,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    client = session or requests.Session()
    frames: list[pd.DataFrame] = []
    for query in queries or PILOT_QUERIES:
        html = submit_mar_lookup(
            query.get("street_number", ""),
            query.get("street_name", ""),
            timeout=timeout,
            session=client,
        )
        frame = parse_mar_results(html)
        frame["search_street_number"] = query.get("street_number", "")
        frame["search_street_name"] = query.get("street_name", "")
        frame["lookup_success"] = not frame.empty
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


def summarize_mar_pilot(results: pd.DataFrame, *, queries_run: int | None = None) -> pd.DataFrame:
    if results.empty:
        return pd.DataFrame(
            [
                {"metric": "queries_run", "value": int(queries_run or 0)},
                {"metric": "rows_returned", "value": 0},
                {"metric": "unique_addresses", "value": 0},
                {"metric": "unique_parcels", "value": 0},
            ]
        )

    return pd.DataFrame(
        [
            {
                "metric": "queries_run",
                "value": int(
                    queries_run
                    if queries_run is not None
                    else results[["search_street_number", "search_street_name"]].drop_duplicates().shape[0]
                ),
            },
            {"metric": "rows_returned", "value": int(len(results))},
            {"metric": "unique_addresses", "value": int(results["address"].nunique(dropna=True))},
            {"metric": "unique_parcels", "value": int(results["parcel"].replace("", pd.NA).dropna().nunique())},
            {"metric": "rows_with_tenancy_date", "value": int(results["tenancy_date"].replace("", pd.NA).dropna().shape[0])},
            {"metric": "rows_with_bedroom_count", "value": int(results["bedrooms"].replace("", pd.NA).dropna().shape[0])},
        ]
    )
