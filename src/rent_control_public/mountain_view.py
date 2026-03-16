from __future__ import annotations

import base64
from typing import Any

import pandas as pd
import requests

PUBLIC_UI_BASE = "https://mvrent.mountainview.gov"
PUBLIC_API_BASE = "https://mvrent-api.mountainview.gov"
AUTH_ENDPOINT = "/rest/oauth/authenticate"
SEARCH_COUNT_ENDPOINT = "/rest/uiengine/app/v1/searchcontentcount"
SEARCH_RESULT_ENDPOINT = "/rest/uiengine/app/v1/searchcontent"
PUBLIC_USERNAME = "VAP"
PUBLIC_PASSWORD = "VAPAPP"
PUBLIC_REALM = "Citizen"
PUBLIC_AUTH_TYPE = "apiclient"
PUBLIC_SCOPE = "write"
PUBLIC_CITY_SERVICES = ["CSD", "SRTYPE", "FAQ", "BLOG", "DMS", "APN", "ADDRESS", "CASE", "CRM"]
PUBLIC_CITY_DIRECTORY = ["ALL"]


def basic_restapp_header() -> str:
    token = base64.b64encode(b"restapp:restapp").decode("ascii")
    return f"Basic {token}"


def authenticate_public_session(timeout: int = 30) -> dict[str, Any]:
    response = requests.post(
        f"{PUBLIC_API_BASE}{AUTH_ENDPOINT}",
        data={
            "scope": PUBLIC_SCOPE,
            "grant_type": "password",
            "authType": PUBLIC_AUTH_TYPE,
            "password": PUBLIC_PASSWORD,
            "realm": PUBLIC_REALM,
            "username": PUBLIC_USERNAME,
        },
        headers={
            "Authorization": basic_restapp_header(),
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json, text/plain, */*",
            "Referer": f"{PUBLIC_UI_BASE}/",
        },
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def build_search_payload(keyword: str, *, page_number: int = 1, page_size: int = 20) -> dict[str, Any]:
    return {
        "pageNumber": page_number,
        "pageSize": page_size,
        "cityService": PUBLIC_CITY_SERVICES,
        "cityDirectory": PUBLIC_CITY_DIRECTORY,
        "language": "en",
        "keyword": keyword,
    }


def _authorized_headers(access_token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json, text/plain, */*",
        "Origin": PUBLIC_UI_BASE,
        "Referer": f"{PUBLIC_UI_BASE}/",
    }


def search_content_count(keyword: str, *, access_token: str, timeout: int = 30) -> dict[str, Any]:
    response = requests.post(
        f"{PUBLIC_API_BASE}{SEARCH_COUNT_ENDPOINT}",
        json=build_search_payload(keyword, page_size=5),
        headers=_authorized_headers(access_token),
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def search_content(keyword: str, *, access_token: str, page_size: int = 20, timeout: int = 30) -> dict[str, Any]:
    response = requests.post(
        f"{PUBLIC_API_BASE}{SEARCH_RESULT_ENDPOINT}",
        json=build_search_payload(keyword, page_size=page_size),
        headers=_authorized_headers(access_token),
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def parse_search_results(response_json: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for item in response_json.get("response", []):
        additional = ((item.get("parameters") or {}).get("additional") or {})
        site_addresses = additional.get("siteAddresses") or []
        primary_address = site_addresses[0] if site_addresses else {}
        other_attributes = additional.get("otherAttributes") or {}
        rows.append(
            {
                "title": item.get("title"),
                "type": item.get("type"),
                "matching_count": item.get("matchingCount"),
                "apn_number": additional.get("apnNumber"),
                "asset_type": additional.get("assetType"),
                "total_units": additional.get("totalUnits"),
                "rental_fee_paid": additional.get("rentalFeePaid"),
                "prev_rental_fee_paid": additional.get("prevRentalFeePaid"),
                "full_address": primary_address.get("fullAddress") or additional.get("address"),
                "house_number": primary_address.get("houseNumber"),
                "street_name": primary_address.get("streetName"),
                "street_type": primary_address.get("streetTypeCd"),
                "city": primary_address.get("city"),
                "state": primary_address.get("state"),
                "zip": primary_address.get("zip"),
                "latitude": primary_address.get("latitude") or additional.get("latitude"),
                "longitude": primary_address.get("longitude") or additional.get("longitude"),
                "case_id": additional.get("caseId"),
                "case_type": additional.get("caseType"),
                "category": additional.get("category"),
                "created_on": additional.get("createdOn"),
                "state_name": additional.get("stateName"),
                "annual_cycle_tag": other_attributes.get("annualCycleTag"),
                "case_view_type": other_attributes.get("caseViewType"),
                "note": other_attributes.get("note"),
            }
        )
    return pd.DataFrame(rows)


def summarize_results(df: pd.DataFrame, *, keyword: str, count_json: dict[str, Any]) -> pd.DataFrame:
    type_counts = df["type"].value_counts(dropna=False).to_dict() if not df.empty else {}
    return pd.DataFrame(
        [
            {"metric": "keyword", "value": keyword},
            {"metric": "rows_returned", "value": int(len(df))},
            {"metric": "apn_rows", "value": int(type_counts.get("APN", 0))},
            {"metric": "case_rows", "value": int(type_counts.get("CASE", 0))},
            {"metric": "address_rows", "value": int(type_counts.get("ADDRESS", 0))},
            {"metric": "count_apn", "value": int((count_json.get("count") or {}).get("APN", 0))},
            {"metric": "count_case", "value": int((count_json.get("count") or {}).get("CASE", 0))},
            {"metric": "count_address", "value": int((count_json.get("count") or {}).get("ADDRESS", 0))},
            {
                "metric": "fully_covered_rows",
                "value": int(df["asset_type"].eq("Fully Covered Rental Property").sum()) if "asset_type" in df else 0,
            },
        ]
    )


def summarize_keyword_batch(results: pd.DataFrame, counts_by_keyword: dict[str, dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    per_keyword_rows: list[dict[str, Any]] = []
    for keyword, group in results.groupby("search_keyword", dropna=False):
        type_counts = group["type"].value_counts(dropna=False).to_dict()
        count_json = counts_by_keyword.get(str(keyword), {})
        count_map = count_json.get("count") or {}
        per_keyword_rows.append(
            {
                "keyword": keyword,
                "rows_returned": int(len(group)),
                "apn_rows": int(type_counts.get("APN", 0)),
                "case_rows": int(type_counts.get("CASE", 0)),
                "address_rows": int(type_counts.get("ADDRESS", 0)),
                "count_apn": int(count_map.get("APN", 0)),
                "count_case": int(count_map.get("CASE", 0)),
                "count_address": int(count_map.get("ADDRESS", 0)),
                "fully_covered_rows": int(group["asset_type"].eq("Fully Covered Rental Property").sum()),
            }
        )

    per_keyword = pd.DataFrame(per_keyword_rows).sort_values("keyword").reset_index(drop=True)

    case_types = (
        results[results["type"] == "CASE"]["case_type"]
        .fillna("Unknown")
        .value_counts()
        .rename_axis("case_type")
        .reset_index(name="rows")
    )
    asset_types = (
        results[results["type"] == "APN"]["asset_type"]
        .fillna("Unknown")
        .value_counts()
        .rename_axis("asset_type")
        .reset_index(name="rows")
    )
    return per_keyword, case_types, asset_types
