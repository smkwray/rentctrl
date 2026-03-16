"""Berkeley Rent Registry public-search helpers.

This module uses the live public Berkeley rent-registry API rather than the
stale informational landing page on rentboard.berkeleyca.gov. The current
bounded pilot is address-keyed search, not a unit-level lawful-rent extractor.
"""
from __future__ import annotations

import base64
import json
import subprocess
import tempfile
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pandas as pd
import requests

REGISTRY_HOME_URL = "https://rentregistry.cityofberkeley.info/#/homepage"
REGISTRY_API_BASE = "https://rentregistry-api.cityofberkeley.info"
CONFIG_REFRESH_URL = f"{REGISTRY_API_BASE}/rest/uiengine/config/v1/get_dataonrefresh"
NONCE_URL = f"{REGISTRY_API_BASE}/rest/idm/account/v1/get_nonce"
AUTH_URL = f"{REGISTRY_API_BASE}/rest/oauth/authenticate"
SEARCH_COUNT_URL = f"{REGISTRY_API_BASE}/rest/uiengine/app/v1/searchcontentcount"
SEARCH_RESULT_URL = f"{REGISTRY_API_BASE}/rest/uiengine/app/v1/searchcontent"

SEARCH_DISPLAY = ["CSD", "SRTYPE", "FAQ", "BLOG", "DMS", "APN", "ADDRESS", "CASE", "CRM"]
PILOT_SEARCH_DISPLAY = ["ADDRESS"]
DEFAULT_CITY_DIRECTORY = ["ALL"]
PUBLIC_REALM = "Citizen"
PUBLIC_AUTHTYPE = "apiclient"
PUBLIC_SCOPE = "write"
RESTAPP_BASIC_AUTH = "Basic cmVzdGFwcDpyZXN0YXBw"

EXPECTED_FIELDS = [
    "keyword",
    "type",
    "title",
    "apn_number",
    "full_address",
    "total_units",
    "site_address_count",
    "matching_count",
]

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/html,application/xhtml+xml,*/*",
}


class BerkeleyLookupError(RuntimeError):
    """Raised when the public Berkeley registry flow fails."""


class _TextExtractor(HTMLParser):
    """Collect visible text from HTML."""

    def __init__(self) -> None:
        super().__init__()
        self.chunks: list[str] = []
        self._skip = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in ("script", "style"):
            self._skip = True

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style"):
            self._skip = False

    def handle_data(self, data: str) -> None:
        if not self._skip:
            stripped = data.strip()
            if stripped:
                self.chunks.append(stripped)


@dataclass(frozen=True)
class BerkeleyGuestCredentials:
    username: str
    password: str


def _extract_visible_text(html: str) -> list[str]:
    parser = _TextExtractor()
    parser.feed(html)
    return parser.chunks


def discover_lookup_mechanics() -> dict[str, Any]:
    """Return the live public API mechanics for Berkeley's registry."""
    credentials = get_homepage_credentials()
    return {
        "registry_home_url": REGISTRY_HOME_URL,
        "config_refresh_url": CONFIG_REFRESH_URL,
        "nonce_url": NONCE_URL,
        "auth_url": AUTH_URL,
        "search_count_url": SEARCH_COUNT_URL,
        "search_result_url": SEARCH_RESULT_URL,
        "public_username": credentials.username,
        "public_password_present": bool(credentials.password),
        "search_display": SEARCH_DISPLAY,
        "city_directory": DEFAULT_CITY_DIRECTORY,
        "blocker_summary": (
            "Public API confirmed. Guest credentials are exposed through the "
            "registry config endpoint, password auth requires RSA encryption "
            "against the nonce public key, and address/APN search works via "
            "the uiengine search endpoints."
        ),
    }


# Small bounded address sample for the initial public pilot.
PILOT_ADDRESSES: list[dict[str, str]] = [
    {"address": "2000 University Ave", "unit": ""},
    {"address": "2100 Milvia St", "unit": ""},
    {"address": "1500 Shattuck Ave", "unit": ""},
]


def get_homepage_credentials(*, timeout: int = 30, session: requests.Session | None = None) -> BerkeleyGuestCredentials:
    client = session or requests.Session()
    try:
        resp = client.get(CONFIG_REFRESH_URL, headers=_BROWSER_HEADERS, timeout=timeout)
    except requests.RequestException as exc:
        raise BerkeleyLookupError(f"Failed to fetch registry config: {exc}") from exc
    if resp.status_code != 200:
        raise BerkeleyLookupError(f"HTTP {resp.status_code} from {CONFIG_REFRESH_URL}")
    payload = resp.json()
    params = {item.get("paramName"): item.get("paramValue") for item in payload.get("configParam", [])}
    username = params.get("homePageLoginUserName")
    password = params.get("homePageLoginPassword")
    if not username or not password:
        raise BerkeleyLookupError("Guest homepage credentials were not exposed in config data")
    return BerkeleyGuestCredentials(username=username, password=password)


def get_public_key(*, timeout: int = 30, session: requests.Session | None = None) -> str:
    client = session or requests.Session()
    try:
        resp = client.get(NONCE_URL, headers=_BROWSER_HEADERS, timeout=timeout)
    except requests.RequestException as exc:
        raise BerkeleyLookupError(f"Failed to fetch nonce public key: {exc}") from exc
    if resp.status_code != 200:
        raise BerkeleyLookupError(f"HTTP {resp.status_code} from {NONCE_URL}")
    payload = resp.json()
    public_key = payload.get("nonce", {}).get("publicKey")
    if not public_key:
        raise BerkeleyLookupError("Nonce response did not include a public key")
    return public_key


def encrypt_password_with_openssl(password: str, public_key: str) -> str:
    pem = "-----BEGIN PUBLIC KEY-----\n" + public_key + "\n-----END PUBLIC KEY-----\n"
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".pem") as tmp:
        tmp.write(pem)
        pem_path = Path(tmp.name)
    try:
        proc = subprocess.run(
            ["openssl", "pkeyutl", "-encrypt", "-pubin", "-inkey", str(pem_path)],
            input=password.encode("utf-8"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
    except FileNotFoundError as exc:
        raise BerkeleyLookupError("openssl is required for Berkeley registry guest auth") from exc
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.decode("utf-8", errors="ignore").strip()
        raise BerkeleyLookupError(f"openssl encryption failed: {stderr or exc}") from exc
    finally:
        pem_path.unlink(missing_ok=True)
    return quote(base64.b64encode(proc.stdout).decode("ascii"), safe="")


def authenticate_public_session(*, timeout: int = 30, session: requests.Session | None = None) -> str:
    client = session or requests.Session()
    creds = get_homepage_credentials(timeout=timeout, session=client)
    public_key = get_public_key(timeout=timeout, session=client)
    encrypted_password = encrypt_password_with_openssl(creds.password, public_key)
    body = (
        f"scope={PUBLIC_SCOPE}"
        f"&grant_type=password"
        f"&authType={PUBLIC_AUTHTYPE}"
        f"&password={encrypted_password}"
        f"&realm={PUBLIC_REALM}"
        f"&username={creds.username}"
    )
    headers = {
        "Authorization": RESTAPP_BASIC_AUTH,
        "Content-Type": "application/x-www-form-urlencoded",
        **_BROWSER_HEADERS,
    }
    try:
        resp = client.post(AUTH_URL, headers=headers, data=body, timeout=timeout)
    except requests.RequestException as exc:
        raise BerkeleyLookupError(f"Guest auth failed: {exc}") from exc
    if resp.status_code != 200:
        raise BerkeleyLookupError(f"HTTP {resp.status_code} from {AUTH_URL}: {resp.text[:200]}")
    payload = resp.json()
    token = payload.get("access_token")
    if not token:
        raise BerkeleyLookupError("Guest auth succeeded without an access token")
    return token


def build_search_payload(keyword: str, *, page_number: int = 1, page_size: int = 20, services: list[str] | None = None) -> dict[str, Any]:
    if services is None:
        services = PILOT_SEARCH_DISPLAY
    return {
        "pageNumber": page_number,
        "pageSize": page_size,
        "cityService": services,
        "cityDirectory": DEFAULT_CITY_DIRECTORY,
        "language": "en",
        "keyword": keyword,
    }


def _authorized_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "language": "en",
        **_BROWSER_HEADERS,
    }




def _curl_json_post(url: str, *, headers: dict[str, str], payload: dict[str, Any], timeout: int = 30) -> dict[str, Any]:
    cmd = [
        "curl",
        "--silent",
        "--show-error",
        "--fail-with-body",
        "--max-time",
        str(timeout),
        "-X",
        "POST",
        url,
    ]
    for key, value in headers.items():
        cmd.extend(["-H", f"{key}: {value}"])
    try:
        proc = subprocess.run(
            cmd + ["--data-binary", "@-"],
            input=json.dumps(payload).encode("utf-8"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
    except FileNotFoundError as exc:
        raise BerkeleyLookupError("curl is required for Berkeley registry search calls") from exc
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.decode("utf-8", errors="ignore").strip()
        raise BerkeleyLookupError(f"curl POST failed for {url}: {stderr or exc}") from exc
    try:
        return json.loads(proc.stdout.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise BerkeleyLookupError(f"Non-JSON response from {url}") from exc

def search_content_count(keyword: str, *, token: str, timeout: int = 30, session: requests.Session | None = None) -> dict[str, Any]:
    payload = build_search_payload(keyword)
    return _curl_json_post(
        SEARCH_COUNT_URL,
        headers=_authorized_headers(token),
        payload=payload,
        timeout=timeout,
    )


def search_content(keyword: str, *, token: str, timeout: int = 30, session: requests.Session | None = None, page_size: int = 20) -> dict[str, Any]:
    payload = build_search_payload(keyword, page_size=page_size)
    return _curl_json_post(
        SEARCH_RESULT_URL,
        headers=_authorized_headers(token),
        payload=payload,
        timeout=timeout,
    )


def parse_search_results(payload: dict[str, Any], *, keyword: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for result in payload.get("response", []) or []:
        params = result.get("parameters") or {}
        additional = params.get("additional") or {}
        site_addresses = additional.get("siteAddresses") or []
        first_site = site_addresses[0] if site_addresses else {}
        rows.append(
            {
                "keyword": keyword,
                "type": result.get("type", ""),
                "title": result.get("title", ""),
                "apn_number": additional.get("apnNumber", ""),
                "full_address": first_site.get("fullAddress", ""),
                "total_units": additional.get("totalUnits"),
                "site_address_count": len(site_addresses),
                "matching_count": result.get("matchingCount"),
                "raw_parameters_json": json.dumps(params, sort_keys=True),
            }
        )
    return pd.DataFrame(rows)


def run_pilot_sample(addresses: list[dict[str, str]] | None = None, *, timeout: int = 30) -> tuple[pd.DataFrame, pd.DataFrame]:
    if addresses is None:
        addresses = PILOT_ADDRESSES
    session = requests.Session()
    token = authenticate_public_session(timeout=timeout, session=session)
    detail_frames: list[pd.DataFrame] = []
    summary_rows: list[dict[str, Any]] = []
    for entry in addresses:
        keyword = entry["address"]
        try:
            count_payload = search_content_count(keyword, token=token, timeout=timeout, session=session)
            result_payload = search_content(keyword, token=token, timeout=timeout, session=session)
            detail = parse_search_results(result_payload, keyword=keyword)
            if detail.empty:
                detail = pd.DataFrame(
                    [{field: "" for field in EXPECTED_FIELDS} | {"keyword": keyword, "fetch_status": "ok_no_results"}]
                )
            else:
                detail["fetch_status"] = "ok"
            counts = count_payload.get("count", {})
            summary_rows.append(
                {
                    "keyword": keyword,
                    "rows_returned": int(result_payload.get("totalRecord", len(detail.index)) or 0),
                    "address_rows": int((detail["type"] == "ADDRESS").sum()) if "type" in detail else 0,
                    "apn_rows": int((detail["type"] == "APN").sum()) if "type" in detail else 0,
                    "case_rows": int((detail["type"] == "CASE").sum()) if "type" in detail else 0,
                    "count_address": int(counts.get("ADDRESS", 0) or 0),
                    "count_apn": int(counts.get("APN", 0) or 0),
                    "count_case": int(counts.get("CASE", 0) or 0),
                    "unique_apn_numbers": int(detail["apn_number"].replace("", pd.NA).dropna().nunique()) if "apn_number" in detail else 0,
                    "fetch_status": "ok",
                }
            )
        except BerkeleyLookupError as exc:
            detail = pd.DataFrame(
                [{field: "" for field in EXPECTED_FIELDS} | {"keyword": keyword, "fetch_status": f"error: {exc}"}]
            )
            summary_rows.append(
                {
                    "keyword": keyword,
                    "rows_returned": 0,
                    "address_rows": 0,
                    "apn_rows": 0,
                    "case_rows": 0,
                    "count_address": 0,
                    "count_apn": 0,
                    "count_case": 0,
                    "unique_apn_numbers": 0,
                    "fetch_status": f"error: {exc}",
                }
            )
        detail_frames.append(detail)
    detail_df = pd.concat(detail_frames, ignore_index=True) if detail_frames else pd.DataFrame(columns=EXPECTED_FIELDS + ["fetch_status"])
    summary_df = pd.DataFrame(summary_rows)
    return detail_df, summary_df


def summarize_pilot(summary_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df.empty:
        return pd.DataFrame(columns=["metric", "value"])
    rows = [
        {"metric": "total_keywords", "value": int(summary_df["keyword"].nunique())},
        {"metric": "successful_keywords", "value": int((summary_df["fetch_status"] == "ok").sum())},
        {"metric": "total_rows_returned", "value": int(summary_df["rows_returned"].sum())},
        {"metric": "total_address_rows", "value": int(summary_df["address_rows"].sum())},
        {"metric": "total_apn_rows", "value": int(summary_df["apn_rows"].sum())},
        {"metric": "total_case_rows", "value": int(summary_df["case_rows"].sum())},
        {"metric": "total_unique_apn_numbers", "value": int(summary_df["unique_apn_numbers"].sum())},
    ]
    return pd.DataFrame(rows)
