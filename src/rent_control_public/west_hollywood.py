from __future__ import annotations

import calendar
import io
import re
import subprocess
from pathlib import Path

import pandas as pd
import requests

RSO_PDF_URL = (
    "https://www.weho.org/home/showpublisheddocument/21438/635786832495570000"
)

RSO_PAGE_URL = (
    "https://www.weho.org/city-government/rent-stabilization/"
    "rental-housing/for-tenants/rent-stabilized-units"
)
BUYOUT_CSV_URL = "https://data.weho.org/api/views/di8z-6ihr/rows.csv?accessType=DOWNLOAD"
SEISMIC_CSV_URL = "https://data.weho.org/api/views/52pw-42ra/rows.csv?accessType=DOWNLOAD"

RSO_COLUMNS = ["address", "unit", "parcel"]
MINUTES_APPEAL_COLUMNS = [
    "meeting_date",
    "application_id",
    "appeal_address",
    "appeal_unit",
    "source_file",
]

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/pdf,*/*",
}


class WeHoDownloadError(RuntimeError):
    """Raised when the RSO PDF cannot be downloaded with actionable detail."""


def download_rso_pdf(dest: Path, *, timeout: int = 120) -> Path:
    """Download the West Hollywood rent-stabilized addresses PDF.

    Tries with browser-like headers to work around Akamai WAF.
    On failure, raises ``WeHoDownloadError`` with specific diagnostics.
    """
    try:
        resp = requests.get(
            RSO_PDF_URL, headers=_BROWSER_HEADERS, timeout=timeout
        )
    except requests.ConnectionError as exc:
        raise WeHoDownloadError(
            f"Connection failed for {RSO_PDF_URL}: {exc}"
        ) from exc
    except requests.Timeout as exc:
        raise WeHoDownloadError(
            f"Request timed out after {timeout}s for {RSO_PDF_URL}"
        ) from exc

    if resp.status_code == 403:
        server = resp.headers.get("server", "unknown")
        raise WeHoDownloadError(
            f"HTTP 403 from {RSO_PDF_URL} (server: {server}). "
            f"The city's CDN (likely Akamai) blocks automated downloads. "
            f"Workaround: download the PDF manually from {RSO_PAGE_URL} "
            f"and pass the local path via --pdf-path."
        )

    if resp.status_code != 200:
        raise WeHoDownloadError(
            f"HTTP {resp.status_code} from {RSO_PDF_URL}. "
            f"Check whether the document ID has changed at {RSO_PAGE_URL}."
        )

    content_type = resp.headers.get("content-type", "")
    if "pdf" not in content_type and len(resp.content) < 5000:
        raise WeHoDownloadError(
            f"Response content-type is '{content_type}' (expected PDF). "
            f"The URL may have changed. Check {RSO_PAGE_URL}."
        )

    dest.write_bytes(resp.content)
    return dest


def extract_pdf_text(pdf_path: Path) -> str:
    """Extract text from the RSO PDF using pdftotext."""
    result = subprocess.run(
        ["pdftotext", "-layout", str(pdf_path), "-"],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def parse_rso_text(text: str) -> pd.DataFrame:
    """Parse the West Hollywood rent-stabilized addresses PDF text.

    The PDF is a unit-level list with columns for street address, unit, and
    parcel number. The exact layout may vary, so this parser uses heuristics:
    it looks for lines that contain a parcel-number pattern (a sequence of
    digits, possibly with dashes) and treats preceding text as address/unit.
    """
    rows: list[dict[str, str]] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        # Skip obvious header / footer lines
        if _is_header_or_footer(stripped):
            continue
        parsed = _parse_address_line(stripped)
        if parsed is not None:
            rows.append(parsed)
    return pd.DataFrame(rows, columns=RSO_COLUMNS)


def _is_header_or_footer(line: str) -> bool:
    upper = line.upper()
    if "RENT STABILIZED" in upper and "ADDRESS" in upper:
        return True
    if "PAGE" in upper and re.search(r"PAGE\s+\d+", upper):
        return True
    if upper.startswith("ADDRESS") or upper.startswith("UNIT") or upper.startswith("PARCEL"):
        return True
    if upper.startswith("CITY OF WEST HOLLYWOOD"):
        return True
    if "PREPARED BY" in upper or "AS OF" in upper:
        return True
    return False


# Parcel numbers in LA County APN format: typically ####-###-### or similar
_PARCEL_RE = re.compile(r"(\d{4}[-\s]?\d{3}[-\s]?\d{3})")


def _parse_address_line(line: str) -> dict[str, str] | None:
    """Try to extract address, unit, and parcel from a single line."""
    m = _PARCEL_RE.search(line)
    if m is None:
        return None
    parcel = re.sub(r"[\s-]", "", m.group(1))
    # Normalize parcel to ####-###-### format
    if len(parcel) == 10:
        parcel = f"{parcel[:4]}-{parcel[4:7]}-{parcel[7:]}"
    prefix = line[: m.start()].strip()
    # Try to split address and unit; unit is often the last token if it's
    # short (e.g., "101", "A", "2B") preceded by '#' or 'APT' or 'UNIT'
    address, unit = _split_address_unit(prefix)
    return {"address": address, "unit": unit, "parcel": parcel}


_UNIT_SPLIT_RE = re.compile(
    r"^(.*?)\s+(?:#|APT\.?|UNIT\.?|STE\.?|SUITE\.?)\s*(\S+)\s*$", re.IGNORECASE
)


def _split_address_unit(text: str) -> tuple[str, str]:
    m = _UNIT_SPLIT_RE.match(text)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    # Fallback: if the last whitespace-separated token looks like a unit
    # (short alphanumeric), split it off only if there are at least 3 tokens
    parts = text.rsplit(None, 1)
    if len(parts) == 2 and len(parts[1]) <= 4 and parts[1] not in ("ST", "DR", "AVE", "BLVD", "CT", "PL", "RD", "LN", "WAY", "CIR"):
        candidate = parts[1]
        if re.match(r"^\d{1,4}[A-Z]?$", candidate, re.IGNORECASE):
            return parts[0].strip(), candidate.strip()
    return text.strip(), ""


def summarize_rso_stock(df: pd.DataFrame) -> pd.DataFrame:
    """Produce a summary of the parsed rent-stabilized stock."""
    total_rows = len(df)
    unique_addresses = df["address"].nunique()
    unique_parcels = df["parcel"].nunique()
    has_unit = (df["unit"] != "").sum()
    summary = pd.DataFrame(
        [
            {
                "metric": "total_rows",
                "value": total_rows,
            },
            {
                "metric": "unique_addresses",
                "value": unique_addresses,
            },
            {
                "metric": "unique_parcels",
                "value": unique_parcels,
            },
            {
                "metric": "rows_with_unit",
                "value": int(has_unit),
            },
        ]
    )
    return summary


_CITY_STATE_ZIP_RE = re.compile(
    r",\s*WEST\s+HOLLYWOOD\s*,?\s*CA(?:LIFORNIA)?(?:\s+\d{5}(?:-\d{4})?)?$",
    re.IGNORECASE,
)
_EXTRA_SPACE_RE = re.compile(r"\s+")
_LEADING_DIR_RE = re.compile(r"^\s*([NSEW])\s+")

_SUFFIX_REPLACEMENTS = {
    "AVENUE": "AVE",
    "BOULEVARD": "BLVD",
    "DRIVE": "DR",
    "STREET": "ST",
    "PLACE": "PL",
    "COURT": "CT",
    "ROAD": "RD",
    "LANE": "LN",
    "TERRACE": "TER",
    "CIRCLE": "CIR",
}


def normalize_address(value: str) -> str:
    """Normalize West Hollywood addresses for string joins."""
    if not isinstance(value, str):
        return ""
    text = value.upper().strip()
    text = _CITY_STATE_ZIP_RE.sub("", text)
    text = text.replace(",", " ")
    text = text.replace(".", "")
    text = text.replace("½", "1/2")
    text = re.sub(r"\s*/\s*", "/", text)
    text = _EXTRA_SPACE_RE.sub(" ", text).strip()
    text = re.sub(r"\s+#\s*", " #", text)
    parts = text.split()
    normalized_parts: list[str] = []
    for part in parts:
        normalized_parts.append(_SUFFIX_REPLACEMENTS.get(part, part))
    text = " ".join(normalized_parts)
    text = _EXTRA_SPACE_RE.sub(" ", text).strip()
    return text


def normalize_unit(value: str) -> str:
    """Normalize unit strings for lightweight matching."""
    if not isinstance(value, str):
        return ""
    text = value.upper().strip()
    text = text.replace(".", "")
    text = text.replace("UNIT ", "")
    text = text.replace("APT ", "")
    text = text.replace("#", "")
    text = _EXTRA_SPACE_RE.sub("", text)
    return text


def normalize_parcel(value: str) -> str:
    """Normalize parcel/APN strings to ####-###-### when possible."""
    if not isinstance(value, str):
        return ""
    digits = re.sub(r"[^0-9]", "", value)
    if len(digits) == 10:
        return f"{digits[:4]}-{digits[4:7]}-{digits[7:]}"
    return value.strip().upper()


def prepare_rso_linkage(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["normalized_address"] = out["address"].map(normalize_address)
    out["normalized_unit"] = out["unit"].fillna("").map(normalize_unit)
    out["normalized_parcel"] = out["parcel"].fillna("").map(normalize_parcel)
    return out


def download_csv_dataframe(url: str, *, timeout: int = 120) -> pd.DataFrame:
    """Download a CSV-backed public dataset into a dataframe."""
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return pd.read_csv(io.StringIO(response.text))


def load_buyout_tracking(*, timeout: int = 120) -> pd.DataFrame:
    df = download_csv_dataframe(BUYOUT_CSV_URL, timeout=timeout)
    df["normalized_address"] = df["Address"].fillna("").map(normalize_address)
    return df


def load_seismic_retrofit(*, timeout: int = 120) -> pd.DataFrame:
    df = download_csv_dataframe(SEISMIC_CSV_URL, timeout=timeout)
    df["normalized_parcel_list"] = df["APN"].fillna("").map(_split_parcel_list)
    return df


def _split_parcel_list(value: str) -> list[str]:
    if not isinstance(value, str) or not value.strip():
        return []
    parts = [normalize_parcel(part) for part in value.split(" - ")]
    return [part for part in parts if part]


def parse_minutes_directory(minutes_dir: Path) -> pd.DataFrame:
    """Parse bounded commission-minute PDFs into appeal-level rows."""
    rows: list[dict[str, str]] = []
    for pdf_path in sorted(minutes_dir.glob("*.pdf")):
        text = extract_pdf_text(pdf_path)
        meeting_date = _parse_meeting_date(text) or _parse_meeting_date_from_filename(
            pdf_path.name
        )
        for appeal in extract_appeals_from_minutes_text(text):
            rows.append(
                {
                    "meeting_date": meeting_date,
                    "application_id": appeal["application_id"],
                    "appeal_address": appeal["appeal_address"],
                    "appeal_unit": appeal["appeal_unit"],
                    "source_file": pdf_path.name,
                }
            )
    return pd.DataFrame(rows, columns=MINUTES_APPEAL_COLUMNS)


_DATE_RE = re.compile(
    r"(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s+(\d{1,2}),\s+(\d{4})"
)
_APPEAL_LINE_RE = re.compile(
    r"^[A-Z]\.\s+(D-\s*\d+[A-Z0-9-]*)\s+(.+?)\s*$",
    re.IGNORECASE,
)


def _parse_meeting_date(text: str) -> str:
    match = _DATE_RE.search(text.upper())
    if not match:
        return ""
    month = list(calendar.month_name).index(match.group(1).title())
    day = int(match.group(2))
    year = int(match.group(3))
    return f"{year:04d}-{month:02d}-{day:02d}"


def _parse_meeting_date_from_filename(name: str) -> str:
    stem = Path(name).stem.replace("Rent_Stabilization_Commission_Minutes_", "")
    match = re.match(r"([A-Za-z]+)_(\d{1,2})_(\d{4})", stem)
    if not match:
        return ""
    month = list(calendar.month_name).index(match.group(1))
    day = int(match.group(2))
    year = int(match.group(3))
    return f"{year:04d}-{month:02d}-{day:02d}"


def extract_appeals_from_minutes_text(text: str) -> list[dict[str, str]]:
    appeals: list[dict[str, str]] = []
    for raw_line in text.splitlines():
        line = _EXTRA_SPACE_RE.sub(" ", raw_line.strip())
        if not line:
            continue
        parsed = parse_appeal_line(line)
        if parsed is not None:
            appeals.append(parsed)
    return appeals


def parse_appeal_line(line: str) -> dict[str, str] | None:
    match = _APPEAL_LINE_RE.match(line)
    if not match:
        return None
    application_id = re.sub(r"\s+", "", match.group(1).upper())
    address_text = match.group(2).strip()
    address, unit = _split_appeal_address_unit(address_text)
    return {
        "application_id": application_id,
        "appeal_address": address,
        "appeal_unit": unit,
    }


def _split_appeal_address_unit(text: str) -> tuple[str, str]:
    match = re.match(r"^(.*?)\s+#\s*(.+)$", text)
    if match:
        return match.group(1).strip(), match.group(2).strip()
    return text.strip(), ""


def match_minutes_appeals(
    rso_df: pd.DataFrame, appeals_df: pd.DataFrame
) -> pd.DataFrame:
    stock = prepare_rso_linkage(rso_df)
    stock_units = stock[
        ["address", "unit", "parcel", "normalized_address", "normalized_unit", "normalized_parcel"]
    ].drop_duplicates()

    appeals = appeals_df.copy()
    appeals["normalized_address"] = appeals["appeal_address"].fillna("").map(normalize_address)
    appeals["normalized_unit"] = appeals["appeal_unit"].fillna("").map(normalize_unit)

    unit_matches = appeals.merge(
        stock_units,
        how="left",
        on=["normalized_address", "normalized_unit"],
        suffixes=("", "_rso"),
    )
    unresolved = unit_matches["parcel"].isna()
    if unresolved.any():
        address_only = appeals.loc[unresolved, :].merge(
            stock.groupby("normalized_address", as_index=False)
            .agg(
                address=("address", "first"),
                parcel=("normalized_parcel", "first"),
                unit_count=("unit", "size"),
            ),
            how="left",
            on="normalized_address",
        )
        unit_matches.loc[unresolved, "address"] = address_only["address"].values
        unit_matches.loc[unresolved, "parcel"] = address_only["parcel"].values
        unit_matches.loc[unresolved, "unit_count"] = address_only["unit_count"].values
        address_only_found = address_only["parcel"].fillna("") != ""
        unresolved_index = unit_matches.index[unresolved]
        unit_matches.loc[unresolved_index[address_only_found], "match_type"] = "address_only"
    unit_matches.loc[~unresolved, "unit_count"] = 1
    unit_matches.loc[~unresolved, "match_type"] = "address_and_unit"
    unit_matches["matched_rso"] = unit_matches["parcel"].fillna("") != ""
    return unit_matches


def match_buyouts(rso_df: pd.DataFrame, buyout_df: pd.DataFrame) -> pd.DataFrame:
    stock = prepare_rso_linkage(rso_df)
    address_summary = (
        stock.groupby("normalized_address", as_index=False)
        .agg(
            rso_address=("address", "first"),
            rso_parcel_count=("normalized_parcel", "nunique"),
            rso_unit_count=("unit", "size"),
        )
    )
    matched = buyout_df.merge(address_summary, how="left", on="normalized_address")
    matched["matched_rso"] = matched["rso_address"].fillna("") != ""
    return matched


def match_seismic(rso_df: pd.DataFrame, seismic_df: pd.DataFrame) -> pd.DataFrame:
    stock = prepare_rso_linkage(rso_df)
    parcel_summary = (
        stock.groupby("normalized_parcel", as_index=False)
        .agg(
            address=("address", "first"),
            parcel=("parcel", "first"),
            rso_unit_count=("unit", "size"),
        )
    )
    exploded = seismic_df.copy().explode("normalized_parcel_list")
    exploded = exploded.rename(columns={"normalized_parcel_list": "normalized_parcel"})
    matched = exploded.merge(
        parcel_summary,
        how="left",
        on="normalized_parcel",
        suffixes=("", "_rso"),
    )
    matched["matched_rso"] = matched["address"].fillna("") != ""
    return matched


def summarize_stock_denominators(rso_df: pd.DataFrame) -> pd.DataFrame:
    stock = prepare_rso_linkage(rso_df)
    return pd.DataFrame(
        [
            {"metric": "rso_unit_rows", "value": int(len(stock))},
            {"metric": "rso_unique_addresses", "value": int(stock["normalized_address"].nunique())},
            {"metric": "rso_unique_parcels", "value": int(stock["normalized_parcel"].nunique())},
            {"metric": "rso_rows_with_unit", "value": int(stock["normalized_unit"].ne("").sum())},
        ]
    )


def summarize_surface_match_rates(
    rso_df: pd.DataFrame,
    buyout_matches: pd.DataFrame,
    seismic_matches: pd.DataFrame,
    appeal_matches: pd.DataFrame,
) -> pd.DataFrame:
    stock = prepare_rso_linkage(rso_df)
    stock_addresses = max(int(stock["normalized_address"].nunique()), 1)
    stock_parcels = max(int(stock["normalized_parcel"].nunique()), 1)
    return pd.DataFrame(
        [
            {
                "surface": "commission_appeals",
                "total_rows": int(len(appeal_matches)),
                "matched_rows": int(appeal_matches["matched_rso"].sum()),
                "matched_unique_addresses": int(
                    appeal_matches.loc[appeal_matches["matched_rso"], "normalized_address"].nunique()
                ),
                "match_rate": float(appeal_matches["matched_rso"].mean()) if len(appeal_matches) else 0.0,
                "stock_share": int(
                    appeal_matches.loc[appeal_matches["matched_rso"], "normalized_address"].nunique()
                ) / stock_addresses,
            },
            {
                "surface": "buyouts",
                "total_rows": int(len(buyout_matches)),
                "matched_rows": int(buyout_matches["matched_rso"].sum()),
                "matched_unique_addresses": int(
                    buyout_matches.loc[buyout_matches["matched_rso"], "normalized_address"].nunique()
                ),
                "match_rate": float(buyout_matches["matched_rso"].mean()) if len(buyout_matches) else 0.0,
                "stock_share": int(
                    buyout_matches.loc[buyout_matches["matched_rso"], "normalized_address"].nunique()
                ) / stock_addresses,
            },
            {
                "surface": "seismic",
                "total_rows": int(len(seismic_matches)),
                "matched_rows": int(seismic_matches["matched_rso"].sum()),
                "matched_unique_addresses": int(
                    seismic_matches.loc[seismic_matches["matched_rso"], "address"].nunique()
                ),
                "match_rate": float(seismic_matches["matched_rso"].mean()) if len(seismic_matches) else 0.0,
                "stock_share": int(
                    seismic_matches.loc[seismic_matches["matched_rso"], "normalized_parcel"].nunique()
                ) / stock_parcels,
            },
        ]
    )


def summarize_buyout_footprint(buyout_matches: pd.DataFrame) -> pd.DataFrame:
    matched = buyout_matches.loc[buyout_matches["matched_rso"]].copy()
    if matched.empty:
        return pd.DataFrame(columns=["normalized_address", "buyout_rows", "rso_parcel_count", "rso_unit_count"])
    return (
        matched.groupby("normalized_address", as_index=False)
        .agg(
            buyout_rows=("normalized_address", "size"),
            rso_parcel_count=("rso_parcel_count", "max"),
            rso_unit_count=("rso_unit_count", "max"),
        )
        .sort_values(["buyout_rows", "normalized_address"], ascending=[False, True])
        .reset_index(drop=True)
    )


def summarize_seismic_footprint(seismic_matches: pd.DataFrame) -> pd.DataFrame:
    matched = seismic_matches.loc[seismic_matches["matched_rso"]].copy()
    if matched.empty:
        return pd.DataFrame(columns=["normalized_parcel", "address", "seismic_rows", "rso_unit_count"])
    return (
        matched.groupby(["normalized_parcel", "address"], as_index=False)
        .agg(
            seismic_rows=("normalized_parcel", "size"),
            rso_unit_count=("rso_unit_count", "max"),
        )
        .sort_values(["seismic_rows", "normalized_parcel"], ascending=[False, True])
        .reset_index(drop=True)
    )


def summarize_appeal_match_types(appeal_matches: pd.DataFrame) -> pd.DataFrame:
    if appeal_matches.empty:
        return pd.DataFrame(columns=["match_type", "appeal_rows"])
    frame = appeal_matches.copy()
    frame["match_type"] = frame["match_type"].fillna("unmatched")
    return (
        frame.groupby("match_type", as_index=False)
        .agg(appeal_rows=("application_id", "size"))
        .sort_values(["appeal_rows", "match_type"], ascending=[False, True])
        .reset_index(drop=True)
    )
