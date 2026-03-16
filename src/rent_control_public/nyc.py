from __future__ import annotations

from io import StringIO
from pathlib import Path
import time
import re
import subprocess

import pandas as pd
import requests


RSBL_PDF_URLS_2024 = {
    "MANHATTAN": "https://rentguidelinesboard.cityofnewyork.us/wp-content/uploads/2025/12/2024-DHCR-Bldg-File-Manhattan.pdf",
    "BRONX": "https://rentguidelinesboard.cityofnewyork.us/wp-content/uploads/2025/12/2024-DHCR-Bldg-File-Bronx.pdf",
    "BROOKLYN": "https://rentguidelinesboard.cityofnewyork.us/wp-content/uploads/2025/12/2024-DHCR-Bldg-File-Brooklyn.pdf",
    "QUEENS": "https://rentguidelinesboard.cityofnewyork.us/wp-content/uploads/2025/12/2024-DHCR-Bldg-File-Queens.pdf",
    "STATEN ISLAND": "https://rentguidelinesboard.cityofnewyork.us/wp-content/uploads/2025/12/2024-DHCR-Bldg-File-Staten-Island.pdf",
}

HPD_VIOLATIONS_URL = "https://data.cityofnewyork.us/resource/csn4-vhvf.csv"
PLUTO_URL = "https://data.cityofnewyork.us/resource/64uk-42ks.csv"
MDR_URL = "https://data.cityofnewyork.us/resource/tesw-yqqr.csv"
DEFAULT_HPD_COLUMNS = [
    "violationid",
    "buildingid",
    "registrationid",
    "boroid",
    "boro",
    "housenumber",
    "streetname",
    "zip",
    "block",
    "lot",
    "inspectiondate",
    "currentstatus",
    "currentstatusdate",
]

RSBL_COLUMNS = [
    "zip",
    "bldgno1",
    "street1",
    "stsufx1",
    "bldgno2",
    "street2",
    "stsufx2",
    "city",
    "county",
    "status1",
    "status2",
    "status3",
    "block",
    "lot",
]

RSBL_HEADER_MARKERS = ["ZIP", "BLDGNO1", "STREET1", "STSUFX1", "BLDGNO2", "STREET2", "STSUFX2", "CITY", "COUNTY", "STATUS1", "STATUS2", "STATUS3", "BLOCK", "LOT"]
PLUTO_SELECT_COLUMNS = [
    "bbl",
    "borough",
    "block",
    "lot",
    "address",
    "yearbuilt",
    "unitsres",
    "unitstotal",
    "landuse",
    "bldgclass",
    "cd",
    "zipcode",
    "latitude",
    "longitude",
]

_BORO_NAME_TO_CODE: dict[str, str] = {
    "manhattan": "1",
    "bronx": "2",
    "brooklyn": "3",
    "queens": "4",
    "staten island": "5",
    "mn": "1",
    "bx": "2",
    "bk": "3",
    "qn": "4",
    "si": "5",
    "new york": "1",
    "kings": "3",
    "richmond": "5",
}
_BORO_CODE_TO_NAME: dict[str, str] = {
    "1": "MANHATTAN",
    "2": "BRONX",
    "3": "BROOKLYN",
    "4": "QUEENS",
    "5": "STATEN ISLAND",
}
_STREET_SUFFIX_MAP: dict[str, str] = {
    "avenue": "AVE",
    "ave": "AVE",
    "av": "AVE",
    "boulevard": "BLVD",
    "blvd": "BLVD",
    "court": "CT",
    "ct": "CT",
    "drive": "DR",
    "dr": "DR",
    "east": "E",
    "lane": "LN",
    "ln": "LN",
    "north": "N",
    "place": "PL",
    "pl": "PL",
    "road": "RD",
    "rd": "RD",
    "south": "S",
    "square": "SQ",
    "sq": "SQ",
    "street": "ST",
    "str": "ST",
    "st": "ST",
    "terrace": "TER",
    "ter": "TER",
    "west": "W",
    "way": "WAY",
}


def normalize_borough_code(value: str | int) -> str:
    raw = str(value).strip()
    if raw in _BORO_CODE_TO_NAME:
        return raw
    key = raw.lower()
    if key in _BORO_NAME_TO_CODE:
        return _BORO_NAME_TO_CODE[key]
    raise ValueError(f"Unrecognized borough value: {value!r}")


def normalize_borough_name(value: str | int) -> str:
    return _BORO_CODE_TO_NAME[normalize_borough_code(value)]


def normalize_block(value: str | int) -> str:
    raw = str(value).strip()
    if not raw:
        raise ValueError("Block value is empty")
    digits = raw.lstrip("0") or "0"
    if not digits.isdigit():
        raise ValueError(f"Non-numeric block value: {value!r}")
    return digits.zfill(5)


def normalize_lot(value: str | int) -> str:
    raw = str(value).strip()
    if not raw:
        raise ValueError("Lot value is empty")
    digits = raw.lstrip("0") or "0"
    if not digits.isdigit():
        raise ValueError(f"Non-numeric lot value: {value!r}")
    return digits.zfill(4)


def make_bbl(borough: str | int, block: str | int, lot: str | int) -> str:
    return f"{normalize_borough_code(borough)}{normalize_block(block)}{normalize_lot(lot)}"


def make_boro_block_lot(borough: str | int, block: str | int, lot: str | int) -> str:
    return make_bbl(borough, block, lot)


def normalize_street_name(name: str) -> str:
    text = " ".join(name.upper().split()).replace(".", "")
    tokens = []
    for token in text.split():
        low = token.lower()
        tokens.append(_STREET_SUFFIX_MAP.get(low, token))
    result = " ".join(tokens)
    return re.sub(r"(\d+)\s+(ST|ND|RD|TH)\b", r"\1\2", result)


def normalize_house_number(value: str) -> str:
    raw = value.strip().upper()
    if not raw:
        raise ValueError("House number is empty")
    if "-" in raw:
        left, right = raw.split("-", 1)
        return f"{left.lstrip('0') or '0'}-{right.lstrip('0') or '0'}"
    return raw.lstrip("0") or "0"


def add_bbl_column(
    df: pd.DataFrame,
    *,
    borough_col: str,
    block_col: str,
    lot_col: str,
    output_col: str = "boro_block_lot",
) -> pd.DataFrame:
    out = df.copy()
    out[output_col] = out.apply(lambda row: make_boro_block_lot(row[borough_col], row[block_col], row[lot_col]), axis=1)
    return out


def chunk_values(values: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        raise ValueError("size must be positive")
    return [values[idx: idx + size] for idx in range(0, len(values), size)]


def canonical_bbl_to_pluto_bbl(value: str | int) -> str:
    raw = str(value).strip()
    if "." in raw:
        return raw
    digits = re.sub(r"\D", "", raw)
    if len(digits) != 10:
        raise ValueError(f"Expected a 10-digit NYC BBL, got {value!r}")
    return f"{digits}.00000000"


def build_socrata_bbl_where(values: list[str], *, field: str = "bbl", pluto_format: bool = True) -> str:
    cleaned = [canonical_bbl_to_pluto_bbl(value) if pluto_format else str(value) for value in values]
    quoted = ",".join(f"'{value}'" for value in cleaned)
    return f"{field} in ({quoted})"


def _get_csv(
    url: str,
    *,
    params: dict[str, str],
    timeout: int,
    retries: int = 3,
    backoff_seconds: int = 2,
) -> pd.DataFrame:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return pd.read_csv(StringIO(response.text))
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as exc:
            last_error = exc
            if attempt == retries:
                break
            time.sleep(backoff_seconds * attempt)
    assert last_error is not None
    raise last_error


def download_file(url: str, destination: Path, timeout: int = 120) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    destination.write_bytes(response.content)
    return destination


def extract_pdf_text(pdf_path: Path) -> str:
    result = subprocess.run(
        ["pdftotext", "-layout", str(pdf_path), "-"],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def _header_positions(header_line: str) -> list[tuple[str, int, int | None]]:
    starts = [(marker.lower(), header_line.index(marker)) for marker in RSBL_HEADER_MARKERS]
    positions: list[tuple[str, int, int | None]] = []
    for idx, (name, start) in enumerate(starts):
        end = starts[idx + 1][1] if idx + 1 < len(starts) else None
        positions.append((name, start, end))
    return positions


def _is_data_line(line: str) -> bool:
    stripped = line.replace("\x0c", "").rstrip()
    if not stripped:
        return False
    if stripped.startswith("List of ") or stripped.startswith("Source:"):
        return False
    if all(marker in stripped for marker in ("ZIP", "BLOCK", "LOT")):
        return False
    return bool(re.match(r"^\s*\d{5}\s", stripped))


def parse_rsbl_text(text: str, borough: str | int) -> pd.DataFrame:
    borough_name = normalize_borough_name(borough)
    lines = text.splitlines()
    header_line = next((line for line in lines if all(marker in line for marker in ("ZIP", "BLDGNO1", "BLOCK", "LOT"))), None)
    if header_line is None:
        raise ValueError("Could not find RSBL header line in extracted text.")
    positions = _header_positions(header_line)
    left_positions = [(name, start, end) for name, start, end in positions if name not in {"block", "lot"}]

    rows: list[dict[str, object]] = []
    for line in lines:
        if not _is_data_line(line):
            continue
        stripped = line.rstrip()
        tail_match = re.search(r"(?P<prefix>.*?)(?P<block>\d+)\s+(?P<lot>\d+)\s*$", stripped)
        if tail_match is None:
            continue
        record: dict[str, object] = {}
        prefix = tail_match.group("prefix")
        for name, start, end in left_positions:
            record[name] = prefix[start:end].strip() if end is not None else prefix[start:].strip()
        record["block"] = tail_match.group("block")
        record["lot"] = tail_match.group("lot")
        record["borough"] = borough_name
        record["boro_block_lot"] = make_boro_block_lot(borough_name, record["block"], record["lot"])
        primary_street = " ".join(part for part in [record.get("street1", ""), record.get("stsufx1", "")] if part).strip()
        record["primary_street_normalized"] = normalize_street_name(primary_street) if primary_street else ""
        record["primary_house_number_normalized"] = normalize_house_number(str(record["bldgno1"])) if record.get("bldgno1") and " TO " not in str(record["bldgno1"]) else str(record.get("bldgno1", "")).strip()
        rows.append(record)
    columns = RSBL_COLUMNS + ["borough", "boro_block_lot", "primary_street_normalized", "primary_house_number_normalized"]
    return pd.DataFrame(rows, columns=columns)


def fetch_hpd_violations_sample(
    *,
    borough: str | int,
    limit: int = 5000,
    timeout: int = 120,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    borough_name = normalize_borough_name(borough)
    select_cols = columns or DEFAULT_HPD_COLUMNS
    params = {
        "$select": ",".join(select_cols),
        "$where": f"boro='{borough_name}'",
        "$limit": str(limit),
    }
    return _get_csv(HPD_VIOLATIONS_URL, params=params, timeout=timeout)


def fetch_hpd_violation_building_summary(
    *,
    borough: str | int,
    limit: int = 50000,
    timeout: int = 180,
) -> pd.DataFrame:
    borough_name = normalize_borough_name(borough)
    params = {
        "$select": "boroid,boro,block,lot,count(*) as violation_count",
        "$where": f"boro='{borough_name}'",
        "$group": "boroid,boro,block,lot",
        "$limit": str(limit),
    }
    return _get_csv(HPD_VIOLATIONS_URL, params=params, timeout=timeout)


def fetch_hpd_violation_building_year_summary(
    *,
    borough: str | int,
    limit: int = 250000,
    since_year: int | None = None,
    timeout: int = 240,
) -> pd.DataFrame:
    borough_name = normalize_borough_name(borough)
    where = [f"boro='{borough_name}'", "inspectiondate IS NOT NULL"]
    if since_year is not None:
        where.append(f"inspectiondate >= '{since_year}-01-01T00:00:00'")
    params = {
        "$select": "boroid,boro,block,lot,date_extract_y(inspectiondate) as inspection_year,count(*) as violation_count",
        "$where": " AND ".join(where),
        "$group": "boroid,boro,block,lot,inspection_year",
        "$limit": str(limit),
    }
    out = _get_csv(HPD_VIOLATIONS_URL, params=params, timeout=timeout)
    if "inspection_year" in out.columns:
        out["inspection_year"] = pd.to_numeric(out["inspection_year"], errors="coerce").astype("Int64")
    return out


def fetch_mdr_registration_summary(
    *,
    limit: int = 250000,
    timeout: int = 180,
) -> pd.DataFrame:
    params = {
        "$select": ",".join(
            [
                "boroid",
                "boro",
                "block",
                "lot",
                "max(lastregistrationdate) as lastregistrationdate",
                "max(registrationenddate) as registrationenddate",
                "count(registrationid) as registration_count",
                "count(buildingid) as building_count",
                "max(communityboard) as communityboard",
            ]
        ),
        "$group": "boroid,boro,block,lot",
        "$limit": str(limit),
    }
    out = _get_csv(MDR_URL, params=params, timeout=timeout)
    out = add_bbl_column(out, borough_col="boroid", block_col="block", lot_col="lot")
    out["mdr_registered"] = 1
    for col in ["registration_count", "building_count", "communityboard"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def fetch_pluto_controls(
    bbls: list[str],
    *,
    chunk_size: int = 500,
    timeout: int = 180,
) -> pd.DataFrame:
    unique_bbls = sorted({str(value) for value in bbls if str(value).strip()})
    parts: list[pd.DataFrame] = []
    for chunk in chunk_values(unique_bbls, chunk_size):
        params = {
            "$select": ",".join(PLUTO_SELECT_COLUMNS),
            "$where": build_socrata_bbl_where(chunk),
            "$limit": str(len(chunk) + 5),
        }
        part = _get_csv(PLUTO_URL, params=params, timeout=timeout)
        if not part.empty:
            parts.append(part)
    if not parts:
        return pd.DataFrame(columns=PLUTO_SELECT_COLUMNS + ["boro_block_lot"])
    out = pd.concat(parts, ignore_index=True).drop_duplicates(subset=["bbl"])
    out = add_bbl_column(out, borough_col="borough", block_col="block", lot_col="lot")
    for col in ["yearbuilt", "unitsres", "unitstotal", "landuse", "cd", "zipcode", "latitude", "longitude"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def combine_rsbl_frames(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Combine per-borough RSBL DataFrames into a single citywide frame.

    *frames* maps borough name/code → parsed RSBL DataFrame (as returned by
    :func:`parse_rsbl_text`).  Each frame gets a normalised ``borough`` column
    before concatenation.  Returns a single DataFrame sorted by
    ``boro_block_lot`` with a reset index.
    """
    parts: list[pd.DataFrame] = []
    for borough_key, df in frames.items():
        part = df.copy()
        part["borough"] = normalize_borough_name(borough_key)
        if "boro_block_lot" in part.columns:
            part["boro_block_lot"] = part["boro_block_lot"].astype(str)
        parts.append(part)
    if not parts:
        return pd.DataFrame(columns=RSBL_COLUMNS + ["borough", "boro_block_lot"])
    combined = pd.concat(parts, ignore_index=True)
    combined.sort_values("boro_block_lot", inplace=True)
    combined.reset_index(drop=True, inplace=True)
    return combined


def borough_rsbl_iter() -> list[tuple[str, str]]:
    """Return a list of ``(borough_name, pdf_url)`` pairs for all 2024 RSBL PDFs."""
    return list(RSBL_PDF_URLS_2024.items())


def hpd_violations_to_monthly_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate HPD violation records to monthly building-level counts.

    Expects *df* to contain ``boro_block_lot`` and ``inspectiondate`` columns.
    Returns a DataFrame with columns ``boro_block_lot``, ``year``, ``month``,
    and ``violation_count``.
    """
    out = df.copy()
    out["inspectiondate"] = pd.to_datetime(out["inspectiondate"], errors="coerce")
    out = out.dropna(subset=["inspectiondate", "boro_block_lot"])
    out["year"] = out["inspectiondate"].dt.year.astype(int)
    out["month"] = out["inspectiondate"].dt.month.astype(int)
    grouped = (
        out.groupby(["boro_block_lot", "year", "month"], as_index=False)
        .size()
        .rename(columns={"size": "violation_count"})
    )
    grouped.sort_values(["boro_block_lot", "year", "month"], inplace=True)
    grouped.reset_index(drop=True, inplace=True)
    return grouped


def hpd_violations_to_yearly_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate HPD violation records to yearly building-level counts.

    Expects *df* to contain ``boro_block_lot`` and ``inspectiondate`` columns.
    Returns a DataFrame with columns ``boro_block_lot``, ``year``, and
    ``violation_count``.
    """
    out = df.copy()
    out["inspectiondate"] = pd.to_datetime(out["inspectiondate"], errors="coerce")
    out = out.dropna(subset=["inspectiondate", "boro_block_lot"])
    out["year"] = out["inspectiondate"].dt.year.astype(int)
    grouped = (
        out.groupby(["boro_block_lot", "year"], as_index=False)
        .size()
        .rename(columns={"size": "violation_count"})
    )
    grouped.sort_values(["boro_block_lot", "year"], inplace=True)
    grouped.reset_index(drop=True, inplace=True)
    return grouped


def summarize_rsbl_hpd_match(rsbl: pd.DataFrame, hpd: pd.DataFrame, *, borough: str | int) -> pd.DataFrame:
    rsbl_keys = set(rsbl["boro_block_lot"].dropna().astype(str))
    hpd_keys = set(hpd["boro_block_lot"].dropna().astype(str))
    matched = rsbl_keys & hpd_keys
    summary = pd.DataFrame(
        [
            {
                "borough": normalize_borough_name(borough),
                "rsbl_rows": len(rsbl),
                "rsbl_unique_boro_block_lot": len(rsbl_keys),
                "hpd_rows": len(hpd),
                "hpd_unique_boro_block_lot": len(hpd_keys),
                "matched_boro_block_lot": len(matched),
                "rsbl_match_rate_pct": len(matched) / len(rsbl_keys) * 100 if rsbl_keys else pd.NA,
                "hpd_match_rate_pct": len(matched) / len(hpd_keys) * 100 if hpd_keys else pd.NA,
            }
        ]
    )
    return summary


def summarize_rsbl_hpd_match_citywide(rsbl: pd.DataFrame, hpd: pd.DataFrame) -> pd.DataFrame:
    rsbl_keys = set(rsbl["boro_block_lot"].dropna().astype(str))
    hpd_keys = set(hpd["boro_block_lot"].dropna().astype(str))
    matched = rsbl_keys & hpd_keys
    return pd.DataFrame(
        [
            {
                "geography": "NYC",
                "rsbl_rows": len(rsbl),
                "rsbl_unique_boro_block_lot": len(rsbl_keys),
                "hpd_rows": len(hpd),
                "hpd_unique_boro_block_lot": len(hpd_keys),
                "matched_boro_block_lot": len(matched),
                "rsbl_match_rate_pct": len(matched) / len(rsbl_keys) * 100 if rsbl_keys else pd.NA,
                "hpd_match_rate_pct": len(matched) / len(hpd_keys) * 100 if hpd_keys else pd.NA,
            }
        ]
    )


def build_matched_rsbl_building_year_panel(
    rsbl: pd.DataFrame,
    matched_year_counts: pd.DataFrame,
    *,
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    rsbl_unique = rsbl.drop_duplicates(subset=["boro_block_lot"]).copy()
    rsbl_unique["boro_block_lot"] = rsbl_unique["boro_block_lot"].astype(str)

    years = pd.DataFrame({"inspection_year": list(range(start_year, end_year + 1))})
    rsbl_unique["_merge_key"] = 1
    years["_merge_key"] = 1
    panel = rsbl_unique.merge(years, on="_merge_key", how="outer").drop(columns="_merge_key")

    counts = matched_year_counts.copy()
    counts["boro_block_lot"] = counts["boro_block_lot"].astype(str)
    counts["inspection_year"] = pd.to_numeric(counts["inspection_year"], errors="coerce").astype("Int64")
    counts["violation_count"] = pd.to_numeric(counts["violation_count"], errors="coerce")
    if "borough" in counts.columns:
        counts = counts.drop(columns=["borough"])

    panel = panel.merge(counts, on=["boro_block_lot", "inspection_year"], how="left")
    panel["violation_count"] = panel["violation_count"].fillna(0).astype(int)
    panel["treated_rsbl"] = 1
    return panel.sort_values(["boro_block_lot", "inspection_year"]).reset_index(drop=True)


def build_hpd_comparison_building_year_panel(
    rsbl: pd.DataFrame,
    hpd_buildings: pd.DataFrame,
    hpd_year_counts: pd.DataFrame,
    *,
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    rsbl_keys = set(rsbl["boro_block_lot"].astype(str))

    building_universe = hpd_buildings.drop_duplicates(subset=["boro_block_lot"]).copy()
    building_universe["boro_block_lot"] = building_universe["boro_block_lot"].astype(str)
    if "violation_count" in building_universe.columns:
        building_universe = building_universe.drop(columns=["violation_count"])
    if "borough" not in building_universe.columns and "boro" in building_universe.columns:
        building_universe["borough"] = building_universe["boro"].map(normalize_borough_name)
    building_universe["treated_rsbl"] = building_universe["boro_block_lot"].isin(rsbl_keys).astype(int)

    years = pd.DataFrame({"inspection_year": list(range(start_year, end_year + 1))})
    building_universe["_merge_key"] = 1
    years["_merge_key"] = 1
    panel = building_universe.merge(years, on="_merge_key", how="outer").drop(columns="_merge_key")

    counts = hpd_year_counts.copy()
    counts["boro_block_lot"] = counts["boro_block_lot"].astype(str)
    counts["inspection_year"] = pd.to_numeric(counts["inspection_year"], errors="coerce").astype("Int64")
    counts["violation_count"] = pd.to_numeric(counts["violation_count"], errors="coerce")
    if "borough" in counts.columns:
        counts = counts.drop(columns=["borough"])

    panel = panel.merge(counts, on=["boro_block_lot", "inspection_year"], how="left")
    panel["violation_count"] = panel["violation_count"].fillna(0).astype(int)
    return panel.sort_values(["boro_block_lot", "inspection_year"]).reset_index(drop=True)


def select_control_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    existing = [column for column in columns if column in df.columns]
    return df.loc[:, existing].copy()


def merge_control_frame(
    panel: pd.DataFrame,
    controls: pd.DataFrame,
    *,
    on: str = "boro_block_lot",
) -> pd.DataFrame:
    base = panel.copy()
    control_frame = controls.drop_duplicates(subset=[on]).copy()
    if on in base.columns:
        base[on] = base[on].astype(str)
    if on in control_frame.columns:
        control_frame[on] = control_frame[on].astype(str)
    return base.merge(control_frame, on=on, how="left")


def yearbuilt_to_bin(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    return pd.Series(
        pd.cut(
            numeric,
            bins=[0, 1939, 1969, 1999, float("inf")],
            labels=["prewar", "1940_1969", "1970_1999", "2000_plus"],
            include_lowest=True,
        ),
        index=values.index,
        dtype="object",
    )


def units_to_bin(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    return pd.Series(
        pd.cut(
            numeric,
            bins=[0, 2, 5, 19, 49, float("inf")],
            labels=["1_2", "3_5", "6_19", "20_49", "50_plus"],
            include_lowest=True,
        ),
        index=values.index,
        dtype="object",
    )


def build_nyc_enriched_analytic_panel(
    panel: pd.DataFrame,
    *,
    pluto_controls: pd.DataFrame,
    mdr_summary: pd.DataFrame,
) -> pd.DataFrame:
    out = merge_control_frame(panel, select_control_columns(pluto_controls, ["boro_block_lot", "yearbuilt", "unitsres", "unitstotal", "landuse", "bldgclass", "cd", "zipcode"]))
    out = merge_control_frame(out, select_control_columns(mdr_summary, ["boro_block_lot", "mdr_registered", "registration_count", "building_count", "communityboard", "lastregistrationdate", "registrationenddate"]))
    out["mdr_registered"] = out["mdr_registered"].fillna(0).astype(int)
    out["communityboard"] = pd.to_numeric(out["communityboard"], errors="coerce").fillna(pd.to_numeric(out.get("cd"), errors="coerce"))
    out["yearbuilt_bin"] = yearbuilt_to_bin(out["yearbuilt"])
    out["units_bin"] = units_to_bin(out["unitstotal"])
    return out


def build_stratified_registered_rental_panel(panel: pd.DataFrame) -> pd.DataFrame:
    eligible = panel[
        (panel["treated_rsbl"] == 1) | (panel["mdr_registered"] == 1)
    ].copy()
    eligible["unitstotal"] = pd.to_numeric(eligible["unitstotal"], errors="coerce")
    eligible = eligible[eligible["unitstotal"] >= 3].copy()
    eligible = eligible.dropna(subset=["communityboard", "yearbuilt_bin", "units_bin"])
    eligible["communityboard"] = eligible["communityboard"].astype("Int64").astype(str)
    eligible["stratum"] = (
        eligible["borough"].astype(str)
        + "|cb"
        + eligible["communityboard"].astype(str)
        + "|yb:"
        + eligible["yearbuilt_bin"].astype(str)
        + "|u:"
        + eligible["units_bin"].astype(str)
    )
    return eligible


def aggregate_panel_stratum_year(
    panel: pd.DataFrame,
    *,
    value_col: str = "violation_count",
) -> pd.DataFrame:
    grouped = (
        panel.groupby(["stratum", "borough", "inspection_year", "treated_rsbl"], as_index=False)
        .agg(
            building_count=(value_col, "size"),
            mean_violation_count=(value_col, "mean"),
            total_violation_count=(value_col, "sum"),
        )
    )
    grouped.sort_values(["stratum", "inspection_year", "treated_rsbl"], inplace=True)
    grouped.reset_index(drop=True, inplace=True)
    return grouped


def summarize_treated_control_balance(panel: pd.DataFrame) -> pd.DataFrame:
    building_panel = panel.drop_duplicates(subset=["boro_block_lot"]).copy()
    summary = (
        building_panel.groupby("treated_rsbl", as_index=False)
        .agg(
            buildings=("boro_block_lot", "nunique"),
            mean_yearbuilt=("yearbuilt", "mean"),
            mean_unitstotal=("unitstotal", "mean"),
            median_unitstotal=("unitstotal", "median"),
            registered_share=("mdr_registered", "mean"),
        )
        .sort_values("treated_rsbl", ascending=False)
    )
    return summary


def build_preperiod_building_features(
    panel: pd.DataFrame,
    *,
    pre_years: tuple[int, ...] = (2019, 2020, 2021),
    value_col: str = "violation_count",
) -> pd.DataFrame:
    pre = panel[panel["inspection_year"].isin(pre_years)].copy()
    grouped = (
        pre.groupby("boro_block_lot", as_index=False)
        .agg(
            pre_mean_violation_count=(value_col, "mean"),
            pre_total_violation_count=(value_col, "sum"),
            pre_nonzero_years=(value_col, lambda s: int((pd.to_numeric(s, errors="coerce").fillna(0) > 0).sum())),
        )
    )
    static_cols = [
        "boro_block_lot",
        "borough",
        "treated_rsbl",
        "mdr_registered",
        "communityboard",
        "yearbuilt",
        "unitstotal",
        "yearbuilt_bin",
        "units_bin",
        "stratum",
    ]
    existing_static_cols = [col for col in static_cols if col in panel.columns]
    static = panel[existing_static_cols].drop_duplicates(subset=["boro_block_lot"]).copy()
    out = static.merge(grouped, on="boro_block_lot", how="left")
    out["pre_mean_violation_count"] = pd.to_numeric(out["pre_mean_violation_count"], errors="coerce").fillna(0.0)
    out["pre_total_violation_count"] = pd.to_numeric(out["pre_total_violation_count"], errors="coerce").fillna(0.0)
    out["pre_nonzero_years"] = pd.to_numeric(out["pre_nonzero_years"], errors="coerce").fillna(0).astype(int)
    out["pre_mean_bin"] = pd.cut(
        out["pre_mean_violation_count"],
        bins=[-0.001, 0, 0.5, 1, 2, 5, float("inf")],
        labels=["0", "0_5", "0.5_1", "1_2", "2_5", "5_plus"],
    ).astype("object")
    out["pre_total_bin"] = pd.cut(
        out["pre_total_violation_count"],
        bins=[-0.001, 0, 1, 3, 6, 15, float("inf")],
        labels=["0", "0_1", "1_3", "3_6", "6_15", "15_plus"],
    ).astype("object")
    return out


def choose_nearest_control(
    treated_row: pd.Series,
    controls: pd.DataFrame,
    *,
    distance_cols: tuple[str, ...] = ("pre_mean_violation_count", "unitstotal", "yearbuilt"),
    distance_weights: dict[str, float] | None = None,
    prefer_same_or_lower_pre_mean: bool = False,
    max_abs_pre_mean_gap: float | None = None,
) -> str | None:
    if controls.empty:
        return None
    candidate = controls.copy()
    if prefer_same_or_lower_pre_mean:
        treated_pre_mean = pd.to_numeric(pd.Series([treated_row.get("pre_mean_violation_count")]), errors="coerce").iloc[0]
        same_or_lower = candidate[pd.to_numeric(candidate["pre_mean_violation_count"], errors="coerce") <= treated_pre_mean].copy()
        if not same_or_lower.empty:
            candidate = same_or_lower
    if max_abs_pre_mean_gap is not None:
        treated_pre_mean = pd.to_numeric(pd.Series([treated_row.get("pre_mean_violation_count")]), errors="coerce").iloc[0]
        allowed = candidate[(pd.to_numeric(candidate["pre_mean_violation_count"], errors="coerce") - treated_pre_mean).abs() <= max_abs_pre_mean_gap].copy()
        if not allowed.empty:
            candidate = allowed
    candidate["_distance"] = 0.0
    weights = distance_weights or {}
    for col in distance_cols:
        treated_value = pd.to_numeric(pd.Series([treated_row.get(col)]), errors="coerce").iloc[0]
        candidate_value = pd.to_numeric(candidate[col], errors="coerce")
        gap = (candidate_value - treated_value).abs()
        weight = float(weights.get(col, 1.0))
        candidate["_distance"] = candidate["_distance"] + (weight * gap.fillna(gap.max() if gap.notna().any() else 0))
    candidate = candidate.sort_values(["_distance", "pre_mean_violation_count", "unitstotal", "yearbuilt", "boro_block_lot"])
    return str(candidate["boro_block_lot"].iloc[0])


def match_treated_to_controls(
    features: pd.DataFrame,
    *,
    exact_match_cols: tuple[str, ...] = ("borough", "communityboard", "yearbuilt_bin", "units_bin"),
    distance_cols: tuple[str, ...] = ("pre_mean_violation_count", "unitstotal", "yearbuilt"),
    distance_weights: dict[str, float] | None = None,
    allow_replacement: bool = False,
    prefer_same_or_lower_pre_mean: bool = False,
    max_abs_pre_mean_gap: float | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    features = features.copy()
    features["boro_block_lot"] = features["boro_block_lot"].astype(str)
    sort_cols = list(exact_match_cols) + ["pre_mean_violation_count", "unitstotal", "yearbuilt", "boro_block_lot"]
    treated = features[features["treated_rsbl"] == 1].sort_values(sort_cols)
    controls = features[(features["treated_rsbl"] == 0) & (features["mdr_registered"] == 1)].copy()

    for group_values, treated_group in treated.groupby(list(exact_match_cols), dropna=False):
        if not isinstance(group_values, tuple):
            group_values = (group_values,)
        mask = pd.Series(True, index=controls.index)
        for col, value in zip(exact_match_cols, group_values):
            if pd.isna(value):
                mask = mask & controls[col].isna()
            else:
                mask = mask & (controls[col] == value)
        available = controls[mask].copy()
        if available.empty:
            continue
        for _, treated_row in treated_group.iterrows():
            control_key = choose_nearest_control(
                treated_row,
                available,
                distance_cols=distance_cols,
                distance_weights=distance_weights,
                prefer_same_or_lower_pre_mean=prefer_same_or_lower_pre_mean,
                max_abs_pre_mean_gap=max_abs_pre_mean_gap,
            )
            if control_key is None:
                break
            control_row = available[available["boro_block_lot"].astype(str) == str(control_key)].iloc[0]
            record = {
                "treated_boro_block_lot": str(treated_row["boro_block_lot"]),
                "control_boro_block_lot": str(control_key),
                "match_id": f"{treated_row['boro_block_lot']}__{control_key}",
                "pre_mean_gap": float(treated_row["pre_mean_violation_count"]) - float(control_row["pre_mean_violation_count"]),
                "treated_pre_mean_violation_count": float(treated_row["pre_mean_violation_count"]),
                "control_pre_mean_violation_count": float(control_row["pre_mean_violation_count"]),
                "treated_unitstotal": treated_row.get("unitstotal"),
                "control_unitstotal": control_row.get("unitstotal"),
                "treated_yearbuilt": treated_row.get("yearbuilt"),
                "control_yearbuilt": control_row.get("yearbuilt"),
            }
            for col, value in zip(exact_match_cols, group_values):
                record[col] = value
            rows.append(record)
            if not allow_replacement:
                available = available[available["boro_block_lot"].astype(str) != str(control_key)].copy()
                controls = controls[controls["boro_block_lot"].astype(str) != str(control_key)].copy()
                if available.empty:
                    break
    return pd.DataFrame(rows)


def build_matched_pair_panel(panel: pd.DataFrame, matches: pd.DataFrame) -> pd.DataFrame:
    if matches.empty:
        return pd.DataFrame(columns=list(panel.columns) + ["match_id", "match_role"])
    treated = matches[["match_id", "treated_boro_block_lot"]].rename(columns={"treated_boro_block_lot": "boro_block_lot"})
    treated["match_role"] = "treated"
    control = matches[["match_id", "control_boro_block_lot"]].rename(columns={"control_boro_block_lot": "boro_block_lot"})
    control["match_role"] = "control"
    lookup = pd.concat([treated, control], ignore_index=True)
    lookup["boro_block_lot"] = lookup["boro_block_lot"].astype(str)
    out = panel.copy()
    out["boro_block_lot"] = out["boro_block_lot"].astype(str)
    out = out.merge(lookup, on="boro_block_lot", how="inner")
    return out.sort_values(["match_id", "inspection_year", "match_role"]).reset_index(drop=True)


def aggregate_matched_pair_year(panel: pd.DataFrame, *, value_col: str = "violation_count") -> pd.DataFrame:
    grouped = (
        panel.groupby(["match_id", "inspection_year", "match_role"], as_index=False)
        .agg(
            mean_violation_count=(value_col, "mean"),
            total_violation_count=(value_col, "sum"),
            building_count=(value_col, "size"),
        )
    )
    grouped["treated_rsbl"] = grouped["match_role"].map({"treated": 1, "control": 0}).astype(int)
    return grouped.sort_values(["match_id", "inspection_year", "treated_rsbl"]).reset_index(drop=True)


def build_treated_year_event_design(
    panel: pd.DataFrame,
    *,
    treated_col: str = "treated_rsbl",
    year_col: str = "inspection_year",
    baseline_year: int | None = None,
) -> tuple[pd.DataFrame, list[int]]:
    out = panel[[treated_col, year_col]].copy()
    out[treated_col] = pd.to_numeric(out[treated_col], errors="coerce").fillna(0).astype(int)
    out[year_col] = pd.to_numeric(out[year_col], errors="coerce").astype(int)
    years = sorted(out[year_col].dropna().unique().tolist())
    if not years:
        return pd.DataFrame(index=out.index), []
    base_year = baseline_year if baseline_year is not None else years[0]
    if base_year not in years:
        raise ValueError(f"baseline_year {base_year} not found in panel years {years}")
    design = pd.DataFrame(index=out.index)
    event_years = [year for year in years if year != base_year]
    for year in event_years:
        design[f"treated_x_{year}"] = ((out[treated_col] == 1) & (out[year_col] == year)).astype(float)
    return design, event_years


def two_way_demean(
    panel: pd.DataFrame,
    *,
    group_col: str,
    time_col: str,
    value_cols: tuple[str, ...],
) -> pd.DataFrame:
    out = pd.concat(
        [
            panel[[group_col, time_col]].reset_index(drop=True),
            panel.loc[:, list(value_cols)].astype(float).reset_index(drop=True),
        ],
        axis=1,
    )
    if out.empty:
        return out
    grand_mean = out.loc[:, list(value_cols)].mean()
    group_mean = out.groupby(group_col)[list(value_cols)].transform("mean")
    time_mean = out.groupby(time_col)[list(value_cols)].transform("mean")
    out.loc[:, list(value_cols)] = out.loc[:, list(value_cols)] - group_mean - time_mean + grand_mean
    return out


def aggregate_panel_borough_year(
    panel: pd.DataFrame,
    *,
    value_col: str = "violation_count",
) -> pd.DataFrame:
    """Aggregate a building-year panel to borough x year x treated_rsbl summaries.

    *panel* must contain ``borough``, ``inspection_year``, ``treated_rsbl``,
    and *value_col* columns (as produced by
    :func:`build_hpd_comparison_building_year_panel`).

    Returns a DataFrame with columns ``borough``, ``inspection_year``,
    ``treated_rsbl``, ``building_count``, ``mean_{value_col}``, and
    ``total_{value_col}``, sorted by borough / year / treated_rsbl.
    """
    grouped = (
        panel.groupby(["borough", "inspection_year", "treated_rsbl"], as_index=False)
        .agg(
            building_count=(value_col, "size"),
            **{
                f"mean_{value_col}": (value_col, "mean"),
                f"total_{value_col}": (value_col, "sum"),
            },
        )
    )
    grouped.sort_values(["borough", "inspection_year", "treated_rsbl"], inplace=True)
    grouped.reset_index(drop=True, inplace=True)
    return grouped


def borough_year_treated_control_diff(
    panel: pd.DataFrame,
    *,
    value_col: str = "violation_count",
) -> pd.DataFrame:
    """Compute treated-minus-control mean difference by borough and year.

    *panel* must contain ``borough``, ``inspection_year``, ``treated_rsbl``,
    and *value_col* columns.

    Returns a DataFrame with columns ``borough``, ``inspection_year``,
    ``mean_treated``, ``mean_control``, ``diff``, ``n_treated``, and
    ``n_control``.  Borough-years lacking either group are included with
    ``NaN`` for the missing side.
    """
    agg = aggregate_panel_borough_year(panel, value_col=value_col)
    mean_col = f"mean_{value_col}"

    treated = agg.loc[agg["treated_rsbl"] == 1, ["borough", "inspection_year", mean_col, "building_count"]].rename(
        columns={mean_col: "mean_treated", "building_count": "n_treated"},
    )
    control = agg.loc[agg["treated_rsbl"] == 0, ["borough", "inspection_year", mean_col, "building_count"]].rename(
        columns={mean_col: "mean_control", "building_count": "n_control"},
    )
    merged = treated.merge(control, on=["borough", "inspection_year"], how="outer")
    merged["diff"] = merged["mean_treated"] - merged["mean_control"]
    merged.sort_values(["borough", "inspection_year"], inplace=True)
    merged.reset_index(drop=True, inplace=True)
    return merged


def build_borough_year_summary_table(
    panel: pd.DataFrame,
    *,
    value_col: str = "violation_count",
) -> pd.DataFrame:
    """Build a concise pivot-style summary of treated vs control by borough-year.

    Returns one row per borough-year with columns: ``borough``,
    ``inspection_year``, ``n_treated``, ``n_control``,
    ``mean_treated``, ``mean_control``, ``diff``, and ``ratio``
    (treated / control, ``NaN`` when control mean is zero).
    """
    diff = borough_year_treated_control_diff(panel, value_col=value_col)
    diff["ratio"] = diff["mean_treated"] / diff["mean_control"].replace(0, pd.NA)
    return diff


def classify_gap_direction(
    values: pd.Series,
    *,
    tolerance: float = 0.0,
) -> pd.Series:
    """Classify signed gap changes as increase, decrease, or flat."""
    numeric = pd.to_numeric(values, errors="coerce")

    def _classify(value: float) -> str | pd.NA:
        if pd.isna(value):
            return pd.NA
        if value > tolerance:
            return "increase"
        if value < -tolerance:
            return "decrease"
        return "flat"

    return numeric.apply(_classify)


def build_borough_pre_post_gap_summary(
    panel: pd.DataFrame,
    *,
    value_col: str = "violation_count",
    pre_years: tuple[int, ...] = (2019, 2020, 2021),
    post_years: tuple[int, ...] = (2022, 2023, 2024, 2025),
    tolerance: float = 0.0,
) -> pd.DataFrame:
    """Summarize treated-control gaps by borough across pre/post windows."""
    diff = borough_year_treated_control_diff(panel, value_col=value_col)
    pre = (
        diff[diff["inspection_year"].isin(pre_years)]
        .groupby("borough", as_index=False)
        .agg(pre_mean_gap=("diff", "mean"))
    )
    post = (
        diff[diff["inspection_year"].isin(post_years)]
        .groupby("borough", as_index=False)
        .agg(post_mean_gap=("diff", "mean"))
    )
    merged = pre.merge(post, on="borough", how="outer")
    merged["change_in_gap"] = merged["post_mean_gap"] - merged["pre_mean_gap"]
    merged["gap_direction"] = classify_gap_direction(merged["change_in_gap"], tolerance=tolerance)
    merged.sort_values(["change_in_gap", "borough"], ascending=[False, True], inplace=True)
    merged.reset_index(drop=True, inplace=True)
    return merged
