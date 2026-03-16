"""San Francisco Rent Board Housing Inventory backend.

Source: DataSF Rent Board Housing Inventory (dataset ID ``gdc7-dmcn``).
URL: https://data.sfgov.org/resource/gdc7-dmcn.json

The dataset contains unit-level housing inventory submissions reported
annually by property owners to the SF Rent Board, with addresses
anonymized to block level.  Fields include submission year, occupancy
type, bedroom/bathroom counts (banded), square footage (banded),
monthly rent (banded), year built, analysis neighborhood, and
supervisor district.
"""

from __future__ import annotations

import re

import pandas as pd
import requests

DATASET_ID = "gdc7-dmcn"
SOCRATA_BASE = "https://data.sfgov.org/resource"
RESOURCE_URL = f"{SOCRATA_BASE}/{DATASET_ID}.csv"

# Geography constants
SF_COUNTY_FIPS = "06075"
SF_PLACE_FIPS = "0667000"
SF_MSA_CBSA = "41860"
CA_STATE_FIPS = "06"

# Inventory began with 2022 submissions
INVENTORY_START_YEAR = 2022

# Fields to request from the Socrata CSV export
DEFAULT_FIELDS = [
    "unique_id",
    "block_num",
    "unit_count",
    "case_type_name",
    "submission_year",
    "block_address",
    "occupancy_type",
    "bedroom_count",
    "bathroom_count",
    "square_footage",
    "monthly_rent",
    "year_property_built",
    "analysis_neighborhood",
    "supervisor_district",
    "signature_date",
    "data_as_of",
]


class SFFetchError(RuntimeError):
    """Raised when the Socrata endpoint returns an unexpected response."""


def fetch_inventory(
    *,
    limit: int = 50_000,
    offset: int = 0,
    submission_year: int | None = None,
    fields: list[str] | None = None,
    timeout: int = 120,
    app_token: str | None = None,
) -> pd.DataFrame:
    """Fetch rows from the SF Rent Board Housing Inventory via Socrata.

    Parameters
    ----------
    limit : int
        Maximum rows per request (Socrata CSV default cap is 50 000).
    offset : int
        Row offset for pagination.
    submission_year : int or None
        If provided, filter to a single submission year.
    fields : list[str] or None
        Columns to request.  Defaults to :data:`DEFAULT_FIELDS`.
    timeout : int
        HTTP request timeout in seconds.
    app_token : str or None
        Optional Socrata app token for higher rate limits.

    Returns
    -------
    pd.DataFrame
    """
    cols = fields or DEFAULT_FIELDS
    params: dict[str, str | int] = {
        "$select": ",".join(cols),
        "$limit": limit,
        "$offset": offset,
        "$order": "submission_year DESC, unique_id",
    }
    if submission_year is not None:
        params["$where"] = f"submission_year='{submission_year}'"
    headers: dict[str, str] = {}
    if app_token:
        headers["X-App-Token"] = app_token
    resp = requests.get(RESOURCE_URL, params=params, headers=headers, timeout=timeout)
    resp.raise_for_status()
    if not resp.text.strip():
        raise SFFetchError("Socrata returned empty response")
    from io import StringIO
    df = pd.read_csv(StringIO(resp.text))
    if df.empty:
        return df
    # coerce types
    for col in ["unit_count", "submission_year", "supervisor_district"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "signature_date" in df.columns:
        df["signature_date"] = pd.to_datetime(df["signature_date"], errors="coerce")
    if "data_as_of" in df.columns:
        df["data_as_of"] = pd.to_datetime(df["data_as_of"], errors="coerce")
    return df


def fetch_full_inventory(
    *,
    page_size: int = 50_000,
    submission_year: int | None = None,
    fields: list[str] | None = None,
    timeout: int = 120,
    app_token: str | None = None,
    row_limit: int | None = None,
) -> pd.DataFrame:
    """Page through the full inventory and return a single DataFrame."""
    frames: list[pd.DataFrame] = []
    offset = 0
    while True:
        batch = fetch_inventory(
            limit=page_size,
            offset=offset,
            submission_year=submission_year,
            fields=fields,
            timeout=timeout,
            app_token=app_token,
        )
        if batch.empty:
            break
        frames.append(batch)
        if row_limit is not None and sum(len(f) for f in frames) >= row_limit:
            break
        if len(batch) < page_size:
            break
        offset += len(batch)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Rent-band parsing
# ---------------------------------------------------------------------------

_RENT_PATTERN = re.compile(r"\$(\d[\d,]*)")


def parse_rent_midpoint(band: str) -> float | None:
    """Parse a banded monthly rent string into a numeric midpoint.

    Examples: ``"$2001-$2250"`` → ``2125.5``, ``"$5001 or more"`` → ``5001.0``.
    Returns ``None`` for unparseable values.
    """
    if not isinstance(band, str):
        return None
    nums = [int(s.replace(",", "")) for s in _RENT_PATTERN.findall(band)]
    if len(nums) == 2:
        return (nums[0] + nums[1]) / 2
    if len(nums) == 1:
        return float(nums[0])
    return None


_BEDROOM_MAP = {
    "Studio": 0,
    "One-Bedroom": 1,
    "Two-Bedroom": 2,
    "Three-Bedroom": 3,
    "Four-Bedroom": 4,
    "Five or more bedrooms": 5,
}


def parse_bedroom_count(band: str) -> int | None:
    """Map a banded bedroom string to an integer."""
    return _BEDROOM_MAP.get(band)


# ---------------------------------------------------------------------------
# Summarization helpers
# ---------------------------------------------------------------------------


def add_parsed_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add numeric rent midpoint and bedroom count columns."""
    out = df.copy()
    if "monthly_rent" in out.columns:
        out["rent_midpoint"] = out["monthly_rent"].map(parse_rent_midpoint)
    if "bedroom_count" in out.columns:
        out["bedrooms"] = out["bedroom_count"].map(parse_bedroom_count)
    return out


def summarize_by_year(df: pd.DataFrame) -> pd.DataFrame:
    """Summarize inventory submissions by year."""
    data = add_parsed_columns(df)
    agg = (
        data.groupby("submission_year", dropna=False)
        .agg(
            unit_rows=("unique_id", "count"),
            unique_blocks=("block_num", "nunique"),
            neighborhoods=("analysis_neighborhood", "nunique"),
            median_rent_midpoint=("rent_midpoint", "median"),
            mean_rent_midpoint=("rent_midpoint", "mean"),
            occupied_nonowner=("occupancy_type", lambda s: (s == "Occupied by non-owner").sum()),
            occupied_owner=("occupancy_type", lambda s: (s == "Occupied by owner").sum()),
        )
        .reset_index()
        .sort_values("submission_year")
        .reset_index(drop=True)
    )
    if "occupied_nonowner" in agg.columns and "unit_rows" in agg.columns:
        agg["nonowner_share"] = agg["occupied_nonowner"] / agg["unit_rows"]
    return agg


def summarize_by_neighborhood(df: pd.DataFrame) -> pd.DataFrame:
    """Summarize inventory submissions by analysis neighborhood."""
    data = add_parsed_columns(df)
    return (
        data.groupby("analysis_neighborhood", dropna=False)
        .agg(
            unit_rows=("unique_id", "count"),
            unique_blocks=("block_num", "nunique"),
            median_rent_midpoint=("rent_midpoint", "median"),
            mean_rent_midpoint=("rent_midpoint", "mean"),
            median_bedrooms=("bedrooms", "median"),
            occupied_nonowner=("occupancy_type", lambda s: (s == "Occupied by non-owner").sum()),
        )
        .reset_index()
        .sort_values("unit_rows", ascending=False)
        .reset_index(drop=True)
    )


def summarize_by_district(df: pd.DataFrame) -> pd.DataFrame:
    """Summarize inventory submissions by supervisor district."""
    data = add_parsed_columns(df)
    return (
        data.groupby("supervisor_district", dropna=False)
        .agg(
            unit_rows=("unique_id", "count"),
            unique_blocks=("block_num", "nunique"),
            median_rent_midpoint=("rent_midpoint", "median"),
            mean_rent_midpoint=("rent_midpoint", "mean"),
            occupied_nonowner=("occupancy_type", lambda s: (s == "Occupied by non-owner").sum()),
        )
        .reset_index()
        .sort_values("supervisor_district")
        .reset_index(drop=True)
    )


def summarize_year_by_neighborhood(df: pd.DataFrame) -> pd.DataFrame:
    """Summarize inventory by submission year and neighborhood."""
    data = add_parsed_columns(df)
    out = (
        data.groupby(["submission_year", "analysis_neighborhood"], dropna=False)
        .agg(
            unit_rows=("unique_id", "count"),
            unique_blocks=("block_num", "nunique"),
            median_rent_midpoint=("rent_midpoint", "median"),
            mean_rent_midpoint=("rent_midpoint", "mean"),
            occupied_nonowner=("occupancy_type", lambda s: (s == "Occupied by non-owner").sum()),
            occupied_owner=("occupancy_type", lambda s: (s == "Occupied by owner").sum()),
        )
        .reset_index()
        .sort_values(["submission_year", "unit_rows", "analysis_neighborhood"], ascending=[True, False, True])
        .reset_index(drop=True)
    )
    out["nonowner_share"] = out["occupied_nonowner"] / out["unit_rows"]
    return out


def summarize_year_by_district(df: pd.DataFrame) -> pd.DataFrame:
    """Summarize inventory by submission year and supervisor district."""
    data = add_parsed_columns(df)
    out = (
        data.groupby(["submission_year", "supervisor_district"], dropna=False)
        .agg(
            unit_rows=("unique_id", "count"),
            unique_blocks=("block_num", "nunique"),
            median_rent_midpoint=("rent_midpoint", "median"),
            mean_rent_midpoint=("rent_midpoint", "mean"),
            occupied_nonowner=("occupancy_type", lambda s: (s == "Occupied by non-owner").sum()),
            occupied_owner=("occupancy_type", lambda s: (s == "Occupied by owner").sum()),
        )
        .reset_index()
        .sort_values(["submission_year", "supervisor_district"])
        .reset_index(drop=True)
    )
    out["nonowner_share"] = out["occupied_nonowner"] / out["unit_rows"]
    return out


def summarize_rent_bands(df: pd.DataFrame) -> pd.DataFrame:
    """Summarize unit counts by rent band and year."""
    data = add_parsed_columns(df)
    return (
        data.groupby(["submission_year", "monthly_rent"], dropna=False)
        .agg(
            unit_rows=("unique_id", "count"),
            mean_rent_midpoint=("rent_midpoint", "mean"),
        )
        .reset_index()
        .sort_values(["submission_year", "unit_rows", "monthly_rent"], ascending=[True, False, True])
        .reset_index(drop=True)
    )


def summarize_occupancy(df: pd.DataFrame) -> pd.DataFrame:
    """Summarize occupancy mix by year and geography."""
    data = add_parsed_columns(df)
    out = (
        data.groupby(["submission_year", "occupancy_type"], dropna=False)
        .agg(
            unit_rows=("unique_id", "count"),
            neighborhoods=("analysis_neighborhood", "nunique"),
            districts=("supervisor_district", "nunique"),
        )
        .reset_index()
        .sort_values(["submission_year", "unit_rows"], ascending=[True, False])
        .reset_index(drop=True)
    )
    year_totals = (
        data.groupby("submission_year", dropna=False)
        .size()
        .rename("year_total")
        .reset_index()
    )
    out = out.merge(year_totals, how="left", on="submission_year")
    out["share_of_year"] = out["unit_rows"] / out["year_total"]
    return out


def summarize_reporting_rollout(df: pd.DataFrame) -> pd.DataFrame:
    """Summarize reporting/compliance rollout by year."""
    data = add_parsed_columns(df)
    out = (
        data.groupby("submission_year", dropna=False)
        .agg(
            unit_rows=("unique_id", "count"),
            unique_blocks=("block_num", "nunique"),
            unique_neighborhoods=("analysis_neighborhood", "nunique"),
            nonowner_rows=("occupancy_type", lambda s: (s == "Occupied by non-owner").sum()),
            owner_rows=("occupancy_type", lambda s: (s == "Occupied by owner").sum()),
        )
        .reset_index()
        .sort_values("submission_year")
        .reset_index(drop=True)
    )
    out["unit_row_growth"] = out["unit_rows"].diff()
    out["block_growth"] = out["unique_blocks"].diff()
    out["nonowner_share"] = out["nonowner_rows"] / out["unit_rows"]
    return out


def summarize_overall(df: pd.DataFrame) -> pd.DataFrame:
    """Return high-level inventory summary metrics."""
    data = add_parsed_columns(df)
    years = sorted(data["submission_year"].dropna().unique())
    return pd.DataFrame(
        [
            {"metric": "total_unit_rows", "value": len(data)},
            {"metric": "submission_years", "value": ", ".join(str(int(y)) for y in years)},
            {"metric": "unique_blocks", "value": int(data["block_num"].nunique())},
            {"metric": "neighborhoods", "value": int(data["analysis_neighborhood"].nunique())},
            {"metric": "supervisor_districts", "value": int(data["supervisor_district"].dropna().nunique())},
            {"metric": "median_rent_midpoint", "value": round(data["rent_midpoint"].median(), 1) if "rent_midpoint" in data.columns else None},
            {"metric": "nonowner_occupied_rows", "value": int((data["occupancy_type"] == "Occupied by non-owner").sum())},
        ]
    )
