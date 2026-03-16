from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd


STATE_COLUMNS = [
    "survey",
    "state_fips",
    "region_code",
    "division_code",
    "state_name",
    "u1_bldgs",
    "u1_units",
    "u1_value",
    "u2_bldgs",
    "u2_units",
    "u2_value",
    "u34_bldgs",
    "u34_units",
    "u34_value",
    "u5p_bldgs",
    "u5p_units",
    "u5p_value",
    "rep_u1_bldgs",
    "rep_u1_units",
    "rep_u1_value",
    "rep_u2_bldgs",
    "rep_u2_units",
    "rep_u2_value",
    "rep_u34_bldgs",
    "rep_u34_units",
    "rep_u34_value",
    "rep_u5p_bldgs",
    "rep_u5p_units",
    "rep_u5p_value",
]

COUNTY_COLUMNS = [
    "survey",
    "state_fips",
    "county_fips",
    "region_code",
    "division_code",
    "county_name",
    "u1_bldgs",
    "u1_units",
    "u1_value",
    "u2_bldgs",
    "u2_units",
    "u2_value",
    "u34_bldgs",
    "u34_units",
    "u34_value",
    "u5p_bldgs",
    "u5p_units",
    "u5p_value",
    "rep_u1_bldgs",
    "rep_u1_units",
    "rep_u1_value",
    "rep_u2_bldgs",
    "rep_u2_units",
    "rep_u2_value",
    "rep_u34_bldgs",
    "rep_u34_units",
    "rep_u34_value",
    "rep_u5p_bldgs",
    "rep_u5p_units",
    "rep_u5p_value",
]


NUMERIC_COLUMNS = [c for c in STATE_COLUMNS if c not in {"survey", "state_fips", "region_code", "division_code", "state_name"}]
COUNTY_NUMERIC_COLUMNS = [c for c in COUNTY_COLUMNS if c not in {"survey", "state_fips", "county_fips", "region_code", "division_code", "county_name"}]


def _clean_frame(df: pd.DataFrame, numeric_columns: list[str]) -> pd.DataFrame:
    df = df.copy()
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    if "survey" in df.columns:
        df["year"] = df["survey"].astype(str).str[:4].astype(int)
    if "state_name" in df.columns:
        df["state_name"] = df["state_name"].astype(str).str.strip()
    if "county_name" in df.columns:
        df["county_name"] = df["county_name"].astype(str).str.strip()
    if "state_fips" in df.columns:
        df["state_fips"] = df["state_fips"].astype(str).str.zfill(2)
    if "county_fips" in df.columns:
        df["county_fips"] = df["county_fips"].astype(str).str.zfill(3)

    if {"u1_units", "u2_units", "u34_units", "u5p_units"}.issubset(df.columns):
        df["permits_units_total"] = df[["u1_units", "u2_units", "u34_units", "u5p_units"]].sum(axis=1)
        df["permits_units_multifamily"] = df[["u2_units", "u34_units", "u5p_units"]].sum(axis=1)
        df["permits_units_multifamily_share"] = df["permits_units_multifamily"] / df["permits_units_total"].replace({0: pd.NA})
    return df


def parse_state_annual_file(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    df = pd.read_csv(path, skiprows=3, header=None, names=STATE_COLUMNS, dtype=str)
    df = _clean_frame(df, NUMERIC_COLUMNS)
    df["source_file"] = path.name
    return df


def load_state_annual_dir(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    files = sorted(path.glob("st*a.txt"))
    if not files:
        raise FileNotFoundError(f"No BPS state files found in {path}")
    frames = [parse_state_annual_file(fp) for fp in files]
    out = pd.concat(frames, ignore_index=True)
    return out.sort_values(["state_fips", "year"]).reset_index(drop=True)


def parse_county_ytd_file(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    df = pd.read_csv(path, skiprows=3, header=None, names=COUNTY_COLUMNS, dtype=str)
    df = _clean_frame(df, COUNTY_NUMERIC_COLUMNS)
    df["source_file"] = path.name
    df["state_county_fips"] = df["state_fips"] + df["county_fips"]
    return df
