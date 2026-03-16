from __future__ import annotations

from typing import Iterable
import pandas as pd
import requests


QCEW_URL = "https://data.bls.gov/cew/data/api/{year}/{quarter}/area/{area_code}.csv"


def build_area_slice_url(year: int, quarter: int, area_code: str) -> str:
    return QCEW_URL.format(year=year, quarter=quarter, area_code=area_code)


def state_area_code(state_fips: str) -> str:
    return f"{str(state_fips).zfill(2)}000"


def fetch_area_slice(year: int, quarter: int, area_code: str, timeout: int = 60) -> pd.DataFrame:
    url = build_area_slice_url(year, quarter, area_code)
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return pd.read_csv(pd.io.common.StringIO(response.text))


def filter_state_total_covered(df: pd.DataFrame) -> pd.DataFrame:
    needed = {"own_code", "industry_code", "agglvl_code"}
    if not needed.issubset(df.columns):
        raise ValueError(f"QCEW frame missing required columns: {needed - set(df.columns)}")
    out = df[
        (df["own_code"].astype(str) == "0")
        & (df["industry_code"].astype(str) == "10")
        & (df["agglvl_code"].astype(str) == "50")
    ].copy()
    return out.reset_index(drop=True)


def filter_state_private_total(df: pd.DataFrame) -> pd.DataFrame:
    needed = {"own_code", "industry_code", "agglvl_code"}
    if not needed.issubset(df.columns):
        raise ValueError(f"QCEW frame missing required columns: {needed - set(df.columns)}")
    out = df[
        (df["own_code"].astype(str) == "5")
        & (df["industry_code"].astype(str) == "10")
        & (df["agglvl_code"].astype(str) == "51")
    ].copy()
    return out.reset_index(drop=True)


def reshape_qcew_core(panel: pd.DataFrame, *, prefix: str) -> pd.DataFrame:
    keep = [
        "state_abbr",
        "state_fips",
        "year",
        "quarter",
        "qtrly_estabs",
        "month1_emplvl",
        "month2_emplvl",
        "month3_emplvl",
        "total_qtrly_wages",
        "avg_wkly_wage",
    ]
    out = panel[keep].copy()
    out[f"{prefix}_estabs"] = out["qtrly_estabs"]
    out[f"{prefix}_emplvl"] = out[["month1_emplvl", "month2_emplvl", "month3_emplvl"]].mean(axis=1)
    out[f"{prefix}_wages"] = out["total_qtrly_wages"]
    out[f"{prefix}_avg_wkly_wage"] = out["avg_wkly_wage"]
    return out[
        [
            "state_abbr",
            "state_fips",
            "year",
            "quarter",
            f"{prefix}_estabs",
            f"{prefix}_emplvl",
            f"{prefix}_wages",
            f"{prefix}_avg_wkly_wage",
        ]
    ]


def annualize_core(panel: pd.DataFrame) -> pd.DataFrame:
    agg = {"state_fips": "first"}
    value_columns = [col for col in panel.columns if col not in {"state_abbr", "state_fips", "year", "quarter"}]
    for col in value_columns:
        agg[col] = "sum" if col.endswith("_wages") else "mean"
    return panel.groupby(["state_abbr", "year"], as_index=False).agg(agg)
