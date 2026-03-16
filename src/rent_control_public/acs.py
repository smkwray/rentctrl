from __future__ import annotations

from typing import Iterable
import requests
import pandas as pd

from rent_control_public.constants import get_acs_profile_variables_for_year


ACS_BASE = "https://api.census.gov/data/{year}/acs/acs1/profile"


def build_state_profile_url(year: int, variables: Iterable[str]) -> str:
    var_string = ",".join(["NAME", *variables])
    return f"{ACS_BASE.format(year=year)}?get={var_string}&for=state:*"


def fetch_state_profile(year: int, variables: Iterable[str] | None = None, timeout: int = 60) -> pd.DataFrame:
    if variables is None:
        variables = get_acs_profile_variables_for_year(year)
    url = build_state_profile_url(year, variables)
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    data = response.json()
    df = pd.DataFrame(data[1:], columns=data[0])
    df["year"] = year
    return df


def add_computed_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if col not in {"NAME", "state"}:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    year = int(out["year"].dropna().iloc[0]) if "year" in out.columns and out["year"].notna().any() else None
    if year is not None:
        if year <= 2018:
            same_house_col = "DP02_0079PE"
            moved_within_us_col = "DP02_0080PE"
            moved_different_state_col = "DP02_0084PE"
        else:
            same_house_col = "DP02_0080PE"
            moved_within_us_col = "DP02_0081PE"
            moved_different_state_col = "DP02_0085PE"

        if same_house_col in out.columns:
            out["same_house_1y_pct"] = out[same_house_col]
            out["moved_last_year_pct"] = (100 - out[same_house_col]).round(1)
        if moved_within_us_col in out.columns:
            out["moved_within_us_pct"] = out[moved_within_us_col]
        if moved_different_state_col in out.columns:
            out["moved_different_state_pct"] = out[moved_different_state_col]

        if year <= 2014:
            if "DP04_0132E" in out.columns:
                out["DP04_0134E"] = out["DP04_0132E"]
                out["median_gross_rent"] = out["DP04_0132E"]
            if "DP04_0139PE" in out.columns:
                out["DP04_0141PE"] = out["DP04_0139PE"]
                out["rent_burden_30_34_9_pct"] = out["DP04_0139PE"]
            if "DP04_0140PE" in out.columns:
                out["DP04_0142PE"] = out["DP04_0140PE"]
                out["rent_burden_35_plus_pct"] = out["DP04_0140PE"]
        else:
            if "DP04_0134E" in out.columns:
                out["median_gross_rent"] = out["DP04_0134E"]
            if "DP04_0141PE" in out.columns:
                out["rent_burden_30_34_9_pct"] = out["DP04_0141PE"]
            if "DP04_0142PE" in out.columns:
                out["rent_burden_35_plus_pct"] = out["DP04_0142PE"]

    if {"DP04_0141PE", "DP04_0142PE"}.issubset(out.columns):
        out["rent_burden_30_plus_pct"] = out[["DP04_0141PE", "DP04_0142PE"]].sum(axis=1, min_count=1)
    return out
