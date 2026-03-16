from __future__ import annotations

from pathlib import Path

import pandas as pd


def load_master(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["yr"] = pd.to_numeric(df["yr"], errors="coerce").astype("Int64")
    df["period"] = pd.to_numeric(df["period"], errors="coerce").astype("Int64")
    df["index_nsa"] = pd.to_numeric(df["index_nsa"], errors="coerce")
    df["index_sa"] = pd.to_numeric(df["index_sa"], errors="coerce")
    df["quarter"] = df["period"].map({1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4"})
    df["year_quarter"] = df["yr"].astype(str) + df["quarter"].fillna("")
    return df


def filter_state_quarterly(
    df: pd.DataFrame,
    *,
    flavor: str = "purchase-only",
    min_year: int = 2010,
) -> pd.DataFrame:
    out = df[
        (df["level"] == "State")
        & (df["frequency"] == "quarterly")
        & (df["hpi_type"] == "traditional")
        & (df["hpi_flavor"] == flavor)
        & (df["yr"] >= min_year)
    ].copy()
    out = out.rename(columns={"place_name": "state_name", "place_id": "fhfa_place_id"})
    return out.sort_values(["state_name", "yr", "period"]).reset_index(drop=True)


def aggregate_state_annual(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby(["state_name", "fhfa_place_id", "yr"], as_index=False)
        .agg(index_nsa_mean=("index_nsa", "mean"), index_sa_mean=("index_sa", "mean"))
        .rename(columns={"yr": "year"})
    )
    return out.sort_values(["state_name", "year"]).reset_index(drop=True)


def filter_msa_quarterly_for_state_abbrs(
    df: pd.DataFrame,
    state_abbrs: list[str],
    *,
    flavor: str = "purchase-only",
    min_year: int = 2010,
) -> pd.DataFrame:
    pattern = tuple(f", {abbr}" for abbr in state_abbrs)
    out = df[
        (df["level"] == "MSA")
        & (df["frequency"] == "quarterly")
        & (df["hpi_type"] == "traditional")
        & (df["hpi_flavor"] == flavor)
        & (df["yr"] >= min_year)
        & df["place_name"].str.endswith(pattern)
    ].copy()
    out = out.rename(columns={"place_name": "msa_name", "place_id": "fhfa_place_id"})
    return out.sort_values(["msa_name", "yr", "period"]).reset_index(drop=True)
