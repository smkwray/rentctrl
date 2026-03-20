from __future__ import annotations

from pathlib import Path
import pandas as pd


def load_policy_events(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype={"state_fips": str})
    df["state_fips"] = df["state_fips"].str.zfill(2)
    df["effective_date"] = pd.to_datetime(df["effective_date"], errors="coerce")
    return df


def expand_quarterly_policy_panel(
    state_metadata: pd.DataFrame,
    policy_events: pd.DataFrame,
    *,
    start: str = "2010Q1",
    end: str = "2026Q4",
) -> pd.DataFrame:
    periods = pd.period_range(start=start, end=end, freq="Q")
    base = (
        state_metadata.assign(key=1)
        .merge(pd.DataFrame({"period": periods, "key": 1}), on="key", how="outer")
        .drop(columns="key")
    )
    base["year"] = base["period"].dt.year
    base["quarter"] = "Q" + base["period"].dt.quarter.astype(str)
    base["calendar_period"] = base["period"].astype(str)

    merged = base.merge(
        policy_events[
            [
                "state_abbr",
                "effective_date",
                "preferred_quarterly_treat_period",
                "alternative_quarterly_treat_period",
                "preferred_annual_treat_year",
                "analysis_role",
                "policy_name",
            ]
        ],
        on="state_abbr",
        how="left",
        suffixes=("", "_policy"),
    )

    merged["ever_treated"] = merged["ever_treated"].fillna(0).astype(int)
    merged["preferred_treat_period"] = pd.PeriodIndex(
        merged["preferred_quarterly_treat_period"].fillna("2099Q4"), freq="Q"
    )
    merged["alternative_treat_period"] = pd.PeriodIndex(
        merged["alternative_quarterly_treat_period"].fillna("2099Q4"), freq="Q"
    )
    merged["policy_active_preferred"] = (
        merged["ever_treated"].eq(1) & (merged["period"] >= merged["preferred_treat_period"])
    ).astype(int)
    merged["policy_active_alternative"] = (
        merged["ever_treated"].eq(1) & (merged["period"] >= merged["alternative_treat_period"])
    ).astype(int)
    merged["event_time_quarters_preferred"] = (
        (merged["period"].dt.year - merged["preferred_treat_period"].dt.year) * 4
        + (merged["period"].dt.quarter - merged["preferred_treat_period"].dt.quarter)
    )
    merged["event_time_quarters_alternative"] = (
        (merged["period"].dt.year - merged["alternative_treat_period"].dt.year) * 4
        + (merged["period"].dt.quarter - merged["alternative_treat_period"].dt.quarter)
    )
    merged.loc[merged["ever_treated"].eq(0), "event_time_quarters_preferred"] = pd.NA
    merged.loc[merged["ever_treated"].eq(0), "event_time_quarters_alternative"] = pd.NA
    return merged.sort_values(["state_abbr", "period"]).reset_index(drop=True)


def aggregate_annual_policy_panel(quarterly_panel: pd.DataFrame) -> pd.DataFrame:
    out = (
        quarterly_panel.groupby(
            ["state_name", "state_abbr", "state_fips", "analysis_role", "ever_treated", "year"],
            as_index=False,
        )
        .agg(
            policy_active_preferred=("policy_active_preferred", "max"),
            policy_active_alternative=("policy_active_alternative", "max"),
            first_event_time_quarters=("event_time_quarters_preferred", "min"),
            first_event_time_quarters_alternative=("event_time_quarters_alternative", "min"),
        )
    )
    out["event_time_years_preferred"] = out["first_event_time_quarters"] / 4.0
    out["event_time_years_alternative"] = out["first_event_time_quarters_alternative"] / 4.0
    return out.sort_values(["state_abbr", "year"]).reset_index(drop=True)
