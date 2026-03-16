from __future__ import annotations

import pandas as pd


# Saint Paul rent stabilization policy timeline
# Source: https://www.stpaul.gov/departments/safety-inspections/rent-buy-sell-property/rent-stabilization
POLICY_EVENTS = [
    {"date": "2021-11-02", "event": "ballot_adoption", "description": "Voters approved rent stabilization ballot initiative"},
    {"date": "2022-05-01", "event": "ordinance_effective", "description": "Ordinance took effect, 3% annual cap"},
    {"date": "2022-07-01", "event": "stakeholder_report", "description": "Stakeholder group final report released"},
    {"date": "2023-01-01", "event": "amendment_2023", "description": "Council amendments effective: new construction exemption, vacancy decontrol, self-certification"},
    {"date": "2025-06-13", "event": "amendment_2025", "description": "Additional amendments effective: expanded exemptions"},
]

# Geography constants
RAMSEY_COUNTY_FIPS = "27123"
TWIN_CITIES_MSA_CBSA = "33460"
SAINT_PAUL_PLACE_FIPS = "2758000"
MN_STATE_FIPS = "27"

# Potential comparison counties (Minnesota)
COMPARISON_COUNTIES = {
    "Hennepin": "27053",
    "Dakota": "27037",
    "Olmsted": "27109",
}

PRIMARY_CONTROL_COUNTIES = {
    "Hennepin County": "27053",
    "Dakota County": "27037",
}


def build_policy_event_table() -> pd.DataFrame:
    """Return the Saint Paul rent stabilization policy event timeline."""
    df = pd.DataFrame(POLICY_EVENTS)
    df["date"] = pd.to_datetime(df["date"])
    return df


def label_pre_post(
    df: pd.DataFrame,
    date_col: str,
    *,
    event: str = "ordinance_effective",
) -> pd.DataFrame:
    """Add a ``period`` column labeling rows as pre or post a policy event.

    Parameters
    ----------
    df : DataFrame
        Must contain a datetime-like column named *date_col*.
    date_col : str
        Name of the date column to compare against the event date.
    event : str
        Key into ``POLICY_EVENTS`` (matched on the ``event`` field).

    Returns
    -------
    DataFrame
        Copy of *df* with an added ``period`` column (``"pre"`` or ``"post"``).
    """
    events = build_policy_event_table()
    match = events.loc[events["event"] == event, "date"]
    if match.empty:
        raise ValueError(f"Unknown policy event: {event!r}")
    cutoff = match.iloc[0]
    out = df.copy()
    dates = pd.to_datetime(out[date_col])
    out["period"] = "pre"
    out.loc[dates >= cutoff, "period"] = "post"
    return out


def summarize_by_period(
    df: pd.DataFrame,
    value_col: str,
    *,
    group_col: str | None = None,
) -> pd.DataFrame:
    """Compute mean and count of *value_col* by period (and optional group).

    Expects a ``period`` column as produced by :func:`label_pre_post`.
    """
    if "period" not in df.columns:
        raise ValueError("DataFrame must contain a 'period' column; call label_pre_post first.")
    group_keys = ["period"]
    if group_col is not None:
        group_keys.insert(0, group_col)
    summary = (
        df.groupby(group_keys, sort=False)[value_col]
        .agg(["mean", "count"])
        .reset_index()
    )
    summary.columns = [*group_keys, f"{value_col}_mean", "n"]
    return summary


def add_quarter_period(df: pd.DataFrame, *, year_col: str = "year", quarter_col: str = "quarter") -> pd.DataFrame:
    out = df.copy()
    out["calendar_period"] = (
        out[year_col].astype(int).astype(str) + "Q" + out[quarter_col].astype(int).astype(str)
    )
    out["date"] = pd.PeriodIndex(out["calendar_period"], freq="Q").to_timestamp(how="end")
    return out


def summarize_treated_vs_controls(
    df: pd.DataFrame,
    *,
    value_col: str,
    group_col: str,
    treated_group: str,
    control_groups: list[str],
    date_col: str = "date",
    event: str = "ordinance_effective",
) -> pd.DataFrame:
    sample = df[df[group_col].isin([treated_group, *control_groups])].copy()
    sample = label_pre_post(sample, date_col, event=event)
    treated = summarize_by_period(sample[sample[group_col] == treated_group], value_col)
    treated = treated.rename(columns={f"{value_col}_mean": "treated_mean", "n": "treated_n"})

    controls = (
        sample[sample[group_col].isin(control_groups)]
        .groupby(["period", date_col], as_index=False)[value_col]
        .mean()
    )
    control_summary = summarize_by_period(controls, value_col)
    control_summary = control_summary.rename(columns={f"{value_col}_mean": "control_mean", "n": "control_n"})

    out = treated.merge(control_summary, on="period", how="outer")
    out["treated_change"] = out["treated_mean"] - out.loc[out["period"] == "pre", "treated_mean"].iloc[0]
    out["control_change"] = out["control_mean"] - out.loc[out["period"] == "pre", "control_mean"].iloc[0]
    out["diff_in_diff"] = out["treated_change"] - out["control_change"]
    out["value_col"] = value_col
    out["treated_group"] = treated_group
    out["control_groups"] = ", ".join(control_groups)
    return out[
        [
            "value_col",
            "period",
            "treated_group",
            "control_groups",
            "treated_mean",
            "treated_n",
            "control_mean",
            "control_n",
            "treated_change",
            "control_change",
            "diff_in_diff",
        ]
    ]
