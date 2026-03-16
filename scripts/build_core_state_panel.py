from __future__ import annotations

from pathlib import Path
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = ROOT / "data" / "processed"


def maybe_read(path: Path) -> pd.DataFrame | None:
    p = Path(path)
    if p.exists():
        return pd.read_csv(p, dtype={"state_fips": str})
    return None


def build_annual_panel() -> pd.DataFrame:
    policy = maybe_read(PROCESSED_DIR / "policy_panel_state_annual_2010_2026.csv")
    fhfa = maybe_read(PROCESSED_DIR / "fhfa_state_annual_purchase_only_2010_2025.csv")
    bps = maybe_read(PROCESSED_DIR / "bps_state_annual_2010_2024.csv")
    acs = maybe_read(PROCESSED_DIR / "acs_state_profile_panel.csv")
    qcew = maybe_read(PROCESSED_DIR / "qcew_state_annual_core.csv")
    if qcew is None:
        qcew = maybe_read(PROCESSED_DIR / "qcew_state_annual_total_covered.csv")

    if policy is None:
        raise FileNotFoundError(f"Missing {PROCESSED_DIR / 'policy_panel_state_annual_2010_2026.csv'}")

    panel = policy.copy()

    if fhfa is not None:
        panel = panel.merge(
            fhfa[["state_name", "year", "index_sa_mean", "index_nsa_mean"]],
            on=["state_name", "year"],
            how="left",
        )

    if bps is not None:
        panel = panel.merge(
            bps[
                [
                    "state_name",
                    "year",
                    "permits_units_total",
                    "u5p_units",
                    "permits_units_multifamily",
                    "permits_units_multifamily_share",
                ]
            ],
            on=["state_name", "year"],
            how="left",
        )

    if acs is not None:
        acs = acs.rename(columns={"NAME": "state_name", "state": "state_fips"})
        acs["state_fips"] = acs["state_fips"].astype(str).str.zfill(2)
        keep = [
            "state_name",
            "state_fips",
            "year",
            "same_house_1y_pct",
            "moved_last_year_pct",
            "moved_within_us_pct",
            "moved_different_state_pct",
            "median_household_income",
            "renter_households",
            "renter_share_pct",
            "median_gross_rent",
            "DP02_0080PE",
            "DP02_0081PE",
            "DP02_0082PE",
            "DP02_0086PE",
            "DP03_0062E",
            "DP04_0047E",
            "DP04_0047PE",
            "DP04_0134E",
            "DP04_0141PE",
            "DP04_0142PE",
            "rent_burden_30_plus_pct",
        ]
        keep = [c for c in keep if c in acs.columns]
        panel = panel.merge(acs[keep], on=["state_name", "state_fips", "year"], how="left")

    if qcew is not None:
        join_cols = [c for c in ["state_abbr", "state_fips", "year"] if c in qcew.columns]
        panel = panel.merge(qcew, on=join_cols, how="left", suffixes=("", "_qcew"))

    alias_map = {
        "u5p_units": "permits_units_5plus",
        "DP04_0047E": "renter_households",
        "DP03_0062E": "median_household_income",
        "DP04_0047PE": "renter_share_pct",
        "DP04_0134E": "median_gross_rent",
        "qcew_total_covered_avg_wkly_wage": "qcew_total_covered_avg_weekly_wage",
        "qcew_private_avg_wkly_wage": "qcew_private_avg_weekly_wage",
    }
    for source_col, alias_col in alias_map.items():
        if source_col in panel.columns and alias_col not in panel.columns:
            panel[alias_col] = panel[source_col]

    return panel


def build_quarterly_panel() -> pd.DataFrame:
    policy = maybe_read(PROCESSED_DIR / "policy_panel_state_quarterly_2010_2026.csv")
    fhfa = maybe_read(PROCESSED_DIR / "fhfa_state_quarterly_purchase_only_2010_2025.csv")
    qcew = maybe_read(PROCESSED_DIR / "qcew_state_quarterly_core.csv")
    state_meta = maybe_read(ROOT / "config" / "state_metadata.csv")

    if policy is None:
        raise FileNotFoundError(f"Missing {PROCESSED_DIR / 'policy_panel_state_quarterly_2010_2026.csv'}")

    panel = policy.copy()

    if fhfa is not None:
        if state_meta is not None:
            selected_state_abbrs = set(state_meta["state_abbr"])
            fhfa = fhfa[fhfa["fhfa_place_id"].isin(selected_state_abbrs)].copy()
        fhfa = fhfa.rename(columns={"fhfa_place_id": "state_abbr", "yr": "year"})
        panel = panel.merge(
            fhfa[["state_abbr", "year", "quarter", "index_sa", "index_nsa", "year_quarter"]],
            on=["state_abbr", "year", "quarter"],
            how="left",
        )

    if qcew is not None:
        qcew = qcew.copy()
        qcew["quarter"] = qcew["quarter"].astype(str).map(lambda value: value if value.startswith("Q") else f"Q{value}")
        panel = panel.merge(qcew, on=["state_abbr", "state_fips", "year", "quarter"], how="left")

    quarterly_alias_map = {
        "qcew_total_covered_avg_wkly_wage": "qcew_total_covered_avg_weekly_wage",
        "qcew_private_avg_wkly_wage": "qcew_private_avg_weekly_wage",
    }
    for source_col, alias_col in quarterly_alias_map.items():
        if source_col in panel.columns and alias_col not in panel.columns:
            panel[alias_col] = panel[source_col]

    return panel


def main() -> None:
    annual_panel = build_annual_panel()
    quarterly_panel = build_quarterly_panel()

    annual_output = PROCESSED_DIR / "core_state_panel_annual.csv"
    quarterly_output = PROCESSED_DIR / "core_state_panel_quarterly.csv"
    annual_output.parent.mkdir(parents=True, exist_ok=True)

    annual_panel.to_csv(annual_output, index=False)
    quarterly_panel.to_csv(quarterly_output, index=False)
    print(f"wrote {annual_output}")
    print(f"wrote {quarterly_output}")


if __name__ == "__main__":
    main()
