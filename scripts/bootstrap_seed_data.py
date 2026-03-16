from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from rent_control_public.bps import load_state_annual_dir, parse_county_ytd_file
from rent_control_public.constants import (
    CORE_TREATED_STATES,
    DESCRIPTIVE_EXTENSION_STATES,
    DONOR_POOL_STATES,
    STATE_NAME_TO_ABBR,
)
from rent_control_public.fhfa import (
    aggregate_state_annual,
    filter_msa_quarterly_for_state_abbrs,
    filter_state_quarterly,
    load_master,
)
from rent_control_public.paths import FIGURES_DIR, PROCESSED_DIR, RAW_DIR, TABLES_DIR, ensure_project_dirs
from rent_control_public.policy import aggregate_annual_policy_panel, expand_quarterly_policy_panel, load_policy_events


def main() -> None:
    ensure_project_dirs()

    # Build BPS outputs
    bps_state = load_state_annual_dir(RAW_DIR / "bps" / "state")
    bps_state.to_csv(PROCESSED_DIR / "bps_state_annual_2010_2024.csv", index=False)

    selected_states = CORE_TREATED_STATES + DESCRIPTIVE_EXTENSION_STATES + DONOR_POOL_STATES
    bps_core = bps_state[bps_state["state_name"].isin(selected_states)].copy()
    bps_core["state_abbr"] = bps_core["state_name"].map(STATE_NAME_TO_ABBR)
    bps_core.to_csv(PROCESSED_DIR / "bps_state_annual_core_states_2010_2024.csv", index=False)

    bps_county = parse_county_ytd_file(RAW_DIR / "bps" / "county" / "co2412y.txt")
    bps_county_treated = bps_county[bps_county["state_fips"].isin(["06", "41", "53"])].copy()
    bps_county_treated.to_csv(PROCESSED_DIR / "bps_county_2024_treated_states.csv", index=False)

    # FHFA outputs
    fhfa_master = load_master(RAW_DIR / "fhfa" / "hpi_master.csv")
    fhfa_state_po = filter_state_quarterly(fhfa_master, flavor="purchase-only", min_year=2010)
    fhfa_state_po.to_csv(PROCESSED_DIR / "fhfa_state_quarterly_purchase_only_2010_2025.csv", index=False)

    fhfa_state_all = filter_state_quarterly(fhfa_master, flavor="all-transactions", min_year=2010)
    fhfa_state_all.to_csv(PROCESSED_DIR / "fhfa_state_quarterly_all_transactions_2010_2025.csv", index=False)

    fhfa_state_annual = aggregate_state_annual(fhfa_state_po)
    fhfa_state_annual.to_csv(PROCESSED_DIR / "fhfa_state_annual_purchase_only_2010_2025.csv", index=False)

    fhfa_msa = filter_msa_quarterly_for_state_abbrs(
        fhfa_master,
        ["CA", "OR", "WA"],
        flavor="purchase-only",
        min_year=2010,
    )
    fhfa_msa.to_csv(PROCESSED_DIR / "fhfa_msa_treated_states_purchase_only_2010_2025.csv", index=False)

    # Policy panels
    state_meta = pd.read_csv(ROOT / "config" / "state_metadata.csv", dtype={"state_fips": str})
    policy_events = load_policy_events(ROOT / "config" / "policy_events_core.csv")
    policy_q = expand_quarterly_policy_panel(state_meta, policy_events, start="2010Q1", end="2026Q4")
    policy_q.to_csv(PROCESSED_DIR / "policy_panel_state_quarterly_2010_2026.csv", index=False)

    policy_y = aggregate_annual_policy_panel(policy_q)
    policy_y.to_csv(PROCESSED_DIR / "policy_panel_state_annual_2010_2026.csv", index=False)

    # Copy the small ACS sample into processed
    acs_sample = pd.read_csv(RAW_DIR / "acs" / "acs1_state_median_gross_rent_2024_sample.csv", dtype={"state": str})
    acs_sample = acs_sample.rename(columns={"NAME": "state_name", "B25064_001E": "median_gross_rent"})
    acs_sample["state_abbr"] = acs_sample["state_name"].map(STATE_NAME_TO_ABBR)
    acs_selected = acs_sample[acs_sample["state_abbr"].notna()].copy()
    acs_selected.to_csv(PROCESSED_DIR / "acs_state_median_gross_rent_2024_selected_states_sample.csv", index=False)

    # Summary tables
    donor_mean_bps = (
        bps_core[bps_core["state_name"].isin(DONOR_POOL_STATES)]
        .groupby("year", as_index=False)
        .agg(
            donor_mean_permits_units_total=("permits_units_total", "mean"),
            donor_mean_multifamily_share=("permits_units_multifamily_share", "mean"),
        )
    )

    treated_bps = bps_core[bps_core["state_name"].isin(CORE_TREATED_STATES + DESCRIPTIVE_EXTENSION_STATES)][
        ["state_name", "state_abbr", "year", "permits_units_total", "permits_units_multifamily_share"]
    ].copy()

    permits_summary = treated_bps.merge(donor_mean_bps, on="year", how="left")
    permits_summary.to_csv(TABLES_DIR / "bps_treated_vs_donor_mean_2010_2024.csv", index=False)

    selected_latest_permits = bps_core[bps_core["year"] == bps_core["year"].max()][
        ["state_name", "state_abbr", "permits_units_total", "permits_units_multifamily", "permits_units_multifamily_share"]
    ].sort_values("permits_units_total", ascending=False)
    selected_latest_permits.to_csv(TABLES_DIR / "bps_selected_states_latest_year.csv", index=False)

    fhfa_selected = fhfa_state_po[fhfa_state_po["state_name"].isin(selected_states)].copy()
    donor_mean_hpi = (
        fhfa_selected[fhfa_selected["state_name"].isin(DONOR_POOL_STATES)]
        .groupby(["yr", "period"], as_index=False)
        .agg(donor_mean_index_sa=("index_sa", "mean"))
    )
    treated_hpi = fhfa_selected[fhfa_selected["state_name"].isin(CORE_TREATED_STATES + DESCRIPTIVE_EXTENSION_STATES)][
        ["state_name", "yr", "period", "quarter", "index_sa"]
    ].copy()
    hpi_summary = treated_hpi.merge(donor_mean_hpi, on=["yr", "period"], how="left")
    hpi_summary.to_csv(TABLES_DIR / "fhfa_treated_vs_donor_mean_quarterly_2010_2025.csv", index=False)

    policy_events.to_csv(TABLES_DIR / "policy_summary.csv", index=False)

    # Figures
    # 1. FHFA treated vs donor mean, recent window
    fig_df = hpi_summary[hpi_summary["yr"] >= 2018].copy()
    fig_df["date_label"] = fig_df["yr"].astype(str) + fig_df["quarter"]

    plt.figure(figsize=(11, 6))
    for state_name in sorted(fig_df["state_name"].unique()):
        state_slice = fig_df[fig_df["state_name"] == state_name]
        plt.plot(state_slice["date_label"], state_slice["index_sa"], label=state_name)
    donor_line = (
        donor_mean_hpi[donor_mean_hpi["yr"] >= 2018]
        .assign(date_label=lambda x: x["yr"].astype(str) + "Q" + x["period"].astype(str))
        .sort_values(["yr", "period"])
    )
    plt.plot(donor_line["date_label"], donor_line["donor_mean_index_sa"], label="Donor mean")
    plt.xticks(rotation=90)
    plt.title("FHFA state HPI, purchase-only, treated states vs donor mean")
    plt.tight_layout()
    plt.legend()
    plt.savefig(FIGURES_DIR / "fhfa_treated_vs_donor_mean_2018_2025.png", dpi=150)
    plt.close()

    # 2. BPS permits treated vs donor mean
    bps_plot = permits_summary[permits_summary["year"] >= 2010].copy()
    plt.figure(figsize=(10, 6))
    for state_name in sorted(bps_plot["state_name"].unique()):
        state_slice = bps_plot[bps_plot["state_name"] == state_name]
        plt.plot(state_slice["year"], state_slice["permits_units_total"], label=state_name)
    donor_bps_line = donor_mean_bps.sort_values("year")
    plt.plot(donor_bps_line["year"], donor_bps_line["donor_mean_permits_units_total"], label="Donor mean")
    plt.title("BPS annual permits units, treated states vs donor mean")
    plt.xlabel("Year")
    plt.ylabel("Permitted units")
    plt.tight_layout()
    plt.legend()
    plt.savefig(FIGURES_DIR / "bps_treated_vs_donor_mean_2010_2024.png", dpi=150)
    plt.close()

    # 3. 2024 selected-state permits bar chart
    latest = selected_latest_permits.sort_values("permits_units_total", ascending=False)
    plt.figure(figsize=(10, 6))
    plt.bar(latest["state_abbr"], latest["permits_units_total"])
    plt.title("BPS latest included year: total permitted units by selected state")
    plt.xlabel("State")
    plt.ylabel("Permitted units")
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "bps_selected_states_latest_year.png", dpi=150)
    plt.close()

    # Basic seed inventory table
    inventory = pd.DataFrame(
        [
            {"artifact": "bps_state_annual_2010_2024.csv", "rows": len(bps_state)},
            {"artifact": "bps_county_2024_treated_states.csv", "rows": len(bps_county_treated)},
            {"artifact": "fhfa_state_quarterly_purchase_only_2010_2025.csv", "rows": len(fhfa_state_po)},
            {"artifact": "fhfa_state_annual_purchase_only_2010_2025.csv", "rows": len(fhfa_state_annual)},
            {"artifact": "policy_panel_state_quarterly_2010_2026.csv", "rows": len(policy_q)},
            {"artifact": "policy_panel_state_annual_2010_2026.csv", "rows": len(policy_y)},
            {"artifact": "acs_state_median_gross_rent_2024_selected_states_sample.csv", "rows": len(acs_selected)},
        ]
    )
    inventory.to_csv(TABLES_DIR / "seed_artifact_inventory.csv", index=False)

    print("Seed data build complete.")
    print(f"Wrote processed files to: {PROCESSED_DIR}")
    print(f"Wrote tables to: {TABLES_DIR}")
    print(f"Wrote figures to: {FIGURES_DIR}")


if __name__ == "__main__":
    main()
