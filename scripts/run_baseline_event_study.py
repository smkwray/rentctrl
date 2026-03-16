from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from rent_control_public.event_study import add_binned_event_time_dummies, fit_twfe_event_study
from rent_control_public.policy import load_policy_events


DEFAULT_OUTCOMES = [
    "permits_units_total",
    "permits_units_5plus",
    "permits_units_multifamily_share",
    "index_sa_mean",
    "qcew_total_covered_emplvl",
    "qcew_total_covered_avg_weekly_wage",
    "same_house_1y_pct",
    "moved_last_year_pct",
    "moved_different_state_pct",
    "renter_share_pct",
    "DP04_0134E",
    "rent_burden_30_plus_pct",
]

OUTCOME_METADATA = {
    "permits_units_total": {
        "display_name": "Total permitted units",
        "source": "Census Building Permits Survey",
        "frequency": "annual",
        "plot_filename": "baseline_permits_units_total_annual.png",
    },
    "permits_units_5plus": {
        "display_name": "5+ unit permitted units",
        "source": "Census Building Permits Survey",
        "frequency": "annual",
        "plot_filename": "baseline_permits_units_5plus_annual.png",
    },
    "permits_units_multifamily_share": {
        "display_name": "Multifamily permit share",
        "source": "Census Building Permits Survey",
        "frequency": "annual",
        "plot_filename": None,
    },
    "index_sa_mean": {
        "display_name": "FHFA purchase-only HPI (SA mean)",
        "source": "FHFA HPI",
        "frequency": "annual",
        "plot_filename": "baseline_hpi_index_sa_mean_annual.png",
    },
    "qcew_total_covered_emplvl": {
        "display_name": "QCEW total covered employment",
        "source": "QCEW",
        "frequency": "annual",
        "plot_filename": "baseline_qcew_total_covered_emplvl_annual.png",
    },
    "qcew_total_covered_avg_weekly_wage": {
        "display_name": "QCEW average weekly wage",
        "source": "QCEW",
        "frequency": "annual",
        "plot_filename": "baseline_qcew_total_covered_avg_weekly_wage_annual.png",
    },
    "same_house_1y_pct": {
        "display_name": "Same house one year ago",
        "source": "ACS 1-year profile",
        "frequency": "annual",
        "plot_filename": "baseline_same_house_1y_pct_annual.png",
    },
    "moved_last_year_pct": {
        "display_name": "Moved last year",
        "source": "ACS 1-year profile",
        "frequency": "annual",
        "plot_filename": "baseline_moved_last_year_pct_annual.png",
    },
    "moved_different_state_pct": {
        "display_name": "Moved from different state",
        "source": "ACS 1-year profile",
        "frequency": "annual",
        "plot_filename": "baseline_moved_different_state_pct_annual.png",
    },
    "renter_share_pct": {
        "display_name": "Renter share",
        "source": "ACS 1-year profile",
        "frequency": "annual",
        "plot_filename": "baseline_renter_share_pct_annual.png",
    },
    "DP04_0134E": {
        "display_name": "Median gross rent",
        "source": "ACS 1-year profile",
        "frequency": "annual",
        "plot_filename": "baseline_median_gross_rent_annual.png",
    },
    "rent_burden_30_plus_pct": {
        "display_name": "Rent burden 30%+",
        "source": "ACS 1-year profile",
        "frequency": "annual",
        "plot_filename": None,
    },
}

PLOT_OUTCOMES = [
    "DP04_0134E",
    "permits_units_total",
    "permits_units_5plus",
    "index_sa_mean",
    "qcew_total_covered_emplvl",
    "qcew_total_covered_avg_weekly_wage",
    "same_house_1y_pct",
    "moved_last_year_pct",
    "moved_different_state_pct",
    "renter_share_pct",
]


def policy_timing_note(policy_events: pd.DataFrame) -> str:
    note_parts = []
    for row in policy_events.itertuples(index=False):
        note_parts.append(
            f"{row.state_abbr}: effective {row.effective_date.date()}, annual treat year {row.preferred_annual_treat_year}, quarterly treat period {row.preferred_quarterly_treat_period}"
        )
    return "; ".join(note_parts)


def donor_pool_note(df: pd.DataFrame) -> str:
    donor_states = sorted(df.loc[df["analysis_role"] == "donor", "state_abbr"].dropna().unique())
    return f"Baseline donor pool: {', '.join(donor_states)}"


def outcome_limitations(outcome: str) -> str:
    if outcome in {"DP04_0134E", "rent_burden_30_plus_pct"}:
        return "ACS coverage is limited to 2010-2019 and 2021-2024 in this workspace."
    if outcome in {"same_house_1y_pct", "moved_last_year_pct", "moved_different_state_pct", "renter_share_pct"}:
        return "ACS coverage is limited to 2010-2019 and 2021-2024 in this workspace."
    if outcome == "qcew_total_covered_emplvl":
        return "QCEW area-slice coverage begins in 2014 for this workflow."
    if outcome == "qcew_total_covered_avg_weekly_wage":
        return "QCEW area-slice coverage begins in 2014 for this workflow."
    return "Washington is excluded from baseline causal estimates and retained for descriptive work only."


def write_baseline_plots(df: pd.DataFrame, policy_events: pd.DataFrame, figures_dir: Path) -> None:
    figures_dir.mkdir(parents=True, exist_ok=True)
    sample = df[df["analysis_role"].isin(["core_treated", "donor"])].copy()

    for outcome in PLOT_OUTCOMES:
        if outcome not in sample.columns:
            continue
        plot_df = sample.dropna(subset=[outcome]).copy()
        if plot_df.empty:
            continue

        donor_mean = (
            plot_df[plot_df["analysis_role"] == "donor"]
            .groupby("year", as_index=False)
            .agg(donor_mean=(outcome, "mean"))
        )
        treated = plot_df[plot_df["analysis_role"] == "core_treated"].copy()
        if treated.empty or donor_mean.empty:
            continue

        meta = OUTCOME_METADATA[outcome]
        plt.figure(figsize=(10, 6))
        for state_name in sorted(treated["state_name"].unique()):
            state_slice = treated[treated["state_name"] == state_name].sort_values("year")
            plt.plot(state_slice["year"], state_slice[outcome], linewidth=2, label=state_name)

        donor_line = donor_mean.sort_values("year")
        plt.plot(donor_line["year"], donor_line["donor_mean"], linewidth=2.5, linestyle="--", color="black", label="Donor mean")

        for row in policy_events[policy_events["analysis_role"] == "core_treated"].itertuples(index=False):
            plt.axvline(row.preferred_annual_treat_year, color="gray", linestyle=":", linewidth=1)
            plt.text(row.preferred_annual_treat_year + 0.05, plt.ylim()[1], row.state_abbr, va="top", fontsize=9)

        plt.title(f"{meta['display_name']} ({meta['frequency']})")
        plt.xlabel("Year")
        plt.ylabel(meta["display_name"])
        plt.legend()
        plt.tight_layout()
        plt.savefig(figures_dir / meta["plot_filename"], dpi=150)
        plt.close()


def write_summary_tables(df: pd.DataFrame, policy_events: pd.DataFrame, results_dir: Path) -> None:
    results_dir.mkdir(parents=True, exist_ok=True)
    sample = df[df["analysis_role"].isin(["core_treated", "donor"])].copy()
    timing_note = policy_timing_note(policy_events)
    donor_note = donor_pool_note(df)

    outcome_rows = []
    for outcome in DEFAULT_OUTCOMES:
        if outcome not in sample.columns:
            continue
        outcome_sample = sample.dropna(subset=[outcome]).copy()
        if outcome_sample.empty:
            continue
        meta = OUTCOME_METADATA[outcome]
        outcome_rows.append(
            {
                "outcome": outcome,
                "display_name": meta["display_name"],
                "source": meta["source"],
                "frequency": meta["frequency"],
                "sample_start_year": int(outcome_sample["year"].min()),
                "sample_end_year": int(outcome_sample["year"].max()),
                "non_missing_rows": len(outcome_sample),
                "non_missing_states": outcome_sample["state_abbr"].nunique(),
                "treated_states_included": "CA, OR",
                "descriptive_state_excluded": "WA",
                "treatment_timing_note": timing_note,
                "donor_pool_note": donor_note,
                "limitations": outcome_limitations(outcome),
            }
        )

    pd.DataFrame(outcome_rows).to_csv(results_dir / "baseline_outcome_summary.csv", index=False)

    policy_table = policy_events[
        [
            "state_abbr",
            "analysis_role",
            "policy_name",
            "effective_date",
            "preferred_annual_treat_year",
            "preferred_quarterly_treat_period",
            "notes",
        ]
    ].copy()
    policy_table["effective_date"] = pd.to_datetime(policy_table["effective_date"]).dt.date.astype(str)
    policy_table.to_csv(results_dir / "baseline_policy_timing_notes.csv", index=False)

    donor_states = (
        df[df["analysis_role"] == "donor"][["state_name", "state_abbr", "state_fips"]]
        .drop_duplicates()
        .sort_values("state_abbr")
        .copy()
    )
    donor_states["state_fips"] = donor_states["state_fips"].astype(str).str.zfill(2)
    donor_states["note"] = "Included in the baseline donor pool."
    donor_states.to_csv(results_dir / "baseline_donor_pool_notes.csv", index=False)


def main() -> None:
    panel_path = Path("data/processed/core_state_panel_annual.csv")
    if not panel_path.exists():
        raise FileNotFoundError(f"Missing {panel_path}. Run scripts/build_core_state_panel.py first.")

    df = pd.read_csv(panel_path)
    results_dir = Path("results/tables")
    figures_dir = Path("results/figures")
    results_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)
    policy_events = load_policy_events(ROOT / "config" / "policy_events_core.csv")

    for outcome in DEFAULT_OUTCOMES:
        if outcome not in df.columns:
            continue
        sample = df[df["analysis_role"].isin(["core_treated", "donor"])].copy()
        sample = sample.dropna(subset=[outcome])
        if sample.empty or len(sample) < 10 or sample["year"].nunique() < 3 or sample["state_name"].nunique() < 3:
            print(f"skipped {outcome}: insufficient non-missing sample for baseline event study")
            continue
        sample["event_time_int"] = pd.to_numeric(sample["event_time_years_preferred"], errors="coerce").round().astype("Int64")
        sample = add_binned_event_time_dummies(
            sample,
            "event_time_int",
            min_bin=-4,
            max_bin=4,
            reference_period=-1,
        )
        result = fit_twfe_event_study(
            sample,
            outcome=outcome,
            unit_col="state_name",
            time_col="year",
        )
        out_path = results_dir / f"event_study_{outcome}.txt"
        out_path.write_text(result.model_summary)
        print(f"wrote {out_path}")

    write_baseline_plots(df, policy_events, figures_dir)
    write_summary_tables(df, policy_events, results_dir)
    print(f"wrote baseline figures to {figures_dir}")
    print(f"wrote baseline summary tables to {results_dir}")


if __name__ == "__main__":
    main()
