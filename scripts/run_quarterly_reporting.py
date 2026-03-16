from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


QUARTERLY_OUTCOMES = {
    "index_sa": {
        "display_name": "FHFA HPI (seasonally adjusted)",
        "plot_filename": "quarterly_fhfa_treated_vs_donor.png",
        "timing_plot_filename": "quarterly_fhfa_timing_sensitivity.png",
        "sample_start_year": 2010,
    },
    "qcew_total_covered_emplvl": {
        "display_name": "QCEW total covered employment",
        "plot_filename": "quarterly_qcew_treated_vs_donor.png",
        "timing_plot_filename": "quarterly_qcew_timing_sensitivity.png",
        "sample_start_year": 2014,
    },
    "qcew_total_covered_avg_weekly_wage": {
        "display_name": "QCEW average weekly wage",
        "plot_filename": "quarterly_qcew_avg_weekly_wage_treated_vs_donor.png",
        "timing_plot_filename": "quarterly_qcew_avg_weekly_wage_timing_sensitivity.png",
        "sample_start_year": 2014,
    },
}

TREATED_STATES = ["CA", "OR"]
DONOR_STATES = ["AZ", "CO", "FL", "GA", "ID", "NC", "NV", "TN", "TX", "UT", "VA"]


def write_treated_vs_donor_plot(df: pd.DataFrame, *, outcome: str, title: str, path: Path, sample_start_year: int) -> None:
    plot_df = df[
        df["analysis_role"].isin(["core_treated", "donor"]) & df[outcome].notna() & (df["year"] >= sample_start_year)
    ].copy()
    donor = (
        plot_df[plot_df["analysis_role"] == "donor"]
        .groupby("calendar_period", as_index=False)
        .agg(donor_mean=(outcome, "mean"))
        .sort_values("calendar_period")
    )
    treated = plot_df[plot_df["analysis_role"] == "core_treated"].copy()

    plt.figure(figsize=(11, 6))
    for state_abbr in TREATED_STATES:
        state_df = treated[treated["state_abbr"] == state_abbr].sort_values("calendar_period")
        plt.plot(state_df["calendar_period"], state_df[outcome], linewidth=2, label=state_abbr)
    plt.plot(donor["calendar_period"], donor["donor_mean"], linestyle="--", color="black", linewidth=2.5, label="Donor mean")
    plt.xticks(rotation=90)
    plt.title(title)
    plt.xlabel("Quarter")
    plt.ylabel(title)
    plt.legend()
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def write_timing_sensitivity_plot(preferred: pd.DataFrame, alternative: pd.DataFrame, *, title: str, path: Path) -> None:
    plt.figure(figsize=(9, 5.5))
    plt.axhline(0, color="black", linewidth=1)
    plt.axvline(-1, color="gray", linestyle=":", linewidth=1)
    plt.plot(preferred["event_time"], preferred["coef"], marker="o", linewidth=2, label="Preferred timing")
    plt.plot(alternative["event_time"], alternative["coef"], marker="o", linewidth=2, label="Alternative timing")
    plt.title(title)
    plt.xlabel("Event time (quarters)")
    plt.ylabel("Coefficient")
    plt.legend()
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def write_quarterly_summary(df: pd.DataFrame, results_dir: Path) -> None:
    rows = []
    for outcome, meta in QUARTERLY_OUTCOMES.items():
        sample = df[df[outcome].notna() & df["analysis_role"].isin(["core_treated", "donor"])].copy()
        rows.append(
            {
                "outcome": outcome,
                "display_name": meta["display_name"],
                "frequency": "quarterly",
                "sample_start_period": sample["calendar_period"].min(),
                "sample_end_period": sample["calendar_period"].max(),
                "sample_rows": len(sample),
                "treated_states": "CA, OR",
                "donor_pool": ", ".join(DONOR_STATES),
                "preferred_treatment_timing": "OR: 2019Q2; CA: 2020Q1",
                "alternative_treatment_timing": "OR: 2019Q1; CA: 2020Q1",
                "limitations": "WA excluded from causal baseline. QCEW starts in 2014. Quarterly timing sensitivity has been run.",
            }
        )
    pd.DataFrame(rows).to_csv(results_dir / "quarterly_reporting_summary.csv", index=False)


def write_quarterly_note(path: Path) -> None:
    text = """# Quarterly Reporting Note

This package focuses on the two quarterly outcomes that fit the current statewide design:

- FHFA HPI
- QCEW total covered employment
- QCEW average weekly wage

## Why this package exists

The quarterly series are the cleanest way to show treatment-timing sensitivity because Oregon's preferred quarterly treatment period (`2019Q2`) differs from the alternative (`2019Q1`).

## What is included

- treated-vs-donor quarterly trend figures
- preferred-vs-alternative treatment-timing coefficient comparisons
- a quarterly summary table with donor-pool and timing notes

## Why Eviction Lab is not the chosen extension here

The currently accessible Eviction Lab public monthly tracking file does not include California, Oregon, or Washington. That makes it a poor fit for this project's treated states, so it should not be the next extension for this statewide OR/CA design.

## Reporting posture

- Treat quarterly FHFA as the strongest quarterly outcome.
- Treat quarterly QCEW employment and wage outcomes as exploratory and sensitivity-heavy.
- Keep Washington descriptive only.
"""
    path.write_text(text)


def run() -> None:
    quarterly = pd.read_csv(ROOT / "data" / "processed" / "core_state_panel_quarterly.csv", dtype={"state_fips": str})
    results_dir = ROOT / "results" / "tables"
    figures_dir = ROOT / "results" / "figures"
    results_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    for outcome, meta in QUARTERLY_OUTCOMES.items():
        write_treated_vs_donor_plot(
            quarterly,
            outcome=outcome,
            title=meta["display_name"],
            path=figures_dir / meta["plot_filename"],
            sample_start_year=meta["sample_start_year"],
        )

        preferred = pd.read_csv(results_dir / f"pretrend_coefficients_{outcome}_quarterly_preferred_treatment.csv")
        alternative = pd.read_csv(results_dir / f"pretrend_coefficients_{outcome}_quarterly_alternative_treatment.csv")
        write_timing_sensitivity_plot(
            preferred,
            alternative,
            title=f"{meta['display_name']}: preferred vs alternative quarterly timing",
            path=figures_dir / meta["timing_plot_filename"],
        )

    write_quarterly_summary(quarterly, results_dir)
    write_quarterly_note(ROOT / "results" / "quarterly_reporting.md")
    print(f"wrote quarterly figures to {figures_dir}")
    print(f"wrote quarterly reporting note to {ROOT / 'results' / 'quarterly_reporting.md'}")


if __name__ == "__main__":
    run()
