from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import statsmodels.formula.api as smf

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from rent_control_public.nyc import (
    aggregate_matched_pair_year,
    build_matched_pair_panel,
    build_preperiod_building_features,
    match_treated_to_controls,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a within-community-board matched NYC comparison from the enriched panel.")
    parser.add_argument("--since-year", type=int, default=2019)
    parser.add_argument("--pre-years", nargs="+", type=int, default=[2019, 2020, 2021])
    return parser


def load_enriched_panel(since_year: int) -> pd.DataFrame:
    path = ROOT / "data" / "processed" / f"nyc_hpd_building_year_analytic_panel_enriched_since_{since_year}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}. Run scripts/run_nyc_enriched_stratified_comparison.py first.")
    return pd.read_csv(path)


def build_year_gap(pair_year: pd.DataFrame) -> pd.DataFrame:
    treated = pair_year[pair_year["treated_rsbl"] == 1][["match_id", "inspection_year", "mean_violation_count"]].rename(
        columns={"mean_violation_count": "mean_treated"}
    )
    control = pair_year[pair_year["treated_rsbl"] == 0][["match_id", "inspection_year", "mean_violation_count"]].rename(
        columns={"mean_violation_count": "mean_control"}
    )
    merged = treated.merge(control, on=["match_id", "inspection_year"], how="inner")
    merged["diff"] = merged["mean_treated"] - merged["mean_control"]
    return merged.sort_values(["inspection_year", "match_id"]).reset_index(drop=True)


def summarize_gap_by_year(pair_gap: pd.DataFrame) -> pd.DataFrame:
    out = (
        pair_gap.groupby("inspection_year", as_index=False)
        .agg(
            matched_pairs=("match_id", "nunique"),
            mean_treated=("mean_treated", "mean"),
            mean_control=("mean_control", "mean"),
            mean_gap=("diff", "mean"),
            median_gap=("diff", "median"),
        )
    )
    out["gap_ratio"] = out["mean_treated"] / out["mean_control"].replace(0, pd.NA)
    return out.sort_values("inspection_year").reset_index(drop=True)


def fit_gap_model(pair_gap: pd.DataFrame):
    model_df = pair_gap.copy()
    model_df["inspection_year"] = pd.to_numeric(model_df["inspection_year"], errors="coerce").astype(int)
    return smf.ols("diff ~ C(inspection_year)", data=model_df).fit(cov_type="HC1")


def write_plot(year_summary: pd.DataFrame, output_path: Path) -> None:
    plt.figure(figsize=(10, 6))
    plt.plot(year_summary["inspection_year"], year_summary["mean_gap"], linewidth=2.5, label="Matched treated-control gap")
    plt.axhline(0, color="black", linewidth=1, alpha=0.5)
    plt.title("NYC HPD Violations: Within-Community-Board Matched Gap")
    plt.xlabel("Inspection year")
    plt.ylabel("Mean treated-control gap")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()


def main() -> None:
    args = build_parser().parse_args()
    processed_dir = ROOT / "data" / "processed"
    results_dir = ROOT / "results" / "tables"
    figures_dir = ROOT / "results" / "figures"
    processed_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    panel = load_enriched_panel(args.since_year)
    features = build_preperiod_building_features(panel, pre_years=tuple(args.pre_years))
    matches = match_treated_to_controls(features)
    matched_panel = build_matched_pair_panel(panel, matches)
    pair_year = aggregate_matched_pair_year(matched_panel)
    pair_gap = build_year_gap(pair_year)
    year_summary = summarize_gap_by_year(pair_gap)
    model = fit_gap_model(pair_gap)

    balance = pd.DataFrame(
        [
            {
                "matched_pairs": matches["match_id"].nunique(),
                "treated_pre_mean": matches["treated_pre_mean_violation_count"].mean(),
                "control_pre_mean": matches["control_pre_mean_violation_count"].mean(),
                "mean_pre_gap": matches["pre_mean_gap"].mean(),
                "mean_treated_units": matches["treated_unitstotal"].mean(),
                "mean_control_units": matches["control_unitstotal"].mean(),
            }
        ]
    )

    coef = pd.DataFrame(
        {
            "term": model.params.index,
            "coef": model.params.values,
            "std_err": model.bse.values,
            "t": model.tvalues.values,
            "p_value": model.pvalues.values,
        }
    )

    matches_path = processed_dir / f"nyc_within_cb_matches_since_{args.since_year}.csv"
    matched_panel_path = processed_dir / f"nyc_within_cb_matched_panel_since_{args.since_year}.csv"
    balance_path = results_dir / f"nyc_within_cb_match_balance_since_{args.since_year}.csv"
    year_gap_path = results_dir / f"nyc_within_cb_year_gap_summary_since_{args.since_year}.csv"
    coef_path = results_dir / f"nyc_within_cb_gap_model_coefficients_since_{args.since_year}.csv"
    model_path = results_dir / f"nyc_within_cb_gap_model_summary_since_{args.since_year}.txt"
    figure_path = figures_dir / f"nyc_within_cb_gap_since_{args.since_year}.png"

    matches.to_csv(matches_path, index=False)
    matched_panel.to_csv(matched_panel_path, index=False)
    balance.to_csv(balance_path, index=False)
    year_summary.to_csv(year_gap_path, index=False)
    coef.to_csv(coef_path, index=False)
    model_path.write_text(model.summary().as_text())
    write_plot(year_summary, figure_path)

    print(f"wrote {matches_path}")
    print(f"wrote {matched_panel_path}")
    print(f"wrote {balance_path}")
    print(f"wrote {year_gap_path}")
    print(f"wrote {coef_path}")
    print(f"wrote {model_path}")
    print(f"wrote {figure_path}")
    print(balance.to_string(index=False))


if __name__ == "__main__":
    main()
