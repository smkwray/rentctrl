from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import statsmodels.formula.api as smf

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from rent_control_public.nyc import (
    aggregate_panel_stratum_year,
    build_nyc_enriched_analytic_panel,
    build_stratified_registered_rental_panel,
    fetch_mdr_registration_summary,
    fetch_pluto_controls,
    summarize_treated_control_balance,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build an enriched NYC RSBL vs control comparison using PLUTO and MDR.")
    parser.add_argument("--since-year", type=int, default=2019)
    parser.add_argument("--refresh", action="store_true", help="Ignore cached control files and refetch.")
    parser.add_argument("--pluto-chunk-size", type=int, default=500)
    return parser


def load_analytic_panel(since_year: int) -> pd.DataFrame:
    path = ROOT / "data" / "processed" / f"nyc_hpd_building_year_analytic_panel_since_{since_year}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}. Run scripts/run_nyc_rsbl_hpd_citywide.py first.")
    return pd.read_csv(path)


def filter_complete_years(panel: pd.DataFrame) -> pd.DataFrame:
    out = panel.copy()
    out["inspection_year"] = pd.to_numeric(out["inspection_year"], errors="coerce")
    current_year = date.today().year
    if out["inspection_year"].max() == current_year:
        out = out[out["inspection_year"] < current_year].copy()
    return out


def fit_stratified_model(summary: pd.DataFrame):
    model_df = summary.copy()
    model_df["inspection_year"] = pd.to_numeric(model_df["inspection_year"], errors="coerce").astype(int)
    model_df["treated_rsbl"] = pd.to_numeric(model_df["treated_rsbl"], errors="coerce").astype(int)
    model_df["building_count"] = pd.to_numeric(model_df["building_count"], errors="coerce")
    model_df["mean_violation_count"] = pd.to_numeric(model_df["mean_violation_count"], errors="coerce")
    formula = "mean_violation_count ~ treated_rsbl * C(inspection_year) + C(stratum)"
    return smf.wls(formula, data=model_df, weights=model_df["building_count"]).fit(cov_type="HC1")


def build_year_summary(summary: pd.DataFrame) -> pd.DataFrame:
    year_summary = (
        summary.groupby(["inspection_year", "treated_rsbl"], as_index=False)
        .agg(
            building_count=("building_count", "sum"),
            total_violation_count=("total_violation_count", "sum"),
            represented_strata=("stratum", "nunique"),
        )
    )
    year_summary["mean_violation_count"] = year_summary["total_violation_count"] / year_summary["building_count"]
    return year_summary.sort_values(["inspection_year", "treated_rsbl"]).reset_index(drop=True)


def build_year_gap(year_summary: pd.DataFrame) -> pd.DataFrame:
    treated = year_summary[year_summary["treated_rsbl"] == 1][["inspection_year", "mean_violation_count", "building_count", "represented_strata"]].rename(
        columns={
            "mean_violation_count": "mean_treated",
            "building_count": "n_treated_buildings",
            "represented_strata": "treated_strata",
        }
    )
    control = year_summary[year_summary["treated_rsbl"] == 0][["inspection_year", "mean_violation_count", "building_count", "represented_strata"]].rename(
        columns={
            "mean_violation_count": "mean_control",
            "building_count": "n_control_buildings",
            "represented_strata": "control_strata",
        }
    )
    out = treated.merge(control, on="inspection_year", how="outer")
    out["diff"] = out["mean_treated"] - out["mean_control"]
    out["ratio"] = out["mean_treated"] / out["mean_control"].replace(0, pd.NA)
    return out.sort_values("inspection_year").reset_index(drop=True)


def write_plot(year_summary: pd.DataFrame, output_path: Path) -> None:
    treated = year_summary[year_summary["treated_rsbl"] == 1].sort_values("inspection_year")
    control = year_summary[year_summary["treated_rsbl"] == 0].sort_values("inspection_year")

    plt.figure(figsize=(10, 6))
    plt.plot(control["inspection_year"], control["mean_violation_count"], linewidth=2.5, label="Registered non-RSBL controls")
    plt.plot(treated["inspection_year"], treated["mean_violation_count"], linewidth=2.5, label="RSBL-listed buildings")
    plt.title("NYC HPD Violations: Stratified RSBL vs Registered Controls")
    plt.xlabel("Inspection year")
    plt.ylabel("Mean violations per building")
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

    analytic_panel = filter_complete_years(load_analytic_panel(args.since_year))
    building_universe = analytic_panel[["boro_block_lot", "borough", "treated_rsbl"]].drop_duplicates().copy()

    mdr_path = processed_dir / "nyc_mdr_registration_summary.csv"
    if args.refresh or not mdr_path.exists():
        mdr_summary = fetch_mdr_registration_summary()
        mdr_summary.to_csv(mdr_path, index=False)
    else:
        mdr_summary = pd.read_csv(mdr_path)

    mdr_keys = set(mdr_summary["boro_block_lot"].astype(str))
    treated_keys = set(building_universe.loc[building_universe["treated_rsbl"] == 1, "boro_block_lot"].astype(str))
    eligible_keys = sorted(treated_keys.union(set(building_universe.loc[building_universe["boro_block_lot"].astype(str).isin(mdr_keys), "boro_block_lot"].astype(str))))

    pluto_path = processed_dir / f"nyc_pluto_controls_for_registered_or_treated_since_{args.since_year}.csv"
    if args.refresh or not pluto_path.exists():
        pluto_controls = fetch_pluto_controls(eligible_keys, chunk_size=args.pluto_chunk_size)
        pluto_controls.to_csv(pluto_path, index=False)
    else:
        pluto_controls = pd.read_csv(pluto_path)

    enriched_panel = build_nyc_enriched_analytic_panel(analytic_panel, pluto_controls=pluto_controls, mdr_summary=mdr_summary)
    restricted_panel = build_stratified_registered_rental_panel(enriched_panel)
    stratified_summary = aggregate_panel_stratum_year(restricted_panel)
    year_summary = build_year_summary(stratified_summary)
    year_gap = build_year_gap(year_summary)
    balance_summary = summarize_treated_control_balance(restricted_panel)
    model = fit_stratified_model(stratified_summary)

    coverage = pd.DataFrame(
        [
            {"metric": "analytic_buildings_total", "value": analytic_panel["boro_block_lot"].nunique()},
            {"metric": "analytic_buildings_treated", "value": analytic_panel.loc[analytic_panel["treated_rsbl"] == 1, "boro_block_lot"].nunique()},
            {"metric": "mdr_registered_buildings", "value": int(enriched_panel.loc[enriched_panel["mdr_registered"] == 1, "boro_block_lot"].nunique())},
            {"metric": "pluto_matched_buildings", "value": int(enriched_panel.loc[enriched_panel["yearbuilt"].notna() | enriched_panel["unitstotal"].notna(), "boro_block_lot"].nunique())},
            {"metric": "restricted_registered_or_treated_buildings", "value": restricted_panel["boro_block_lot"].nunique()},
            {"metric": "restricted_strata", "value": restricted_panel["stratum"].nunique()},
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

    enriched_panel_path = processed_dir / f"nyc_hpd_building_year_analytic_panel_enriched_since_{args.since_year}.csv"
    coverage_path = results_dir / f"nyc_control_enrichment_coverage_since_{args.since_year}.csv"
    balance_path = results_dir / f"nyc_control_balance_summary_since_{args.since_year}.csv"
    year_summary_path = results_dir / f"nyc_stratified_year_summary_since_{args.since_year}.csv"
    year_gap_path = results_dir / f"nyc_stratified_year_gap_summary_since_{args.since_year}.csv"
    coef_path = results_dir / f"nyc_stratified_model_coefficients_since_{args.since_year}.csv"
    model_summary_path = results_dir / f"nyc_stratified_model_summary_since_{args.since_year}.txt"
    figure_path = figures_dir / f"nyc_stratified_treated_vs_control_mean_violations_since_{args.since_year}.png"

    enriched_panel.to_csv(enriched_panel_path, index=False)
    coverage.to_csv(coverage_path, index=False)
    balance_summary.to_csv(balance_path, index=False)
    year_summary.to_csv(year_summary_path, index=False)
    year_gap.to_csv(year_gap_path, index=False)
    coef.to_csv(coef_path, index=False)
    model_summary_path.write_text(model.summary().as_text())
    write_plot(year_summary, figure_path)

    print(f"wrote {enriched_panel_path}")
    print(f"wrote {coverage_path}")
    print(f"wrote {balance_path}")
    print(f"wrote {year_summary_path}")
    print(f"wrote {year_gap_path}")
    print(f"wrote {coef_path}")
    print(f"wrote {model_summary_path}")
    print(f"wrote {figure_path}")
    print(coverage.to_string(index=False))


if __name__ == "__main__":
    main()
