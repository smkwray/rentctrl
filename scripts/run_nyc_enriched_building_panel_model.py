from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import statsmodels.api as sm

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from rent_control_public.nyc import (
    build_stratified_registered_rental_panel,
    build_treated_year_event_design,
    two_way_demean,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fit a NYC building-level fixed-effects event model on the enriched panel.")
    parser.add_argument("--since-year", type=int, default=2019)
    parser.add_argument("--baseline-year", type=int, default=2019)
    return parser


def load_enriched_panel(since_year: int) -> pd.DataFrame:
    path = ROOT / "data" / "processed" / f"nyc_hpd_building_year_analytic_panel_enriched_since_{since_year}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}. Run scripts/run_nyc_enriched_stratified_comparison.py first.")
    return pd.read_csv(path)


def build_balanced_panel(panel: pd.DataFrame) -> tuple[pd.DataFrame, list[int]]:
    out = build_stratified_registered_rental_panel(panel)
    out["inspection_year"] = pd.to_numeric(out["inspection_year"], errors="coerce").astype(int)
    years = sorted(out["inspection_year"].dropna().unique().tolist())
    required_years = len(years)
    counts = out.groupby("boro_block_lot")["inspection_year"].nunique()
    balanced_keys = counts[counts == required_years].index.astype(str)
    balanced = out[out["boro_block_lot"].astype(str).isin(set(balanced_keys))].copy()
    balanced.sort_values(["boro_block_lot", "inspection_year"], inplace=True)
    balanced.reset_index(drop=True, inplace=True)
    return balanced, years


def fit_building_fe_event_model(panel: pd.DataFrame, *, baseline_year: int):
    design, event_years = build_treated_year_event_design(panel, baseline_year=baseline_year)
    work = panel[["boro_block_lot", "inspection_year", "violation_count"]].copy()
    work = pd.concat([work, design], axis=1)
    demeaned = two_way_demean(
        work,
        group_col="boro_block_lot",
        time_col="inspection_year",
        value_cols=("violation_count", *tuple(design.columns)),
    )
    y = pd.to_numeric(demeaned["violation_count"], errors="coerce")
    X = demeaned.loc[:, list(design.columns)].astype(float)
    model = sm.OLS(y, X).fit(
        cov_type="cluster",
        cov_kwds={"groups": panel["boro_block_lot"].astype(str)},
    )
    return model, event_years


def build_year_summary(panel: pd.DataFrame) -> pd.DataFrame:
    out = (
        panel.groupby(["inspection_year", "treated_rsbl"], as_index=False)
        .agg(
            building_count=("boro_block_lot", "nunique"),
            mean_violation_count=("violation_count", "mean"),
            total_violation_count=("violation_count", "sum"),
        )
        .sort_values(["inspection_year", "treated_rsbl"])
        .reset_index(drop=True)
    )
    return out


def build_year_gap(year_summary: pd.DataFrame) -> pd.DataFrame:
    treated = year_summary[year_summary["treated_rsbl"] == 1][["inspection_year", "building_count", "mean_violation_count"]].rename(
        columns={"building_count": "n_treated_buildings", "mean_violation_count": "mean_treated"}
    )
    control = year_summary[year_summary["treated_rsbl"] == 0][["inspection_year", "building_count", "mean_violation_count"]].rename(
        columns={"building_count": "n_control_buildings", "mean_violation_count": "mean_control"}
    )
    out = treated.merge(control, on="inspection_year", how="inner")
    out["diff"] = out["mean_treated"] - out["mean_control"]
    out["ratio"] = out["mean_treated"] / out["mean_control"].replace(0, pd.NA)
    return out


def build_coverage(panel: pd.DataFrame, *, years: list[int]) -> pd.DataFrame:
    treated = panel.loc[panel["treated_rsbl"] == 1, "boro_block_lot"].nunique()
    control = panel.loc[panel["treated_rsbl"] == 0, "boro_block_lot"].nunique()
    return pd.DataFrame(
        [
            {"metric": "balanced_buildings_total", "value": int(panel["boro_block_lot"].nunique())},
            {"metric": "balanced_treated_buildings", "value": int(treated)},
            {"metric": "balanced_control_buildings", "value": int(control)},
            {"metric": "balanced_rows", "value": int(len(panel))},
            {"metric": "model_years", "value": len(years)},
            {"metric": "first_year", "value": min(years)},
            {"metric": "last_year", "value": max(years)},
        ]
    )


def build_coef_table(model, *, baseline_year: int, event_years: list[int]) -> pd.DataFrame:
    rows: list[dict[str, float | int | str]] = [
        {
            "year": baseline_year,
            "term": f"treated_x_{baseline_year}",
            "coef": 0.0,
            "std_err": 0.0,
            "t": 0.0,
            "p_value": pd.NA,
        }
    ]
    for year in event_years:
        term = f"treated_x_{year}"
        rows.append(
            {
                "year": year,
                "term": term,
                "coef": model.params.get(term, pd.NA),
                "std_err": model.bse.get(term, pd.NA),
                "t": model.tvalues.get(term, pd.NA),
                "p_value": model.pvalues.get(term, pd.NA),
            }
        )
    return pd.DataFrame(rows).sort_values("year").reset_index(drop=True)


def write_plot(coef: pd.DataFrame, output_path: Path) -> None:
    plot_df = coef.copy()
    plot_df["coef"] = pd.to_numeric(plot_df["coef"], errors="coerce")
    plot_df["std_err"] = pd.to_numeric(plot_df["std_err"], errors="coerce")
    plot_df["ci_low"] = plot_df["coef"] - (1.96 * plot_df["std_err"].fillna(0))
    plot_df["ci_high"] = plot_df["coef"] + (1.96 * plot_df["std_err"].fillna(0))

    plt.figure(figsize=(10, 6))
    plt.plot(plot_df["year"], plot_df["coef"], linewidth=2.5, marker="o", label="Building FE treated-year coefficient")
    plt.fill_between(plot_df["year"], plot_df["ci_low"], plot_df["ci_high"], alpha=0.2)
    plt.axhline(0, color="black", linewidth=1, alpha=0.5)
    plt.title("NYC HPD Violations: Building-Level FE Event Profile")
    plt.xlabel("Inspection year")
    plt.ylabel("Within-building treated differential vs baseline year")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()


def main() -> None:
    args = build_parser().parse_args()
    results_dir = ROOT / "results" / "tables"
    figures_dir = ROOT / "results" / "figures"
    results_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    enriched_panel = load_enriched_panel(args.since_year)
    balanced_panel, years = build_balanced_panel(enriched_panel)
    model, event_years = fit_building_fe_event_model(balanced_panel, baseline_year=args.baseline_year)
    coverage = build_coverage(balanced_panel, years=years)
    year_summary = build_year_summary(balanced_panel)
    year_gap = build_year_gap(year_summary)
    coef = build_coef_table(model, baseline_year=args.baseline_year, event_years=event_years)

    coverage_path = results_dir / f"nyc_building_fe_coverage_since_{args.since_year}.csv"
    year_summary_path = results_dir / f"nyc_building_fe_year_summary_since_{args.since_year}.csv"
    year_gap_path = results_dir / f"nyc_building_fe_year_gap_summary_since_{args.since_year}.csv"
    coef_path = results_dir / f"nyc_building_fe_model_coefficients_since_{args.since_year}.csv"
    model_summary_path = results_dir / f"nyc_building_fe_model_summary_since_{args.since_year}.txt"
    figure_path = figures_dir / f"nyc_building_fe_event_profile_since_{args.since_year}.png"

    coverage.to_csv(coverage_path, index=False)
    year_summary.to_csv(year_summary_path, index=False)
    year_gap.to_csv(year_gap_path, index=False)
    coef.to_csv(coef_path, index=False)
    model_summary_path.write_text(model.summary().as_text())
    write_plot(coef, figure_path)

    print(f"wrote {coverage_path}")
    print(f"wrote {year_summary_path}")
    print(f"wrote {year_gap_path}")
    print(f"wrote {coef_path}")
    print(f"wrote {model_summary_path}")
    print(f"wrote {figure_path}")
    print(coverage.to_string(index=False))


if __name__ == "__main__":
    main()
