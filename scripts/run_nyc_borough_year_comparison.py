from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import statsmodels.formula.api as smf

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from rent_control_public.nyc import (
    aggregate_panel_borough_year,
    build_borough_pre_post_gap_summary,
    borough_year_treated_control_diff,
    build_borough_year_summary_table,
)


def load_panel() -> pd.DataFrame:
    path = ROOT / "data" / "processed" / "nyc_hpd_building_year_analytic_panel_since_2019.csv"
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


def fit_borough_year_model(summary: pd.DataFrame):
    model_df = summary.copy()
    model_df["inspection_year"] = pd.to_numeric(model_df["inspection_year"], errors="coerce").astype(int)
    model_df["treated_rsbl"] = pd.to_numeric(model_df["treated_rsbl"], errors="coerce").astype(int)
    model_df["mean_violation_count"] = pd.to_numeric(model_df["mean_violation_count"], errors="coerce")
    model_df["building_count"] = pd.to_numeric(model_df["building_count"], errors="coerce")

    formula = "mean_violation_count ~ treated_rsbl * C(inspection_year) + C(borough)"
    return smf.wls(formula, data=model_df, weights=model_df["building_count"]).fit(cov_type="HC1")


def write_citywide_year_plot(summary: pd.DataFrame, figures_dir: Path) -> None:
    citywide = (
        summary.groupby(["inspection_year", "treated_rsbl"], as_index=False)
        .agg(
            building_count=("building_count", "sum"),
            total_violation_count=("total_violation_count", "sum"),
        )
    )
    citywide["mean_violation_count"] = citywide["total_violation_count"] / citywide["building_count"]

    treated = citywide[citywide["treated_rsbl"] == 1].sort_values("inspection_year")
    control = citywide[citywide["treated_rsbl"] == 0].sort_values("inspection_year")

    plt.figure(figsize=(10, 6))
    plt.plot(control["inspection_year"], control["mean_violation_count"], linewidth=2.5, label="Non-listed HPD buildings")
    plt.plot(treated["inspection_year"], treated["mean_violation_count"], linewidth=2.5, label="RSBL-listed HPD buildings")
    plt.title("NYC HPD Violations: RSBL-Listed vs Non-Listed Buildings")
    plt.xlabel("Inspection year")
    plt.ylabel("Mean violations per building")
    plt.legend()
    plt.tight_layout()
    plt.savefig(figures_dir / "nyc_hpd_treated_vs_control_mean_violations_since_2019.png", dpi=150)
    plt.close()


def main() -> None:
    panel = filter_complete_years(load_panel())
    summary = aggregate_panel_borough_year(panel, value_col="violation_count")
    gap = borough_year_treated_control_diff(panel, value_col="violation_count")
    summary_table = build_borough_year_summary_table(panel, value_col="violation_count")
    pre_post = build_borough_pre_post_gap_summary(panel, value_col="violation_count")
    model = fit_borough_year_model(summary)

    coef = pd.DataFrame(
        {
            "term": model.params.index,
            "coef": model.params.values,
            "std_err": model.bse.values,
            "t": model.tvalues.values,
            "p_value": model.pvalues.values,
        }
    )

    results_dir = ROOT / "results" / "tables"
    figures_dir = ROOT / "results" / "figures"
    results_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    summary_path = results_dir / "nyc_hpd_borough_year_summary_since_2019.csv"
    gap_path = results_dir / "nyc_hpd_borough_year_gap_summary_since_2019.csv"
    concise_path = results_dir / "nyc_hpd_borough_year_treated_control_summary_since_2019.csv"
    pre_post_path = results_dir / "nyc_hpd_borough_pre_post_gap_summary_since_2019.csv"
    coef_path = results_dir / "nyc_hpd_borough_year_model_coefficients_since_2019.csv"
    summary_txt_path = results_dir / "nyc_hpd_borough_year_model_summary_since_2019.txt"

    summary.to_csv(summary_path, index=False)
    gap.to_csv(gap_path, index=False)
    summary_table.to_csv(concise_path, index=False)
    pre_post.to_csv(pre_post_path, index=False)
    coef.to_csv(coef_path, index=False)
    summary_txt_path.write_text(model.summary().as_text())
    write_citywide_year_plot(summary, figures_dir)

    print(f"wrote {summary_path}")
    print(f"wrote {gap_path}")
    print(f"wrote {concise_path}")
    print(f"wrote {pre_post_path}")
    print(f"wrote {coef_path}")
    print(f"wrote {summary_txt_path}")
    print(f"wrote {figures_dir / 'nyc_hpd_treated_vs_control_mean_violations_since_2019.png'}")
    print(coef.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
