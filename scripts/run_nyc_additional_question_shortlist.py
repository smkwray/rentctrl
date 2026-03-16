from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from rent_control_public.nyc import (
    add_bbl_column,
    add_registration_lifecycle_bins,
    aggregate_margin_group,
    aggregate_panel_group_year,
    build_complete_month_index,
    build_group_year_gap_summary,
    build_margin_gap_summary,
    build_stratified_registered_rental_panel,
    build_treated_selection_stage_frame,
    classify_hpd_current_status,
    fetch_hpd_violation_building_month_summary,
    fetch_hpd_violation_status_summary,
    summarize_treated_selection_coverage,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build follow-on NYC question-shortlist artifacts from landed panels.")
    parser.add_argument("--since-year", type=int, default=2019)
    parser.add_argument("--rsbl-year", type=int, default=2024)
    parser.add_argument("--include-live-extensions", action="store_true", help="Fetch or use cached monthly/status grouped HPD summaries for Q5/Q6.")
    parser.add_argument("--live-extension-since-year", type=int, default=2022)
    parser.add_argument("--refresh-live", action="store_true", help="Ignore cached live extension files and refetch grouped HPD summaries.")
    parser.add_argument("--live-page-size", type=int, default=50000)
    return parser


def _read_cached_csv(path: Path, **kwargs: object) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        return pd.read_csv(path, low_memory=False, **kwargs)
    except EmptyDataError:
        path.unlink(missing_ok=True)
        return None


def _load_csv(path: Path, *, message: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}. {message}")
    return pd.read_csv(path, low_memory=False)


def filter_complete_years(panel: pd.DataFrame) -> pd.DataFrame:
    out = panel.copy()
    out["inspection_year"] = pd.to_numeric(out["inspection_year"], errors="coerce")
    current_year = date.today().year
    if out["inspection_year"].max() == current_year:
        out = out[out["inspection_year"] < current_year].copy()
    return out


def fill_group_missing(panel: pd.DataFrame, group_col: str, *, fill_value: str = "missing") -> pd.DataFrame:
    out = panel.copy()
    out[group_col] = out[group_col].astype("string").fillna(fill_value)
    return out


def build_treated_control_gap(summary: pd.DataFrame, *, key_cols: list[str], mean_col: str) -> pd.DataFrame:
    treated = summary[summary["treated_rsbl"] == 1].copy()
    control = summary[summary["treated_rsbl"] == 0].copy()
    treated = treated.rename(columns={col: f"{col}_treated" for col in summary.columns if col not in key_cols + ["treated_rsbl"]})
    control = control.rename(columns={col: f"{col}_control" for col in summary.columns if col not in key_cols + ["treated_rsbl"]})
    out = treated.merge(control, on=key_cols, how="outer")
    out["diff"] = out[f"{mean_col}_treated"] - out[f"{mean_col}_control"]
    out["ratio"] = out[f"{mean_col}_treated"] / out[f"{mean_col}_control"].replace(0, pd.NA)
    return out.sort_values(key_cols).reset_index(drop=True)


def load_or_fetch_live_grouped_summaries(
    *,
    boroughs: list[str],
    processed_dir: Path,
    since_year: int,
    refresh: bool,
    page_size: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    monthly_frames: list[pd.DataFrame] = []
    status_frames: list[pd.DataFrame] = []
    current_year = date.today().year

    for borough_name in boroughs:
        borough_slug = borough_name.lower().replace(" ", "_")
        for year in range(since_year, current_year):
            month_path = processed_dir / f"nyc_hpd_violation_building_month_summary_{borough_slug}_{year}.csv"
            status_path = processed_dir / f"nyc_hpd_violation_status_summary_{borough_slug}_{year}.csv"

            monthly = None if refresh else _read_cached_csv(month_path, dtype={"block": str, "lot": str})
            if monthly is None:
                print(f"[{borough_name} {year}] fetching monthly grouped HPD summary")
                monthly = fetch_hpd_violation_building_month_summary(
                    borough=borough_name,
                    exact_year=year,
                    limit=page_size,
                )
                monthly = add_bbl_column(monthly, borough_col="boroid", block_col="block", lot_col="lot")
                monthly["borough"] = borough_name
                month_path.parent.mkdir(parents=True, exist_ok=True)
                monthly.to_csv(month_path, index=False)
            monthly_frames.append(monthly)

            status = None if refresh else _read_cached_csv(status_path, dtype={"block": str, "lot": str})
            if status is None:
                print(f"[{borough_name} {year}] fetching status grouped HPD summary")
                status = fetch_hpd_violation_status_summary(
                    borough=borough_name,
                    exact_year=year,
                    limit=page_size,
                )
                status = add_bbl_column(status, borough_col="boroid", block_col="block", lot_col="lot")
                status["borough"] = borough_name
                status_path.parent.mkdir(parents=True, exist_ok=True)
                status.to_csv(status_path, index=False)
            status_frames.append(status)

    monthly_citywide = pd.concat(monthly_frames, ignore_index=True) if monthly_frames else pd.DataFrame()
    status_citywide = pd.concat(status_frames, ignore_index=True) if status_frames else pd.DataFrame()
    return monthly_citywide, status_citywide


def build_monthly_timing_outputs(
    building_universe: pd.DataFrame,
    monthly_sparse: pd.DataFrame,
    *,
    since_year: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    current_year = date.today().year
    sparse = monthly_sparse.copy()
    sparse["boro_block_lot"] = sparse["boro_block_lot"].astype(str)
    if "borough" in sparse.columns:
        sparse = sparse.drop(columns=["borough"])
    sparse["inspection_year"] = pd.to_numeric(sparse["inspection_year"], errors="coerce").astype("Int64")
    sparse["inspection_month"] = pd.to_numeric(sparse["inspection_month"], errors="coerce").astype("Int64")
    sparse["violation_count"] = pd.to_numeric(sparse["violation_count"], errors="coerce").fillna(0)
    sparse = sparse[sparse["inspection_year"] < current_year].copy()

    static = building_universe[["boro_block_lot", "borough", "treated_rsbl"]].drop_duplicates().copy()
    static["boro_block_lot"] = static["boro_block_lot"].astype(str)
    denom_city = static.groupby("treated_rsbl", as_index=False).agg(building_count=("boro_block_lot", "nunique"))
    denom_borough = static.groupby(["borough", "treated_rsbl"], as_index=False).agg(building_count=("boro_block_lot", "nunique"))
    month_axis = build_complete_month_index(start_year=since_year, end_year=current_year - 1)

    merged = sparse.merge(static, on="boro_block_lot", how="inner")
    city_observed = (
        merged.groupby(["inspection_year", "inspection_month", "treated_rsbl"], as_index=False)
        .agg(
            total_violations=("violation_count", "sum"),
            buildings_with_violations=("boro_block_lot", "nunique"),
        )
    )
    city_grid = month_axis.assign(_key=1).merge(denom_city.assign(_key=1), on="_key", how="outer").drop(columns="_key")
    city_summary = city_grid.merge(city_observed, on=["inspection_year", "inspection_month", "treated_rsbl"], how="left")
    city_summary["total_violations"] = city_summary["total_violations"].fillna(0)
    city_summary["buildings_with_violations"] = city_summary["buildings_with_violations"].fillna(0)
    city_summary["mean_violation_count"] = city_summary["total_violations"] / city_summary["building_count"].replace(0, pd.NA)
    city_summary["building_share_with_violations"] = city_summary["buildings_with_violations"] / city_summary["building_count"].replace(0, pd.NA)
    city_gap = build_treated_control_gap(
        city_summary,
        key_cols=["inspection_year", "inspection_month", "year_month"],
        mean_col="mean_violation_count",
    )

    borough_observed = (
        merged.groupby(["borough", "inspection_year", "inspection_month", "treated_rsbl"], as_index=False)
        .agg(
            total_violations=("violation_count", "sum"),
            buildings_with_violations=("boro_block_lot", "nunique"),
        )
    )
    borough_grid = month_axis.assign(_key=1).merge(denom_borough.assign(_key=1), on="_key", how="outer").drop(columns="_key")
    borough_summary = borough_grid.merge(
        borough_observed,
        on=["borough", "inspection_year", "inspection_month", "treated_rsbl"],
        how="left",
    )
    borough_summary["total_violations"] = borough_summary["total_violations"].fillna(0)
    borough_summary["buildings_with_violations"] = borough_summary["buildings_with_violations"].fillna(0)
    borough_summary["mean_violation_count"] = borough_summary["total_violations"] / borough_summary["building_count"].replace(0, pd.NA)
    borough_summary["building_share_with_violations"] = borough_summary["buildings_with_violations"] / borough_summary["building_count"].replace(0, pd.NA)
    borough_gap = build_treated_control_gap(
        borough_summary,
        key_cols=["borough", "inspection_year", "inspection_month", "year_month"],
        mean_col="mean_violation_count",
    )
    return city_summary.sort_values(["inspection_year", "inspection_month", "treated_rsbl"]).reset_index(drop=True), city_gap, borough_gap


def build_status_outputs(
    building_universe: pd.DataFrame,
    status_sparse: pd.DataFrame,
    *,
    since_year: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    current_year = date.today().year
    sparse = status_sparse.copy()
    sparse["boro_block_lot"] = sparse["boro_block_lot"].astype(str)
    if "borough" in sparse.columns:
        sparse = sparse.drop(columns=["borough"])
    sparse["inspection_year"] = pd.to_numeric(sparse["inspection_year"], errors="coerce").astype("Int64")
    sparse["violation_count"] = pd.to_numeric(sparse["violation_count"], errors="coerce").fillna(0)
    sparse = sparse[sparse["inspection_year"] < current_year].copy()
    sparse["status_family"] = sparse["currentstatus"].map(classify_hpd_current_status)

    top_status = (
        sparse.groupby("currentstatus", as_index=False)
        .agg(total_violations=("violation_count", "sum"))
        .sort_values("total_violations", ascending=False)
        .head(25)
        .reset_index(drop=True)
    )

    static = building_universe[["boro_block_lot", "treated_rsbl"]].drop_duplicates().copy()
    static["boro_block_lot"] = static["boro_block_lot"].astype(str)
    denom = static.groupby("treated_rsbl", as_index=False).agg(building_count=("boro_block_lot", "nunique"))
    merged = sparse.merge(static, on="boro_block_lot", how="inner")
    family_levels = pd.DataFrame({"status_family": ["open_reinspection", "pending_administrative", "resolved_or_certified", "other"]})
    year_axis = pd.DataFrame({"inspection_year": list(range(since_year, current_year))})
    grid = year_axis.assign(_key=1).merge(denom.assign(_key=1), on="_key", how="outer").merge(
        family_levels.assign(_key=1),
        on="_key",
        how="outer",
    ).drop(columns="_key")

    observed = (
        merged.groupby(["inspection_year", "treated_rsbl", "status_family"], as_index=False)
        .agg(
            total_violations=("violation_count", "sum"),
            buildings_with_status=("boro_block_lot", "nunique"),
        )
    )
    family_summary = grid.merge(observed, on=["inspection_year", "treated_rsbl", "status_family"], how="left")
    family_summary["total_violations"] = family_summary["total_violations"].fillna(0)
    family_summary["buildings_with_status"] = family_summary["buildings_with_status"].fillna(0)
    family_summary["mean_violation_count"] = family_summary["total_violations"] / family_summary["building_count"].replace(0, pd.NA)
    family_summary["building_share_with_status"] = family_summary["buildings_with_status"] / family_summary["building_count"].replace(0, pd.NA)
    yearly_totals = family_summary.groupby(["inspection_year", "treated_rsbl"], as_index=False).agg(total_violations_all=("total_violations", "sum"))
    family_summary = family_summary.merge(yearly_totals, on=["inspection_year", "treated_rsbl"], how="left")
    family_summary["status_share_of_violations"] = family_summary["total_violations"] / family_summary["total_violations_all"].replace(0, pd.NA)

    family_gap = build_treated_control_gap(
        family_summary,
        key_cols=["inspection_year", "status_family"],
        mean_col="mean_violation_count",
    )
    family_gap["building_share_with_status_diff"] = (
        family_gap["building_share_with_status_treated"] - family_gap["building_share_with_status_control"]
    )
    family_gap["status_share_of_violations_diff"] = (
        family_gap["status_share_of_violations_treated"] - family_gap["status_share_of_violations_control"]
    )
    return top_status, family_summary.sort_values(["inspection_year", "treated_rsbl", "status_family"]).reset_index(drop=True), family_gap


def write_note(
    output_path: Path,
    *,
    since_year: int,
    overall_coverage: pd.DataFrame,
    borough_gap: pd.DataFrame,
    yearbuilt_gap: pd.DataFrame,
    margin_gap: pd.DataFrame,
    extension_since_year: int | None = None,
    registration_gap: pd.DataFrame | None = None,
    monthly_gap: pd.DataFrame | None = None,
    status_gap: pd.DataFrame | None = None,
) -> None:
    overall = overall_coverage.iloc[0]
    latest_year = int(margin_gap["inspection_year"].max())

    latest_borough = borough_gap[borough_gap["inspection_year"] == latest_year].sort_values("diff", ascending=False).head(1)
    if latest_borough.empty:
        latest_borough_line = f"   - Borough concentration artifact: `results/tables/nyc_geography_gap_by_borough_year_since_{since_year}.csv`."
    else:
        row = latest_borough.iloc[0]
        latest_borough_line = (
            f"   - Borough concentration artifact: `results/tables/nyc_geography_gap_by_borough_year_since_{since_year}.csv` "
            f"(largest {latest_year} treated-control gap in `{row['borough']}` at `{row['diff']:.3f}` violations per building)."
        )

    latest_yearbuilt = yearbuilt_gap[yearbuilt_gap["inspection_year"] == latest_year].sort_values("diff", ascending=False).head(1)
    if latest_yearbuilt.empty:
        latest_yearbuilt_line = f"   - Building-type artifact: `results/tables/nyc_building_type_gap_by_yearbuilt_bin_year_since_{since_year}.csv`."
    else:
        row = latest_yearbuilt.iloc[0]
        latest_yearbuilt_line = (
            f"   - Building-type artifact: `results/tables/nyc_building_type_gap_by_yearbuilt_bin_year_since_{since_year}.csv` "
            f"(largest {latest_year} age-bin gap in `{row['yearbuilt_bin']}` at `{row['diff']:.3f}`)."
        )

    margin_row = margin_gap[margin_gap["inspection_year"] == latest_year].iloc[0]
    if registration_gap is not None and not registration_gap.empty and monthly_gap is not None and extension_since_year is not None:
        latest_registration = registration_gap[registration_gap["inspection_year"] == latest_year].sort_values("diff", ascending=False).head(1)
        if latest_registration.empty:
            q5_text = (
                f"   - Artifacts: `results/tables/nyc_registration_gap_by_recency_bin_year_since_{since_year}.csv`, "
                f"`results/tables/nyc_timing_monthly_gap_summary_since_{extension_since_year}.csv`."
            )
        else:
            row = latest_registration.iloc[0]
            q5_text = (
                f"   - Current read: by `{latest_year}`, the largest registration-recency gap is in "
                f"`{row['registration_recency_bin']}` at `{row['diff']:.3f}`; monthly timing artifacts now live in "
                f"`results/tables/nyc_timing_monthly_gap_summary_since_{extension_since_year}.csv` and "
                f"`results/tables/nyc_timing_monthly_gap_by_borough_since_{extension_since_year}.csv`."
            )
    else:
        q5_text = (
            f"   - Current read: registration-lifecycle splits now live in "
            f"`results/tables/nyc_registration_gap_by_recency_bin_year_since_{since_year}.csv`; "
            f"monthly timing remains backend-ready until the live extension path is run."
        )

    if status_gap is not None and not status_gap.empty and extension_since_year is not None:
        latest_status = status_gap[
            (status_gap["inspection_year"] == latest_year) & (status_gap["status_family"] == "open_reinspection")
        ]
        if latest_status.empty:
            q6_text = f"   - Artifacts: `results/tables/nyc_status_family_gap_summary_since_{extension_since_year}.csv`."
        else:
            row = latest_status.iloc[0]
            q6_text = (
                f"   - Current read: in `{latest_year}`, `open_reinspection` carries a treated-control mean gap of "
                f"`{row['diff']:.3f}` and a treated-control status-share gap of `{row['status_share_of_violations_diff']:.3f}`."
            )
    else:
        q6_text = f"   - Backend-ready extension artifact: `results/tables/nyc_backend_extension_manifest_since_{since_year}.csv`"
    if status_gap is not None and extension_since_year is not None:
        q6_header = "Main artifacts:"
        q6_artifacts = (
            f"     - `results/tables/nyc_status_top_counts_since_{extension_since_year}.csv`\n"
            f"     - `results/tables/nyc_status_family_summary_since_{extension_since_year}.csv`\n"
            f"     - `results/tables/nyc_status_family_gap_summary_since_{extension_since_year}.csv`"
        )
    else:
        q6_header = "Backend-ready path: `fetch_hpd_violation_status_summary(...)`"
        q6_artifacts = ""
    scope_q56_line = (
        f"- Questions 5-6 now include a narrowed live extension using grouped HPD pulls since {extension_since_year}"
        if extension_since_year is not None and status_gap is not None
        else "- Questions 5-6 are backend-ready through grouped HPD fetch helpers, but are not run by default here"
    )

    text = f"""# NYC Additional Question Shortlist

Date:
- {date.today().isoformat()}

Scope:
- follow-on NYC question package using the landed RSBL + HPD + MapPLUTO + MDR backend
- immediate artifacts answer Questions 1-4 from saved panels
{scope_q56_line}

## Ranked questions and artifacts

1. **Selection and coverage**
   - Main artifact: `results/tables/nyc_treated_selection_coverage_overall_since_{since_year}.csv`
   - Supporting artifacts:
     - `results/tables/nyc_treated_selection_coverage_by_borough_since_{since_year}.csv`
     - `results/tables/nyc_treated_selection_coverage_by_communityboard_since_{since_year}.csv`
     - `results/tables/nyc_treated_selection_coverage_by_yearbuilt_bin_since_{since_year}.csv`
     - `results/tables/nyc_treated_selection_coverage_by_units_bin_since_{since_year}.csv`
   - Current read: the landed treated-stock universe contains `{int(overall['rsbl_buildings'])}` RSBL buildings, of which `{int(overall['hpd_observed_buildings'])}` are HPD-observed, `{int(overall['stratified_eligible_buildings'])}` survive the richer stratified design, and `{int(overall['refined_cb_matched_buildings'])}` survive the refined community-board matcher.

2. **Geographic concentration of the gap**
   - Main artifacts:
     - `results/tables/nyc_geography_gap_by_borough_year_since_{since_year}.csv`
     - `results/tables/nyc_geography_gap_by_communityboard_year_since_{since_year}.csv`
{latest_borough_line}

3. **Building-type heterogeneity**
   - Main artifacts:
     - `results/tables/nyc_building_type_gap_by_yearbuilt_bin_year_since_{since_year}.csv`
     - `results/tables/nyc_building_type_gap_by_units_bin_year_since_{since_year}.csv`
     - `results/tables/nyc_building_type_gap_by_bldgclass_year_since_{since_year}.csv`
{latest_yearbuilt_line}

4. **Extensive versus intensive margin**
   - Main artifacts:
     - `results/tables/nyc_margin_summary_since_{since_year}.csv`
     - `results/tables/nyc_margin_gap_summary_since_{since_year}.csv`
   - Current read: by `{latest_year}`, the treated-control gap is `{margin_row['any_violation_rate_diff']:.3f}` on the any-violation rate and `{margin_row['mean_positive_violation_count_diff']:.3f}` on positive-only counts.

5. **Registration lifecycle and enforcement timing**
   - Main artifacts:
     - `results/tables/nyc_registration_gap_by_count_bin_year_since_{since_year}.csv`
     - `results/tables/nyc_registration_gap_by_recency_bin_year_since_{since_year}.csv`
     - `results/tables/nyc_registration_gap_by_end_bin_year_since_{since_year}.csv`
{q5_text}

6. **Resolution dynamics**
   - {q6_header}
{q6_artifacts}
{q6_text}

## Interpretation guardrails

- These follow-on artifacts are descriptive support for the existing NYC package.
- The geography and building-type outputs use the richer restricted comparison universe, not the refined matched estimator.
- The shortlist is designed to explain where the broader positive differential-growth pattern appears strongest and where treated-stock attrition is concentrated before any stronger causal framing.
"""
    output_path.write_text(text)


def main() -> None:
    args = build_parser().parse_args()
    processed_dir = ROOT / "data" / "processed"
    results_dir = ROOT / "results"
    tables_dir = results_dir / "tables"
    results_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)

    rsbl_citywide = _load_csv(
        processed_dir / f"nyc_rsbl_citywide_{args.rsbl_year}.csv",
        message="Run scripts/run_nyc_rsbl_hpd_citywide.py first.",
    )
    analytic_panel = filter_complete_years(
        _load_csv(
            processed_dir / f"nyc_hpd_building_year_analytic_panel_since_{args.since_year}.csv",
            message="Run scripts/run_nyc_rsbl_hpd_citywide.py first.",
        )
    )
    enriched_panel = filter_complete_years(
        _load_csv(
            processed_dir / f"nyc_hpd_building_year_analytic_panel_enriched_since_{args.since_year}.csv",
            message="Run scripts/run_nyc_enriched_stratified_comparison.py first.",
        )
    )
    refined_matches = _load_csv(
        processed_dir / f"nyc_within_cb_refined_matches_since_{args.since_year}.csv",
        message="Run scripts/run_nyc_within_communityboard_refined_matching.py first.",
    )
    block_matches = _load_csv(
        processed_dir / f"nyc_within_block_matches_since_{args.since_year}.csv",
        message="Run scripts/run_nyc_within_block_matching.py first.",
    )

    restricted_panel = build_stratified_registered_rental_panel(enriched_panel)
    restricted_panel = add_registration_lifecycle_bins(restricted_panel)
    treated_stage_frame = build_treated_selection_stage_frame(
        rsbl_citywide,
        analytic_panel,
        enriched_panel,
        refined_matches=refined_matches,
        block_matches=block_matches,
    )

    coverage_overall = summarize_treated_selection_coverage(treated_stage_frame)
    coverage_borough = summarize_treated_selection_coverage(treated_stage_frame, group_col="borough")
    coverage_communityboard = summarize_treated_selection_coverage(treated_stage_frame, group_col="communityboard")
    coverage_yearbuilt = summarize_treated_selection_coverage(treated_stage_frame, group_col="yearbuilt_bin")
    coverage_units = summarize_treated_selection_coverage(treated_stage_frame, group_col="units_bin")

    geography_borough_summary = aggregate_panel_group_year(restricted_panel, group_cols="borough")
    geography_borough_gap = build_group_year_gap_summary(geography_borough_summary, group_cols="borough")

    geography_cb_panel = fill_group_missing(restricted_panel, "communityboard")
    geography_cb_summary = aggregate_panel_group_year(geography_cb_panel, group_cols=("borough", "communityboard"))
    geography_cb_gap = build_group_year_gap_summary(geography_cb_summary, group_cols=("borough", "communityboard"))

    yearbuilt_summary = aggregate_panel_group_year(restricted_panel, group_cols="yearbuilt_bin")
    yearbuilt_gap = build_group_year_gap_summary(yearbuilt_summary, group_cols="yearbuilt_bin")

    units_summary = aggregate_panel_group_year(restricted_panel, group_cols="units_bin")
    units_gap = build_group_year_gap_summary(units_summary, group_cols="units_bin")

    bldgclass_panel = fill_group_missing(restricted_panel, "bldgclass")
    bldgclass_summary = aggregate_panel_group_year(bldgclass_panel, group_cols="bldgclass")
    bldgclass_gap = build_group_year_gap_summary(bldgclass_summary, group_cols="bldgclass")

    margin_summary = aggregate_margin_group(restricted_panel, group_cols="inspection_year")
    margin_gap = build_margin_gap_summary(margin_summary, group_cols="inspection_year")

    registration_count_summary = aggregate_panel_group_year(restricted_panel, group_cols="registration_count_bin")
    registration_count_gap = build_group_year_gap_summary(registration_count_summary, group_cols="registration_count_bin")
    registration_recency_summary = aggregate_panel_group_year(restricted_panel, group_cols="registration_recency_bin")
    registration_recency_gap = build_group_year_gap_summary(registration_recency_summary, group_cols="registration_recency_bin")
    registration_end_summary = aggregate_panel_group_year(restricted_panel, group_cols="registration_end_bin")
    registration_end_gap = build_group_year_gap_summary(registration_end_summary, group_cols="registration_end_bin")

    building_universe = restricted_panel[["boro_block_lot", "borough", "treated_rsbl"]].drop_duplicates().copy()
    monthly_summary = None
    monthly_gap = None
    monthly_gap_borough = None
    status_top = None
    status_family_summary = None
    status_family_gap = None
    live_extension_status = "backend_ready_not_run"

    if args.include_live_extensions:
        monthly_sparse, status_sparse = load_or_fetch_live_grouped_summaries(
            boroughs=sorted(building_universe["borough"].dropna().astype(str).unique().tolist()),
            processed_dir=processed_dir,
            since_year=args.live_extension_since_year,
            refresh=args.refresh_live,
            page_size=args.live_page_size,
        )
        monthly_summary, monthly_gap, monthly_gap_borough = build_monthly_timing_outputs(
            building_universe,
            monthly_sparse,
            since_year=args.live_extension_since_year,
        )
        status_top, status_family_summary, status_family_gap = build_status_outputs(
            building_universe,
            status_sparse,
            since_year=args.live_extension_since_year,
        )
        live_extension_status = "built"

    backend_manifest = pd.DataFrame(
        [
            {
                "question_id": "NYC_Q5",
                "question": "Does the gap differ by registration lifecycle or finer monthly timing?",
                "status": live_extension_status,
                "source_function": "fetch_hpd_violation_building_month_summary",
                "next_artifact": (
                    f"results/tables/nyc_timing_monthly_gap_summary_since_{args.live_extension_since_year}.csv"
                    if args.include_live_extensions
                    else "monthly treated-control timing table by building or geography"
                ),
            },
            {
                "question_id": "NYC_Q6",
                "question": "Are stabilized-building violations more likely to remain open or unresolved?",
                "status": live_extension_status,
                "source_function": "fetch_hpd_violation_status_summary",
                "next_artifact": (
                    f"results/tables/nyc_status_family_gap_summary_since_{args.live_extension_since_year}.csv"
                    if args.include_live_extensions
                    else "status-grouped yearly summary by treated/control"
                ),
            },
        ]
    )

    outputs: list[tuple[pd.DataFrame, Path]] = [
        (coverage_overall, tables_dir / f"nyc_treated_selection_coverage_overall_since_{args.since_year}.csv"),
        (coverage_borough, tables_dir / f"nyc_treated_selection_coverage_by_borough_since_{args.since_year}.csv"),
        (coverage_communityboard, tables_dir / f"nyc_treated_selection_coverage_by_communityboard_since_{args.since_year}.csv"),
        (coverage_yearbuilt, tables_dir / f"nyc_treated_selection_coverage_by_yearbuilt_bin_since_{args.since_year}.csv"),
        (coverage_units, tables_dir / f"nyc_treated_selection_coverage_by_units_bin_since_{args.since_year}.csv"),
        (geography_borough_gap, tables_dir / f"nyc_geography_gap_by_borough_year_since_{args.since_year}.csv"),
        (geography_cb_gap, tables_dir / f"nyc_geography_gap_by_communityboard_year_since_{args.since_year}.csv"),
        (yearbuilt_gap, tables_dir / f"nyc_building_type_gap_by_yearbuilt_bin_year_since_{args.since_year}.csv"),
        (units_gap, tables_dir / f"nyc_building_type_gap_by_units_bin_year_since_{args.since_year}.csv"),
        (bldgclass_gap, tables_dir / f"nyc_building_type_gap_by_bldgclass_year_since_{args.since_year}.csv"),
        (margin_summary, tables_dir / f"nyc_margin_summary_since_{args.since_year}.csv"),
        (margin_gap, tables_dir / f"nyc_margin_gap_summary_since_{args.since_year}.csv"),
        (registration_count_gap, tables_dir / f"nyc_registration_gap_by_count_bin_year_since_{args.since_year}.csv"),
        (registration_recency_gap, tables_dir / f"nyc_registration_gap_by_recency_bin_year_since_{args.since_year}.csv"),
        (registration_end_gap, tables_dir / f"nyc_registration_gap_by_end_bin_year_since_{args.since_year}.csv"),
        (backend_manifest, tables_dir / f"nyc_backend_extension_manifest_since_{args.since_year}.csv"),
    ]
    if monthly_summary is not None and monthly_gap is not None and monthly_gap_borough is not None:
        outputs.extend(
            [
                (monthly_summary, tables_dir / f"nyc_timing_monthly_summary_since_{args.live_extension_since_year}.csv"),
                (monthly_gap, tables_dir / f"nyc_timing_monthly_gap_summary_since_{args.live_extension_since_year}.csv"),
                (monthly_gap_borough, tables_dir / f"nyc_timing_monthly_gap_by_borough_since_{args.live_extension_since_year}.csv"),
            ]
        )
    if status_top is not None and status_family_summary is not None and status_family_gap is not None:
        outputs.extend(
            [
                (status_top, tables_dir / f"nyc_status_top_counts_since_{args.live_extension_since_year}.csv"),
                (status_family_summary, tables_dir / f"nyc_status_family_summary_since_{args.live_extension_since_year}.csv"),
                (status_family_gap, tables_dir / f"nyc_status_family_gap_summary_since_{args.live_extension_since_year}.csv"),
            ]
        )

    for frame, path in outputs:
        frame.to_csv(path, index=False)
        print(f"wrote {path}")

    note_path = results_dir / "nyc_additional_question_shortlist.md"
    write_note(
        note_path,
        since_year=args.since_year,
        overall_coverage=coverage_overall,
        borough_gap=geography_borough_gap,
        yearbuilt_gap=yearbuilt_gap,
        margin_gap=margin_gap,
        extension_since_year=args.live_extension_since_year if args.include_live_extensions else None,
        registration_gap=registration_recency_gap,
        monthly_gap=monthly_gap,
        status_gap=status_family_gap,
    )
    print(f"wrote {note_path}")
    print(coverage_overall.to_string(index=False))


if __name__ == "__main__":
    main()
