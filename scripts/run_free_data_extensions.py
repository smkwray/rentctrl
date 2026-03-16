from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from rent_control_public.reporting import add_per_1000_metric, summarize_event_window_coefficients


OUTCOME_METADATA = {
    "DP04_0134E": {"display_name": "Median gross rent", "domain": "rent_level"},
    "rent_burden_30_plus_pct": {"display_name": "Rent burden 30%+", "domain": "affordability"},
    "same_house_1y_pct": {"display_name": "Same house one year ago", "domain": "stability"},
    "moved_last_year_pct": {"display_name": "Moved last year", "domain": "stability"},
    "moved_different_state_pct": {"display_name": "Moved from different state", "domain": "stability"},
    "permits_units_total": {"display_name": "Permitted units", "domain": "supply"},
    "permits_units_5plus": {"display_name": "5+ unit permits", "domain": "supply"},
    "permits_units_multifamily_share": {"display_name": "Multifamily permit share", "domain": "supply"},
    "index_sa_mean": {"display_name": "FHFA HPI (SA mean)", "domain": "capitalization"},
    "qcew_total_covered_emplvl": {"display_name": "QCEW covered employment", "domain": "labor"},
    "qcew_total_covered_avg_weekly_wage": {"display_name": "QCEW average weekly wage", "domain": "labor"},
    "renter_share_pct": {"display_name": "Renter share", "domain": "tenure_mix"},
}

QUESTION_ROWS = [
    ("Q1", "Did statewide rent caps reduce occupied-rent growth?"),
    ("Q2", "Did statewide rent caps reduce renter cost burden?"),
    ("Q3", "Did statewide rent caps increase residential stability?"),
    ("Q4", "Did statewide rent caps reduce housing supply or shift the composition of supply?"),
    ("Q5", "Did statewide rent caps change housing price dynamics?"),
    ("Q6", "Did statewide rent caps change labor-market outcomes?"),
    ("Q7", "Were effects larger in states with higher baseline rent burden?"),
    ("Q8", "Were effects larger where supply was already constrained?"),
    ("Q9", "Did impacts differ for rent levels versus burden versus mobility?"),
    ("Q10", "Were California and Oregon similar enough to pool?"),
]


def load_panel() -> pd.DataFrame:
    panel = pd.read_csv(ROOT / "data" / "processed" / "core_state_panel_annual.csv", dtype={"state_fips": str})
    renter_household_col = "DP04_0047E" if "DP04_0047E" in panel.columns else "renter_households"
    panel = add_per_1000_metric(
        panel,
        numerator_col="permits_units_total",
        denominator_col=renter_household_col,
        output_col="permits_per_1000_renter_households",
    )
    panel = add_per_1000_metric(
        panel,
        numerator_col="permits_units_5plus",
        denominator_col=renter_household_col,
        output_col="permits_5plus_per_1000_renter_households",
    )
    return panel


def build_prepolicy_profiles(panel: pd.DataFrame) -> pd.DataFrame:
    baseline = panel[
        panel["analysis_role"].isin(["core_treated", "donor"])
        & panel["year"].between(2015, 2018)
    ].copy()
    profile_cols = [
        "rent_burden_30_plus_pct",
        "renter_share_pct",
        "same_house_1y_pct",
        "moved_last_year_pct",
        "moved_different_state_pct",
        "permits_units_total",
        "permits_units_5plus",
        "permits_units_multifamily_share",
        "permits_per_1000_renter_households",
        "permits_5plus_per_1000_renter_households",
        "index_sa_mean",
        "qcew_total_covered_emplvl",
        "qcew_total_covered_avg_weekly_wage",
    ]
    keep = ["state_name", "state_abbr", "analysis_role", "year"] + [col for col in profile_cols if col in baseline.columns]
    profile = baseline[keep].groupby(["state_name", "state_abbr", "analysis_role"], as_index=False).mean(numeric_only=True)
    profile["baseline_window"] = "2015-2018"
    return profile.sort_values(["analysis_role", "state_abbr"]).reset_index(drop=True)


def summarize_pooled_effects(panel: pd.DataFrame, results_dir: Path) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    treated_pre = panel[(panel["analysis_role"] == "core_treated") & (panel["policy_active_preferred"] == 0)].copy()
    for outcome, meta in OUTCOME_METADATA.items():
        coef_path = results_dir / f"pretrend_coefficients_{outcome}_baseline.csv"
        if not coef_path.exists():
            continue
        coef = pd.read_csv(coef_path)
        summary = summarize_event_window_coefficients(coef)
        if summary.empty:
            continue
        pre_mean = pd.to_numeric(treated_pre[outcome], errors="coerce").mean() if outcome in treated_pre.columns else pd.NA
        row = summary.iloc[0].to_dict()
        row.update(
            {
                "outcome": outcome,
                "display_name": meta["display_name"],
                "domain": meta["domain"],
                "treated_pre_policy_mean": pre_mean,
            }
        )
        if pd.notna(pre_mean) and pre_mean not in {0, 0.0} and pd.notna(row["avg_post_coef"]):
            row["avg_post_pct_of_pre_policy_mean"] = row["avg_post_coef"] / pre_mean * 100
        else:
            row["avg_post_pct_of_pre_policy_mean"] = pd.NA
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["domain", "display_name"]).reset_index(drop=True)


def summarize_state_specific_effects(panel: pd.DataFrame, results_dir: Path) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    treated_pre = panel[(panel["analysis_role"] == "core_treated") & (panel["policy_active_preferred"] == 0)].copy()
    for outcome, meta in OUTCOME_METADATA.items():
        coef_path = results_dir / f"pretrend_coefficients_{outcome}_state_interactions.csv"
        if not coef_path.exists():
            continue
        coef = pd.read_csv(coef_path)
        summary = summarize_event_window_coefficients(coef, group_cols=["state_abbr"])
        if summary.empty:
            continue
        pre_means = (
            treated_pre.groupby("state_abbr", as_index=False)[outcome]
            .mean()
            .rename(columns={outcome: "treated_pre_policy_mean"})
        )
        summary = summary.merge(pre_means, on="state_abbr", how="left")
        summary["outcome"] = outcome
        summary["display_name"] = meta["display_name"]
        summary["domain"] = meta["domain"]
        denominator = pd.to_numeric(summary["treated_pre_policy_mean"], errors="coerce")
        numerator = pd.to_numeric(summary["avg_post_coef"], errors="coerce")
        summary["avg_post_pct_of_pre_policy_mean"] = numerator.div(denominator.where(denominator.ne(0))).mul(100)
        rows.append(summary)
    if not rows:
        return pd.DataFrame()
    combined = pd.concat(rows, ignore_index=True)
    return combined.sort_values(["domain", "display_name", "state_abbr"]).reset_index(drop=True)


def build_question_coverage() -> pd.DataFrame:
    rows = [
        {
            "question_id": "Q1",
            "question": QUESTION_ROWS[0][1],
            "status": "answered_with_limits",
            "primary_artifacts": "results/tables/event_study_DP04_0134E.txt; results/tables/pretrend_coefficients_DP04_0134E_baseline.csv; results/figures/baseline_median_gross_rent_annual.png",
            "notes": "ACS occupied-rent coverage is limited to 2010-2019 and 2021-2024.",
        },
        {
            "question_id": "Q2",
            "question": QUESTION_ROWS[1][1],
            "status": "answered_with_limits",
            "primary_artifacts": "results/tables/event_study_rent_burden_30_plus_pct.txt; results/tables/pretrend_coefficients_rent_burden_30_plus_pct_baseline.csv",
            "notes": "ACS burden coverage is limited to 2010-2019 and 2021-2024.",
        },
        {
            "question_id": "Q3",
            "question": QUESTION_ROWS[2][1],
            "status": "answered_with_limits",
            "primary_artifacts": "results/tables/event_study_same_house_1y_pct.txt; results/tables/event_study_moved_last_year_pct.txt; results/tables/event_study_moved_different_state_pct.txt",
            "notes": "Mobility outcomes use ACS 2010-2019 and 2021-2024 only.",
        },
        {
            "question_id": "Q4",
            "question": QUESTION_ROWS[3][1],
            "status": "answered",
            "primary_artifacts": "results/tables/event_study_permits_units_total.txt; results/tables/event_study_permits_units_5plus.txt; results/tables/event_study_permits_units_multifamily_share.txt",
            "notes": "Annual BPS outcomes are fully covered in the current pipeline.",
        },
        {
            "question_id": "Q5",
            "question": QUESTION_ROWS[4][1],
            "status": "answered",
            "primary_artifacts": "results/tables/event_study_index_sa_mean.txt; results/tables/pretrend_coefficients_index_sa_mean_baseline.csv; results/figures/quarterly_fhfa_treated_vs_donor.png",
            "notes": "FHFA remains the strongest secondary outcome family in the current build.",
        },
        {
            "question_id": "Q6",
            "question": QUESTION_ROWS[5][1],
            "status": "answered_with_limits",
            "primary_artifacts": "results/tables/event_study_qcew_total_covered_emplvl.txt; results/tables/event_study_qcew_total_covered_avg_weekly_wage.txt; results/figures/quarterly_qcew_treated_vs_donor.png",
            "notes": "QCEW area-slice coverage begins in 2014 and should remain exploratory.",
        },
        {
            "question_id": "Q7",
            "question": QUESTION_ROWS[6][1],
            "status": "suggestive_treated_state_contrast",
            "primary_artifacts": "results/tables/prepolicy_state_profiles.csv; results/tables/state_specific_effect_summary.csv; results/free_data_extensions.md",
            "notes": "CA has higher baseline rent burden than OR. With only two treated states, heterogeneity is a treated-state contrast rather than a powered pooled interaction.",
        },
        {
            "question_id": "Q8",
            "question": QUESTION_ROWS[7][1],
            "status": "suggestive_treated_state_contrast",
            "primary_artifacts": "results/tables/prepolicy_state_profiles.csv; results/tables/state_specific_effect_summary.csv; results/free_data_extensions.md",
            "notes": "CA has lower permits per renter household than OR in 2015-2018, so the supply-constraint comparison is treated as a CA-versus-OR contrast.",
        },
        {
            "question_id": "Q9",
            "question": QUESTION_ROWS[8][1],
            "status": "answered_with_cross_outcome_summary",
            "primary_artifacts": "results/tables/domain_comparison_summary.csv; results/credibility_interpretation.md; results/free_data_extensions.md",
            "notes": "Cross-domain comparison uses pooled baseline coefficient summaries normalized to treated pre-policy means where possible.",
        },
        {
            "question_id": "Q10",
            "question": QUESTION_ROWS[9][1],
            "status": "answered",
            "primary_artifacts": "results/tables/credibility_checks_summary.csv; results/tables/pretrend_coefficients_index_sa_mean_ca_only.csv; results/tables/pretrend_coefficients_index_sa_mean_or_only.csv; results/tables/pretrend_coefficients_index_sa_mean_state_interactions.csv",
            "notes": "CA-only, OR-only, and pooled state-interaction outputs are now part of the standard package.",
        },
    ]
    return pd.DataFrame(rows)


def write_extension_note(
    *,
    profile_path: Path,
    pooled_path: Path,
    state_path: Path,
    coverage_path: Path,
) -> None:
    profiles = pd.read_csv(profile_path)
    state_effects = pd.read_csv(state_path)
    pooled = pd.read_csv(pooled_path)

    treated = profiles[profiles["state_abbr"].isin(["CA", "OR"])].copy()
    treated = treated.set_index("state_abbr")
    ca_burden = treated.loc["CA", "rent_burden_30_plus_pct"]
    or_burden = treated.loc["OR", "rent_burden_30_plus_pct"]
    ca_supply = treated.loc["CA", "permits_per_1000_renter_households"]
    or_supply = treated.loc["OR", "permits_per_1000_renter_households"]

    domain_lines = []
    for domain in ["rent_level", "affordability", "stability", "supply", "capitalization", "labor"]:
        domain_slice = pooled[pooled["domain"] == domain].copy()
        if domain_slice.empty:
            continue
        labels = ", ".join(domain_slice["display_name"].tolist())
        domain_lines.append(f"- `{domain}`: {labels}")

    text = f"""# Free Data Extension Note

This package closes out the remaining useful phase-1 questions that can be answered from the current free public data stack.

## Question coverage

The coverage matrix is written to `{coverage_path.relative_to(ROOT)}`.

## Question 7: Higher baseline burden

- California's 2015-2018 pre-policy rent burden mean is `{ca_burden:.2f}%`.
- Oregon's 2015-2018 pre-policy rent burden mean is `{or_burden:.2f}%`.
- Use `{state_path.relative_to(ROOT)}` to compare CA-only and OR-only event-study response patterns.

Interpretation rule:
- Treat this as a CA-versus-OR contrast, not a fully powered heterogeneity design. There are only two treated states.

## Question 8: Supply constraint

- California's 2015-2018 permits per 1,000 renter households mean is `{ca_supply:.2f}`.
- Oregon's 2015-2018 permits per 1,000 renter households mean is `{or_supply:.2f}`.
- Lower baseline permit intensity implies California is the more supply-constrained treated state in this public-data setup.

Interpretation rule:
- Use the same state-specific contrast table, alongside the pre-policy profile table in `{profile_path.relative_to(ROOT)}`.

## Question 9: Cross-outcome comparison

The pooled comparison table is written to `{pooled_path.relative_to(ROOT)}` and should be read outcome by outcome, not as a single scalar ranking.

Outcome families included:
{chr(10).join(domain_lines)}

Reporting posture:
- `FHFA` and `BPS` remain the cleanest headline outcome families.
- ACS rent, burden, and mobility outcomes are informative but coverage-limited.
- QCEW employment and wage outcomes remain exploratory because the annual and quarterly state slices start in 2014.
"""
    (ROOT / "results" / "free_data_extensions.md").write_text(text)


def main() -> None:
    results_dir = ROOT / "results" / "tables"
    results_dir.mkdir(parents=True, exist_ok=True)

    panel = load_panel()
    profiles = build_prepolicy_profiles(panel)
    pooled = summarize_pooled_effects(panel, results_dir)
    state_effects = summarize_state_specific_effects(panel, results_dir)
    coverage = build_question_coverage()

    profile_path = results_dir / "prepolicy_state_profiles.csv"
    pooled_path = results_dir / "domain_comparison_summary.csv"
    state_path = results_dir / "state_specific_effect_summary.csv"
    coverage_path = results_dir / "free_data_question_coverage.csv"

    profiles.to_csv(profile_path, index=False)
    pooled.to_csv(pooled_path, index=False)
    state_effects.to_csv(state_path, index=False)
    coverage.to_csv(coverage_path, index=False)
    write_extension_note(
        profile_path=profile_path,
        pooled_path=pooled_path,
        state_path=state_path,
        coverage_path=coverage_path,
    )
    print(f"wrote {profile_path}")
    print(f"wrote {pooled_path}")
    print(f"wrote {state_path}")
    print(f"wrote {coverage_path}")
    print(f"wrote {ROOT / 'results' / 'free_data_extensions.md'}")


if __name__ == "__main__":
    main()
