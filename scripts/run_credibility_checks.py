from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import statsmodels.formula.api as smf

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from rent_control_public.event_study import add_binned_event_time_dummies, extract_event_study_coefficients, fit_twfe_event_study
from rent_control_public.pipeline import (
    DEFAULT_ANNUAL_REQUIRED_DOMAINS,
    DEFAULT_QUARTERLY_REQUIRED_DOMAINS,
    require_manifest_readiness,
)


OUTCOME_METADATA = {
    "permits_units_5plus": {"label": "5+ unit permits", "min_obs": 40},
    "permits_units_total": {"label": "Permitted units", "min_obs": 40},
    "permits_units_multifamily_share": {"label": "Multifamily permit share", "min_obs": 40},
    "index_sa_mean": {"label": "FHFA HPI (SA mean)", "min_obs": 40},
    "qcew_total_covered_emplvl": {"label": "QCEW covered employment", "min_obs": 40},
    "qcew_total_covered_avg_weekly_wage": {"label": "QCEW average weekly wage", "min_obs": 40},
    "same_house_1y_pct": {"label": "Same house one year ago", "min_obs": 30},
    "moved_last_year_pct": {"label": "Moved last year", "min_obs": 30},
    "moved_different_state_pct": {"label": "Moved from different state", "min_obs": 30},
    "renter_share_pct": {"label": "Renter share", "min_obs": 30},
    "DP04_0134E": {"label": "Median gross rent", "min_obs": 30},
    "rent_burden_30_plus_pct": {"label": "Rent burden 30%+", "min_obs": 30},
}

MAIN_OUTCOMES = list(OUTCOME_METADATA)
LEAVE_ONE_DONOR_OUT_MAX = 3
QUARTERLY_OUTCOMES = {
    "index_sa": {"label": "FHFA HPI (SA)", "min_obs": 80},
    "qcew_total_covered_emplvl": {"label": "QCEW covered employment", "min_obs": 80},
    "qcew_total_covered_avg_weekly_wage": {"label": "QCEW average weekly wage", "min_obs": 80},
}
ANNUAL_RESAMPLE_COUNT = 60
QUARTERLY_RESAMPLE_COUNT = 60


def prepare_event_sample(
    df: pd.DataFrame,
    *,
    outcome: str,
    treated_states: list[str] | None = None,
    donor_states: list[str] | None = None,
    placebo_shift_years: int = 0,
    min_bin: int = -4,
    max_bin: int = 4,
) -> pd.DataFrame:
    sample = df[df["analysis_role"].isin(["core_treated", "donor"])].copy()
    if treated_states is not None:
        sample = sample[
            sample["state_abbr"].isin(treated_states)
            | ((sample["analysis_role"] == "donor") & sample["state_abbr"].isin(donor_states or []))
        ].copy()
    elif donor_states is not None:
        sample = sample[
            (sample["analysis_role"] == "core_treated") | ((sample["analysis_role"] == "donor") & sample["state_abbr"].isin(donor_states))
        ].copy()

    sample = sample.dropna(subset=[outcome]).copy()
    if sample.empty:
        return sample

    if treated_states is not None:
        sample = sample[
            (sample["state_abbr"].isin(treated_states) & sample["analysis_role"].eq("core_treated"))
            | sample["analysis_role"].eq("donor")
        ].copy()

    event_time = pd.to_numeric(sample["event_time_years_preferred"], errors="coerce")
    if placebo_shift_years:
        event_time = event_time + placebo_shift_years
    sample["event_time_int"] = event_time.round().astype("Int64")
    sample = add_binned_event_time_dummies(
        sample,
        "event_time_int",
        min_bin=min_bin,
        max_bin=max_bin,
        reference_period=-1,
    )
    return sample


def is_usable_sample(sample: pd.DataFrame, *, outcome: str) -> bool:
    if sample.empty:
        return False
    min_obs = OUTCOME_METADATA[outcome]["min_obs"]
    return len(sample) >= min_obs and sample["year"].nunique() >= 4 and sample["state_abbr"].nunique() >= 4


def fit_state_interaction_model(sample: pd.DataFrame, *, outcome: str, treated_states: list[str]) -> pd.DataFrame:
    interaction_cols = []
    interacted = sample.copy()
    base_event_cols = [c for c in interacted.columns if c.startswith("evt_")]
    for state_abbr in treated_states:
        state_mask = interacted["state_abbr"].eq(state_abbr).astype(int)
        for col in base_event_cols:
            name = f"{col}_{state_abbr}"
            interacted[name] = interacted[col] * state_mask
            interaction_cols.append(name)

    rhs = " + ".join(interaction_cols + ["C(state_name)", "C(year)"])
    model = smf.ols(f"{outcome} ~ {rhs}", data=interacted).fit(cov_type="HC1")
    coef = pd.DataFrame(
        {
            "term": model.params.index,
            "coef": model.params.values,
            "std_err": model.bse.values,
            "p_value": model.pvalues.values,
        }
    )
    coef = coef[coef["term"].str.startswith("evt_")].copy()

    def split_interaction_term(term: str) -> tuple[str, str]:
        for state_abbr in treated_states:
            suffix = f"_{state_abbr}"
            if term.endswith(suffix):
                return term[: -len(suffix)], state_abbr
        raise ValueError(f"Unexpected interaction term: {term}")

    parsed = coef["term"].map(split_interaction_term)
    coef["base_term"] = parsed.str[0]
    coef["state_abbr"] = parsed.str[1]
    coef["event_time"] = coef["base_term"].map(lambda term: -int(term[5:]) if "_m" in term else int(term[5:]))
    coef["ci_low"] = coef["coef"] - 1.96 * coef["std_err"]
    coef["ci_high"] = coef["coef"] + 1.96 * coef["std_err"]
    return coef.sort_values(["state_abbr", "event_time"]).reset_index(drop=True)


def write_pretrend_plot(coef: pd.DataFrame, *, title: str, path: Path) -> None:
    ci_low_col = "ci_low_resampled" if "ci_low_resampled" in coef.columns and coef["ci_low_resampled"].notna().any() else "ci_low"
    ci_high_col = "ci_high_resampled" if "ci_high_resampled" in coef.columns and coef["ci_high_resampled"].notna().any() else "ci_high"
    event_time = pd.to_numeric(coef["event_time"], errors="coerce")
    coef_values = pd.to_numeric(coef["coef"], errors="coerce")
    ci_low = pd.to_numeric(coef[ci_low_col], errors="coerce")
    ci_high = pd.to_numeric(coef[ci_high_col], errors="coerce")
    plt.figure(figsize=(9, 5.5))
    plt.axhline(0, color="black", linewidth=1)
    plt.axvline(-1, color="gray", linestyle=":", linewidth=1)
    plt.plot(event_time, coef_values, marker="o", linewidth=2, color="#1f4e79")
    plt.fill_between(event_time, ci_low, ci_high, color="#9ec5e5", alpha=0.4)
    plt.title(title)
    plt.xlabel("Event time (years)")
    plt.ylabel("Coefficient")
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def write_interaction_plot(coef: pd.DataFrame, *, title: str, path: Path) -> None:
    plt.figure(figsize=(9, 5.5))
    plt.axhline(0, color="black", linewidth=1)
    plt.axvline(-1, color="gray", linestyle=":", linewidth=1)
    for state_abbr, state_df in coef.groupby("state_abbr"):
        plt.plot(state_df["event_time"], state_df["coef"], marker="o", linewidth=2, label=state_abbr)
    plt.title(title)
    plt.xlabel("Event time (years)")
    plt.ylabel("Coefficient")
    plt.legend()
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def prepare_quarterly_event_sample(
    df: pd.DataFrame,
    *,
    outcome: str,
    event_time_col: str,
    min_bin: int = -8,
    max_bin: int = 8,
) -> pd.DataFrame:
    sample = df[df["analysis_role"].isin(["core_treated", "donor"])].dropna(subset=[outcome]).copy()
    if sample.empty:
        return sample
    sample["event_time_int"] = pd.to_numeric(sample[event_time_col], errors="coerce").astype("Int64")
    sample = add_binned_event_time_dummies(
        sample,
        "event_time_int",
        min_bin=min_bin,
        max_bin=max_bin,
        reference_period=-1,
    )
    return sample


def add_alternative_quarter_event_time(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    period = pd.PeriodIndex(out["calendar_period"], freq="Q")
    alt_treat = pd.PeriodIndex(out["alternative_treat_period"], freq="Q")
    event_time = (period.year - alt_treat.year) * 4 + (period.quarter - alt_treat.quarter)
    out["event_time_quarters_alternative"] = event_time
    out.loc[out["analysis_role"] == "donor", "event_time_quarters_alternative"] = pd.NA
    return out


def run() -> None:
    require_manifest_readiness(
        ROOT,
        annual_domains=DEFAULT_ANNUAL_REQUIRED_DOMAINS,
        quarterly_domains=DEFAULT_QUARTERLY_REQUIRED_DOMAINS,
    )
    panel = pd.read_csv(ROOT / "data" / "processed" / "core_state_panel_annual.csv", dtype={"state_fips": str})
    quarterly_panel = pd.read_csv(ROOT / "data" / "processed" / "core_state_panel_quarterly.csv", dtype={"state_fips": str})
    quarterly_panel = add_alternative_quarter_event_time(quarterly_panel)
    results_dir = ROOT / "results" / "tables"
    figures_dir = ROOT / "results" / "figures"
    results_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    donor_states = sorted(panel.loc[panel["analysis_role"] == "donor", "state_abbr"].dropna().unique())
    treated_states = ["CA", "OR"]
    summary_rows: list[dict[str, object]] = []

    for outcome in MAIN_OUTCOMES:
        sample = prepare_event_sample(panel, outcome=outcome, donor_states=donor_states)
        if is_usable_sample(sample, outcome=outcome):
            baseline = fit_twfe_event_study(
                sample,
                outcome=outcome,
                unit_col="state_name",
                time_col="year",
                event_time_col="event_time_int",
                resampled_inference="permutation",
                resample_count=ANNUAL_RESAMPLE_COUNT,
                random_seed=101,
            )
            coef = extract_event_study_coefficients(baseline)
            coef.to_csv(results_dir / f"pretrend_coefficients_{outcome}_baseline.csv", index=False)
            write_pretrend_plot(
                coef,
                title=f"Pre-trend and event coefficients: {OUTCOME_METADATA[outcome]['label']}",
                path=figures_dir / f"pretrend_{outcome}_baseline.png",
            )
            summary_rows.append(
                {
                    "outcome": outcome,
                    "check_name": "baseline",
                    "spec_group": "pooled",
                    "treated_states": "CA,OR",
                    "donor_pool": ",".join(donor_states),
                    "sample_rows": len(sample),
                    "sample_start_year": int(sample["year"].min()),
                    "sample_end_year": int(sample["year"].max()),
                    "status": "ok",
                    "notes": "Baseline pooled TWFE event study with donor states retained and pre-trend coefficients exported.",
                }
            )
        else:
            summary_rows.append(
                {
                    "outcome": outcome,
                    "check_name": "baseline",
                    "spec_group": "pooled",
                    "treated_states": "CA,OR",
                    "donor_pool": ",".join(donor_states),
                    "sample_rows": len(sample),
                    "sample_start_year": pd.NA,
                    "sample_end_year": pd.NA,
                    "status": "skipped",
                    "notes": "Insufficient baseline sample.",
                }
            )
            continue

        placebo_sample = prepare_event_sample(panel, outcome=outcome, donor_states=donor_states, placebo_shift_years=2)
        if is_usable_sample(placebo_sample, outcome=outcome):
            placebo = fit_twfe_event_study(
                placebo_sample,
                outcome=outcome,
                unit_col="state_name",
                time_col="year",
                event_time_col="event_time_int",
                resampled_inference="permutation",
                resample_count=ANNUAL_RESAMPLE_COUNT,
                random_seed=202,
            )
            placebo_coef = extract_event_study_coefficients(placebo)
            placebo_coef.to_csv(results_dir / f"pretrend_coefficients_{outcome}_placebo_2y_early.csv", index=False)
            summary_rows.append(
                {
                    "outcome": outcome,
                    "check_name": "placebo_2y_early",
                    "spec_group": "pooled",
                    "treated_states": "CA,OR",
                    "donor_pool": ",".join(donor_states),
                    "sample_rows": len(placebo_sample),
                    "sample_start_year": int(placebo_sample["year"].min()),
                    "sample_end_year": int(placebo_sample["year"].max()),
                    "status": "ok",
                    "notes": "Placebo treatment years shifted two years earlier than the preferred annual treatment year.",
                }
            )

        west_donors = ["AZ", "CO", "ID", "NV", "UT"]
        west_sample = prepare_event_sample(panel, outcome=outcome, donor_states=west_donors)
        if is_usable_sample(west_sample, outcome=outcome):
            fit_twfe_event_study(
                west_sample,
                outcome=outcome,
                unit_col="state_name",
                time_col="year",
                event_time_col="event_time_int",
                resampled_inference="permutation",
                resample_count=ANNUAL_RESAMPLE_COUNT,
                random_seed=303,
            )
            summary_rows.append(
                {
                    "outcome": outcome,
                    "check_name": "donor_pool_west_only",
                    "spec_group": "donor_sensitivity",
                    "treated_states": "CA,OR",
                    "donor_pool": ",".join(west_donors),
                    "sample_rows": len(west_sample),
                    "sample_start_year": int(west_sample["year"].min()),
                    "sample_end_year": int(west_sample["year"].max()),
                    "status": "ok",
                    "notes": "Restricts the donor pool to western donor states only.",
                }
            )

        for dropped_donor in donor_states[:LEAVE_ONE_DONOR_OUT_MAX]:
            reduced_donors = [state for state in donor_states if state != dropped_donor]
            reduced_sample = prepare_event_sample(panel, outcome=outcome, donor_states=reduced_donors)
            if not is_usable_sample(reduced_sample, outcome=outcome):
                continue
            fit_twfe_event_study(
                reduced_sample,
                outcome=outcome,
                unit_col="state_name",
                time_col="year",
                event_time_col="event_time_int",
                resampled_inference="permutation",
                resample_count=ANNUAL_RESAMPLE_COUNT,
                random_seed=404,
            )
            summary_rows.append(
                {
                    "outcome": outcome,
                    "check_name": f"leave_one_donor_out_{dropped_donor}",
                    "spec_group": "donor_sensitivity",
                    "treated_states": "CA,OR",
                    "donor_pool": ",".join(reduced_donors),
                    "sample_rows": len(reduced_sample),
                    "sample_start_year": int(reduced_sample["year"].min()),
                    "sample_end_year": int(reduced_sample["year"].max()),
                    "status": "ok",
                    "notes": f"Baseline donor pool excluding {dropped_donor}.",
                }
            )

        for treated_state in treated_states:
            single_sample = prepare_event_sample(panel, outcome=outcome, treated_states=[treated_state], donor_states=donor_states)
            if not is_usable_sample(single_sample, outcome=outcome):
                continue
            single_result = fit_twfe_event_study(
                single_sample,
                outcome=outcome,
                unit_col="state_name",
                time_col="year",
                event_time_col="event_time_int",
                resampled_inference="permutation",
                resample_count=ANNUAL_RESAMPLE_COUNT,
                random_seed=505,
            )
            single_coef = extract_event_study_coefficients(single_result)
            single_coef.to_csv(results_dir / f"pretrend_coefficients_{outcome}_{treated_state.lower()}_only.csv", index=False)
            summary_rows.append(
                {
                    "outcome": outcome,
                    "check_name": f"{treated_state.lower()}_only",
                    "spec_group": "single_treated",
                    "treated_states": treated_state,
                    "donor_pool": ",".join(donor_states),
                    "sample_rows": len(single_sample),
                    "sample_start_year": int(single_sample["year"].min()),
                    "sample_end_year": int(single_sample["year"].max()),
                    "status": "ok",
                    "notes": f"Single-treated-state run for {treated_state} with the full donor pool.",
                }
            )

        interaction_sample = prepare_event_sample(panel, outcome=outcome, donor_states=donor_states)
        if is_usable_sample(interaction_sample, outcome=outcome):
            interaction_coef = fit_state_interaction_model(interaction_sample, outcome=outcome, treated_states=treated_states)
            interaction_coef.to_csv(results_dir / f"pretrend_coefficients_{outcome}_state_interactions.csv", index=False)
            write_interaction_plot(
                interaction_coef,
                title=f"State-specific event interactions: {OUTCOME_METADATA[outcome]['label']}",
                path=figures_dir / f"pretrend_{outcome}_state_interactions.png",
            )
            summary_rows.append(
                {
                    "outcome": outcome,
                    "check_name": "state_specific_interactions",
                    "spec_group": "interaction",
                    "treated_states": "CA,OR",
                    "donor_pool": ",".join(donor_states),
                    "sample_rows": len(interaction_sample),
                    "sample_start_year": int(interaction_sample["year"].min()),
                    "sample_end_year": int(interaction_sample["year"].max()),
                    "status": "ok",
                    "notes": "Pooled model with separate event-time interactions for CA and OR.",
                }
            )

    for outcome, meta in QUARTERLY_OUTCOMES.items():
        for check_name, event_col in [
            ("quarterly_preferred_treatment", "event_time_quarters_preferred"),
            ("quarterly_alternative_treatment", "event_time_quarters_alternative"),
        ]:
            sample = prepare_quarterly_event_sample(quarterly_panel, outcome=outcome, event_time_col=event_col)
            if sample.empty or len(sample) < meta["min_obs"] or sample["calendar_period"].nunique() < 8:
                summary_rows.append(
                    {
                        "outcome": outcome,
                        "check_name": check_name,
                        "spec_group": "quarterly_treatment_timing",
                        "treated_states": "CA,OR",
                        "donor_pool": ",".join(donor_states),
                        "sample_rows": len(sample),
                        "sample_start_year": pd.NA,
                        "sample_end_year": pd.NA,
                        "status": "skipped",
                        "notes": f"Insufficient quarterly sample for {event_col}.",
                    }
                )
                continue

            result = fit_twfe_event_study(
                sample,
                outcome=outcome,
                unit_col="state_name",
                time_col="calendar_period",
                event_time_col="event_time_int",
                resampled_inference="permutation",
                resample_count=QUARTERLY_RESAMPLE_COUNT,
                random_seed=606,
            )
            coef = extract_event_study_coefficients(result)
            coef.to_csv(results_dir / f"pretrend_coefficients_{outcome}_{check_name}.csv", index=False)
            write_pretrend_plot(
                coef,
                title=f"Quarterly treatment timing sensitivity: {meta['label']} ({check_name})",
                path=figures_dir / f"pretrend_{outcome}_{check_name}.png",
            )
            summary_rows.append(
                {
                    "outcome": outcome,
                    "check_name": check_name,
                    "spec_group": "quarterly_treatment_timing",
                    "treated_states": "CA,OR",
                    "donor_pool": ",".join(donor_states),
                    "sample_rows": len(sample),
                    "sample_start_year": int(sample["year"].min()),
                    "sample_end_year": int(sample["year"].max()),
                    "status": "ok",
                    "notes": f"Quarterly TWFE event study using `{event_col}`.",
                }
            )

    pd.DataFrame(summary_rows).to_csv(results_dir / "credibility_checks_summary.csv", index=False)
    print(f"wrote {results_dir / 'credibility_checks_summary.csv'}")


if __name__ == "__main__":
    run()
