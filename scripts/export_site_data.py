"""Export pipeline results to docs/data/site_data.js for the static site.

Reads CSV outputs from the baseline pipeline and writes a single JS file
that sets window.SITE_DATA.  The HTML pages include this via a <script> tag,
avoiding hard-coded chart arrays and keeping the site coupled to the pipeline.

Usage:
    python -B scripts/export_site_data.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
TABLES = ROOT / "results" / "tables"
OUT = ROOT / "docs" / "data" / "site_data.js"

# ── Outcome display order (matches the site chart) ──────────────────────
OUTCOME_ORDER = [
    "permits_units_total",
    "permits_units_5plus",
    "permits_units_multifamily_share",
    "index_sa_mean",
    "DP04_0134E",
    "rent_burden_30_plus_pct",
    "same_house_1y_pct",
    "moved_last_year_pct",
    "moved_different_state_pct",
    "renter_share_pct",
    "qcew_total_covered_avg_weekly_wage",
    "qcew_total_covered_emplvl",
]

# Short chart labels (matching the existing site style)
CHART_LABELS = {
    "permits_units_total": "Total permits",
    "permits_units_5plus": "5+ unit permits",
    "permits_units_multifamily_share": "Multifamily share",
    "index_sa_mean": "FHFA HPI",
    "DP04_0134E": "Median gross rent",
    "rent_burden_30_plus_pct": "Rent burden 30%+",
    "same_house_1y_pct": "Same house 1yr",
    "moved_last_year_pct": "Moved last year",
    "moved_different_state_pct": "Moved diff state",
    "renter_share_pct": "Renter share",
    "qcew_total_covered_avg_weekly_wage": "QCEW weekly wage",
    "qcew_total_covered_emplvl": "QCEW employment",
}

DOMAIN_COLORS = {
    "supply": "accent",
    "capitalization": "green",
    "rent_level": "orange",
    "affordability": "orange",
    "stability": "orange",
    "tenure_mix": "orange",
    "labor": "red",
}

# Spec counts per outcome from credibility_checks_summary.csv grouping.
# FHFA and QCEW outcomes include quarterly timing checks (+2 each).
QUARTERLY_OUTCOMES = {"index_sa_mean", "qcew_total_covered_emplvl",
                      "qcew_total_covered_avg_weekly_wage"}
BASE_SPEC_COUNT = 9


def _round(v: float, n: int = 1) -> float | None:
    """Round a value, returning None if not finite."""
    if pd.isna(v):
        return None
    return round(float(v), n)


def _is_valid_pre_mean(outcome: str, value: float) -> bool:
    """Detect obviously broken treated pre-policy means."""
    if pd.isna(value):
        return False
    # Percentage outcomes should be between 0 and 100
    pct_outcomes = {
        "rent_burden_30_plus_pct", "renter_share_pct",
        "same_house_1y_pct", "moved_last_year_pct",
        "moved_different_state_pct",
        "permits_units_multifamily_share",
    }
    if outcome in pct_outcomes and (value < 0 or value > 100):
        return False
    return True


def _read_coef_csv(outcome: str) -> pd.DataFrame | None:
    """Read the baseline coefficient CSV for an outcome, if it exists."""
    path = TABLES / f"pretrend_coefficients_{outcome}_baseline.csv"
    if not path.exists():
        return None
    return pd.read_csv(path)


def _extract_uncertainty(coef: pd.DataFrame, pre_mean: float) -> dict:
    """Extract per-outcome uncertainty from a coefficient table.

    Returns uncertainty for the last post-treatment coefficient (largest
    event time) and a summary across all post coefficients.  CIs are
    expressed both in raw units and as % of pre-policy mean.
    """
    # Handle both old and new column names for permutation null band
    perm_lo = ("perm_null_q025" if "perm_null_q025" in coef.columns
               else "ci_low_resampled" if "ci_low_resampled" in coef.columns
               else None)
    perm_hi = ("perm_null_q975" if "perm_null_q975" in coef.columns
               else "ci_high_resampled" if "ci_high_resampled" in coef.columns
               else None)

    post = coef[coef["event_time"] >= 0].copy()
    if post.empty:
        return {}

    last = post.loc[post["event_time"].idxmax()]
    can_pct = pd.notna(pre_mean) and pre_mean != 0

    out: dict = {
        "lastPostEventTime": int(last["event_time"]),
        "lastPostCoef": _round(last["coef"], 2),
        "lastPostCILow": _round(last["ci_low"], 2),
        "lastPostCIHigh": _round(last["ci_high"], 2),
        "lastPostPConv": _round(last["p_value"], 4),
        "inferMethod": last.get("infer_method", "conventional_hc1"),
    }

    # Permutation p-value for last post coefficient
    if pd.notna(last.get("p_value_resampled")):
        out["lastPostPPerm"] = _round(last["p_value_resampled"], 4)

    # Permutation null band for last post coefficient
    if perm_lo and pd.notna(last.get(perm_lo)):
        out["lastPostPermNullLow"] = _round(last[perm_lo], 2)
    if perm_hi and pd.notna(last.get(perm_hi)):
        out["lastPostPermNullHigh"] = _round(last[perm_hi], 2)

    # Express CI as % of pre-mean for tooltip use
    if can_pct:
        out["lastPostCILowPct"] = _round(last["ci_low"] / pre_mean * 100)
        out["lastPostCIHighPct"] = _round(last["ci_high"] / pre_mean * 100)

    # Average permutation p across post coefficients
    if "p_value_resampled" in post.columns:
        perm_ps = pd.to_numeric(post["p_value_resampled"], errors="coerce").dropna()
        if not perm_ps.empty:
            out["avgPostPPerm"] = _round(perm_ps.mean(), 3)

    return out


def build_cross_outcome(summary: pd.DataFrame,
                        profiles: pd.DataFrame) -> dict:
    """Build the cross-outcome bar chart data with uncertainty."""
    summary = summary.set_index("outcome")
    treated = profiles[profiles["analysis_role"] == "core_treated"]

    chart_labels, values, domains, domain_colors = [], [], [], []
    pre_means, post_shifts = [], []
    uncertainty = []

    for outcome in OUTCOME_ORDER:
        if outcome not in summary.index:
            continue
        row = summary.loc[outcome]
        pre_mean = row["treated_pre_policy_mean"]
        pct = row["avg_post_pct_of_pre_policy_mean"]

        # Fix broken pre-means using profile fallback
        if not _is_valid_pre_mean(outcome, pre_mean):
            if outcome in treated.columns:
                fallback = treated[outcome].mean()
                if pd.notna(fallback) and fallback != 0:
                    pre_mean = fallback
                    pct = row["avg_post_coef"] / pre_mean * 100
                else:
                    pct = None
            else:
                pct = None

        chart_labels.append(CHART_LABELS.get(outcome, row["display_name"]))
        values.append(_round(pct))
        domains.append(row["domain"])
        domain_colors.append(DOMAIN_COLORS.get(row["domain"], "muted"))
        pre_means.append(_round(pre_mean, 1))
        post_shifts.append(_round(row["avg_post_coef"], 1))

        # Extract uncertainty from coefficient CSV
        coef = _read_coef_csv(outcome)
        if coef is not None:
            uncertainty.append(_extract_uncertainty(coef, pre_mean))
        else:
            uncertainty.append({})

    return {
        "labels": chart_labels,
        "values": values,
        "domains": domains,
        "domainColors": domain_colors,
        "preMeans": pre_means,
        "postShifts": post_shifts,
        "uncertainty": uncertainty,
    }


def build_state_contrasts(state_effects: pd.DataFrame) -> dict:
    """Build CA vs OR comparison chart data."""
    # Outcomes that appear in the state-contrast chart (subset)
    contrast_outcomes = [
        "permits_units_total", "permits_units_5plus",
        "permits_units_multifamily_share", "index_sa_mean",
        "DP04_0134E", "rent_burden_30_plus_pct",
        "qcew_total_covered_emplvl", "qcew_total_covered_avg_weekly_wage",
    ]
    contrast_labels = [
        "Total permits", "5+ permits", "MF share", "FHFA HPI",
        "Median rent", "Burden 30%+", "QCEW empl", "QCEW wage",
    ]

    ca = state_effects[state_effects["state_abbr"] == "CA"].set_index("outcome")
    or_ = state_effects[state_effects["state_abbr"] == "OR"].set_index("outcome")

    ca_vals, or_vals = [], []
    for outcome in contrast_outcomes:
        ca_vals.append(_round(ca.loc[outcome, "avg_post_pct_of_pre_policy_mean"])
                       if outcome in ca.index else None)
        or_vals.append(_round(or_.loc[outcome, "avg_post_pct_of_pre_policy_mean"])
                       if outcome in or_.index else None)

    return {
        "labels": contrast_labels,
        "california": ca_vals,
        "oregon": or_vals,
    }


def build_state_profiles(profiles: pd.DataFrame) -> dict:
    """Build the pre-policy state profile chart data."""
    # Sort treated states first, then donors alphabetically (matches site)
    profiles = profiles.copy()
    profiles["_sort"] = profiles["analysis_role"].map(
        {"core_treated": 0}).fillna(1)
    profiles = profiles.sort_values(["_sort", "state_abbr"])
    return {
        "states": profiles["state_abbr"].tolist(),
        "permitsPerRenter": [_round(v) for v in
                             profiles["permits_per_1000_renter_households"]],
        "treated": profiles["analysis_role"].eq("core_treated").tolist(),
    }


def build_credibility() -> dict:
    """Build spec-count chart data."""
    labels = [
        "Permits (total)", "Permits (5+)", "MF share", "FHFA HPI",
        "Median rent", "Rent burden", "Same house", "Moved last yr",
        "Moved diff st", "Renter share", "QCEW empl", "QCEW wage",
    ]
    specs = []
    for outcome in OUTCOME_ORDER:
        count = BASE_SPEC_COUNT + (2 if outcome in QUARTERLY_OUTCOMES else 0)
        specs.append(count)
    return {"labels": labels, "specs": specs}


def build_coverage() -> dict:
    """Build coverage-window chart data (static metadata)."""
    return {
        "labels": ["BPS (permits)", "FHFA HPI", "ACS (2015-19)",
                    "ACS (2021-24)", "QCEW"],
        "ranges": [[2009.5, 2024.5], [2009.5, 2025.5],
                    [2014.5, 2019.5], [2020.5, 2024.5], [2013.5, 2024.5]],
    }


def main() -> None:
    # Read pipeline outputs
    summary_path = TABLES / "domain_comparison_summary.csv"
    profiles_path = TABLES / "prepolicy_state_profiles.csv"
    state_effects_path = TABLES / "state_specific_effect_summary.csv"

    missing = [p for p in [summary_path, profiles_path, state_effects_path]
               if not p.exists()]
    if missing:
        print(f"Missing required files: {missing}", file=sys.stderr)
        print("Run the baseline pipeline first.", file=sys.stderr)
        sys.exit(1)

    summary = pd.read_csv(summary_path)
    profiles = pd.read_csv(profiles_path)
    state_effects = pd.read_csv(state_effects_path)

    data = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "crossOutcome": build_cross_outcome(summary, profiles),
        "stateContrasts": build_state_contrasts(state_effects),
        "stateProfiles": build_state_profiles(profiles),
        "credibility": build_credibility(),
        "coverage": build_coverage(),
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    js = "/* Auto-generated by scripts/export_site_data.py — do not edit. */\n"
    js += "window.SITE_DATA = " + json.dumps(data, indent=2) + ";\n"
    OUT.write_text(js, encoding="utf-8")
    print(f"Wrote {OUT} ({len(js):,} bytes)")


if __name__ == "__main__":
    main()
