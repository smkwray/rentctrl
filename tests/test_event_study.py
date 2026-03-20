from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from rent_control_public.event_study import EventStudyResult, extract_event_study_coefficients, fit_twfe_event_study
from rent_control_public.pipeline import (
    coverage_manifest_path,
    load_coverage_manifest,
    manifest_domain_status,
    parse_domain_list,
    require_manifest_readiness,
)


def test_parse_domain_list_uses_defaults_when_missing() -> None:
    default = ["policy", "fhfa", "qcew"]

    assert parse_domain_list(None, default) == default
    assert parse_domain_list(" policy , qcew ", default) == ["policy", "qcew"]
    assert parse_domain_list("  ", default) == default


def test_coverage_manifest_helpers_round_trip(tmp_path: Path) -> None:
    root = tmp_path
    manifest_path = coverage_manifest_path(root)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    manifest = pd.DataFrame(
        [
            {
                "panel_frequency": "annual",
                "domain": "policy",
                "source_path": "policy.csv",
                "file_present": True,
                "required_in_strict_mode": True,
                "row_count": 3,
                "min_year": 2010,
                "max_year": 2024,
                "required_columns_ok": True,
                "join_keys_ok": True,
                "headline_non_missing_share": 1.0,
                "ready_for_baseline": True,
            },
            {
                "panel_frequency": "quarterly",
                "domain": "fhfa",
                "source_path": "fhfa.csv",
                "file_present": True,
                "required_in_strict_mode": True,
                "row_count": 8,
                "min_year": 2010,
                "max_year": 2024,
                "required_columns_ok": True,
                "join_keys_ok": True,
                "headline_non_missing_share": 1.0,
                "ready_for_baseline": True,
            },
            {
                "panel_frequency": "quarterly",
                "domain": "qcew",
                "source_path": "qcew.csv",
                "file_present": True,
                "required_in_strict_mode": True,
                "row_count": 8,
                "min_year": 2014,
                "max_year": 2024,
                "required_columns_ok": True,
                "join_keys_ok": True,
                "headline_non_missing_share": 0.92,
                "ready_for_baseline": False,
            },
        ]
    )
    manifest.to_csv(manifest_path, index=False)

    loaded = load_coverage_manifest(root)
    assert len(loaded) == 3
    assert manifest_domain_status(loaded, panel_frequency="quarterly", domains=["fhfa", "qcew"]) == {
        "fhfa": True,
        "qcew": False,
    }

    with pytest.raises(RuntimeError, match="domains not ready for baseline"):
        require_manifest_readiness(root, annual_domains=["policy"], quarterly_domains=["fhfa", "qcew"])


def test_extract_event_study_coefficients_orders_terms() -> None:
    result = EventStudyResult(
        formula="y ~ evt_m2 + evt_p1",
        model_summary="summary",
        coefficient_table=pd.DataFrame(
            [
                {"term": "Intercept", "coef": 1.0, "std_err": 0.1, "t": 10.0, "p_value": 0.0},
                {"term": "evt_p1", "coef": 0.3, "std_err": 0.2, "t": 1.5, "p_value": 0.2},
                {"term": "evt_m2", "coef": -0.4, "std_err": 0.1, "t": -4.0, "p_value": 0.01},
            ]
        ),
    )

    out = extract_event_study_coefficients(result)

    assert out["event_time"].tolist() == [-2, 1]
    assert out["ci_low"].iloc[0] < out["coef"].iloc[0] < out["ci_high"].iloc[0]


def test_fit_twfe_event_study_adds_resampled_columns() -> None:
    sample = pd.DataFrame(
        {
            "state_name": ["CA", "CA", "CA", "OR", "OR", "OR", "AZ", "AZ", "AZ", "NV", "NV", "NV"],
            "year": [2018, 2019, 2020] * 4,
            "outcome": [10, 11, 13, 8, 9, 11, 7, 7.5, 8, 6.5, 7.0, 7.2],
            "evt_m1": [1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0],
            "evt_p0": [0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0],
            "evt_p1": [0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0],
            "event_time_int": [-1, 0, 1, -1, 0, 1, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA],
        }
    )

    result = fit_twfe_event_study(
        sample,
        outcome="outcome",
        unit_col="state_name",
        time_col="year",
        event_time_col="event_time_int",
        resampled_inference="permutation",
        resample_count=8,
        random_seed=11,
    )

    out = extract_event_study_coefficients(result)

    assert {"infer_method", "p_value_resampled", "ci_low_resampled", "ci_high_resampled", "resample_count"}.issubset(out.columns)
    assert out["resample_count"].max() >= 0
