from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "run_free_data_extensions.py"
SPEC = importlib.util.spec_from_file_location("run_free_data_extensions", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_summarize_state_specific_effects_handles_zero_pre_policy_mean(tmp_path: Path) -> None:
    panel = pd.DataFrame(
        {
            "analysis_role": ["core_treated", "core_treated"],
            "policy_active_preferred": [0, 0],
            "state_abbr": ["CA", "OR"],
            "qcew_total_covered_emplvl": [0.0, 10.0],
        }
    )
    coef = pd.DataFrame(
        {
            "state_abbr": ["CA", "CA", "OR", "OR"],
            "event_time": [-2, 0, -2, 0],
            "coef": [0.0, 5.0, 1.0, 2.0],
        }
    )
    coef.to_csv(tmp_path / "pretrend_coefficients_qcew_total_covered_emplvl_state_interactions.csv", index=False)

    out = MODULE.summarize_state_specific_effects(panel, tmp_path)

    ca_row = out[out["state_abbr"] == "CA"].iloc[0]
    or_row = out[out["state_abbr"] == "OR"].iloc[0]

    assert pd.isna(ca_row["avg_post_pct_of_pre_policy_mean"])
    assert or_row["avg_post_pct_of_pre_policy_mean"] == 20.0


def test_load_panel_uses_alias_renter_households_when_raw_column_missing(tmp_path: Path) -> None:
    data_dir = tmp_path / "data" / "processed"
    data_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "state_fips": ["06"],
            "permits_units_total": [50],
            "permits_units_5plus": [25],
            "renter_households": [1000],
        }
    ).to_csv(data_dir / "core_state_panel_annual.csv", index=False)

    original_root = MODULE.ROOT
    try:
        MODULE.ROOT = tmp_path
        out = MODULE.load_panel()
    finally:
        MODULE.ROOT = original_root

    assert out.loc[0, "permits_per_1000_renter_households"] == 50.0
    assert out.loc[0, "permits_5plus_per_1000_renter_households"] == 25.0
