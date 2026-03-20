from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest


pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = ROOT / "scripts" / "build_core_state_panel.py"
FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"

PROCESSED_FIXTURES = [
    "policy_panel_state_annual_2010_2026.csv",
    "policy_panel_state_quarterly_2010_2026.csv",
    "bps_state_annual_2010_2024.csv",
    "fhfa_state_annual_purchase_only_2010_2025.csv",
    "fhfa_state_quarterly_purchase_only_2010_2025.csv",
    "acs_state_profile_panel.csv",
    "qcew_state_annual_core.csv",
    "qcew_state_quarterly_core.csv",
]


def _seed_processed_tree(tmp_path: Path) -> Path:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ROOT / "config" / "state_metadata.csv", config_dir / "state_metadata.csv")

    processed_dir = tmp_path / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    for name in PROCESSED_FIXTURES:
        shutil.copyfile(FIXTURES_DIR / "processed" / name, processed_dir / name)
    return processed_dir


def _run_builder(tmp_path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-B", str(SCRIPT_PATH), "--root", str(tmp_path), *args],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def test_build_core_state_panel_non_strict_writes_manifest_for_missing_domain(tmp_path: Path) -> None:
    processed_dir = _seed_processed_tree(tmp_path)
    (processed_dir / "acs_state_profile_panel.csv").unlink()

    result = _run_builder(tmp_path)
    assert result.returncode == 0, result.stderr

    manifest = pd.read_csv(processed_dir / "data_coverage_manifest.csv")
    acs_row = manifest[(manifest["panel_frequency"] == "annual") & (manifest["domain"] == "acs")].iloc[0]

    assert bool(acs_row["file_present"]) is False
    assert bool(acs_row["ready_for_baseline"]) is False
    assert (processed_dir / "core_state_panel_annual.csv").exists()
    assert (processed_dir / "core_state_panel_quarterly.csv").exists()


def test_build_core_state_panel_strict_fails_when_required_domain_missing(tmp_path: Path) -> None:
    processed_dir = _seed_processed_tree(tmp_path)
    (processed_dir / "acs_state_profile_panel.csv").unlink()

    result = _run_builder(tmp_path, "--strict")

    assert result.returncode != 0
    assert "Strict panel build blocked" in (result.stderr or result.stdout)
    assert (processed_dir / "data_coverage_manifest.csv").exists()


def test_build_core_state_panel_strict_writes_expected_fixture_outputs(tmp_path: Path) -> None:
    processed_dir = _seed_processed_tree(tmp_path)

    result = _run_builder(tmp_path, "--strict")
    assert result.returncode == 0, result.stderr

    annual = pd.read_csv(processed_dir / "core_state_panel_annual.csv", dtype={"state_fips": str})
    quarterly = pd.read_csv(processed_dir / "core_state_panel_quarterly.csv", dtype={"state_fips": str})
    manifest = pd.read_csv(processed_dir / "data_coverage_manifest.csv")

    annual_expected = {
        "DP04_0134E",
        "median_household_income",
        "permits_units_total",
        "qcew_total_covered_emplvl",
        "qcew_private_emplvl",
        "qcew_total_covered_avg_weekly_wage",
    }
    quarterly_expected = {
        "index_sa",
        "qcew_total_covered_emplvl",
        "qcew_private_emplvl",
    }

    assert annual_expected.issubset(annual.columns)
    assert quarterly_expected.issubset(quarterly.columns)
    assert annual["qcew_total_covered_emplvl"].notna().all()
    assert quarterly["qcew_total_covered_emplvl"].notna().all()
    assert manifest["ready_for_baseline"].all()
