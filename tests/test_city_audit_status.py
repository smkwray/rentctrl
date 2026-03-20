from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "generate_city_audit_status.py"
SPEC = importlib.util.spec_from_file_location("generate_city_audit_status", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_build_city_audit_status_updates_current_dc_and_weho_artifacts() -> None:
    source = pd.read_csv(Path("config/city_audit_status_source.csv"))

    out = MODULE.build_city_audit_status(source)

    assert list(out.columns) == [
        "city",
        "status",
        "design_type",
        "audit_artifact",
        "primary_join_key",
        "decision",
        "next_backend_task",
    ]

    dc = out[out["city"] == "Washington, DC"].iloc[0]
    weho = out[out["city"] == "West Hollywood"].iloc[0]

    assert dc["status"] == "pilot"
    assert dc["audit_artifact"] == "results/dc_local_pilot.md"
    assert dc["design_type"] == "parcel_profile"
    assert dc["primary_join_key"] == "SSL"
    assert weho["status"] == "package"
    assert weho["audit_artifact"] == "results/weho_second_stage_linkage.md"
    assert weho["design_type"] == "protected_stock_linkage"


def test_generate_city_audit_status_matches_source_row_count(tmp_path: Path) -> None:
    source = pd.read_csv(Path("config/city_audit_status_source.csv"))
    output = tmp_path / "city_audit_status.csv"

    out = MODULE.build_city_audit_status(source)
    out.to_csv(output, index=False)

    written = pd.read_csv(output)
    assert len(written) == len(source)
    assert set(written["city"]) == set(source["city"])
