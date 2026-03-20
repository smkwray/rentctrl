from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = ROOT / "config" / "city_audit_status_source.csv"
DEFAULT_OUTPUT = ROOT / "results" / "tables" / "city_audit_status.csv"


def build_city_audit_status(source: pd.DataFrame) -> pd.DataFrame:
    required = {
        "city",
        "public_status",
        "current_artifact_path",
        "current_design_type",
        "primary_join_key",
        "current_decision",
        "next_backend_task",
    }
    missing = sorted(required.difference(source.columns))
    if missing:
        raise ValueError(f"Source file is missing required columns: {', '.join(missing)}")

    out = source.copy()
    out = out.rename(
        columns={
            "public_status": "status",
            "current_artifact_path": "audit_artifact",
            "current_design_type": "design_type",
            "current_decision": "decision",
        }
    )
    out = out[
        [
            "city",
            "status",
            "design_type",
            "audit_artifact",
            "primary_join_key",
            "decision",
            "next_backend_task",
        ]
    ].copy()
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the public city audit status table from the checked-in source file.")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    source = pd.read_csv(args.source)
    out = build_city_audit_status(source)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.output, index=False)
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
