from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from rent_control_public.los_angeles import (
    build_property_activity_pilot,
    case_type_counts,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a bounded Los Angeles LAHD Property Activity pilot.")
    parser.add_argument("--street-name", default="MAIN")
    parser.add_argument("--street-no", default="")
    parser.add_argument("--max-properties", type=int, default=5)
    return parser


def slugify(street_name: str, street_no: str) -> str:
    base = "_".join(part for part in [street_no.strip(), street_name.strip()] if part)
    return "_".join(base.lower().split())


def main() -> None:
    args = build_parser().parse_args()
    processed_dir = ROOT / "data" / "processed"
    results_dir = ROOT / "results" / "tables"
    processed_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)

    property_df, case_df, summary_df = build_property_activity_pilot(
        street_no=args.street_no,
        street_name=args.street_name,
        max_properties=args.max_properties,
    )
    case_type_df = case_type_counts(case_df)
    tag = slugify(args.street_name, args.street_no)

    property_path = processed_dir / f"los_angeles_property_activity_{tag}_properties.csv"
    case_path = processed_dir / f"los_angeles_property_activity_{tag}_cases.csv"
    summary_path = results_dir / f"los_angeles_property_activity_{tag}_summary.csv"
    case_type_path = results_dir / f"los_angeles_property_activity_{tag}_case_type_counts.csv"

    property_df.to_csv(property_path, index=False)
    case_df.to_csv(case_path, index=False)
    summary_df.to_csv(summary_path, index=False)
    case_type_df.to_csv(case_type_path, index=False)

    print(f"wrote {property_path}")
    print(f"wrote {case_path}")
    print(f"wrote {summary_path}")
    print(f"wrote {case_type_path}")
    print(summary_df.to_string(index=False))


if __name__ == "__main__":
    main()
