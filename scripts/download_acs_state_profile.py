from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from rent_control_public.acs import ACS_BASE, add_computed_columns, fetch_state_profile
from rent_control_public.constants import get_acs_profile_variables_for_year


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("data/raw/acs"))
    parser.add_argument("--strict", action="store_true", help="Fail on the first unsupported ACS year instead of skipping it.")
    args = parser.parse_args()

    rows = []
    args.output_dir.mkdir(parents=True, exist_ok=True)

    for year in range(args.start_year, args.end_year + 1):
        try:
            df = fetch_state_profile(year, get_acs_profile_variables_for_year(year))
        except requests.HTTPError as exc:
            if args.strict:
                raise
            print(f"skipped {year}: ACS profile variables unavailable ({exc})")
            continue
        df = add_computed_columns(df)
        raw_path = args.output_dir / f"acs_state_profile_{year}.csv"
        df.to_csv(raw_path, index=False)
        rows.append(df)
        print(f"downloaded {raw_path}")

    if not rows:
        raise RuntimeError("No ACS state profile years were downloaded. Try a later start year or use --strict for debugging.")

    panel = pd.concat(rows, ignore_index=True)
    processed_path = Path("data/processed/acs_state_profile_panel.csv")
    if processed_path.exists():
        existing = pd.read_csv(processed_path, dtype={"state": str})
        panel = pd.concat([existing, panel], ignore_index=True)
        dedupe_cols = [col for col in ["NAME", "state", "year"] if col in panel.columns]
        if dedupe_cols:
            panel = panel.sort_values(dedupe_cols).drop_duplicates(subset=dedupe_cols, keep="last")
    panel.to_csv(processed_path, index=False)
    print(f"wrote {processed_path}")


if __name__ == "__main__":
    main()
