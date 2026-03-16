from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from rent_control_public.qcew import (
    annualize_core,
    fetch_area_slice,
    filter_state_private_total,
    filter_state_total_covered,
    reshape_qcew_core,
    state_area_code,
)

QCEW_NUMERIC_COLUMNS = [
    "qtrly_estabs",
    "month1_emplvl",
    "month2_emplvl",
    "month3_emplvl",
    "total_qtrly_wages",
    "avg_wkly_wage",
]


def load_state_rows() -> list[tuple[str, str]]:
    state_meta = pd.read_csv(ROOT / "config" / "state_metadata.csv", dtype={"state_fips": str})
    return list(state_meta[["state_abbr", "state_fips"]].itertuples(index=False, name=None))
def load_or_fetch_area_slice(raw_path: Path, *, year: int, quarter: int, area_code: str, skip_existing: bool) -> tuple[pd.DataFrame, bool]:
    if skip_existing and raw_path.exists():
        return pd.read_csv(raw_path), False
    df = fetch_area_slice(year, quarter, area_code)
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(raw_path, index=False)
    return df, True


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("data/raw/qcew"))
    parser.add_argument("--skip-existing", action="store_true", help="Reuse existing raw quarter files when present.")
    parser.add_argument("--strict", action="store_true", help="Fail on the first unsupported QCEW year instead of skipping it.")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    processed_dir = Path("data/processed")
    processed_dir.mkdir(parents=True, exist_ok=True)

    total_rows = []
    private_rows = []

    for state_abbr, state_fips in load_state_rows():
        area_code = state_area_code(state_fips)
        for year in range(args.start_year, args.end_year + 1):
            for quarter in [1, 2, 3, 4]:
                raw_path = args.output_dir / f"qcew_{state_abbr}_{year}Q{quarter}.csv"
                try:
                    df, downloaded = load_or_fetch_area_slice(
                        raw_path,
                        year=year,
                        quarter=quarter,
                        area_code=area_code,
                        skip_existing=args.skip_existing,
                    )
                except requests.HTTPError as exc:
                    if args.strict:
                        raise
                    print(f"skipped {state_abbr} {year}Q{quarter}: QCEW area slice unavailable ({exc})")
                    continue
                for num_col in QCEW_NUMERIC_COLUMNS:
                    if num_col in df.columns:
                        df[num_col] = pd.to_numeric(df[num_col], errors="coerce")

                total = filter_state_total_covered(df)
                total["state_abbr"] = state_abbr
                total["state_fips"] = state_fips
                total["year"] = year
                total["quarter"] = quarter
                total_rows.append(total)

                private = filter_state_private_total(df)
                private["state_abbr"] = state_abbr
                private["state_fips"] = state_fips
                private["year"] = year
                private["quarter"] = quarter
                private_rows.append(private)

                action = "downloaded" if downloaded else "reused"
                print(f"{action} {raw_path}")

    if not total_rows or not private_rows:
        raise RuntimeError("No QCEW slices were processed. Try a later start year or use --strict for debugging.")

    total_panel = pd.concat(total_rows, ignore_index=True)
    private_panel = pd.concat(private_rows, ignore_index=True)

    total_quarterly_path = processed_dir / "qcew_state_quarterly_total_covered.csv"
    private_quarterly_path = processed_dir / "qcew_state_quarterly_private.csv"
    total_panel.to_csv(total_quarterly_path, index=False)
    private_panel.to_csv(private_quarterly_path, index=False)

    if not total_panel.empty and not private_panel.empty:
        total_core = reshape_qcew_core(total_panel, prefix="qcew_total_covered")
        private_core = reshape_qcew_core(private_panel, prefix="qcew_private")
        core_quarterly = total_core.merge(private_core, on=["state_abbr", "state_fips", "year", "quarter"], how="outer")
        core_quarterly.to_csv(processed_dir / "qcew_state_quarterly_core.csv", index=False)

    if not total_panel.empty:
        annual_total = (
            total_panel.groupby(["state_abbr", "state_fips", "year"], as_index=False)
            .agg(
                annual_avg_estabs=("qtrly_estabs", "mean"),
                annual_avg_emplvl=("month3_emplvl", "mean"),
                total_annual_wages=("total_qtrly_wages", "sum"),
                annual_avg_wkly_wage=("avg_wkly_wage", "mean"),
            )
        )
        annual_total.to_csv(processed_dir / "qcew_state_annual_total_covered.csv", index=False)

    if not private_panel.empty:
        annual_private = (
            private_panel.groupby(["state_abbr", "state_fips", "year"], as_index=False)
            .agg(
                annual_avg_estabs=("qtrly_estabs", "mean"),
                annual_avg_emplvl=("month3_emplvl", "mean"),
                total_annual_wages=("total_qtrly_wages", "sum"),
                annual_avg_wkly_wage=("avg_wkly_wage", "mean"),
            )
        )
        annual_private.to_csv(processed_dir / "qcew_state_annual_private.csv", index=False)

    if not total_panel.empty and not private_panel.empty:
        annual_core = annualize_core(core_quarterly)
        annual_core.to_csv(processed_dir / "qcew_state_annual_core.csv", index=False)

    print("wrote cleaned QCEW quarterly and annual outputs")


if __name__ == "__main__":
    main()
