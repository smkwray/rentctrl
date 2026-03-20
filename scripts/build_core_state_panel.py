from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import sys

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
MANIFEST_FILENAME = "data_coverage_manifest.csv"
sys.path.insert(0, str(ROOT / "src"))

from rent_control_public.pipeline import (  # noqa: E402
    DEFAULT_ANNUAL_REQUIRED_DOMAINS,
    DEFAULT_QUARTERLY_REQUIRED_DOMAINS,
    parse_domain_list,
)


@dataclass(frozen=True)
class SourceCandidate:
    path: str
    required_columns: tuple[str, ...]
    join_keys: tuple[str, ...]
    headline_columns: tuple[str, ...]


SOURCE_CANDIDATES: dict[str, dict[str, list[SourceCandidate]]] = {
    "annual": {
        "policy": [
            SourceCandidate(
                path="policy_panel_state_annual_2010_2026.csv",
                required_columns=(
                    "state_name",
                    "state_abbr",
                    "state_fips",
                    "analysis_role",
                    "ever_treated",
                    "year",
                    "policy_active_preferred",
                    "policy_active_alternative",
                    "event_time_years_preferred",
                    "event_time_years_alternative",
                ),
                join_keys=("state_name", "state_abbr", "state_fips", "year"),
                headline_columns=("policy_active_preferred", "event_time_years_preferred"),
            )
        ],
        "bps": [
            SourceCandidate(
                path="bps_state_annual_2010_2024.csv",
                required_columns=(
                    "state_name",
                    "state_fips",
                    "year",
                    "permits_units_total",
                    "u5p_units",
                    "permits_units_multifamily",
                    "permits_units_multifamily_share",
                ),
                join_keys=("state_name", "state_fips", "year"),
                headline_columns=("permits_units_total", "permits_units_multifamily_share"),
            )
        ],
        "fhfa": [
            SourceCandidate(
                path="fhfa_state_annual_purchase_only_2010_2025.csv",
                required_columns=("state_name", "year", "index_sa_mean", "index_nsa_mean"),
                join_keys=("state_name", "year"),
                headline_columns=("index_sa_mean", "index_nsa_mean"),
            )
        ],
        "acs": [
            SourceCandidate(
                path="acs_state_profile_panel.csv",
                required_columns=(
                    "NAME",
                    "state",
                    "year",
                    "same_house_1y_pct",
                    "moved_last_year_pct",
                    "moved_different_state_pct",
                    "DP03_0062E",
                    "DP04_0047E",
                    "DP04_0047PE",
                    "DP04_0134E",
                    "rent_burden_30_plus_pct",
                ),
                join_keys=("NAME", "state", "year"),
                headline_columns=("DP04_0134E", "rent_burden_30_plus_pct", "same_house_1y_pct", "DP04_0047PE"),
            )
        ],
        "qcew": [
            SourceCandidate(
                path="qcew_state_annual_core.csv",
                required_columns=(
                    "state_abbr",
                    "state_fips",
                    "year",
                    "qcew_total_covered_emplvl",
                    "qcew_total_covered_avg_wkly_wage",
                ),
                join_keys=("state_abbr", "state_fips", "year"),
                headline_columns=("qcew_total_covered_emplvl", "qcew_total_covered_avg_wkly_wage"),
            ),
            SourceCandidate(
                path="qcew_state_annual_total_covered.csv",
                required_columns=(
                    "state_abbr",
                    "state_fips",
                    "year",
                    "annual_avg_emplvl",
                    "annual_avg_wkly_wage",
                ),
                join_keys=("state_abbr", "state_fips", "year"),
                headline_columns=("annual_avg_emplvl", "annual_avg_wkly_wage"),
            ),
        ],
    },
    "quarterly": {
        "policy": [
            SourceCandidate(
                path="policy_panel_state_quarterly_2010_2026.csv",
                required_columns=(
                    "state_name",
                    "state_abbr",
                    "state_fips",
                    "analysis_role",
                    "ever_treated",
                    "year",
                    "quarter",
                    "calendar_period",
                    "policy_active_preferred",
                    "policy_active_alternative",
                    "preferred_treat_period",
                    "alternative_treat_period",
                    "event_time_quarters_preferred",
                    "event_time_quarters_alternative",
                ),
                join_keys=("state_abbr", "state_fips", "year", "quarter"),
                headline_columns=("policy_active_preferred", "event_time_quarters_preferred"),
            )
        ],
        "fhfa": [
            SourceCandidate(
                path="fhfa_state_quarterly_purchase_only_2010_2025.csv",
                required_columns=("fhfa_place_id", "yr", "quarter", "index_sa", "index_nsa"),
                join_keys=("fhfa_place_id", "yr", "quarter"),
                headline_columns=("index_sa", "index_nsa"),
            )
        ],
        "qcew": [
            SourceCandidate(
                path="qcew_state_quarterly_core.csv",
                required_columns=(
                    "state_abbr",
                    "state_fips",
                    "year",
                    "quarter",
                    "qcew_total_covered_emplvl",
                    "qcew_total_covered_avg_wkly_wage",
                ),
                join_keys=("state_abbr", "state_fips", "year", "quarter"),
                headline_columns=("qcew_total_covered_emplvl", "qcew_total_covered_avg_wkly_wage"),
            )
        ],
    },
}


def _read_csv_if_exists(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    return pd.read_csv(path, dtype={"state_fips": str})


def _coerce_state_fips(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "state_fips" in out.columns:
        out["state_fips"] = out["state_fips"].astype(str).str.zfill(2)
    return out


def _year_range(df: pd.DataFrame) -> tuple[object, object]:
    if df.empty or "year" not in df.columns:
        return pd.NA, pd.NA
    year_values = pd.to_numeric(df["year"], errors="coerce").dropna()
    if year_values.empty:
        return pd.NA, pd.NA
    return int(year_values.min()), int(year_values.max())


def _headline_non_missing_share(df: pd.DataFrame, columns: tuple[str, ...]) -> float:
    shares: list[float] = []
    for col in columns:
        if col not in df.columns:
            shares.append(0.0)
            continue
        shares.append(float(pd.to_numeric(df[col], errors="coerce").notna().mean()))
    return float(sum(shares) / len(shares)) if shares else 0.0


def _join_keys_ok(df: pd.DataFrame, join_keys: tuple[str, ...]) -> bool:
    if not set(join_keys).issubset(df.columns):
        return False
    if df.empty:
        return False
    return bool(df.loc[:, list(join_keys)].notna().all(axis=None))


def assess_domain(
    processed_dir: Path,
    *,
    panel_frequency: str,
    domain: str,
    required_in_strict_mode: bool,
) -> tuple[dict[str, object], pd.DataFrame | None]:
    candidates = SOURCE_CANDIDATES[panel_frequency][domain]
    selected_df: pd.DataFrame | None = None
    selected_candidate: SourceCandidate | None = None
    selected_path: Path | None = None
    for candidate in candidates:
        candidate_path = processed_dir / candidate.path
        df = _read_csv_if_exists(candidate_path)
        if df is not None:
            selected_df = _coerce_state_fips(df)
            selected_candidate = candidate
            selected_path = candidate_path
            break

    if selected_df is None or selected_candidate is None or selected_path is None:
        assessment = {
            "panel_frequency": panel_frequency,
            "domain": domain,
            "source_path": str((processed_dir / candidates[0].path).relative_to(processed_dir.parents[1])),
            "file_present": False,
            "required_in_strict_mode": required_in_strict_mode,
            "row_count": 0,
            "min_year": pd.NA,
            "max_year": pd.NA,
            "required_columns_ok": False,
            "join_keys_ok": False,
            "headline_non_missing_share": 0.0,
            "ready_for_baseline": False,
        }
        return assessment, None

    min_year, max_year = _year_range(selected_df)
    required_columns_ok = set(selected_candidate.required_columns).issubset(selected_df.columns)
    join_keys_ok = _join_keys_ok(selected_df, selected_candidate.join_keys)
    headline_non_missing_share = _headline_non_missing_share(selected_df, selected_candidate.headline_columns)
    ready_for_baseline = bool(
        len(selected_df) > 0 and required_columns_ok and join_keys_ok and headline_non_missing_share > 0
    )
    assessment = {
        "panel_frequency": panel_frequency,
        "domain": domain,
        "source_path": str(selected_path.relative_to(processed_dir.parents[1])),
        "file_present": True,
        "required_in_strict_mode": required_in_strict_mode,
        "row_count": int(len(selected_df)),
        "min_year": min_year,
        "max_year": max_year,
        "required_columns_ok": required_columns_ok,
        "join_keys_ok": join_keys_ok,
        "headline_non_missing_share": headline_non_missing_share,
        "ready_for_baseline": ready_for_baseline,
    }
    return assessment, selected_df


def assess_sources(
    root: Path,
    *,
    annual_required_domains: list[str],
    quarterly_required_domains: list[str],
) -> tuple[pd.DataFrame, dict[str, dict[str, pd.DataFrame | None]]]:
    processed_dir = root / "data" / "processed"
    assessments: list[dict[str, object]] = []
    frames: dict[str, dict[str, pd.DataFrame | None]] = {"annual": {}, "quarterly": {}}

    for frequency, required_domains in [
        ("annual", annual_required_domains),
        ("quarterly", quarterly_required_domains),
    ]:
        for domain in SOURCE_CANDIDATES[frequency]:
            assessment, df = assess_domain(
                processed_dir,
                panel_frequency=frequency,
                domain=domain,
                required_in_strict_mode=domain in required_domains,
            )
            assessments.append(assessment)
            frames[frequency][domain] = df

    manifest = pd.DataFrame(assessments).sort_values(["panel_frequency", "domain"]).reset_index(drop=True)
    return manifest, frames


def build_annual_panel(frames: dict[str, pd.DataFrame | None]) -> pd.DataFrame:
    policy = frames["policy"]
    if policy is None:
        raise FileNotFoundError("Missing annual policy panel.")

    panel = policy.copy()
    fhfa = frames["fhfa"]
    if fhfa is not None:
        panel = panel.merge(
            fhfa[["state_name", "year", "index_sa_mean", "index_nsa_mean"]],
            on=["state_name", "year"],
            how="left",
        )

    bps = frames["bps"]
    if bps is not None:
        panel = panel.merge(
            bps[
                [
                    "state_name",
                    "state_fips",
                    "year",
                    "permits_units_total",
                    "u5p_units",
                    "permits_units_multifamily",
                    "permits_units_multifamily_share",
                ]
            ],
            on=["state_name", "state_fips", "year"],
            how="left",
        )

    acs = frames["acs"]
    if acs is not None:
        acs_frame = acs.rename(columns={"NAME": "state_name", "state": "state_fips"}).copy()
        acs_frame["state_fips"] = acs_frame["state_fips"].astype(str).str.zfill(2)
        keep = [
            "state_name",
            "state_fips",
            "year",
            "same_house_1y_pct",
            "moved_last_year_pct",
            "moved_within_us_pct",
            "moved_different_state_pct",
            "median_household_income",
            "renter_households",
            "renter_share_pct",
            "median_gross_rent",
            "DP02_0080PE",
            "DP02_0081PE",
            "DP02_0082PE",
            "DP02_0086PE",
            "DP03_0062E",
            "DP04_0047E",
            "DP04_0047PE",
            "DP04_0134E",
            "DP04_0141PE",
            "DP04_0142PE",
            "rent_burden_30_plus_pct",
        ]
        panel = panel.merge(acs_frame[[c for c in keep if c in acs_frame.columns]], on=["state_name", "state_fips", "year"], how="left")

    qcew = frames["qcew"]
    if qcew is not None:
        panel = panel.merge(qcew, on=["state_abbr", "state_fips", "year"], how="left", suffixes=("", "_qcew"))

    alias_map = {
        "u5p_units": "permits_units_5plus",
        "DP04_0047E": "renter_households",
        "DP03_0062E": "median_household_income",
        "DP04_0047PE": "renter_share_pct",
        "DP04_0134E": "median_gross_rent",
        "qcew_total_covered_avg_wkly_wage": "qcew_total_covered_avg_weekly_wage",
        "qcew_private_avg_wkly_wage": "qcew_private_avg_weekly_wage",
        "annual_avg_emplvl": "qcew_total_covered_emplvl",
        "annual_avg_wkly_wage": "qcew_total_covered_avg_weekly_wage",
        "annual_avg_estabs": "qcew_total_covered_estabs",
        "total_annual_wages": "qcew_total_covered_wages",
    }
    for source_col, alias_col in alias_map.items():
        if source_col in panel.columns and alias_col not in panel.columns:
            panel[alias_col] = panel[source_col]

    return panel


def build_quarterly_panel(root: Path, frames: dict[str, pd.DataFrame | None]) -> pd.DataFrame:
    policy = frames["policy"]
    if policy is None:
        raise FileNotFoundError("Missing quarterly policy panel.")

    panel = policy.copy()
    fhfa = frames["fhfa"]
    if fhfa is not None:
        state_meta = pd.read_csv(root / "config" / "state_metadata.csv", dtype={"state_fips": str})
        selected_state_abbrs = set(state_meta["state_abbr"])
        fhfa_frame = fhfa[fhfa["fhfa_place_id"].isin(selected_state_abbrs)].rename(columns={"fhfa_place_id": "state_abbr", "yr": "year"}).copy()
        panel = panel.merge(
            fhfa_frame[["state_abbr", "year", "quarter", "index_sa", "index_nsa", "year_quarter"]],
            on=["state_abbr", "year", "quarter"],
            how="left",
        )

    qcew = frames["qcew"]
    if qcew is not None:
        qcew_frame = qcew.copy()
        qcew_frame["quarter"] = qcew_frame["quarter"].astype(str).map(
            lambda value: value if value.startswith("Q") else f"Q{value}"
        )
        panel = panel.merge(qcew_frame, on=["state_abbr", "state_fips", "year", "quarter"], how="left")

    quarterly_alias_map = {
        "qcew_total_covered_avg_wkly_wage": "qcew_total_covered_avg_weekly_wage",
        "qcew_private_avg_wkly_wage": "qcew_private_avg_weekly_wage",
    }
    for source_col, alias_col in quarterly_alias_map.items():
        if source_col in panel.columns and alias_col not in panel.columns:
            panel[alias_col] = panel[source_col]

    return panel


def write_manifest(root: Path, manifest: pd.DataFrame) -> Path:
    path = root / "data" / "processed" / MANIFEST_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    manifest.to_csv(path, index=False)
    return path


def enforce_strict_requirements(
    manifest: pd.DataFrame,
    *,
    annual_required_domains: list[str],
    quarterly_required_domains: list[str],
) -> None:
    required_specs = [("annual", domain) for domain in annual_required_domains] + [
        ("quarterly", domain) for domain in quarterly_required_domains
    ]
    failing = []
    for frequency, domain in required_specs:
        row = manifest[(manifest["panel_frequency"] == frequency) & (manifest["domain"] == domain)]
        if row.empty or not bool(row["ready_for_baseline"].iloc[0]):
            failing.append(f"{frequency}:{domain}")
    if failing:
        raise RuntimeError(
            "Strict panel build blocked by missing or incomplete domains: "
            + ", ".join(failing)
            + ". See data/processed/data_coverage_manifest.csv for details."
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build annual and quarterly core statewide panels.")
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--strict", action="store_true", help="Fail if required annual or quarterly domains are not baseline-ready.")
    parser.add_argument(
        "--require-annual-domains",
        default=",".join(DEFAULT_ANNUAL_REQUIRED_DOMAINS),
        help="Comma-separated annual domains that must be baseline-ready in strict mode.",
    )
    parser.add_argument(
        "--require-quarterly-domains",
        default=",".join(DEFAULT_QUARTERLY_REQUIRED_DOMAINS),
        help="Comma-separated quarterly domains that must be baseline-ready in strict mode.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = args.root.resolve()
    annual_required_domains = parse_domain_list(args.require_annual_domains, DEFAULT_ANNUAL_REQUIRED_DOMAINS)
    quarterly_required_domains = parse_domain_list(args.require_quarterly_domains, DEFAULT_QUARTERLY_REQUIRED_DOMAINS)

    manifest, frames = assess_sources(
        root,
        annual_required_domains=annual_required_domains,
        quarterly_required_domains=quarterly_required_domains,
    )
    manifest_path = write_manifest(root, manifest)
    print(f"wrote {manifest_path}")

    if args.strict:
        enforce_strict_requirements(
            manifest,
            annual_required_domains=annual_required_domains,
            quarterly_required_domains=quarterly_required_domains,
        )

    annual_panel = build_annual_panel(frames["annual"])
    quarterly_panel = build_quarterly_panel(root, frames["quarterly"])

    annual_output = root / "data" / "processed" / "core_state_panel_annual.csv"
    quarterly_output = root / "data" / "processed" / "core_state_panel_quarterly.csv"
    annual_output.parent.mkdir(parents=True, exist_ok=True)

    annual_panel.to_csv(annual_output, index=False)
    quarterly_panel.to_csv(quarterly_output, index=False)
    print(f"wrote {annual_output}")
    print(f"wrote {quarterly_output}")


if __name__ == "__main__":
    main()
