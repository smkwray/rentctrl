from __future__ import annotations

from pathlib import Path

import pandas as pd


DEFAULT_ANNUAL_REQUIRED_DOMAINS = ["policy", "bps", "fhfa", "acs", "qcew"]
DEFAULT_QUARTERLY_REQUIRED_DOMAINS = ["policy", "fhfa", "qcew"]
DEFAULT_MANIFEST_FILENAME = "data_coverage_manifest.csv"


def coverage_manifest_path(root: str | Path) -> Path:
    root_path = Path(root)
    return root_path / "data" / "processed" / DEFAULT_MANIFEST_FILENAME


def load_coverage_manifest(root: str | Path) -> pd.DataFrame:
    path = coverage_manifest_path(root)
    if not path.exists():
        raise FileNotFoundError(
            f"Missing {path}. Run scripts/build_core_state_panel.py first so the coverage manifest is available."
        )
    return pd.read_csv(path)


def parse_domain_list(value: str | None, default: list[str]) -> list[str]:
    if value is None:
        return list(default)
    items = [item.strip() for item in value.split(",") if item.strip()]
    return items or list(default)


def require_manifest_readiness(
    root: str | Path,
    *,
    annual_domains: list[str] | None = None,
    quarterly_domains: list[str] | None = None,
) -> pd.DataFrame:
    manifest = load_coverage_manifest(root)
    required_rows: list[tuple[str, str]] = []

    for domain in annual_domains or []:
        required_rows.append(("annual", domain))
    for domain in quarterly_domains or []:
        required_rows.append(("quarterly", domain))

    missing_specs: list[str] = []
    not_ready_specs: list[str] = []
    for frequency, domain in required_rows:
        row = manifest[(manifest["panel_frequency"] == frequency) & (manifest["domain"] == domain)]
        if row.empty:
            missing_specs.append(f"{frequency}:{domain}")
            continue
        if not bool(row["ready_for_baseline"].iloc[0]):
            not_ready_specs.append(f"{frequency}:{domain}")

    if missing_specs or not_ready_specs:
        problems = []
        if missing_specs:
            problems.append(f"missing manifest rows for {', '.join(missing_specs)}")
        if not_ready_specs:
            problems.append(f"domains not ready for baseline: {', '.join(not_ready_specs)}")
        raise RuntimeError(
            "Coverage manifest blocks this run: " + "; ".join(problems) + ". Rebuild the panels or relax the workflow."
        )

    return manifest


def manifest_domain_status(
    manifest: pd.DataFrame,
    *,
    panel_frequency: str,
    domains: list[str],
) -> dict[str, bool]:
    out: dict[str, bool] = {}
    for domain in domains:
        row = manifest[(manifest["panel_frequency"] == panel_frequency) & (manifest["domain"] == domain)]
        out[domain] = bool(row["ready_for_baseline"].iloc[0]) if not row.empty else False
    return out
