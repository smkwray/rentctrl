from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from rent_control_public.policy import aggregate_annual_policy_panel, expand_quarterly_policy_panel, load_policy_events


def build_policy_panels(root: Path) -> tuple[Path, Path]:
    config_dir = root / "config"
    processed_dir = root / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    state_meta = pd.read_csv(config_dir / "state_metadata.csv", dtype={"state_fips": str})
    policy_events = load_policy_events(config_dir / "policy_events_core.csv")

    policy_q = expand_quarterly_policy_panel(state_meta, policy_events, start="2010Q1", end="2026Q4")
    policy_y = aggregate_annual_policy_panel(policy_q)

    quarterly_path = processed_dir / "policy_panel_state_quarterly_2010_2026.csv"
    annual_path = processed_dir / "policy_panel_state_annual_2010_2026.csv"
    policy_q.to_csv(quarterly_path, index=False)
    policy_y.to_csv(annual_path, index=False)
    return annual_path, quarterly_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Build quarterly and annual statewide policy panels from checked-in config.")
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args()

    annual_path, quarterly_path = build_policy_panels(args.root.resolve())
    print(f"wrote {annual_path}")
    print(f"wrote {quarterly_path}")


if __name__ == "__main__":
    main()
