from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EXTERNAL_PYTHON = Path("/Users/shanewray/venvs/rentctrl/bin/python")


def _parse_dotenv_exports(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    exports: dict[str, str] = {}
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :]
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        exports[key.strip()] = value.strip().strip("\"'")
    return exports


def resolve_python(root: Path) -> Path:
    env_path = os.environ.get("UV_PROJECT_ENVIRONMENT")
    if env_path:
        candidate = Path(env_path).expanduser() / "bin" / "python"
        if candidate.exists():
            return candidate

    dotenv = _parse_dotenv_exports(root / ".env")
    if "UV_PROJECT_ENVIRONMENT" in dotenv:
        candidate = Path(dotenv["UV_PROJECT_ENVIRONMENT"]).expanduser() / "bin" / "python"
        if candidate.exists():
            return candidate

    current = Path(sys.executable)
    if current.exists() and root not in current.parents:
        return current
    return DEFAULT_EXTERNAL_PYTHON


def command_env(root: Path) -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("PYTHONDONTWRITEBYTECODE", "1")
    env.setdefault("PYTHONPYCACHEPREFIX", f"/tmp/{root.name}-pycache")
    env.update(_parse_dotenv_exports(root / ".env"))
    return env


def run_step(python: Path, env: dict[str, str], *args: str) -> None:
    cmd = [str(python), "-B", *args]
    print(f"$ {' '.join(cmd)}")
    subprocess.run(cmd, check=True, cwd=ROOT, env=env)


def print_summary(root: Path) -> None:
    manifest_path = root / "data" / "processed" / "data_coverage_manifest.csv"
    manifest = pd.read_csv(manifest_path)
    ready = manifest[["panel_frequency", "domain", "ready_for_baseline"]].copy()
    print("\nCoverage manifest")
    print(ready.to_string(index=False))

    artifacts = [
        root / "data" / "processed" / "policy_panel_state_annual_2010_2026.csv",
        root / "data" / "processed" / "core_state_panel_annual.csv",
        root / "data" / "processed" / "core_state_panel_quarterly.csv",
        root / "results" / "tables" / "baseline_outcome_summary.csv",
        root / "results" / "tables" / "baseline_annual_timing_sensitivity.csv",
        root / "results" / "tables" / "credibility_checks_summary.csv",
    ]
    print("\nKey artifacts")
    for artifact in artifacts:
        print(f"- {artifact.relative_to(root)}: {'ok' if artifact.exists() else 'missing'}")


def main() -> None:
    python = resolve_python(ROOT)
    if not python.exists():
        raise FileNotFoundError(f"Could not resolve an external project interpreter. Tried {python}.")
    env = command_env(ROOT)

    run_step(python, env, "scripts/download_fhfa_hpi.py")
    run_step(python, env, "scripts/download_bps_state_annual.py", "--start-year", "2010", "--end-year", "2024")
    run_step(python, env, "scripts/download_acs_state_profile.py", "--start-year", "2010", "--end-year", "2019")
    run_step(python, env, "scripts/download_acs_state_profile.py", "--start-year", "2021", "--end-year", "2024")
    run_step(python, env, "scripts/download_qcew_state_quarters.py", "--start-year", "2014", "--end-year", "2024")
    run_step(python, env, "scripts/build_policy_panels.py")
    run_step(python, env, "scripts/build_core_state_panel.py", "--strict")
    run_step(python, env, "scripts/run_baseline_event_study.py")
    run_step(python, env, "scripts/run_credibility_checks.py")

    print_summary(ROOT)


if __name__ == "__main__":
    main()
