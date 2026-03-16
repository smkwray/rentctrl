from __future__ import annotations

import argparse
from pathlib import Path
import requests


URL_PATTERN = "https://www2.census.gov/econ/bps/State/st{year}a.txt"


def download_year(year: int, output_dir: Path, timeout: int = 60) -> Path:
    url = URL_PATTERN.format(year=year)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"st{year}a.txt"
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    path.write_bytes(response.content)
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("data/raw/bps/state"))
    args = parser.parse_args()

    for year in range(args.start_year, args.end_year + 1):
        path = download_year(year, args.output_dir)
        print(f"downloaded {path}")


if __name__ == "__main__":
    main()
