from __future__ import annotations

from pathlib import Path
import requests


URL = "https://www.fhfa.gov/hpi/download/monthly/hpi_master.csv"


def main() -> None:
    output = Path("data/raw/fhfa/hpi_master.csv")
    output.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(URL, timeout=120)
    response.raise_for_status()
    output.write_bytes(response.content)
    print(f"downloaded {output}")


if __name__ == "__main__":
    main()
