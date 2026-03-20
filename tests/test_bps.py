from pathlib import Path

import pytest

from rent_control_public.bps import load_state_annual_dir, parse_county_ytd_file


pytestmark = pytest.mark.integration

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def test_load_state_annual_dir() -> None:
    df = load_state_annual_dir(FIXTURES_DIR / "bps" / "state")
    assert not df.empty
    assert df["year"].min() == 2010
    assert df["year"].max() == 2024
    assert "permits_units_total" in df.columns
    assert "California" in set(df["state_name"])


def test_parse_county_ytd_file() -> None:
    df = parse_county_ytd_file(FIXTURES_DIR / "bps" / "county" / "co2412y.txt")
    assert not df.empty
    assert "state_county_fips" in df.columns
    assert df["year"].nunique() == 1
    assert df["year"].iloc[0] == 2024
