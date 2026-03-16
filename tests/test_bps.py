from pathlib import Path

from rent_control_public.bps import load_state_annual_dir, parse_county_ytd_file


def test_load_state_annual_dir():
    df = load_state_annual_dir(Path("data/raw/bps/state"))
    assert not df.empty
    assert df["year"].min() == 2010
    assert df["year"].max() == 2024
    assert "permits_units_total" in df.columns
    assert "California" in set(df["state_name"])


def test_parse_county_ytd_file():
    df = parse_county_ytd_file(Path("data/raw/bps/county/co2412y.txt"))
    assert not df.empty
    assert "state_county_fips" in df.columns
    assert df["year"].nunique() == 1
    assert df["year"].iloc[0] == 2024
