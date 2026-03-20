from pathlib import Path

import pytest

from rent_control_public.fhfa import aggregate_state_annual, filter_state_quarterly, load_master


pytestmark = pytest.mark.integration

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def test_fhfa_state_filters() -> None:
    df = load_master(FIXTURES_DIR / "fhfa" / "hpi_master_sample.csv")
    out = filter_state_quarterly(df, flavor="purchase-only", min_year=2010)
    assert not out.empty
    assert out["yr"].min() >= 2010
    assert "California" in set(out["state_name"])
    annual = aggregate_state_annual(out)
    assert not annual.empty
    assert annual["year"].min() >= 2010
