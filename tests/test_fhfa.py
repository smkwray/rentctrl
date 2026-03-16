from pathlib import Path

from rent_control_public.fhfa import aggregate_state_annual, filter_state_quarterly, load_master


def test_fhfa_state_filters():
    df = load_master(Path("data/raw/fhfa/hpi_master.csv"))
    out = filter_state_quarterly(df, flavor="purchase-only", min_year=2010)
    assert not out.empty
    assert out["yr"].min() >= 2010
    assert "California" in set(out["state_name"])
    annual = aggregate_state_annual(out)
    assert not annual.empty
    assert annual["year"].min() >= 2010
