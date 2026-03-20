from pathlib import Path

import pandas as pd
import pytest

from rent_control_public.qcew import annualize_core, filter_state_private_total, filter_state_total_covered, reshape_qcew_core


pytestmark = pytest.mark.integration

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def test_qcew_filters_and_core_aggregation() -> None:
    raw_path = FIXTURES_DIR / "qcew" / "qcew_AZ_2024Q1.csv"
    df = pd.read_csv(raw_path)

    total = filter_state_total_covered(df)
    private = filter_state_private_total(df)

    assert len(total) == 1
    assert len(private) == 1

    total = total.assign(state_abbr="AZ", state_fips="04", year=2024, quarter=1)
    private = private.assign(state_abbr="AZ", state_fips="04", year=2024, quarter=1)

    total_core = reshape_qcew_core(total, prefix="qcew_total_covered")
    private_core = reshape_qcew_core(private, prefix="qcew_private")
    quarterly_core = total_core.merge(private_core, on=["state_abbr", "state_fips", "year", "quarter"], how="outer")
    annual_core = annualize_core(quarterly_core)

    assert total_core["qcew_total_covered_estabs"].iloc[0] > 0
    assert private_core["qcew_private_emplvl"].iloc[0] > 0
    assert annual_core["qcew_total_covered_wages"].iloc[0] == quarterly_core["qcew_total_covered_wages"].iloc[0]
    assert annual_core["state_fips"].iloc[0] == "04"
