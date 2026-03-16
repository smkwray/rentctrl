from pathlib import Path

import pandas as pd

from rent_control_public.qcew import annualize_core, filter_state_private_total, filter_state_total_covered, reshape_qcew_core


def test_qcew_filters_and_core_aggregation():
    raw_path = Path("data/raw/qcew/qcew_AZ_2024Q1.csv")
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


def test_core_panels_include_expected_columns():
    annual = pd.read_csv("data/processed/core_state_panel_annual.csv", dtype={"state_fips": str})
    quarterly = pd.read_csv("data/processed/core_state_panel_quarterly.csv", dtype={"state_fips": str})

    annual_expected = {
        "DP04_0134E",
        "rent_burden_30_plus_pct",
        "qcew_total_covered_emplvl",
        "qcew_private_emplvl",
    }
    quarterly_expected = {
        "index_sa",
        "qcew_total_covered_emplvl",
        "qcew_private_emplvl",
    }

    assert annual_expected.issubset(annual.columns)
    assert quarterly_expected.issubset(quarterly.columns)

    annual_2024 = annual[annual["year"] == 2024]
    quarterly_2024 = quarterly[quarterly["year"] == 2024]

    assert annual_2024["qcew_total_covered_emplvl"].notna().any()
    assert quarterly_2024["qcew_total_covered_emplvl"].notna().any()
