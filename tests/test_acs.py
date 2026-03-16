import pandas as pd
import pytest

from rent_control_public.acs import add_computed_columns


def test_add_computed_columns_converts_numeric_fields():
    df = pd.DataFrame(
        {
            "NAME": ["California"],
            "state": ["06"],
            "DP03_0062E": ["96000"],
            "DP04_0141PE": ["12.5"],
            "DP04_0142PE": ["21.0"],
            "year": ["2024"],
        }
    )

    out = add_computed_columns(df)

    assert out["state"].iloc[0] == "06"
    assert out["DP03_0062E"].iloc[0] == 96000
    assert out["year"].iloc[0] == 2024
    assert out["rent_burden_30_plus_pct"].iloc[0] == 33.5


def test_add_computed_columns_harmonizes_legacy_mobility_codes() -> None:
    df = pd.DataFrame(
        {
            "NAME": ["Oregon"],
            "state": ["41"],
            "DP02_0079PE": ["82.8"],
            "DP02_0080PE": ["7.1"],
            "DP02_0084PE": ["2.7"],
            "year": ["2015"],
        }
    )

    out = add_computed_columns(df)

    assert out["same_house_1y_pct"].iloc[0] == 82.8
    assert out["moved_last_year_pct"].iloc[0] == 17.2
    assert out["moved_within_us_pct"].iloc[0] == 7.1
    assert out["moved_different_state_pct"].iloc[0] == 2.7


def test_add_computed_columns_harmonizes_modern_mobility_codes() -> None:
    df = pd.DataFrame(
        {
            "NAME": ["California"],
            "state": ["06"],
            "DP02_0080PE": ["85.3"],
            "DP02_0081PE": ["14.2"],
            "DP02_0085PE": ["3.7"],
            "year": ["2021"],
        }
    )

    out = add_computed_columns(df)

    assert out["same_house_1y_pct"].iloc[0] == 85.3
    assert out["moved_last_year_pct"].iloc[0] == 14.7
    assert out["moved_within_us_pct"].iloc[0] == 14.2
    assert out["moved_different_state_pct"].iloc[0] == 3.7


def test_add_computed_columns_harmonizes_legacy_rent_codes() -> None:
    df = pd.DataFrame(
        {
            "NAME": ["Oregon"],
            "state": ["41"],
            "DP04_0132E": ["924"],
            "DP04_0139PE": ["9.7"],
            "DP04_0140PE": ["45.4"],
            "year": ["2014"],
        }
    )

    out = add_computed_columns(df)

    assert out["DP04_0134E"].iloc[0] == 924
    assert out["median_gross_rent"].iloc[0] == 924
    assert out["DP04_0141PE"].iloc[0] == 9.7
    assert out["DP04_0142PE"].iloc[0] == 45.4
    assert out["rent_burden_30_plus_pct"].iloc[0] == pytest.approx(55.1)
