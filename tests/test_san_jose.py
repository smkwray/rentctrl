import pandas as pd
import pytest

from rent_control_public.san_jose import (
    COMPARISON_COUNTIES,
    COUNTY_NAME_BY_FIPS,
    POLICY_EVENTS,
    SAN_JOSE_METRO_CBSA,
    SANTA_CLARA_COUNTY_FIPS,
    add_quarter_period,
    build_policy_event_table,
    label_pre_post,
    summarize_by_period,
    summarize_treated_vs_controls,
)


class TestBuildPolicyEventTable:
    def test_returns_dataframe(self):
        df = build_policy_event_table()
        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ["date", "event", "description"]

    def test_dates_are_datetime(self):
        df = build_policy_event_table()
        assert pd.api.types.is_datetime64_any_dtype(df["date"])

    def test_event_count(self):
        df = build_policy_event_table()
        assert len(df) == len(POLICY_EVENTS)

    def test_contains_key_events(self):
        df = build_policy_event_table()
        events = set(df["event"])
        assert "aro_adopted" in events
        assert "registry_launched" in events
        assert "ab1482_effective" in events


class TestLabelPrePost:
    @pytest.fixture()
    def sample_df(self):
        return pd.DataFrame({
            "date": pd.to_datetime(["2018-01-01", "2019-06-01", "2022-01-01"]),
            "value": [100, 200, 300],
        })

    def test_default_event(self, sample_df):
        result = label_pre_post(sample_df, "date")
        assert list(result["period"]) == ["pre", "post", "post"]

    def test_custom_event(self):
        df = pd.DataFrame({
            "date": pd.to_datetime(["2016-01-01", "2018-01-01", "2022-01-01"]),
            "value": [100, 200, 300],
        })
        result = label_pre_post(df, "date", event="registry_launched")
        assert list(result["period"]) == ["pre", "post", "post"]

    def test_unknown_event_raises(self, sample_df):
        with pytest.raises(ValueError, match="Unknown policy event"):
            label_pre_post(sample_df, "date", event="nonexistent")

    def test_does_not_mutate_input(self, sample_df):
        original_cols = list(sample_df.columns)
        label_pre_post(sample_df, "date")
        assert list(sample_df.columns) == original_cols


class TestSummarizeByPeriod:
    def test_basic_summary(self):
        df = pd.DataFrame({
            "period": ["pre", "pre", "post", "post"],
            "value": [10, 20, 30, 40],
        })
        result = summarize_by_period(df, "value")
        assert "value_mean" in result.columns
        assert "n" in result.columns
        pre = result.loc[result["period"] == "pre"]
        assert pre["value_mean"].iloc[0] == 15.0
        assert pre["n"].iloc[0] == 2

    def test_with_group_col(self):
        df = pd.DataFrame({
            "county": ["A", "A", "B", "B"],
            "period": ["pre", "post", "pre", "post"],
            "value": [1, 2, 3, 4],
        })
        result = summarize_by_period(df, "value", group_col="county")
        assert len(result) == 4
        assert "county" in result.columns

    def test_missing_period_raises(self):
        df = pd.DataFrame({"value": [1, 2]})
        with pytest.raises(ValueError, match="period"):
            summarize_by_period(df, "value")


class TestConstants:
    def test_santa_clara_fips(self):
        assert SANTA_CLARA_COUNTY_FIPS == "06085"

    def test_san_jose_metro_cbsa(self):
        assert SAN_JOSE_METRO_CBSA == "41940"

    def test_comparison_counties_are_ca(self):
        for fips in COMPARISON_COUNTIES.values():
            assert fips.startswith("06")

    def test_county_name_map_includes_all(self):
        assert SANTA_CLARA_COUNTY_FIPS in COUNTY_NAME_BY_FIPS
        for fips in COMPARISON_COUNTIES.values():
            assert fips in COUNTY_NAME_BY_FIPS


class TestQuarterHelpers:
    def test_add_quarter_period(self):
        df = pd.DataFrame({"year": [2021, 2021], "quarter": [1, 4]})
        out = add_quarter_period(df)
        assert list(out["calendar_period"]) == ["2021Q1", "2021Q4"]
        assert "date" in out.columns


class TestTreatedVsControlsSummary:
    def test_summary_builds_diff_in_diff(self):
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(
                    ["2018-01-01", "2020-01-01", "2018-01-01", "2020-01-01", "2018-01-01", "2020-01-01"]
                ),
                "county_name": [
                    "Santa Clara County",
                    "Santa Clara County",
                    "San Mateo County",
                    "San Mateo County",
                    "Contra Costa County",
                    "Contra Costa County",
                ],
                "value": [10, 12, 20, 21, 30, 31],
            }
        )
        out = summarize_treated_vs_controls(
            df,
            value_col="value",
            group_col="county_name",
            treated_group="Santa Clara County",
            control_groups=["San Mateo County", "Contra Costa County"],
            event="ab1482_effective",
        )
        pre = out[out["period"] == "pre"].iloc[0]
        post = out[out["period"] == "post"].iloc[0]
        assert pre["treated_mean"] == 10
        assert pre["control_mean"] == 25
        assert post["treated_change"] == 2
        assert post["control_change"] == 1
        assert post["diff_in_diff"] == 1
