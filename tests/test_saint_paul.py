import pandas as pd
import pytest

from rent_control_public.saint_paul import (
    COMPARISON_COUNTIES,
    PRIMARY_CONTROL_COUNTIES,
    POLICY_EVENTS,
    RAMSEY_COUNTY_FIPS,
    add_quarter_period,
    build_policy_event_table,
    label_pre_post,
    summarize_treated_vs_controls,
    summarize_by_period,
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
        assert "ballot_adoption" in events
        assert "ordinance_effective" in events
        assert "amendment_2023" in events


class TestLabelPrePost:
    @pytest.fixture()
    def sample_df(self):
        return pd.DataFrame({
            "date": pd.to_datetime(["2020-01-01", "2022-06-01", "2024-01-01"]),
            "value": [100, 200, 300],
        })

    def test_default_event(self, sample_df):
        result = label_pre_post(sample_df, "date")
        assert list(result["period"]) == ["pre", "post", "post"]

    def test_custom_event(self, sample_df):
        result = label_pre_post(sample_df, "date", event="amendment_2023")
        assert list(result["period"]) == ["pre", "pre", "post"]

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
    def test_ramsey_fips(self):
        assert RAMSEY_COUNTY_FIPS == "27123"

    def test_comparison_counties_are_mn(self):
        for fips in COMPARISON_COUNTIES.values():
            assert fips.startswith("27")

    def test_primary_controls_are_subset(self):
        for fips in PRIMARY_CONTROL_COUNTIES.values():
            assert fips in COMPARISON_COUNTIES.values()


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
                    ["2021-01-01", "2023-01-01", "2021-01-01", "2023-01-01", "2021-01-01", "2023-01-01"]
                ),
                "county_name": [
                    "Ramsey County",
                    "Ramsey County",
                    "Hennepin County",
                    "Hennepin County",
                    "Dakota County",
                    "Dakota County",
                ],
                "value": [10, 12, 20, 21, 30, 31],
            }
        )
        out = summarize_treated_vs_controls(
            df,
            value_col="value",
            group_col="county_name",
            treated_group="Ramsey County",
            control_groups=["Hennepin County", "Dakota County"],
            event="amendment_2023",
        )
        pre = out[out["period"] == "pre"].iloc[0]
        post = out[out["period"] == "post"].iloc[0]
        assert pre["treated_mean"] == 10
        assert pre["control_mean"] == 25
        assert post["treated_change"] == 2
        assert post["control_change"] == 1
        assert post["diff_in_diff"] == 1
