import pandas as pd
import pytest

from rent_control_public.san_francisco import (
    DATASET_ID,
    DEFAULT_FIELDS,
    INVENTORY_START_YEAR,
    RESOURCE_URL,
    SF_COUNTY_FIPS,
    add_parsed_columns,
    parse_bedroom_count,
    parse_rent_midpoint,
    summarize_occupancy,
    summarize_reporting_rollout,
    summarize_by_district,
    summarize_by_neighborhood,
    summarize_rent_bands,
    summarize_by_year,
    summarize_year_by_district,
    summarize_year_by_neighborhood,
    summarize_overall,
)


class TestConstants:
    def test_dataset_id(self):
        assert DATASET_ID == "gdc7-dmcn"

    def test_resource_url_contains_dataset_id(self):
        assert DATASET_ID in RESOURCE_URL

    def test_sf_county_fips(self):
        assert SF_COUNTY_FIPS == "06075"

    def test_inventory_start_year(self):
        assert INVENTORY_START_YEAR == 2022

    def test_default_fields_include_key_columns(self):
        for col in ["unique_id", "submission_year", "monthly_rent", "analysis_neighborhood"]:
            assert col in DEFAULT_FIELDS


class TestParseRentMidpoint:
    def test_two_values(self):
        assert parse_rent_midpoint("$2001-$2250") == 2125.5

    def test_single_value_or_more(self):
        assert parse_rent_midpoint("$5001 or more") == 5001.0

    def test_single_value(self):
        assert parse_rent_midpoint("$1000") == 1000.0

    def test_none_for_bad_input(self):
        assert parse_rent_midpoint("N/A") is None

    def test_none_for_non_string(self):
        assert parse_rent_midpoint(None) is None
        assert parse_rent_midpoint(42) is None

    def test_comma_thousands(self):
        assert parse_rent_midpoint("$1,001-$1,250") == 1125.5


class TestParseBedroomCount:
    def test_known_values(self):
        assert parse_bedroom_count("Studio") == 0
        assert parse_bedroom_count("One-Bedroom") == 1
        assert parse_bedroom_count("Two-Bedroom") == 2
        assert parse_bedroom_count("Three-Bedroom") == 3
        assert parse_bedroom_count("Four-Bedroom") == 4
        assert parse_bedroom_count("Five or more bedrooms") == 5

    def test_unknown(self):
        assert parse_bedroom_count("Unknown") is None


@pytest.fixture()
def sample_inventory():
    return pd.DataFrame({
        "unique_id": ["a", "b", "c", "d"],
        "block_num": ["100", "100", "200", "200"],
        "submission_year": [2022, 2022, 2023, 2023],
        "monthly_rent": ["$2001-$2250", "$3001-$3250", "$2501-$2750", "$5001 or more"],
        "bedroom_count": ["Studio", "One-Bedroom", "Two-Bedroom", "Three-Bedroom"],
        "occupancy_type": [
            "Occupied by non-owner",
            "Occupied by owner",
            "Occupied by non-owner",
            "Occupied by non-owner",
        ],
        "analysis_neighborhood": ["Mission", "Mission", "SoMa", "SoMa"],
        "supervisor_district": [9, 9, 6, 6],
    })


class TestAddParsedColumns:
    def test_adds_rent_midpoint(self, sample_inventory):
        out = add_parsed_columns(sample_inventory)
        assert "rent_midpoint" in out.columns
        assert out["rent_midpoint"].iloc[0] == 2125.5

    def test_adds_bedrooms(self, sample_inventory):
        out = add_parsed_columns(sample_inventory)
        assert "bedrooms" in out.columns
        assert out["bedrooms"].iloc[0] == 0

    def test_does_not_mutate(self, sample_inventory):
        orig_cols = list(sample_inventory.columns)
        add_parsed_columns(sample_inventory)
        assert list(sample_inventory.columns) == orig_cols


class TestSummarizeByYear:
    def test_output_shape(self, sample_inventory):
        out = summarize_by_year(sample_inventory)
        assert isinstance(out, pd.DataFrame)
        assert set(out["submission_year"]) == {2022, 2023}
        assert "unit_rows" in out.columns
        assert "median_rent_midpoint" in out.columns
        assert "nonowner_share" in out.columns

    def test_counts(self, sample_inventory):
        out = summarize_by_year(sample_inventory)
        row_2022 = out[out["submission_year"] == 2022].iloc[0]
        assert row_2022["unit_rows"] == 2
        assert row_2022["occupied_nonowner"] == 1


class TestSummarizeByNeighborhood:
    def test_output(self, sample_inventory):
        out = summarize_by_neighborhood(sample_inventory)
        assert len(out) == 2
        assert "median_rent_midpoint" in out.columns


class TestSummarizeByDistrict:
    def test_output(self, sample_inventory):
        out = summarize_by_district(sample_inventory)
        assert len(out) == 2
        assert "median_rent_midpoint" in out.columns


class TestExtendedSummaries:
    def test_year_by_neighborhood(self, sample_inventory):
        out = summarize_year_by_neighborhood(sample_inventory)
        assert set(out.columns) >= {"submission_year", "analysis_neighborhood", "unit_rows", "nonowner_share"}
        assert len(out) == 2

    def test_year_by_district(self, sample_inventory):
        out = summarize_year_by_district(sample_inventory)
        assert set(out.columns) >= {"submission_year", "supervisor_district", "unit_rows", "nonowner_share"}
        assert len(out) == 2

    def test_rent_band_summary(self, sample_inventory):
        out = summarize_rent_bands(sample_inventory)
        assert set(out.columns) >= {"submission_year", "monthly_rent", "unit_rows"}
        assert out["unit_rows"].sum() == 4

    def test_occupancy_summary(self, sample_inventory):
        out = summarize_occupancy(sample_inventory)
        assert set(out.columns) >= {"submission_year", "occupancy_type", "unit_rows", "share_of_year"}
        assert out["unit_rows"].sum() == 4

    def test_reporting_rollout_summary(self, sample_inventory):
        out = summarize_reporting_rollout(sample_inventory)
        assert set(out.columns) >= {"submission_year", "unit_rows", "unit_row_growth", "nonowner_share"}
        assert out.loc[out["submission_year"] == 2023, "unit_row_growth"].iloc[0] == 0


class TestSummarizeOverall:
    def test_returns_metrics(self, sample_inventory):
        out = summarize_overall(sample_inventory)
        metrics = set(out["metric"])
        assert "total_unit_rows" in metrics
        assert "unique_blocks" in metrics
        assert "neighborhoods" in metrics
        assert "median_rent_midpoint" in metrics

    def test_total_rows(self, sample_inventory):
        out = summarize_overall(sample_inventory)
        total = out.loc[out["metric"] == "total_unit_rows", "value"].iloc[0]
        assert total == 4
