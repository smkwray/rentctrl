import pandas as pd
import pytest

from rent_control_public.nyc import (
    add_bbl_column,
    aggregate_panel_borough_year,
    aggregate_matched_pair_year,
    aggregate_panel_stratum_year,
    borough_year_treated_control_diff,
    build_borough_year_summary_table,
    build_borough_pre_post_gap_summary,
    build_matched_pair_panel,
    build_nyc_enriched_analytic_panel,
    build_preperiod_building_features,
    build_treated_year_event_design,
    build_socrata_bbl_where,
    build_stratified_registered_rental_panel,
    build_hpd_comparison_building_year_panel,
    build_matched_rsbl_building_year_panel,
    choose_nearest_control,
    borough_rsbl_iter,
    canonical_bbl_to_pluto_bbl,
    chunk_values,
    classify_gap_direction,
    combine_rsbl_frames,
    extract_pdf_text,
    hpd_violations_to_monthly_summary,
    hpd_violations_to_yearly_summary,
    make_bbl,
    make_boro_block_lot,
    match_treated_to_controls,
    merge_control_frame,
    normalize_block,
    normalize_borough_code,
    normalize_borough_name,
    normalize_house_number,
    normalize_lot,
    parse_rsbl_text,
    normalize_street_name,
    RSBL_PDF_URLS_2024,
    select_control_columns,
    summarize_rsbl_hpd_match,
    summarize_rsbl_hpd_match_citywide,
    summarize_treated_control_balance,
    two_way_demean,
    units_to_bin,
    yearbuilt_to_bin,
)


class TestNormalizeBoroughCode:
    @pytest.mark.parametrize(
        "inp,expected",
        [
            ("1", "1"),
            (1, "1"),
            ("3", "3"),
            (5, "5"),
            ("Manhattan", "1"),
            ("BRONX", "2"),
            ("brooklyn", "3"),
            ("Queens", "4"),
            ("Staten Island", "5"),
            ("MN", "1"),
            ("BK", "3"),
            ("Kings", "3"),
            ("Richmond", "5"),
            ("New York", "1"),
        ],
    )
    def test_valid(self, inp, expected):
        assert normalize_borough_code(inp) == expected

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="Unrecognized"):
            normalize_borough_code("Hoboken")

    def test_whitespace_stripped(self):
        assert normalize_borough_code("  2  ") == "2"


class TestNormalizeBoroughName:
    def test_from_code(self):
        assert normalize_borough_name("1") == "MANHATTAN"
        assert normalize_borough_name(5) == "STATEN ISLAND"

    def test_from_name(self):
        assert normalize_borough_name("brooklyn") == "BROOKLYN"

    def test_from_alias(self):
        assert normalize_borough_name("Kings") == "BROOKLYN"


class TestNormalizeBlock:
    @pytest.mark.parametrize(
        "inp,expected",
        [
            ("123", "00123"),
            (123, "00123"),
            ("00042", "00042"),
            ("0", "00000"),
            (0, "00000"),
            ("10001", "10001"),
            (" 7 ", "00007"),
        ],
    )
    def test_valid(self, inp, expected):
        assert normalize_block(inp) == expected

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="empty"):
            normalize_block("")

    def test_non_numeric_raises(self):
        with pytest.raises(ValueError, match="Non-numeric"):
            normalize_block("12A")


class TestNormalizeLot:
    @pytest.mark.parametrize(
        "inp,expected",
        [
            ("1", "0001"),
            (1, "0001"),
            ("0045", "0045"),
            ("1234", "1234"),
            (0, "0000"),
        ],
    )
    def test_valid(self, inp, expected):
        assert normalize_lot(inp) == expected

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="empty"):
            normalize_lot("")


class TestMakeBBL:
    def test_numeric_inputs(self):
        assert make_bbl(1, 123, 45) == "1001230045"

    def test_string_inputs(self):
        assert make_bbl("Manhattan", "00123", "0045") == "1001230045"

    def test_full_range(self):
        assert make_bbl(5, 10001, 1234) == "5100011234"

    def test_mixed(self):
        assert make_bbl("BK", 42, "1") == "3000420001"

    def test_zero_block_lot(self):
        assert make_bbl(1, 0, 0) == "1000000000"

    def test_alias(self):
        assert make_boro_block_lot("MN", "123", "45") == "1001230045"


class TestNormalizeStreetName:
    @pytest.mark.parametrize(
        "inp,expected",
        [
            ("WEST 42ND STREET", "W 42ND ST"),
            ("West 42nd Street", "W 42ND ST"),
            ("5th Avenue", "5TH AVE"),
            ("  broadway  ", "BROADWAY"),
            ("E. 110th St.", "E 110TH ST"),
            ("EAST   3RD   STREET", "E 3RD ST"),
            ("Saint Nicholas Avenue", "SAINT NICHOLAS AVE"),
            ("1 st place", "1ST PL"),
        ],
    )
    def test_normalization(self, inp, expected):
        assert normalize_street_name(inp) == expected

    def test_ordinal_collapse(self):
        # "1 ST" (ordinal) should collapse to "1ST"
        assert normalize_street_name("1 ST AVENUE") == "1ST AVE"


class TestNormalizeHouseNumber:
    @pytest.mark.parametrize(
        "inp,expected",
        [
            ("123", "123"),
            ("0042", "42"),
            ("  7  ", "7"),
            ("35-40", "35-40"),
            ("035-040", "35-40"),
            ("0", "0"),
        ],
    )
    def test_valid(self, inp, expected):
        assert normalize_house_number(inp) == expected

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="empty"):
            normalize_house_number("")


RSBL_TEXT_SAMPLE = """\
                                                                            List of Manhattan Buildings Containing Stabilized Units
ZIP     BLDGNO1          STREET1                STSUFX1     BLDGNO2       STREET2            STSUFX2      CITY         COUNTY STATUS1               STATUS2                 STATUS3                 BLOCK      LOT
10001   246              10TH                   AVE                                                       NEW YORK     62     MULTIPLE DWELLING A                                                   722        3
10001   320              11TH                   AVE         545577        W 30TH             ST           NEW YORK     62     MULTIPLE DWELLING A   421-A (1-15)                                    702        7502
10001   855              6TH                    AVE         100           W 31ST             ST           NEW YORK     62     MULTIPLE DWELLING A   421-A (16)                                      806        7502
Source: NYS Homes and Community Renewal 2024 Building Registration File                                Page 1 of 259
"""


def test_parse_rsbl_text_extracts_expected_fields() -> None:
    out = parse_rsbl_text(RSBL_TEXT_SAMPLE, "Manhattan")

    assert list(out["zip"]) == ["10001", "10001", "10001"]
    assert list(out["borough"].unique()) == ["MANHATTAN"]
    assert out.loc[0, "boro_block_lot"] == "1007220003"
    assert out.loc[1, "boro_block_lot"] == "1007027502"
    assert out.loc[0, "primary_street_normalized"] == "10TH AVE"
    assert out.loc[0, "primary_house_number_normalized"] == "246"


def test_parse_rsbl_text_accepts_leading_space_rows() -> None:
    text = """\
 ZIP     BLDGNO1          STREET1                 STSUFX1     BLDGNO2     STREET2         STSUFX2   CITY              COUNTY STATUS1               STATUS2                 STATUS3                 BLOCK      LOT
 11201   436              ALBEE                   SQ                                                BROOKLYN          61     MULTIPLE DWELLING A                                                   146        7501
"""
    out = parse_rsbl_text(text, "Brooklyn")

    assert len(out) == 1
    assert out.loc[0, "boro_block_lot"] == "3001467501"
    assert out.loc[0, "borough"] == "BROOKLYN"


def test_add_bbl_column_and_summary() -> None:
    rsbl = pd.DataFrame({"boro_block_lot": ["1007220003", "1007027502"]})
    hpd = pd.DataFrame(
        {
            "boroid": [1, 1, 1],
            "block": [722, 999, 702],
            "lot": [3, 1, 7502],
        }
    )
    hpd = add_bbl_column(hpd, borough_col="boroid", block_col="block", lot_col="lot")

    summary = summarize_rsbl_hpd_match(rsbl, hpd, borough="Manhattan")

    assert summary.loc[0, "matched_boro_block_lot"] == 2
    assert summary.loc[0, "rsbl_match_rate_pct"] == 100.0
    assert round(float(summary.loc[0, "hpd_match_rate_pct"]), 1) == 66.7


def test_citywide_match_summary() -> None:
    rsbl = pd.DataFrame({"boro_block_lot": ["1007220003", "1007027502", "3000010001"]})
    hpd = pd.DataFrame({"boro_block_lot": ["1007220003", "3000010001", "5000000001"]})
    summary = summarize_rsbl_hpd_match_citywide(rsbl, hpd)

    assert summary.loc[0, "matched_boro_block_lot"] == 2
    assert round(float(summary.loc[0, "rsbl_match_rate_pct"]), 1) == 66.7
    assert round(float(summary.loc[0, "hpd_match_rate_pct"]), 1) == 66.7


def test_build_matched_rsbl_building_year_panel_zero_fills_missing_years() -> None:
    rsbl = pd.DataFrame(
        {
            "boro_block_lot": ["1007220003", "1007027502"],
            "borough": ["MANHATTAN", "MANHATTAN"],
            "zip": ["10001", "10001"],
        }
    )
    counts = pd.DataFrame(
        {
            "boro_block_lot": ["1007220003", "1007027502"],
            "inspection_year": [2020, 2021],
            "violation_count": [5, 7],
        }
    )
    panel = build_matched_rsbl_building_year_panel(rsbl, counts, start_year=2020, end_year=2021)

    assert len(panel) == 4
    assert set(panel["inspection_year"]) == {2020, 2021}
    assert panel.loc[(panel["boro_block_lot"] == "1007220003") & (panel["inspection_year"] == 2021), "violation_count"].iloc[0] == 0
    assert panel.loc[(panel["boro_block_lot"] == "1007027502") & (panel["inspection_year"] == 2020), "violation_count"].iloc[0] == 0
    assert set(panel["treated_rsbl"]) == {1}


def test_build_hpd_comparison_building_year_panel_marks_treated_and_controls() -> None:
    rsbl = pd.DataFrame({"boro_block_lot": ["1007220003"]})
    hpd_buildings = pd.DataFrame(
        {
            "boro_block_lot": ["1007220003", "1007027502"],
            "borough": ["MANHATTAN", "MANHATTAN"],
        }
    )
    counts = pd.DataFrame(
        {
            "boro_block_lot": ["1007220003", "1007027502"],
            "inspection_year": [2020, 2021],
            "violation_count": [5, 3],
        }
    )
    panel = build_hpd_comparison_building_year_panel(rsbl, hpd_buildings, counts, start_year=2020, end_year=2021)

    assert len(panel) == 4
    assert panel.loc[(panel["boro_block_lot"] == "1007220003") & (panel["inspection_year"] == 2020), "treated_rsbl"].iloc[0] == 1
    assert panel.loc[(panel["boro_block_lot"] == "1007027502") & (panel["inspection_year"] == 2020), "treated_rsbl"].iloc[0] == 0
    assert panel.loc[(panel["boro_block_lot"] == "1007027502") & (panel["inspection_year"] == 2020), "violation_count"].iloc[0] == 0


# ---------------------------------------------------------------------------
# Borough iteration / combine helpers
# ---------------------------------------------------------------------------


class TestBoroughRsblIter:
    def test_returns_all_five_boroughs(self):
        pairs = borough_rsbl_iter()
        assert len(pairs) == 5
        names = {name for name, _url in pairs}
        assert names == set(RSBL_PDF_URLS_2024.keys())

    def test_urls_match_catalog(self):
        for name, url in borough_rsbl_iter():
            assert url == RSBL_PDF_URLS_2024[name]


class TestCombineRsblFrames:
    def _make_frame(self, borough: str, bbls: list[str]) -> pd.DataFrame:
        return pd.DataFrame({
            "boro_block_lot": bbls,
            "borough": borough,
            "zip": ["10001"] * len(bbls),
        })

    def test_combines_two_boroughs(self):
        frames = {
            "MANHATTAN": self._make_frame("MANHATTAN", ["1007220003"]),
            "BRONX": self._make_frame("BRONX", ["2002340001"]),
        }
        result = combine_rsbl_frames(frames)
        assert len(result) == 2
        assert list(result["borough"].unique()) == ["MANHATTAN", "BRONX"] or set(result["borough"].unique()) == {"MANHATTAN", "BRONX"}
        # sorted by boro_block_lot
        assert list(result["boro_block_lot"]) == sorted(result["boro_block_lot"])

    def test_empty_input(self):
        result = combine_rsbl_frames({})
        assert len(result) == 0
        assert "boro_block_lot" in result.columns
        assert "borough" in result.columns

    def test_normalizes_borough_name(self):
        frames = {"brooklyn": self._make_frame("brooklyn", ["3000010001"])}
        result = combine_rsbl_frames(frames)
        assert result.loc[0, "borough"] == "BROOKLYN"


# ---------------------------------------------------------------------------
# HPD violation summary helpers
# ---------------------------------------------------------------------------


def _sample_hpd_violations() -> pd.DataFrame:
    return pd.DataFrame({
        "boro_block_lot": [
            "1007220003", "1007220003", "1007220003",
            "1007027502", "1007027502",
        ],
        "inspectiondate": [
            "2024-01-15", "2024-01-20", "2024-03-10",
            "2024-01-05", "2024-02-18",
        ],
        "violationid": [1, 2, 3, 4, 5],
    })


class TestHpdViolationsToMonthlySummary:
    def test_groups_by_building_year_month(self):
        df = _sample_hpd_violations()
        result = hpd_violations_to_monthly_summary(df)
        assert set(result.columns) == {"boro_block_lot", "year", "month", "violation_count"}
        # Building 1007220003 has 2 in Jan 2024 and 1 in Mar 2024
        b1_jan = result[(result["boro_block_lot"] == "1007220003") & (result["month"] == 1)]
        assert b1_jan["violation_count"].iloc[0] == 2
        b1_mar = result[(result["boro_block_lot"] == "1007220003") & (result["month"] == 3)]
        assert b1_mar["violation_count"].iloc[0] == 1

    def test_sorted_output(self):
        df = _sample_hpd_violations()
        result = hpd_violations_to_monthly_summary(df)
        keys = list(zip(result["boro_block_lot"], result["year"], result["month"]))
        assert keys == sorted(keys)

    def test_drops_missing_dates(self):
        df = pd.DataFrame({
            "boro_block_lot": ["1007220003", "1007220003"],
            "inspectiondate": ["2024-01-15", None],
        })
        result = hpd_violations_to_monthly_summary(df)
        assert len(result) == 1


class TestHpdViolationsToYearlySummary:
    def test_groups_by_building_year(self):
        df = _sample_hpd_violations()
        result = hpd_violations_to_yearly_summary(df)
        assert set(result.columns) == {"boro_block_lot", "year", "violation_count"}
        b1 = result[result["boro_block_lot"] == "1007220003"]
        assert b1["violation_count"].iloc[0] == 3
        b2 = result[result["boro_block_lot"] == "1007027502"]
        assert b2["violation_count"].iloc[0] == 2

    def test_multi_year(self):
        df = pd.DataFrame({
            "boro_block_lot": ["1007220003", "1007220003"],
            "inspectiondate": ["2023-06-01", "2024-06-01"],
        })
        result = hpd_violations_to_yearly_summary(df)
        assert len(result) == 2
        assert list(result["year"]) == [2023, 2024]


# ---------------------------------------------------------------------------
# Borough-year treated/control summary helpers
# ---------------------------------------------------------------------------


def _sample_comparison_panel() -> pd.DataFrame:
    """A small 2-borough, 2-year panel with treated and control buildings."""
    return pd.DataFrame({
        "boro_block_lot": [
            "1007220003", "1007220003",  # treated, MN, 2020-2021
            "1009990001", "1009990001",  # control, MN, 2020-2021
            "2002340001", "2002340001",  # treated, BX, 2020-2021
            "2009990002", "2009990002",  # control, BX, 2020-2021
        ],
        "borough": [
            "MANHATTAN", "MANHATTAN",
            "MANHATTAN", "MANHATTAN",
            "BRONX", "BRONX",
            "BRONX", "BRONX",
        ],
        "inspection_year": [2020, 2021, 2020, 2021, 2020, 2021, 2020, 2021],
        "treated_rsbl": [1, 1, 0, 0, 1, 1, 0, 0],
        "violation_count": [10, 8, 4, 2, 6, 5, 3, 1],
    })


class TestAggregatePanelBoroughYear:
    def test_returns_expected_columns(self):
        panel = _sample_comparison_panel()
        result = aggregate_panel_borough_year(panel)
        assert {"borough", "inspection_year", "treated_rsbl",
                "building_count", "mean_violation_count",
                "total_violation_count"} == set(result.columns)

    def test_correct_counts(self):
        panel = _sample_comparison_panel()
        result = aggregate_panel_borough_year(panel)
        # Each borough-year-treated cell has exactly 1 building
        assert (result["building_count"] == 1).all()

    def test_means_match_values(self):
        panel = _sample_comparison_panel()
        result = aggregate_panel_borough_year(panel)
        mn_t_2020 = result[
            (result["borough"] == "MANHATTAN")
            & (result["inspection_year"] == 2020)
            & (result["treated_rsbl"] == 1)
        ]
        assert mn_t_2020["mean_violation_count"].iloc[0] == 10.0

    def test_sorted_output(self):
        panel = _sample_comparison_panel()
        result = aggregate_panel_borough_year(panel)
        keys = list(zip(result["borough"], result["inspection_year"], result["treated_rsbl"]))
        assert keys == sorted(keys)

    def test_multi_building_mean(self):
        panel = _sample_comparison_panel()
        # Add a second treated building in Manhattan 2020
        extra = pd.DataFrame({
            "boro_block_lot": ["1008880001", "1008880001"],
            "borough": ["MANHATTAN", "MANHATTAN"],
            "inspection_year": [2020, 2021],
            "treated_rsbl": [1, 1],
            "violation_count": [20, 12],
        })
        big = pd.concat([panel, extra], ignore_index=True)
        result = aggregate_panel_borough_year(big)
        mn_t_2020 = result[
            (result["borough"] == "MANHATTAN")
            & (result["inspection_year"] == 2020)
            & (result["treated_rsbl"] == 1)
        ]
        assert mn_t_2020["building_count"].iloc[0] == 2
        assert mn_t_2020["mean_violation_count"].iloc[0] == 15.0  # (10+20)/2


class TestBoroughYearTreatedControlDiff:
    def test_returns_expected_columns(self):
        panel = _sample_comparison_panel()
        result = borough_year_treated_control_diff(panel)
        assert {"borough", "inspection_year", "mean_treated",
                "mean_control", "diff", "n_treated", "n_control"} == set(result.columns)

    def test_diff_values(self):
        panel = _sample_comparison_panel()
        result = borough_year_treated_control_diff(panel)
        mn_2020 = result[(result["borough"] == "MANHATTAN") & (result["inspection_year"] == 2020)]
        assert mn_2020["mean_treated"].iloc[0] == 10.0
        assert mn_2020["mean_control"].iloc[0] == 4.0
        assert mn_2020["diff"].iloc[0] == 6.0

    def test_sorted_output(self):
        panel = _sample_comparison_panel()
        result = borough_year_treated_control_diff(panel)
        keys = list(zip(result["borough"], result["inspection_year"]))
        assert keys == sorted(keys)


class TestBuildBoroughYearSummaryTable:
    def test_includes_ratio(self):
        panel = _sample_comparison_panel()
        result = build_borough_year_summary_table(panel)
        assert "ratio" in result.columns
        mn_2020 = result[(result["borough"] == "MANHATTAN") & (result["inspection_year"] == 2020)]
        assert mn_2020["ratio"].iloc[0] == pytest.approx(10.0 / 4.0)

    def test_zero_control_gives_na_ratio(self):
        panel = pd.DataFrame({
            "boro_block_lot": ["1007220003", "1007220003"],
            "borough": ["MANHATTAN", "MANHATTAN"],
            "inspection_year": [2020, 2021],
            "treated_rsbl": [1, 1],
            "violation_count": [10, 8],
        })
        result = build_borough_year_summary_table(panel)
        assert result["mean_control"].isna().all()
        assert result["ratio"].isna().all()


class TestClassifyGapDirection:
    def test_classifies_signed_changes(self):
        values = pd.Series([2.0, -1.0, 0.1, 0.0, pd.NA])
        result = classify_gap_direction(values, tolerance=0.25)
        assert list(result.iloc[:4]) == ["increase", "decrease", "flat", "flat"]
        assert pd.isna(result.iloc[4])


class TestBuildBoroughPrePostGapSummary:
    def test_returns_expected_columns(self):
        panel = _sample_comparison_panel()
        result = build_borough_pre_post_gap_summary(
            panel,
            pre_years=(2020,),
            post_years=(2021,),
        )
        assert {
            "borough",
            "pre_mean_gap",
            "post_mean_gap",
            "change_in_gap",
            "gap_direction",
        } == set(result.columns)

    def test_computes_change_in_gap(self):
        panel = _sample_comparison_panel()
        result = build_borough_pre_post_gap_summary(
            panel,
            pre_years=(2020,),
            post_years=(2021,),
        )
        manhattan = result[result["borough"] == "MANHATTAN"].iloc[0]
        assert manhattan["pre_mean_gap"] == pytest.approx(6.0)
        assert manhattan["post_mean_gap"] == pytest.approx(6.0)
        assert manhattan["change_in_gap"] == pytest.approx(0.0)
        assert manhattan["gap_direction"] == "flat"

    def test_sorts_largest_increase_first(self):
        panel = _sample_comparison_panel()
        extra = pd.DataFrame({
            "boro_block_lot": ["2007770003", "2007770003"],
            "borough": ["BRONX", "BRONX"],
            "inspection_year": [2020, 2021],
            "treated_rsbl": [1, 1],
            "violation_count": [6, 12],
        })
        result = build_borough_pre_post_gap_summary(
            pd.concat([panel, extra], ignore_index=True),
            pre_years=(2020,),
            post_years=(2021,),
        )
        assert result.iloc[0]["borough"] == "BRONX"


class TestControlHelpers:
    def test_chunk_values(self):
        assert chunk_values(["a", "b", "c", "d", "e"], 2) == [["a", "b"], ["c", "d"], ["e"]]

    def test_canonical_bbl_to_pluto_bbl(self):
        assert canonical_bbl_to_pluto_bbl("4061730023") == "4061730023.00000000"
        assert canonical_bbl_to_pluto_bbl("4061730023.00000000") == "4061730023.00000000"

    def test_build_socrata_bbl_where(self):
        out = build_socrata_bbl_where(["4061730023", "4061730024"])
        assert out == "bbl in ('4061730023.00000000','4061730024.00000000')"

    def test_select_control_columns(self):
        df = pd.DataFrame({"boro_block_lot": ["1"], "a": [1], "b": [2]})
        out = select_control_columns(df, ["boro_block_lot", "b", "c"])
        assert list(out.columns) == ["boro_block_lot", "b"]

    def test_merge_control_frame(self):
        panel = pd.DataFrame({"boro_block_lot": ["1", "2"], "value": [10, 20]})
        controls = pd.DataFrame({"boro_block_lot": ["1"], "control": [5]})
        out = merge_control_frame(panel, controls)
        assert out.loc[out["boro_block_lot"] == "1", "control"].iloc[0] == 5
        assert pd.isna(out.loc[out["boro_block_lot"] == "2", "control"].iloc[0])


class TestEnrichmentHelpers:
    def test_bins(self):
        year_bins = list(yearbuilt_to_bin(pd.Series([1920, 1950, 1980, 2010])).astype(str))
        unit_bins = list(units_to_bin(pd.Series([2, 5, 10, 30, 80])).astype(str))
        assert year_bins == ["prewar", "1940_1969", "1970_1999", "2000_plus"]
        assert unit_bins == ["1_2", "3_5", "6_19", "20_49", "50_plus"]

    def test_build_nyc_enriched_analytic_panel(self):
        panel = pd.DataFrame(
            {
                "boro_block_lot": ["1000010001", "1000010002"],
                "borough": ["MANHATTAN", "MANHATTAN"],
                "treated_rsbl": [1, 0],
                "inspection_year": [2024, 2024],
                "violation_count": [2, 1],
            }
        )
        pluto = pd.DataFrame(
            {
                "boro_block_lot": ["1000010001", "1000010002"],
                "yearbuilt": [1920, 1975],
                "unitstotal": [10, 20],
                "unitsres": [10, 20],
                "landuse": [3, 3],
                "bldgclass": ["C7", "D1"],
                "cd": [101, 101],
                "zipcode": [10001, 10001],
            }
        )
        mdr = pd.DataFrame(
            {
                "boro_block_lot": ["1000010001"],
                "mdr_registered": [1],
                "registration_count": [2],
                "building_count": [1],
                "communityboard": [101],
                "lastregistrationdate": ["2025-01-01"],
                "registrationenddate": ["2026-09-01"],
            }
        )
        out = build_nyc_enriched_analytic_panel(panel, pluto_controls=pluto, mdr_summary=mdr)
        assert out["mdr_registered"].tolist() == [1, 0]
        assert out["yearbuilt_bin"].tolist() == ["prewar", "1970_1999"]
        assert out["units_bin"].tolist() == ["6_19", "20_49"]

    def test_build_stratified_registered_rental_panel(self):
        panel = pd.DataFrame(
            {
                "boro_block_lot": ["1", "2", "3"],
                "borough": ["MANHATTAN", "MANHATTAN", "BROOKLYN"],
                "treated_rsbl": [1, 0, 0],
                "mdr_registered": [1, 1, 0],
                "inspection_year": [2024, 2024, 2024],
                "violation_count": [2, 1, 3],
                "unitstotal": [10, 20, 2],
                "communityboard": [101, 101, 301],
                "yearbuilt_bin": ["prewar", "prewar", "prewar"],
                "units_bin": ["6_19", "20_49", "1_2"],
            }
        )
        out = build_stratified_registered_rental_panel(panel)
        assert set(out["boro_block_lot"]) == {"1", "2"}
        assert out["stratum"].str.contains("MANHATTAN|cb101").all()

    def test_aggregate_panel_stratum_year(self):
        panel = pd.DataFrame(
            {
                "stratum": ["s1", "s1", "s1", "s1"],
                "borough": ["MANHATTAN"] * 4,
                "inspection_year": [2024, 2024, 2025, 2025],
                "treated_rsbl": [0, 1, 0, 1],
                "violation_count": [1, 3, 2, 4],
            }
        )
        out = aggregate_panel_stratum_year(panel)
        assert len(out) == 4
        assert set(out.columns) == {"stratum", "borough", "inspection_year", "treated_rsbl", "building_count", "mean_violation_count", "total_violation_count"}

    def test_summarize_treated_control_balance(self):
        panel = pd.DataFrame(
            {
                "boro_block_lot": ["1", "2"],
                "treated_rsbl": [1, 0],
                "yearbuilt": [1920, 1980],
                "unitstotal": [10, 20],
                "mdr_registered": [1, 0],
            }
        )
        out = summarize_treated_control_balance(panel)
        assert out["treated_rsbl"].tolist() == [1, 0]
        assert out["buildings"].tolist() == [1, 1]


class TestMatchingHelpers:
    def _sample_matched_panel(self):
        return pd.DataFrame(
            {
                "boro_block_lot": ["t1", "t1", "c1", "c1", "t2", "t2", "c2", "c2"],
                "borough": ["MANHATTAN"] * 8,
                "communityboard": [101] * 8,
                "treated_rsbl": [1, 1, 0, 0, 1, 1, 0, 0],
                "mdr_registered": [1, 1, 1, 1, 1, 1, 1, 1],
                "inspection_year": [2019, 2020, 2019, 2020, 2019, 2020, 2019, 2020],
                "violation_count": [2, 4, 1, 2, 8, 10, 7, 9],
                "yearbuilt": [1920, 1920, 1925, 1925, 1950, 1950, 1955, 1955],
                "unitstotal": [10, 10, 11, 11, 20, 20, 18, 18],
                "yearbuilt_bin": ["prewar", "prewar", "prewar", "prewar", "1940_1969", "1940_1969", "1940_1969", "1940_1969"],
                "units_bin": ["6_19", "6_19", "6_19", "6_19", "20_49", "20_49", "20_49", "20_49"],
                "stratum": ["s1", "s1", "s1", "s1", "s2", "s2", "s2", "s2"],
            }
        )

    def test_build_preperiod_building_features(self):
        panel = self._sample_matched_panel()
        out = build_preperiod_building_features(panel, pre_years=(2019, 2020))
        t1 = out[out["boro_block_lot"] == "t1"].iloc[0]
        assert t1["pre_mean_violation_count"] == pytest.approx(3.0)
        assert t1["pre_total_violation_count"] == pytest.approx(6.0)
        assert t1["pre_nonzero_years"] == 2
        assert t1["pre_mean_bin"] == "2_5"
        assert t1["pre_total_bin"] == "3_6"

    def test_choose_nearest_control(self):
        controls = pd.DataFrame(
            {
                "boro_block_lot": ["c1", "c2"],
                "pre_mean_violation_count": [1.0, 10.0],
                "unitstotal": [10, 10],
                "yearbuilt": [1920, 1920],
            }
        )
        treated = pd.Series({"pre_mean_violation_count": 2.0, "unitstotal": 10, "yearbuilt": 1920})
        assert choose_nearest_control(treated, controls) == "c1"

    def test_choose_nearest_control_prefers_same_or_lower_pre_mean(self):
        controls = pd.DataFrame(
            {
                "boro_block_lot": ["c1", "c2"],
                "pre_mean_violation_count": [1.9, 2.1],
                "unitstotal": [20, 10],
                "yearbuilt": [1930, 1920],
            }
        )
        treated = pd.Series({"pre_mean_violation_count": 2.0, "unitstotal": 10, "yearbuilt": 1920})
        assert choose_nearest_control(treated, controls, prefer_same_or_lower_pre_mean=True) == "c1"

    def test_match_treated_to_controls(self):
        panel = self._sample_matched_panel()
        features = build_preperiod_building_features(panel, pre_years=(2019, 2020))
        out = match_treated_to_controls(features)
        assert len(out) == 2
        assert set(out["treated_boro_block_lot"]) == {"t1", "t2"}
        assert set(out["control_boro_block_lot"]) == {"c1", "c2"}

    def test_match_treated_to_controls_with_replacement(self):
        features = pd.DataFrame(
            {
                "boro_block_lot": ["t1", "t2", "c1"],
                "borough": ["MANHATTAN", "MANHATTAN", "MANHATTAN"],
                "communityboard": [101, 101, 101],
                "treated_rsbl": [1, 1, 0],
                "mdr_registered": [1, 1, 1],
                "yearbuilt": [1920, 1920, 1921],
                "unitstotal": [10, 10, 10],
                "yearbuilt_bin": ["prewar", "prewar", "prewar"],
                "units_bin": ["6_19", "6_19", "6_19"],
                "pre_mean_violation_count": [1.0, 1.2, 1.1],
                "pre_total_violation_count": [2.0, 2.4, 2.2],
                "pre_nonzero_years": [2, 2, 2],
                "pre_mean_bin": ["0.5_1", "1_2", "1_2"],
            }
        )
        out = match_treated_to_controls(
            features,
            exact_match_cols=("borough", "communityboard", "yearbuilt_bin", "units_bin"),
            allow_replacement=True,
        )
        assert len(out) == 2
        assert set(out["control_boro_block_lot"]) == {"c1"}

    def test_build_matched_pair_panel(self):
        panel = self._sample_matched_panel()
        matches = pd.DataFrame(
            {
                "match_id": ["t1__c1"],
                "treated_boro_block_lot": ["t1"],
                "control_boro_block_lot": ["c1"],
            }
        )
        out = build_matched_pair_panel(panel, matches)
        assert set(out["boro_block_lot"]) == {"t1", "c1"}
        assert set(out["match_role"]) == {"treated", "control"}

    def test_aggregate_matched_pair_year(self):
        panel = self._sample_matched_panel()
        matches = pd.DataFrame(
            {
                "match_id": ["t1__c1"],
                "treated_boro_block_lot": ["t1"],
                "control_boro_block_lot": ["c1"],
            }
        )
        matched = build_matched_pair_panel(panel, matches)
        out = aggregate_matched_pair_year(matched)
        assert len(out) == 4
        assert set(out["treated_rsbl"]) == {0, 1}


class TestPanelHelpers:
    def test_build_treated_year_event_design(self):
        panel = pd.DataFrame(
            {
                "treated_rsbl": [1, 1, 0, 0],
                "inspection_year": [2019, 2020, 2019, 2020],
            }
        )
        design, years = build_treated_year_event_design(panel, baseline_year=2019)
        assert years == [2020]
        assert design.columns.tolist() == ["treated_x_2020"]
        assert design["treated_x_2020"].tolist() == [0.0, 1.0, 0.0, 0.0]

    def test_two_way_demean_balanced_panel_zeroes_group_and_time_means(self):
        panel = pd.DataFrame(
            {
                "boro_block_lot": ["a", "a", "b", "b"],
                "inspection_year": [2019, 2020, 2019, 2020],
                "value": [1.0, 3.0, 2.0, 4.0],
            }
        )
        out = two_way_demean(
            panel,
            group_col="boro_block_lot",
            time_col="inspection_year",
            value_cols=("value",),
        )
        assert out.groupby("boro_block_lot")["value"].mean().round(8).tolist() == [0.0, 0.0]
        assert out.groupby("inspection_year")["value"].mean().round(8).tolist() == [0.0, 0.0]
