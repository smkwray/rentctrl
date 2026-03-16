from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from rent_control_public.west_hollywood import (
    _split_parcel_list,
    WeHoDownloadError,
    _is_header_or_footer,
    _parse_address_line,
    extract_appeals_from_minutes_text,
    match_buyouts,
    match_minutes_appeals,
    match_seismic,
    normalize_address,
    normalize_parcel,
    normalize_unit,
    _split_address_unit,
    parse_appeal_line,
    download_rso_pdf,
    parse_rso_text,
    summarize_appeal_match_types,
    summarize_buyout_footprint,
    summarize_seismic_footprint,
    summarize_stock_denominators,
    summarize_surface_match_rates,
    summarize_rso_stock,
)


class TestIsHeaderOrFooter:
    def test_header_line(self):
        assert _is_header_or_footer("Rent Stabilized Addresses") is True

    def test_page_line(self):
        assert _is_header_or_footer("Page 3") is True

    def test_city_header(self):
        assert _is_header_or_footer("City of West Hollywood") is True

    def test_data_line(self):
        assert _is_header_or_footer("1234 SUNSET BLVD 101 4337-001-001") is False


class TestParseAddressLine:
    def test_basic_line(self):
        result = _parse_address_line("1234 SUNSET BLVD  4337-001-002")
        assert result is not None
        assert result["address"] == "1234 SUNSET BLVD"
        assert result["parcel"] == "4337-001-002"

    def test_with_unit(self):
        result = _parse_address_line("1234 SUNSET BLVD APT 101  4337-001-002")
        assert result is not None
        assert result["address"] == "1234 SUNSET BLVD"
        assert result["unit"] == "101"
        assert result["parcel"] == "4337-001-002"

    def test_no_parcel(self):
        result = _parse_address_line("Just some text without a parcel")
        assert result is None

    def test_parcel_no_dashes(self):
        result = _parse_address_line("1234 SUNSET BLVD  4337001002")
        assert result is not None
        assert result["parcel"] == "4337-001-002"


class TestSplitAddressUnit:
    def test_with_hash(self):
        addr, unit = _split_address_unit("1234 SUNSET BLVD #101")
        assert addr == "1234 SUNSET BLVD"
        assert unit == "101"

    def test_with_apt(self):
        addr, unit = _split_address_unit("1234 SUNSET BLVD APT 2B")
        assert addr == "1234 SUNSET BLVD"
        assert unit == "2B"

    def test_no_unit(self):
        addr, unit = _split_address_unit("1234 SUNSET BLVD")
        assert addr == "1234 SUNSET BLVD"
        assert unit == ""

    def test_street_suffix_not_treated_as_unit(self):
        addr, unit = _split_address_unit("1234 SUNSET DR")
        assert addr == "1234 SUNSET DR"
        assert unit == ""


class TestNormalization:
    def test_normalize_address_strips_city_state_zip_and_abbreviates(self):
        value = "1279 N. Harper Avenue, West Hollywood, CA 90046"
        assert normalize_address(value) == "1279 N HARPER AVE"

    def test_normalize_unit(self):
        assert normalize_unit(" Apt. # 2B ") == "2B"

    def test_normalize_parcel(self):
        assert normalize_parcel("5528018044") == "5528-018-044"

    def test_split_parcel_list(self):
        assert _split_parcel_list("5554-001-014 - 5554-001-034") == [
            "5554-001-014",
            "5554-001-034",
        ]


class TestParseRsoText:
    def test_parses_lines(self):
        text = (
            "City of West Hollywood\n"
            "Rent Stabilized Addresses\n"
            "ADDRESS               UNIT    PARCEL\n"
            "1234 SUNSET BLVD      101     4337-001-001\n"
            "5678 SANTA MONICA     APT 2   4337-002-003\n"
            "\n"
        )
        df = parse_rso_text(text)
        assert len(df) == 2
        assert list(df.columns) == ["address", "unit", "parcel"]

    def test_empty_input(self):
        df = parse_rso_text("")
        assert df.empty


class TestDownloadRsoPdf:
    def test_403_raises_with_actionable_message(self, tmp_path):
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_resp.headers = {"server": "AkamaiGHost"}
        with patch("rent_control_public.west_hollywood.requests.get", return_value=mock_resp):
            with pytest.raises(WeHoDownloadError, match="Akamai"):
                download_rso_pdf(tmp_path / "test.pdf")

    def test_non_pdf_content_type_raises(self, tmp_path):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {"content-type": "text/html"}
        mock_resp.content = b"<html>not a pdf</html>"
        with patch("rent_control_public.west_hollywood.requests.get", return_value=mock_resp):
            with pytest.raises(WeHoDownloadError, match="content-type"):
                download_rso_pdf(tmp_path / "test.pdf")


class TestSummarizeRsoStock:
    def test_summary(self):
        df = pd.DataFrame(
            {
                "address": ["1234 SUNSET BLVD", "1234 SUNSET BLVD", "5678 SANTA MONICA"],
                "unit": ["101", "102", ""],
                "parcel": ["4337-001-001", "4337-001-001", "4337-002-003"],
            }
        )
        summary = summarize_rso_stock(df)
        assert len(summary) == 4
        row_map = dict(zip(summary["metric"], summary["value"]))
        assert row_map["total_rows"] == 3
        assert row_map["unique_addresses"] == 2
        assert row_map["unique_parcels"] == 2
        assert row_map["rows_with_unit"] == 2


class TestAppealParsing:
    def test_parse_appeal_line_with_unit(self):
        parsed = parse_appeal_line("A. D- 4930 1279 N. Harper Ave # 104")
        assert parsed == {
            "application_id": "D-4930",
            "appeal_address": "1279 N. Harper Ave",
            "appeal_unit": "104",
        }

    def test_extract_appeals_from_minutes_text(self):
        text = """
        8. APPEAL
        A. D- 4945 948 Hilldale Avenue
        B. D- 4722CD1 8221 De Longpre Avenue # 1
        """
        appeals = extract_appeals_from_minutes_text(text)
        assert appeals == [
            {
                "application_id": "D-4945",
                "appeal_address": "948 Hilldale Avenue",
                "appeal_unit": "",
            },
            {
                "application_id": "D-4722CD1",
                "appeal_address": "8221 De Longpre Avenue",
                "appeal_unit": "1",
            },
        ]


class TestLinkages:
    def test_match_buyouts(self):
        stock = pd.DataFrame(
            {
                "address": ["909 N Westbourne Dr", "501 Alfred St"],
                "unit": ["", "501"],
                "parcel": ["5528-001-001", "5528-018-044"],
            }
        )
        buyouts = pd.DataFrame(
            {
                "Address": [
                    "909 N. Westbourne Drive, West Hollywood, CA 90069",
                    "999 Missing St, West Hollywood, CA 90069",
                ],
                "normalized_address": [
                    normalize_address("909 N. Westbourne Drive, West Hollywood, CA 90069"),
                    normalize_address("999 Missing St, West Hollywood, CA 90069"),
                ],
            }
        )
        matched = match_buyouts(stock, buyouts)
        assert matched["matched_rso"].tolist() == [True, False]

    def test_match_seismic(self):
        stock = pd.DataFrame(
            {
                "address": ["1435 N Fairfax Ave"],
                "unit": [""],
                "parcel": ["5554-001-014"],
            }
        )
        seismic = pd.DataFrame(
            {
                "APN": ["5554-001-014 - 5554-001-034"],
                "normalized_parcel_list": [["5554-001-014", "5554-001-034"]],
            }
        )
        matched = match_seismic(stock, seismic)
        assert int(matched["matched_rso"].sum()) == 1

    def test_match_minutes_appeals(self):
        stock = pd.DataFrame(
            {
                "address": ["1279 N Harper Ave", "948 Hilldale Avenue"],
                "unit": ["104", ""],
                "parcel": ["5528-001-002", "5528-001-003"],
            }
        )
        appeals = pd.DataFrame(
            {
                "meeting_date": ["2025-03-13", "2025-04-10"],
                "application_id": ["D-4930", "D-4945"],
                "appeal_address": ["1279 N. Harper Ave", "948 Hilldale Avenue"],
                "appeal_unit": ["104", ""],
                "source_file": ["a.pdf", "b.pdf"],
            }
        )
        matched = match_minutes_appeals(stock, appeals)
        assert matched["matched_rso"].tolist() == [True, True]
        assert matched["match_type"].tolist()[0] == "address_and_unit"


class TestReportingSummaries:
    def test_stock_denominators(self):
        stock = pd.DataFrame(
            {
                "address": ["909 N Westbourne Dr", "909 N Westbourne Dr", "501 Alfred St"],
                "unit": ["", "2", "501"],
                "parcel": ["5528-001-001", "5528-001-001", "5528-018-044"],
            }
        )
        out = summarize_stock_denominators(stock)
        values = dict(zip(out["metric"], out["value"]))
        assert values["rso_unit_rows"] == 3
        assert values["rso_unique_addresses"] == 2
        assert values["rso_unique_parcels"] == 2
        assert values["rso_rows_with_unit"] == 2

    def test_surface_match_rates(self):
        stock = pd.DataFrame(
            {
                "address": ["909 N Westbourne Dr", "501 Alfred St"],
                "unit": ["", "501"],
                "parcel": ["5528-001-001", "5528-018-044"],
            }
        )
        buyout_matches = pd.DataFrame(
            {
                "normalized_address": ["909 N WESTBOURNE DR", "999 MISSING ST"],
                "matched_rso": [True, False],
            }
        )
        seismic_matches = pd.DataFrame(
            {
                "normalized_parcel": ["5528-001-001", "0000-000-000"],
                "address": ["909 N Westbourne Dr", ""],
                "matched_rso": [True, False],
            }
        )
        appeal_matches = pd.DataFrame(
            {
                "normalized_address": ["909 N WESTBOURNE DR", "999 MISSING ST"],
                "matched_rso": [True, False],
            }
        )
        out = summarize_surface_match_rates(stock, buyout_matches, seismic_matches, appeal_matches)
        assert set(out["surface"]) == {"commission_appeals", "buyouts", "seismic"}
        buyouts = out.loc[out["surface"] == "buyouts"].iloc[0]
        assert buyouts["matched_rows"] == 1
        assert buyouts["matched_unique_addresses"] == 1

    def test_buyout_footprint(self):
        matches = pd.DataFrame(
            {
                "normalized_address": ["909 N WESTBOURNE DR", "909 N WESTBOURNE DR", "999 MISSING ST"],
                "matched_rso": [True, True, False],
                "rso_parcel_count": [1, 1, None],
                "rso_unit_count": [4, 4, None],
            }
        )
        out = summarize_buyout_footprint(matches)
        assert len(out) == 1
        assert out.loc[0, "buyout_rows"] == 2
        assert out.loc[0, "rso_unit_count"] == 4

    def test_seismic_footprint(self):
        matches = pd.DataFrame(
            {
                "normalized_parcel": ["5528-001-001", "5528-001-001", "0000-000-000"],
                "address": ["909 N Westbourne Dr", "909 N Westbourne Dr", ""],
                "matched_rso": [True, True, False],
                "rso_unit_count": [4, 4, None],
            }
        )
        out = summarize_seismic_footprint(matches)
        assert len(out) == 1
        assert out.loc[0, "seismic_rows"] == 2
        assert out.loc[0, "rso_unit_count"] == 4

    def test_appeal_match_types(self):
        matches = pd.DataFrame(
            {
                "application_id": ["D1", "D2", "D3"],
                "match_type": ["address_and_unit", "address_only", None],
            }
        )
        out = summarize_appeal_match_types(matches)
        counts = dict(zip(out["match_type"], out["appeal_rows"]))
        assert counts["address_and_unit"] == 1
        assert counts["address_only"] == 1
        assert counts["unmatched"] == 1
