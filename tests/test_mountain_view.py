import base64

from rent_control_public.mountain_view import (
    PUBLIC_CITY_DIRECTORY,
    PUBLIC_CITY_SERVICES,
    build_search_payload,
    parse_search_results,
    summarize_keyword_batch,
    summarize_results,
)


def test_basic_restapp_header_roundtrip():
    from rent_control_public.mountain_view import basic_restapp_header

    header = basic_restapp_header()
    assert header.startswith("Basic ")
    decoded = base64.b64decode(header.split()[1]).decode("ascii")
    assert decoded == "restapp:restapp"


def test_build_search_payload():
    payload = build_search_payload("301", page_number=2, page_size=7)
    assert payload["keyword"] == "301"
    assert payload["pageNumber"] == 2
    assert payload["pageSize"] == 7
    assert payload["cityService"] == PUBLIC_CITY_SERVICES
    assert payload["cityDirectory"] == PUBLIC_CITY_DIRECTORY


def test_parse_search_results_extracts_apn_and_case_fields():
    response = {
        "response": [
            {
                "title": "19303017",
                "type": "APN",
                "matchingCount": 0,
                "parameters": {
                    "additional": {
                        "apnNumber": "19303017",
                        "assetType": "Fully Covered Rental Property",
                        "totalUnits": 12,
                        "siteAddresses": [
                            {
                                "fullAddress": "1002 BORANDA AV, MOUNTAIN VIEW, CA 94040",
                                "houseNumber": "1002",
                                "streetName": "BORANDA",
                                "streetTypeCd": "AV",
                                "city": "MOUNTAIN VIEW",
                                "state": "CA",
                                "zip": "94040",
                            }
                        ],
                    }
                },
            },
            {
                "title": "CiR2026-262301",
                "type": "CASE",
                "matchingCount": 0,
                "parameters": {
                    "additional": {
                        "caseId": "CiR2026-262301",
                        "caseType": "Change in Rent",
                        "category": "Rent Registry",
                        "address": "413 N RENGSTORFF AV, MOUNTAIN VIEW, CA 94043",
                        "otherAttributes": {"annualCycleTag": "FY 2025-26", "caseViewType": "ANNUAL", "note": "Inaccurate old rent"},
                    }
                },
            },
        ]
    }
    df = parse_search_results(response)
    assert len(df) == 2
    apn = df[df["type"] == "APN"].iloc[0]
    assert apn["apn_number"] == "19303017"
    assert apn["asset_type"] == "Fully Covered Rental Property"
    assert apn["full_address"].startswith("1002 BORANDA")
    case = df[df["type"] == "CASE"].iloc[0]
    assert case["case_id"] == "CiR2026-262301"
    assert case["annual_cycle_tag"] == "FY 2025-26"


def test_summarize_results():
    response = {"count": {"APN": 20, "CASE": 3, "ADDRESS": 0}}
    df = parse_search_results(
        {
            "response": [
                {
                    "title": "19303017",
                    "type": "APN",
                    "matchingCount": 0,
                    "parameters": {"additional": {"assetType": "Fully Covered Rental Property"}},
                },
                {
                    "title": "CASE1",
                    "type": "CASE",
                    "matchingCount": 0,
                    "parameters": {"additional": {"caseId": "CASE1"}},
                },
            ]
        }
    )
    summary = summarize_results(df, keyword="301", count_json=response)
    lookup = dict(zip(summary["metric"], summary["value"]))
    assert lookup["keyword"] == "301"
    assert lookup["rows_returned"] == 2
    assert lookup["apn_rows"] == 1
    assert lookup["case_rows"] == 1
    assert lookup["count_apn"] == 20


def test_summarize_keyword_batch():
    results = parse_search_results(
        {
            "response": [
                {"title": "A1", "type": "APN", "matchingCount": 0, "parameters": {"additional": {"assetType": "Fully Covered Rental Property"}}},
                {"title": "C1", "type": "CASE", "matchingCount": 0, "parameters": {"additional": {"caseType": "Unit Certification"}}},
            ]
        }
    )
    results["search_keyword"] = "301"
    per_keyword, case_types, asset_types = summarize_keyword_batch(results, {"301": {"count": {"APN": 1, "CASE": 0, "ADDRESS": 0}}})
    row = per_keyword.iloc[0]
    assert row["keyword"] == "301"
    assert row["rows_returned"] == 2
    assert row["apn_rows"] == 1
    assert row["case_rows"] == 1
    assert row["count_case"] == 0
    assert case_types.iloc[0]["case_type"] == "Unit Certification"
    assert asset_types.iloc[0]["asset_type"] == "Fully Covered Rental Property"
