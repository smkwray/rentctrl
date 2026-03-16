import pandas as pd

from rent_control_public.los_angeles import (
    _assessor_get,
    _build_ain_exact_clause,
    _build_ain_in_clause,
    _build_ain_window_clause,
    _fetch_assessor_rows_for_ain,
    build_query_coverage_summary,
    build_sample_strata_summary,
    fetch_assessor_records_for_ains,
    build_sample_assessor_group_comparison,
    classify_assessor_proxy,
    build_property_level_case_summary,
    build_registration_comparison,
    case_type_counts,
    merge_sample_with_assessor,
    parse_property_cases,
    parse_property_info,
    parse_property_search_results,
    summarize_sample_assessor_backbone,
    summarize_property_activity_sample,
    summarize_property_activity,
)


SEARCH_HTML = """
<table id="dgProperty2">
  <thead><tr><th>Action</th><th>APN</th><th>ADDRESS</th></tr></thead>
  <tbody>
    <tr>
      <td><a href="javascript:WebForm_DoPostBackWithOptions(new WebForm_PostBackOptions(&quot;dgProperty2$ctl02$lnkSelectProp&quot;, &quot;&quot;, true, &quot;&quot;, &quot;&quot;, false, true))">Select</a></td>
      <td>1234567890</td>
      <td>100 S TEST ST, LOS ANGELES, CA 90012</td>
    </tr>
    <tr>
      <td><a href="javascript:WebForm_DoPostBackWithOptions(new WebForm_PostBackOptions(&quot;dgProperty2$ctl03$lnkSelectProp&quot;, &quot;&quot;, true, &quot;&quot;, &quot;&quot;, false, true))">Select</a></td>
      <td>9999999999</td>
      <td>200 S TEST ST, LOS ANGELES, CA 90012</td>
    </tr>
  </tbody>
</table>
"""

DETAIL_HTML = """
<span id="lblAPN2">1234567890</span>
<span id="lblTotalPropUnits">12</span>
<span id="lblRSU">0001111</span>
<span id="lblCT">207500</span>
<span id="lblCD">14</span>
<span id="lblAddress">100 S TEST ST, Los Angeles, CA 90012</span>
<span id="lblSCEPExemptions">1</span>
<span id="lblRentOfficeID">Central</span>
<span id="lblCodeRegionalArea">East Regional Office</span>
<span id="lblYear">1920</span>
<table id="dgPropCases2">
  <thead><tr><th>Action</th><th>Case Type</th><th>Case Number</th><th>Date Closed</th></tr></thead>
  <tbody>
    <tr><td>Select</td><td>Complaint</td><td>123</td><td>01/01/2020</td></tr>
    <tr><td>Select</td><td>Systematic Code Enforcement Program</td><td>456</td><td>01/01/2021</td></tr>
  </tbody>
</table>
"""


def test_parse_property_search_results() -> None:
    out = parse_property_search_results(SEARCH_HTML)
    assert out["event_target"].tolist() == [
        "dgProperty2$ctl02$lnkSelectProp",
        "dgProperty2$ctl03$lnkSelectProp",
    ]
    assert out["apn"].tolist() == ["1234567890", "9999999999"]


def test_parse_property_info() -> None:
    out = parse_property_info(DETAIL_HTML)
    assert out["apn"] == "1234567890"
    assert out["total_units"] == "12"
    assert out["rent_registration_number"] == "0001111"
    assert out["year_built"] == "1920"


def test_parse_property_cases() -> None:
    out = parse_property_cases(DETAIL_HTML)
    assert out["case_type"].tolist() == ["Complaint", "Systematic Code Enforcement Program"]
    assert out["case_number"].tolist() == ["123", "456"]


def test_summarize_property_activity() -> None:
    property_df = pd.DataFrame(
        [
            {
                "rent_registration_number": "0001111",
                "total_units": "12",
                "year_built": "1920",
            },
            {
                "rent_registration_number": "",
                "total_units": "6",
                "year_built": "1950",
            },
        ]
    )
    case_df = pd.DataFrame(
        [
            {"apn": "1", "case_type": "Complaint"},
            {"apn": "1", "case_type": "Complaint"},
            {"apn": "2", "case_type": "Systematic Code Enforcement Program"},
        ]
    )
    out = summarize_property_activity(property_df, case_df, street_name="MAIN")
    metrics = dict(zip(out["metric"], out["value"]))
    assert metrics["properties_sampled"] == 2
    assert metrics["properties_with_rent_registration"] == 1
    assert metrics["total_cases"] == 3
    assert metrics["case_type_complaint"] == 2


def test_case_type_counts() -> None:
    case_df = pd.DataFrame(
        [
            {"apn": "1", "case_type": "Complaint", "case_number": "123"},
            {"apn": "1", "case_type": "Complaint", "case_number": "124"},
            {"apn": "2", "case_type": "Systematic Code Enforcement Program", "case_number": "125"},
        ]
    )
    out = case_type_counts(case_df)
    assert out.loc[0, "case_type"] == "Complaint"
    assert out.loc[0, "case_count"] == 2


def test_build_property_level_case_summary() -> None:
    case_df = pd.DataFrame(
        [
            {"apn": "1", "official_address": "A", "case_type": "Complaint", "case_number": "123", "date_closed": "01/01/2020"},
            {"apn": "1", "official_address": "A", "case_type": "Systematic Code Enforcement Program", "case_number": "124", "date_closed": "01/01/2021"},
            {"apn": "2", "official_address": "B", "case_type": "Complaint", "case_number": "125", "date_closed": "01/01/2019"},
        ]
    )
    out = build_property_level_case_summary(case_df)
    row = out.loc[out["apn"] == "1"].iloc[0]
    assert row["total_cases"] == 2
    assert row["complaint_cases"] == 1
    assert row["scep_cases"] == 1
    assert row["first_case_year"] == 2020
    assert row["last_case_year"] == 2021


def test_build_registration_comparison() -> None:
    property_df = pd.DataFrame(
        [
            {"apn": "1", "official_address": "A", "rent_registration_number": "100", "total_units": "10", "year_built": "1920"},
            {"apn": "2", "official_address": "B", "rent_registration_number": "", "total_units": "5", "year_built": "1950"},
        ]
    )
    case_df = pd.DataFrame(
        [
            {"apn": "1", "official_address": "A", "case_type": "Complaint", "case_number": "123", "date_closed": "01/01/2020"},
            {"apn": "2", "official_address": "B", "case_type": "Systematic Code Enforcement Program", "case_number": "124", "date_closed": "01/01/2021"},
        ]
    )
    out = build_registration_comparison(property_df, case_df)
    assert set(out["group"]) == {"registered", "not_registered"}
    registered = out.loc[out["group"] == "registered"].iloc[0]
    assert registered["properties"] == 1
    assert registered["mean_total_cases"] == 1


def test_build_query_coverage_summary() -> None:
    property_df = pd.DataFrame(
        [
            {"apn": "1", "official_address": "A", "query_street_name": "MAIN", "rent_registration_number": "100", "detail_error": ""},
            {"apn": "2", "official_address": "B", "query_street_name": "MAIN", "rent_registration_number": "", "detail_error": "timeout"},
            {"apn": "3", "official_address": "C", "query_street_name": "BROADWAY", "rent_registration_number": "", "detail_error": ""},
        ]
    )
    case_df = pd.DataFrame(
        [
            {"apn": "1", "official_address": "A", "case_type": "Complaint", "case_number": "123", "date_closed": "01/01/2020"},
            {"apn": "3", "official_address": "C", "case_type": "Complaint", "case_number": "124", "date_closed": "01/01/2020"},
        ]
    )
    query_df = pd.DataFrame(
        [
            {"street_name": "MAIN", "search_results_sampled": 2, "query_error": ""},
            {"street_name": "BROADWAY", "search_results_sampled": 1, "query_error": ""},
        ]
    )
    out = build_query_coverage_summary(property_df, case_df, query_df)
    main = out.loc[out["query_street_name"] == "MAIN"].iloc[0]
    assert main["sample_properties"] == 2
    assert main["properties_with_case_history"] == 1
    assert main["properties_with_rent_registration"] == 1
    assert main["detail_errors"] == 1


def test_build_sample_strata_summary() -> None:
    merged_df = pd.DataFrame(
        [
            {
                "apn": "1",
                "official_address": "A",
                "rent_registration_number": "100",
                "AIN": "1",
                "rso_eligible_proxy": True,
                "Units": 10,
                "YearBuilt": 1920,
            },
            {
                "apn": "2",
                "official_address": "B",
                "rent_registration_number": "",
                "AIN": "2",
                "rso_eligible_proxy": False,
                "Units": 5,
                "YearBuilt": 1985,
            },
        ]
    )
    case_df = pd.DataFrame(
        [
            {"apn": "1", "official_address": "A", "case_type": "Complaint", "case_number": "123", "date_closed": "01/01/2020"},
        ]
    )
    out = build_sample_strata_summary(merged_df, case_df)
    groups = set(out["sample_group"])
    assert {"all_sampled", "with_case_history", "with_rent_registration", "rso_eligible_proxy"} <= groups
    all_sampled = out.loc[out["sample_group"] == "all_sampled"].iloc[0]
    assert all_sampled["properties"] == 2
    with_case_history = out.loc[out["sample_group"] == "with_case_history"].iloc[0]
    assert with_case_history["properties"] == 1


def test_summarize_property_activity_sample() -> None:
    property_df = pd.DataFrame(
        [
            {"apn": "1", "official_address": "A", "rent_registration_number": "100", "total_units": "10", "year_built": "1920"},
            {"apn": "2", "official_address": "B", "rent_registration_number": "", "total_units": "5", "year_built": "1950"},
        ]
    )
    case_df = pd.DataFrame(
        [
            {"apn": "1", "official_address": "A", "case_type": "Complaint", "case_number": "123", "date_closed": "01/01/2020"},
            {"apn": "2", "official_address": "B", "case_type": "Systematic Code Enforcement Program", "case_number": "124", "date_closed": "01/01/2021"},
        ]
    )
    query_df = pd.DataFrame([{"street_name": "MAIN", "search_results_sampled": 2}])
    out = summarize_property_activity_sample(property_df, case_df, query_df)
    metrics = dict(zip(out["metric"], out["value"]))
    assert metrics["street_queries"] == 1
    assert metrics["properties_with_rent_registration"] == 1
    assert metrics["properties_without_rent_registration"] == 1


def test_classify_assessor_proxy() -> None:
    frame = pd.DataFrame(
        [
            {"AIN": "1", "TaxRateArea_CITY": "LOS ANGELES", "Units": 4, "YearBuilt": "1970"},
            {"AIN": "2", "TaxRateArea_CITY": "LOS ANGELES", "Units": 4, "YearBuilt": "1985"},
            {"AIN": "3", "TaxRateArea_CITY": "PASADENA", "Units": 4, "YearBuilt": "1970"},
        ]
    )
    out = classify_assessor_proxy(frame)
    assert out["rso_eligible_proxy"].tolist() == [True, False, False]


def test_merge_sample_with_assessor() -> None:
    property_df = pd.DataFrame(
        [
            {"apn": "5127028003", "official_address": "A"},
            {"apn": "5111024001", "official_address": "B"},
        ]
    )
    assessor_df = pd.DataFrame(
        [
            {"AIN": "5127028003", "TaxRateArea_CITY": "LOS ANGELES", "Units": 20, "YearBuilt": "1913"},
        ]
    )
    out = merge_sample_with_assessor(property_df, assessor_df)
    assert len(out) == 2
    assert out.loc[out["apn"] == "5127028003", "AIN"].iloc[0] == "5127028003"


def test_build_ain_window_clause() -> None:
    assert _build_ain_window_clause("5127028003") == "(AIN > '5127028002' AND AIN < '5127028004')"
    assert _build_ain_window_clause("ABC") == "(AIN >= 'ABC' AND AIN <= 'ABC')"


def test_build_ain_exact_clause() -> None:
    assert _build_ain_exact_clause("5127028003") == "AIN = '5127028003'"
    assert _build_ain_exact_clause("AB'C") == "AIN = 'AB''C'"


def test_build_ain_in_clause() -> None:
    assert _build_ain_in_clause(["1", "2"]) == "AIN IN ('1','2')"
    assert _build_ain_in_clause(["AB'C"]) == "AIN IN ('AB''C')"


def test_fetch_assessor_rows_for_ain_prefers_exact(monkeypatch) -> None:
    calls: list[str] = []

    def fake_assessor_get(params, *, timeout):
        calls.append(str(params["where"]))
        return {"features": [{"attributes": {"AIN": "5127028003"}}]}

    monkeypatch.setattr("rent_control_public.los_angeles._assessor_get", fake_assessor_get)
    payload = _fetch_assessor_rows_for_ain("5127028003", fields=["AIN"], timeout=5)
    assert payload["features"][0]["attributes"]["AIN"] == "5127028003"
    assert calls == ["AIN = '5127028003'"]


def test_fetch_assessor_rows_for_ain_falls_back_to_window(monkeypatch) -> None:
    calls: list[str] = []

    def fake_assessor_get(params, *, timeout):
        calls.append(str(params["where"]))
        if len(calls) == 1:
            return {"features": []}
        return {"features": [{"attributes": {"AIN": "5127028003"}}]}

    monkeypatch.setattr("rent_control_public.los_angeles._assessor_get", fake_assessor_get)
    payload = _fetch_assessor_rows_for_ain("5127028003", fields=["AIN"], timeout=5)
    assert payload["features"][0]["attributes"]["AIN"] == "5127028003"
    assert calls == [
        "AIN = '5127028003'",
        "(AIN > '5127028002' AND AIN < '5127028004')",
    ]


def test_fetch_assessor_records_for_ains_recovers_from_batch_failure(monkeypatch) -> None:
    def fake_assessor_get(params, *, timeout):
        where = str(params["where"])
        if where.startswith("AIN IN"):
            raise RuntimeError("batch failed")
        return {"features": [{"attributes": {"AIN": where.split("'")[1], "RollYear": "2025"}}]}

    monkeypatch.setattr("rent_control_public.los_angeles._assessor_get", fake_assessor_get)
    out = fetch_assessor_records_for_ains(["1", "2"], roll_year="2025", timeout=5, out_fields=["AIN", "RollYear"])
    assert set(out["AIN"]) == {"1", "2"}


def test_fetch_assessor_records_for_ains_batches_then_falls_back(monkeypatch) -> None:
    calls: list[str] = []

    def fake_assessor_get(params, *, timeout):
        where = str(params["where"])
        calls.append(where)
        if where.startswith("AIN IN"):
            return {"features": [{"attributes": {"AIN": "1", "RollYear": "2025"}}]}
        if where == "AIN = '2'":
            return {"features": []}
        if where == "(AIN > '1' AND AIN < '3')":
            return {"features": [{"attributes": {"AIN": "2", "RollYear": "2025"}}]}
        raise AssertionError(where)

    monkeypatch.setattr("rent_control_public.los_angeles._assessor_get", fake_assessor_get)
    out = fetch_assessor_records_for_ains(["1", "2"], roll_year="2025", timeout=5, out_fields=["AIN", "RollYear"])
    assert sorted(out["AIN"].tolist()) == ["1", "2"]
    assert calls == [
        "AIN IN ('1','2')",
        "AIN = '2'",
        "(AIN > '1' AND AIN < '3')",
    ]


def test_assessor_get_prefers_post(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"features": [{"attributes": {"AIN": "1"}}]}

    class FakeSession:
        def post(self, url, data, headers, timeout):
            calls.append(("post", data))
            return FakeResponse()

        def get(self, url, params, headers, timeout):
            calls.append(("get", params))
            return FakeResponse()

    monkeypatch.setattr("rent_control_public.los_angeles._get_assessor_session", lambda: FakeSession())
    payload = _assessor_get({"f": "json", "where": "AIN = '1'"}, timeout=5)
    assert payload["features"][0]["attributes"]["AIN"] == "1"
    assert calls == [("post", {"f": "json", "where": "AIN = '1'"})]


def test_summarize_sample_assessor_backbone() -> None:
    merged_df = pd.DataFrame(
        [
            {"apn": "1", "AIN": "1", "rso_eligible_proxy": True, "multifamily_proxy": True},
            {"apn": "2", "AIN": "", "rso_eligible_proxy": False, "multifamily_proxy": False},
        ]
    )
    case_df = pd.DataFrame([{"apn": "1"}, {"apn": "1"}])
    out = summarize_sample_assessor_backbone(merged_df, case_df, roll_year="2025")
    metrics = dict(zip(out["metric"], out["value"]))
    assert metrics["roll_year"] == "2025"
    assert metrics["sample_assessor_matches"] == 1
    assert metrics["sample_rso_eligible_proxy"] == 1


def test_build_sample_assessor_group_comparison() -> None:
    merged_df = pd.DataFrame(
        [
            {"apn": "1", "official_address": "A", "rso_eligible_proxy": True, "Units": 10, "YearBuilt": "1920"},
            {"apn": "2", "official_address": "B", "rso_eligible_proxy": False, "Units": 5, "YearBuilt": "1985"},
        ]
    )
    case_df = pd.DataFrame(
        [
            {"apn": "1", "official_address": "A", "case_type": "Complaint", "case_number": "123", "date_closed": "01/01/2020"},
            {"apn": "2", "official_address": "B", "case_type": "Systematic Code Enforcement Program", "case_number": "124", "date_closed": "01/01/2021"},
        ]
    )
    out = build_sample_assessor_group_comparison(merged_df, case_df)
    assert set(out["group"]) == {"rso_eligible_proxy", "not_rso_eligible_proxy"}
