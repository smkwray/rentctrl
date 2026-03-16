import pandas as pd

from rent_control_public.oakland import (
    build_detail_join_key,
    build_rap_search_universe,
    build_search_payload,
    build_code_enforcement_address_summary,
    extract_next_page_target,
    extract_form_state,
    fetch_search_results_page,
    match_rap_to_code_enforcement,
    normalize_address,
    parse_case_detail,
    parse_counter_message,
    parse_search_results,
    summarize_rap_code_enforcement_by_year,
    summarize_rap_coverage,
    summarize_rap_detail_by_year,
    summarize_rap_hearing_officers,
    summarize_rap_progress_activity,
    summarize_ground_search,
)

SEARCH_HTML = '''
<form>
<input type="hidden" name="__OSVSTATE" value="abc123" />
<input type="hidden" name="__VIEWSTATE" value="vs" />
<div class="Counter_Message">1&nbsp;to&nbsp;25&nbsp;of&nbsp;69&nbsp;records</div>
<table id="wt89_OutSystemsUIWeb_wt10_block_wtContent_wtMainContent_wtCaseDataTable">
<tbody>
<tr>
<td class="TableRecords_OddLine"><div align="left"><a href="javascript:__doPostBack(&#39;wt89$OutSystemsUIWeb_wt10$block$wtContent$wtMainContent$wtCaseDataTable$ctl03$wt171&#39;,&#39;&#39;)">Case Number: T26-0027<div><span class="Bold">Petition: Tenant 18327</span></div></a></div></td>
<td class="TableRecords_OddLine"><div align="left">01-30-2026</div></td>
<td class="TableRecords_OddLine">Document(s) Submitted</td>
<td class="TableRecords_OddLine"><div align="left">Linda Moroz</div></td>
<td class="TableRecords_OddLine">In-process</td>
<td class="TableRecords_OddLine">&nbsp;-Notice to Tenants<br/>&nbsp;-Code Violation<br/>&nbsp;-Fewer housing services<br/></td>
<td class="TableRecords_OddLine"><a href="javascript:__doPostBack(&#39;wt89$OutSystemsUIWeb_wt10$block$wtContent$wtMainContent$wtCaseDataTable$ctl03$wt109&#39;,&#39;&#39;)">View</a></td>
</tr>
</tbody>
</table>
</form>
'''

DETAIL_HTML = '''
<div><div class="font-semi-bold"><span style="color: #222;">Case Number</span></div><div><span class="Bold">T26-0027</span></div></div>
<div><div class="font-semi-bold">Petition</div><div>Tenant: 18327</div></div>
<div><div class="font-semi-bold">Date Filed</div><div class="ThemeGrid_Width7">01-30-2026</div></div>
<div><div class="font-semi-bold">Property Address</div><div>500 Vernon Street</div></div>
<div><div class="font-semi-bold">APN</div><div class="ThemeGrid_Width7">010 082903900</div></div>
<div><div class="font-semi-bold">Hearing Date</div><div class="ThemeGrid_Width7">04-13-2026</div></div>
<div><div class="font-semi-bold">Mediation Date</div><div></div></div>
<div><div class="font-semi-bold">Appeal Hearing Date</div><div></div></div>
<div><div class="font-semi-bold">Hearing Officer</div><div>Linda Moroz</div></div>
<div><div class="font-semi-bold"><span style="color: #222;">Program Analyst</span></div><div>Anali Valdez</div></div>
<table id="wtTenantPetitionGroundsTable"><tbody>
<tr><td class="TableRecords_OddLine">Notice to Tenants</td><td class="TableRecords_OddLine">Notice description</td></tr>
<tr><td class="TableRecords_EvenLine">Code Violation</td><td class="TableRecords_EvenLine">Code description</td></tr>
</tbody></table>
<table id="wtCaseActivityStatusTable"><tbody>
<tr><td class="TableRecords_OddLine">Petition submitted</td><td class="TableRecords_OddLine">Submitted</td><td class="TableRecords_OddLine"><div align="left">01-30-2026</div></td></tr>
<tr><td class="TableRecords_EvenLine">Mediation</td><td class="TableRecords_EvenLine">Completed</td><td class="TableRecords_EvenLine"><div align="left">02-03-2026</div></td></tr>
</tbody></table>
'''


def test_extract_form_state():
    state = extract_form_state(SEARCH_HTML)
    assert state["__OSVSTATE"] == "abc123"
    assert state["__VIEWSTATE"] == "vs"


def test_build_search_payload_sets_ground_and_button():
    payload = build_search_payload(SEARCH_HTML, tenant_ground_value="1008", keywords="Vernon")
    assert payload["__OSVSTATE"] == "abc123"
    assert payload[
        "wt89$OutSystemsUIWeb_wt10$block$wtContent$wtMainContent$OutSystemsUIWeb_wtFiltersCard$block$wtContent$OutSystemsUIWeb_wt12$block$wtColumn2$wtTenant_PeititionGroundType_Filter"
    ] == "1008"
    assert payload[
        "wt89$OutSystemsUIWeb_wt10$block$wtContent$wtMainContent$OutSystemsUIWeb_wtFiltersCard$block$wtContent$OutSystemsUIWeb_wt35$block$wtColumn2$wt177"
    ] == "Search"


def test_parse_counter_message():
    assert parse_counter_message(SEARCH_HTML) == "1 to 25 of 69 records"


def test_extract_next_page_target():
    html = (
        '<a class="ListNavigation_Next" '
        'onclick="OsAjax(arguments[0] || window.event,&#39;ignored&#39;,&#39;'
        'wt89$OutSystemsUIWeb_wt10$block$wtContent$wtMainContent$RichWidgets_wt98$block$wt28'
        '&#39;,&#39;&#39;,&#39;__OSVSTATE,&#39;,&#39;&#39;); return false;" href="#">next</a>'
    )
    assert extract_next_page_target(html).endswith("$wt28")


def test_parse_search_results():
    df = parse_search_results(SEARCH_HTML, ground_filter="code_violation", ground_value="1008")
    assert len(df) == 1
    row = df.iloc[0]
    assert row["case_number"] == "T26-0027"
    assert row["petition"] == "Tenant 18327"
    assert row["case_status"] == "In-process"
    assert row["petition_grounds"] == "Notice to Tenants | Code Violation | Fewer housing services"
    assert row["detail_event_target"].endswith("$ctl03$wt109")


def test_parse_case_detail():
    detail, progress = parse_case_detail(DETAIL_HTML, ground_filter="code_violation", ground_value="1008")
    assert detail["case_number"] == "T26-0027"
    assert detail["property_address"] == "500 Vernon Street"
    assert detail["apn"] == "010 082903900"
    assert detail["case_grounds"] == "Notice to Tenants | Code Violation"
    assert len(progress) == 2
    assert progress.iloc[0]["activity"] == "Petition submitted"


def test_summarize_ground_search():
    search_df = pd.DataFrame(
        [
            {
                "ground_filter": "code_violation",
                "ground_value": "1008",
                "counter_message": "1 to 25 of 69 records",
                "case_number": "T26-0027",
            },
            {
                "ground_filter": "code_violation",
                "ground_value": "1008",
                "counter_message": "1 to 25 of 69 records",
                "case_number": "T26-0010",
            },
        ]
    )
    detail_df = pd.DataFrame(
        [
            {"ground_filter": "code_violation", "ground_value": "1008", "case_number": "T26-0027"},
        ]
    )
    summary = summarize_ground_search(search_df, detail_df)
    assert len(summary) == 1
    row = summary.iloc[0]
    assert row["search_results_returned"] == 2
    assert row["detail_cases_fetched"] == 1


def test_normalize_address_drops_city_and_unit():
    assert normalize_address("601 BROOKLYN AVE 203") == "601 BROOKLYN AVE"
    assert normalize_address("925 East 11th Street, Oakland, CA 94606") == "925 EAST 11TH ST"


def test_build_code_enforcement_address_summary():
    detail_df = pd.DataFrame(
        [
            {"property_address": "601 BROOKLYN AVE 203", "petition": "Tenant: 18342", "case_grounds": "Fewer housing services"},
            {"property_address": "601 BROOKLYN AVE 204", "petition": "Tenant: 18343", "case_grounds": "Fewer housing services"},
            {"property_address": "", "petition": "Tenant: 18344", "case_grounds": "Fewer housing services"},
        ]
    )
    summary = build_code_enforcement_address_summary(detail_df)
    assert len(summary) == 1
    row = summary.iloc[0]
    assert row["normalized_address"] == "601 BROOKLYN AVE"
    assert row["rap_case_rows"] == 2


def test_match_rap_to_code_enforcement():
    detail_df = pd.DataFrame(
        [
            {"property_address": "601 BROOKLYN AVE 203", "petition": "Tenant: 18342", "case_grounds": "Fewer housing services"},
            {"property_address": "500 Vernon Street", "petition": "Tenant: 18327", "case_grounds": "Code Violation"},
        ]
    )
    requests_df = pd.DataFrame(
        [
            {"requestid": 1, "datetimeinit": "2026-01-20", "status": "REFERRED", "probaddress": "601 BROOKLYN AVE", "normalized_address": "601 BROOKLYN AVE"},
            {"requestid": 2, "datetimeinit": "2026-01-21", "status": "CLOSED", "probaddress": "999 MISSING ST", "normalized_address": "999 MISSING ST"},
        ]
    )
    matches, address_summary, overall = match_rap_to_code_enforcement(detail_df, requests_df)
    assert len(matches) == 1
    assert matches.iloc[0]["normalized_address"] == "601 BROOKLYN AVE"
    row = address_summary[address_summary["normalized_address"] == "601 BROOKLYN AVE"].iloc[0]
    assert row["code_enforcement_requests"] == 1
    overall_map = dict(zip(overall["metric"], overall["value"]))
    assert overall_map["matched_request_rows"] == 1


def test_summarize_rap_detail_by_year():
    detail_df = pd.DataFrame(
        [
            {"ground_filter": "code_violation", "date_filed": "01-30-2026", "case_number": "A", "property_address": "500 Vernon Street", "apn": "010"},
            {"ground_filter": "code_violation", "date_filed": "02-01-2026", "case_number": "B", "property_address": "500 Vernon Street", "apn": ""},
            {"ground_filter": "fewer_housing_services", "date_filed": "12-31-2025", "case_number": "C", "property_address": "601 BROOKLYN AVE 203", "apn": "011"},
        ]
    )
    out = summarize_rap_detail_by_year(detail_df)
    row = out.loc[(out["ground_filter"] == "code_violation") & (out["date_year"] == 2026)].iloc[0]
    assert row["case_rows"] == 2
    assert row["unique_cases"] == 2
    assert row["unique_addresses"] == 1
    assert row["addresses_with_apn"] == 1


def test_summarize_rap_progress_activity():
    progress_df = pd.DataFrame(
        [
            {"ground_filter": "code_violation", "activity": "Mediation", "status": "Completed", "case_number": "A"},
            {"ground_filter": "code_violation", "activity": "Mediation", "status": "Completed", "case_number": "B"},
            {"ground_filter": "code_violation", "activity": "Petition submitted", "status": "Submitted", "case_number": "A"},
        ]
    )
    out = summarize_rap_progress_activity(progress_df)
    row = out.loc[(out["activity"] == "Mediation") & (out["status"] == "Completed")].iloc[0]
    assert row["activity_count"] == 2
    assert row["unique_cases"] == 2


def test_summarize_rap_hearing_officers():
    detail_df = pd.DataFrame(
        [
            {"ground_filter": "code_violation", "hearing_officer": "Officer A", "case_number": "A", "property_address": "500 Vernon Street"},
            {"ground_filter": "code_violation", "hearing_officer": "Officer A", "case_number": "B", "property_address": "601 BROOKLYN AVE 203"},
        ]
    )
    out = summarize_rap_hearing_officers(detail_df)
    row = out.iloc[0]
    assert row["case_rows"] == 2
    assert row["unique_cases"] == 2
    assert row["unique_addresses"] == 2


def test_summarize_rap_code_enforcement_by_year():
    detail_df = pd.DataFrame(
        [
            {"ground_filter": "code_violation", "date_filed": "01-30-2026", "case_number": "A", "property_address": "500 Vernon Street"},
            {"ground_filter": "code_violation", "date_filed": "02-01-2026", "case_number": "B", "property_address": "500 Vernon Street"},
            {"ground_filter": "fewer_housing_services", "date_filed": "12-31-2025", "case_number": "C", "property_address": "601 BROOKLYN AVE 203"},
        ]
    )
    requests_df = pd.DataFrame(
        [
            {"normalized_address": "500 VERNON ST", "requestid": 1},
            {"normalized_address": "500 VERNON ST", "requestid": 2},
        ]
    )
    out = summarize_rap_code_enforcement_by_year(detail_df, requests_df)
    row = out.loc[(out["ground_filter"] == "code_violation") & (out["date_year"] == 2026)].iloc[0]
    assert row["rap_case_rows"] == 2
    assert row["matched_request_rows"] == 4
    assert row["matched_addresses"] == 1


def test_summarize_rap_coverage():
    search_df = pd.DataFrame(
        [
            {"ground_filter": "code_violation", "case_number": "A"},
            {"ground_filter": "code_violation", "case_number": "B"},
            {"ground_filter": "fewer_housing_services", "case_number": "C"},
        ]
    )
    detail_df = pd.DataFrame(
        [
            {"ground_filter": "code_violation", "case_number": "A", "property_address": "500 Vernon Street", "apn": "010"},
            {"ground_filter": "code_violation", "case_number": "B", "property_address": "500 Vernon Street", "apn": ""},
            {"ground_filter": "fewer_housing_services", "case_number": "C", "property_address": "601 BROOKLYN AVE 203", "apn": "011"},
            {"ground_filter": "fewer_housing_services", "case_number": "C", "property_address": "", "apn": ""},
        ]
    )
    request_matches_df = pd.DataFrame(
        [
            {"normalized_address": "500 VERNON ST", "requestid": 1},
            {"normalized_address": "500 VERNON ST", "requestid": 2},
            {"normalized_address": "601 BROOKLYN AVE", "requestid": 3},
        ]
    )
    out = summarize_rap_coverage(search_df, detail_df, request_matches_df)
    row = out.loc[out["ground_filter"] == "code_violation"].iloc[0]
    assert row["search_rows"] == 2
    assert row["detail_rows"] == 2
    assert row["unique_cases"] == 2
    assert row["normalized_addresses"] == 1
    assert row["matched_addresses"] == 1
    assert row["matched_request_rows"] == 2


def test_build_rap_search_universe_paginates(monkeypatch):
    page_calls = {"page": 0}

    def fake_search_cases(session, *, tenant_ground_value, keywords="", timeout=30):
        page_calls["page"] = 1
        return "page1"

    def fake_fetch_search_event(session, *, search_results_html, event_target, timeout=30):
        if search_results_html == "page1":
            page_calls["page"] = 2
            return "page2"
        return ""

    def fake_parse_search_results(html, *, ground_filter, ground_value):
        if html == "page1":
            return pd.DataFrame([{"ground_filter": ground_filter, "ground_value": ground_value, "counter_message": "1 to 25 of 30 records", "case_number": "A", "date_filed": "01-01-2024", "case_status": "Open"}])
        if html == "page2":
            return pd.DataFrame([{"ground_filter": ground_filter, "ground_value": ground_value, "counter_message": "26 to 30 of 30 records", "case_number": "B", "date_filed": "01-02-2024", "case_status": "Closed"}])
        return pd.DataFrame()

    def fake_extract_next_page_target(html):
        return "next" if html == "page1" else ""

    monkeypatch.setattr("rent_control_public.oakland.search_cases", fake_search_cases)
    monkeypatch.setattr("rent_control_public.oakland.fetch_search_event", fake_fetch_search_event)
    monkeypatch.setattr("rent_control_public.oakland.parse_search_results", fake_parse_search_results)
    monkeypatch.setattr("rent_control_public.oakland.extract_next_page_target", fake_extract_next_page_target)
    monkeypatch.setattr("rent_control_public.oakland.parse_counter_message", lambda html: "count")

    search_df, summary_df, status_df = build_rap_search_universe(
        ground_filters={"code_violation": "1008"},
        max_pages_per_ground=0,
        timeout=5,
    )
    assert len(search_df) == 2
    assert sorted(search_df["search_page"].tolist()) == [1, 2]
    assert int(summary_df["search_results_returned"].sum()) == 2
    assert int(status_df["case_count"].sum()) == 2


def test_fetch_search_results_page_walks_pages(monkeypatch):
    def fake_search_cases(session, *, tenant_ground_value, keywords="", timeout=30):
        return "page1"

    def fake_fetch_search_event(session, *, search_results_html, event_target, timeout=30):
        if search_results_html == "page1":
            return "page2"
        raise AssertionError(search_results_html)

    def fake_extract_next_page_target(html):
        return "next" if html == "page1" else ""

    monkeypatch.setattr("rent_control_public.oakland.search_cases", fake_search_cases)
    monkeypatch.setattr("rent_control_public.oakland.fetch_search_event", fake_fetch_search_event)
    monkeypatch.setattr("rent_control_public.oakland.extract_next_page_target", fake_extract_next_page_target)

    out = fetch_search_results_page(object(), tenant_ground_value="1008", page=2, timeout=5)
    assert out == "page2"


def test_build_detail_join_key():
    frame = pd.DataFrame(
        [
            {"ground_filter": "code_violation", "case_number": "T26-0027", "date_filed": "01-30-2026"},
            {"ground_filter": "code_violation", "case_number": "", "petition": "Tenant 2", "date_filed": "01-31-2026"},
        ]
    )
    out = build_detail_join_key(frame)
    assert out.tolist() == [
        "code_violation||CASE||T26-0027||01-30-2026",
        "code_violation||PETITION||Tenant 2||01-31-2026",
    ]
