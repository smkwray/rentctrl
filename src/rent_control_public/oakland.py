from __future__ import annotations

import re
from html import unescape

import pandas as pd
import requests

SEARCH_CASES_URL = "https://apps.oaklandca.gov/rappetitions/SearchCases.aspx"
CODE_ENFORCEMENT_DATASET_URL = (
    "https://data.oaklandca.gov/resource/quth-gb8e.csv"
    "?$select=requestid,datetimeinit,status,referredto,councildistrict,zipcode,probaddress"
    "&$where=description=%22Code%20Enforcement%22"
    "&$limit=50000"
)

TENANT_GROUND_FILTERS = {
    "code_violation": "1008",
    "decrease_in_services": "9",
    "fewer_housing_services": "1009",
}

SEARCH_RESULT_COLUMNS = [
    "ground_filter",
    "ground_value",
    "counter_message",
    "search_rank",
    "case_number",
    "petition",
    "date_filed",
    "latest_activity",
    "hearing_officer",
    "case_status",
    "petition_grounds",
    "detail_event_target",
    "case_link_event_target",
]

CASE_DETAIL_COLUMNS = [
    "ground_filter",
    "ground_value",
    "case_number",
    "petition",
    "date_filed",
    "property_address",
    "apn",
    "hearing_date",
    "mediation_date",
    "appeal_hearing_date",
    "hearing_officer",
    "program_analyst",
    "case_grounds",
]

CASE_PROGRESS_COLUMNS = [
    "ground_filter",
    "ground_value",
    "case_number",
    "activity",
    "status",
    "activity_date",
]

_FIELD_VALUE_RE = re.compile(r'name="([^"]+)"[^>]*value="([^"]*)"')
_CASE_TABLE_RE = re.compile(
    r'<table[^>]*id="[^"]*wtCaseDataTable[^"]*"[^>]*>.*?<tbody>(.*?)</tbody>',
    re.S,
)
_DETAIL_TARGET_RE = re.compile(r"__doPostBack\(&#39;([^&]+?)&#39;,&#39;&#39;\)")
_CASE_NUMBER_RE = re.compile(r"Case Number:\s*([A-Z]\d{2}-\d{4})", re.I)
_PETITION_RE = re.compile(r"Petition:\s*(.+)$", re.I)
_COUNTER_RE = re.compile(
    r'<div[^>]*class="Counter_Message"[^>]*>([^<]*)</div',
    re.I,
)
_NEXT_PAGE_TARGET_RE = re.compile(
    r"OsAjax\(arguments\[0\] \|\| window\.event,&#39;[^&#]+&#39;,&#39;([^&#]+)&#39;,&#39;&#39;,&#39;__OSVSTATE,&#39;,&#39;&#39;\); return false;\" href=\"#\">next</a>",
    re.S,
)
_TABLE_RE = re.compile(
    r'<table[^>]*id="%s"[^>]*>.*?<tbody>(.*?)</tbody>'
)
_CELL_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.S)
_ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S)
_WS_RE = re.compile(r"\s+")
_CITY_STATE_ZIP_RE = re.compile(
    r",?\s*OAKLAND\s*,?\s*CA(?:LIFORNIA)?(?:\s+\d{5}(?:-\d{4})?)?$",
    re.I,
)
_SUFFIX_REPLACEMENTS = {
    "AVENUE": "AVE",
    "BOULEVARD": "BLVD",
    "DRIVE": "DR",
    "STREET": "ST",
    "PLACE": "PL",
    "COURT": "CT",
    "ROAD": "RD",
    "LANE": "LN",
    "TERRACE": "TER",
    "CIRCLE": "CIR",
}
_STREET_SUFFIXES = {"AVE", "BLVD", "DR", "ST", "PL", "CT", "RD", "LN", "TER", "CIR", "WAY"}


class OaklandRapError(RuntimeError):
    """Raised when the RAP search workflow fails in a non-recoverable way."""


def get_search_page(session: requests.Session | None = None, *, timeout: int = 30) -> str:
    session = session or requests.Session()
    response = session.get(SEARCH_CASES_URL, timeout=timeout)
    response.raise_for_status()
    return response.text


def extract_form_state(html: str) -> dict[str, str]:
    return dict(_FIELD_VALUE_RE.findall(html))


def build_search_payload(
    html: str,
    *,
    tenant_ground_value: str,
    keywords: str = "",
    owner_tenant_value: str = "1",
) -> dict[str, str]:
    state = extract_form_state(html)
    payload = {
        key: value
        for key, value in state.items()
        if key.startswith("__") or "OSVSTATE" in key
    }
    payload[
        "wt89$OutSystemsUIWeb_wt10$block$wtContent$wtMainContent$OutSystemsUIWeb_wt178$block$wtColumn1$wtcmbOwnerTenant"
    ] = owner_tenant_value
    payload[
        "wt89$OutSystemsUIWeb_wt10$block$wtContent$wtMainContent$OutSystemsUIWeb_wtFiltersCard$block$wtContent$OutSystemsUIWeb_wt12$block$wtColumn2$wtTenant_PeititionGroundType_Filter"
    ] = tenant_ground_value
    payload[
        "wt89$OutSystemsUIWeb_wt10$block$wtContent$wtMainContent$OutSystemsUIWeb_wtFiltersCard$block$wtContent$OutSystemsUIWeb_wt35$block$wtColumn1$wtSession_CaseSearch_Keywords"
    ] = keywords
    payload[
        "wt89$OutSystemsUIWeb_wt10$block$wtContent$wtMainContent$OutSystemsUIWeb_wtFiltersCard$block$wtContent$OutSystemsUIWeb_wt35$block$wtColumn2$wt177"
    ] = "Search"
    return payload


def search_cases(
    session: requests.Session,
    *,
    tenant_ground_value: str,
    keywords: str = "",
    timeout: int = 30,
) -> str:
    html = get_search_page(session, timeout=timeout)
    payload = build_search_payload(
        html,
        tenant_ground_value=tenant_ground_value,
        keywords=keywords,
    )
    response = session.post(SEARCH_CASES_URL, data=payload, timeout=timeout)
    response.raise_for_status()
    return response.text


def fetch_case_detail(
    session: requests.Session,
    *,
    search_results_html: str,
    event_target: str,
    timeout: int = 30,
) -> str:
    state = extract_form_state(search_results_html)
    payload = {
        key: value
        for key, value in state.items()
        if key.startswith("__") or "OSVSTATE" in key
    }
    payload["__EVENTTARGET"] = event_target
    response = session.post(SEARCH_CASES_URL, data=payload, timeout=timeout)
    response.raise_for_status()
    return response.text


def fetch_search_event(
    session: requests.Session,
    *,
    search_results_html: str,
    event_target: str,
    timeout: int = 30,
) -> str:
    state = extract_form_state(search_results_html)
    payload = {
        key: value
        for key, value in state.items()
        if key.startswith("__") or "OSVSTATE" in key
    }
    payload["__EVENTTARGET"] = event_target
    response = session.post(SEARCH_CASES_URL, data=payload, timeout=timeout)
    response.raise_for_status()
    return response.text


def parse_counter_message(html: str) -> str:
    match = _COUNTER_RE.search(html)
    if match is None:
        return ""
    return _clean_text(match.group(1))


def extract_next_page_target(html: str) -> str:
    match = _NEXT_PAGE_TARGET_RE.search(html)
    return match.group(1) if match else ""


def parse_search_results(
    html: str,
    *,
    ground_filter: str,
    ground_value: str,
) -> pd.DataFrame:
    match = _CASE_TABLE_RE.search(html)
    counter_message = parse_counter_message(html)
    rows: list[dict[str, object]] = []
    if match is None:
        return pd.DataFrame(rows, columns=SEARCH_RESULT_COLUMNS)
    tbody = match.group(1)
    for rank, row_html in enumerate(_ROW_RE.findall(tbody), start=1):
        cells = _CELL_RE.findall(row_html)
        if len(cells) < 7:
            continue
        cell0 = _clean_text(cells[0])
        if cell0.startswith("Press Search button") or cell0.startswith("No items to show"):
            continue
        case_number_match = _CASE_NUMBER_RE.search(cell0)
        petition_match = _PETITION_RE.search(cell0)
        detail_targets = _DETAIL_TARGET_RE.findall(cells[6])
        case_link_targets = _DETAIL_TARGET_RE.findall(cells[0])
        grounds = _extract_lines(cells[5])
        rows.append(
            {
                "ground_filter": ground_filter,
                "ground_value": ground_value,
                "counter_message": counter_message,
                "search_rank": rank,
                "case_number": case_number_match.group(1) if case_number_match else "",
                "petition": petition_match.group(1).strip() if petition_match else "",
                "date_filed": _clean_text(cells[1]),
                "latest_activity": _clean_text(cells[2]),
                "hearing_officer": _clean_text(cells[3]),
                "case_status": _clean_text(cells[4]),
                "petition_grounds": " | ".join(grounds),
                "detail_event_target": detail_targets[0] if detail_targets else "",
                "case_link_event_target": case_link_targets[0] if case_link_targets else "",
            }
        )
    return pd.DataFrame(rows, columns=SEARCH_RESULT_COLUMNS)


def parse_case_detail(
    html: str,
    *,
    ground_filter: str,
    ground_value: str,
    fallback_case_number: str = "",
) -> tuple[dict[str, str], pd.DataFrame]:
    case_number = _extract_label_value(html, "Case Number") or fallback_case_number
    detail = {
        "ground_filter": ground_filter,
        "ground_value": ground_value,
        "case_number": case_number,
        "petition": _extract_label_value(html, "Petition"),
        "date_filed": _extract_label_value(html, "Date Filed"),
        "property_address": _extract_label_value(html, "Property Address"),
        "apn": _extract_label_value(html, "APN"),
        "hearing_date": _extract_label_value(html, "Hearing Date"),
        "mediation_date": _extract_label_value(html, "Mediation Date"),
        "appeal_hearing_date": _extract_label_value(html, "Appeal Hearing Date"),
        "hearing_officer": _extract_label_value(html, "Hearing Officer"),
        "program_analyst": _extract_label_value(html, "Program Analyst"),
        "case_grounds": " | ".join(_parse_grounds_table(html)),
    }
    progress_rows = [
        {
            "ground_filter": ground_filter,
            "ground_value": ground_value,
            "case_number": case_number,
            "activity": row[0],
            "status": row[1],
            "activity_date": row[2],
        }
        for row in _parse_table_rows(html, "wtCaseActivityStatusTable", expected_cells=3)
    ]
    return detail, pd.DataFrame(progress_rows, columns=CASE_PROGRESS_COLUMNS)


def build_rap_grounds_pilot(
    *,
    ground_filters: dict[str, str] | None = None,
    max_cases_per_ground: int = 10,
    max_pages_per_ground: int = 1,
    timeout: int = 30,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    filters = ground_filters or TENANT_GROUND_FILTERS
    search_frames: list[pd.DataFrame] = []
    detail_rows: list[dict[str, str]] = []
    progress_frames: list[pd.DataFrame] = []
    session = requests.Session()
    for ground_filter, ground_value in filters.items():
        search_html = search_cases(
            session,
            tenant_ground_value=ground_value,
            timeout=timeout,
        )
        current_html = search_html
        page = 1
        detail_fetched = 0
        seen_page_signatures: set[tuple[str, str]] = set()
        while current_html and (max_pages_per_ground <= 0 or page <= max_pages_per_ground):
            result_df = parse_search_results(
                current_html,
                ground_filter=ground_filter,
                ground_value=ground_value,
            )
            if result_df.empty:
                break
            signature = (
                parse_counter_message(current_html),
                "|".join(result_df["case_number"].astype(str).head(3).tolist()),
            )
            if signature in seen_page_signatures:
                break
            seen_page_signatures.add(signature)
            result_df["search_page"] = page
            search_frames.append(result_df)
            remaining_detail = max(0, max_cases_per_ground - detail_fetched)
            for _, row in result_df.head(remaining_detail).iterrows():
                target = row.get("detail_event_target", "")
                if not target:
                    continue
                detail_html = fetch_case_detail(
                    session,
                    search_results_html=current_html,
                    event_target=target,
                    timeout=timeout,
                )
                detail_row, progress_df = parse_case_detail(
                    detail_html,
                    ground_filter=ground_filter,
                    ground_value=ground_value,
                    fallback_case_number=str(row.get("case_number", "")),
                )
                detail_row["search_page"] = page
                detail_rows.append(detail_row)
                detail_fetched += 1
                if not progress_df.empty:
                    progress_df["search_page"] = page
                progress_frames.append(progress_df)
            if detail_fetched >= max_cases_per_ground:
                break
            next_target = extract_next_page_target(current_html)
            if not next_target:
                break
            current_html = fetch_search_event(
                session,
                search_results_html=current_html,
                event_target=next_target,
                timeout=timeout,
            )
            page += 1
    search_df = pd.concat(search_frames, ignore_index=True) if search_frames else pd.DataFrame(columns=SEARCH_RESULT_COLUMNS)
    detail_df = pd.DataFrame(detail_rows, columns=CASE_DETAIL_COLUMNS)
    progress_df = pd.concat(progress_frames, ignore_index=True) if progress_frames else pd.DataFrame(columns=CASE_PROGRESS_COLUMNS)
    summary_df = summarize_ground_search(search_df, detail_df)
    status_df = summarize_ground_status(search_df)
    return search_df, detail_df, progress_df, summary_df, status_df


def build_rap_search_universe(
    *,
    ground_filters: dict[str, str] | None = None,
    max_pages_per_ground: int = 0,
    timeout: int = 30,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    filters = ground_filters or TENANT_GROUND_FILTERS
    search_frames: list[pd.DataFrame] = []
    session = requests.Session()
    for ground_filter, ground_value in filters.items():
        current_html = search_cases(
            session,
            tenant_ground_value=ground_value,
            timeout=timeout,
        )
        page = 1
        seen_page_signatures: set[tuple[str, str]] = set()
        while current_html and (max_pages_per_ground <= 0 or page <= max_pages_per_ground):
            result_df = parse_search_results(
                current_html,
                ground_filter=ground_filter,
                ground_value=ground_value,
            )
            if result_df.empty:
                break
            signature = (
                parse_counter_message(current_html),
                "|".join(result_df["case_number"].astype(str).head(3).tolist()),
            )
            if signature in seen_page_signatures:
                break
            seen_page_signatures.add(signature)
            result_df["search_page"] = page
            search_frames.append(result_df)
            next_target = extract_next_page_target(current_html)
            if not next_target:
                break
            current_html = fetch_search_event(
                session,
                search_results_html=current_html,
                event_target=next_target,
                timeout=timeout,
            )
            page += 1
    search_df = pd.concat(search_frames, ignore_index=True) if search_frames else pd.DataFrame(columns=SEARCH_RESULT_COLUMNS)
    summary_df = summarize_ground_search(search_df, pd.DataFrame(columns=CASE_DETAIL_COLUMNS))
    status_df = summarize_ground_status(search_df)
    return search_df, summary_df, status_df


def fetch_search_results_page(
    session: requests.Session,
    *,
    tenant_ground_value: str,
    page: int,
    timeout: int = 30,
) -> str:
    if page < 1:
        raise ValueError("page must be >= 1")
    current_html = search_cases(
        session,
        tenant_ground_value=tenant_ground_value,
        timeout=timeout,
    )
    current_page = 1
    while current_page < page:
        next_target = extract_next_page_target(current_html)
        if not next_target:
            raise OaklandRapError(f"Could not reach Oakland RAP page {page} for ground {tenant_ground_value}")
        current_html = fetch_search_event(
            session,
            search_results_html=current_html,
            event_target=next_target,
            timeout=timeout,
        )
        current_page += 1
    return current_html


def fetch_code_enforcement_requests(*, timeout: int = 60) -> pd.DataFrame:
    frame = pd.read_csv(CODE_ENFORCEMENT_DATASET_URL)
    if frame.empty:
        return frame
    frame["normalized_address"] = frame["probaddress"].map(normalize_address)
    frame["datetimeinit"] = pd.to_datetime(frame["datetimeinit"], errors="coerce")
    frame["date_year"] = frame["datetimeinit"].dt.year.astype("Int64")
    return frame


def normalize_address(value: str) -> str:
    if not isinstance(value, str):
        return ""
    text = value.upper().strip()
    text = text.replace("\n", " ")
    text = _CITY_STATE_ZIP_RE.sub("", text)
    text = text.replace(",", " ")
    text = text.replace(".", "")
    text = _WS_RE.sub(" ", text).strip()
    parts = [_SUFFIX_REPLACEMENTS.get(part, part) for part in text.split()]
    base, _unit = _split_address_unit(" ".join(parts))
    return _WS_RE.sub(" ", base).strip()


def build_code_enforcement_address_summary(rap_detail_df: pd.DataFrame) -> pd.DataFrame:
    detail = rap_detail_df.copy()
    if detail.empty:
        return pd.DataFrame(
            columns=[
                "normalized_address",
                "rap_case_rows",
                "rap_cases_with_grounds",
            ]
        )
    detail["normalized_address"] = detail["property_address"].map(normalize_address)
    detail = detail.loc[detail["normalized_address"].ne("")].copy()
    if detail.empty:
        return pd.DataFrame(
            columns=[
                "normalized_address",
                "rap_case_rows",
                "rap_cases_with_grounds",
            ]
        )
    summary = (
        detail.groupby("normalized_address", dropna=False)
        .agg(
            rap_case_rows=("petition", "size"),
            rap_cases_with_grounds=("case_grounds", lambda s: int(s.fillna("").ne("").sum())),
        )
        .reset_index()
    )
    return summary


def summarize_rap_coverage(
    search_df: pd.DataFrame,
    detail_df: pd.DataFrame,
    request_matches_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "ground_filter",
        "search_rows",
        "detail_rows",
        "unique_cases",
        "normalized_addresses",
        "addresses_with_apn",
        "matched_addresses",
        "matched_request_rows",
        "detail_rows_per_search_row",
        "matched_address_share",
    ]
    if detail_df.empty:
        return pd.DataFrame(columns=columns)

    detail = detail_df.copy()
    detail["normalized_address"] = detail["property_address"].map(normalize_address)
    detail["has_apn"] = detail["apn"].fillna("").astype(str).str.strip().ne("")
    detail = detail.loc[detail["normalized_address"].ne("")].copy()

    detail_summary = (
        detail.groupby("ground_filter", dropna=False)
        .agg(
            detail_rows=("case_number", "size"),
            unique_cases=("case_number", "nunique"),
            normalized_addresses=("normalized_address", "nunique"),
            addresses_with_apn=("has_apn", "sum"),
        )
        .reset_index()
    )

    if search_df.empty:
        search_summary = pd.DataFrame(columns=["ground_filter", "search_rows"])
    else:
        search_summary = (
            search_df.groupby("ground_filter", dropna=False)
            .size()
            .rename("search_rows")
            .reset_index()
        )

    if request_matches_df.empty:
        match_summary = pd.DataFrame(columns=["ground_filter", "matched_addresses", "matched_request_rows"])
    else:
        ground_addresses = detail[["ground_filter", "normalized_address"]].drop_duplicates()
        matched = ground_addresses.merge(
            request_matches_df[["normalized_address", "requestid"]].drop_duplicates(),
            how="inner",
            on="normalized_address",
        )
        match_summary = (
            matched.groupby("ground_filter", dropna=False)
            .agg(
                matched_addresses=("normalized_address", "nunique"),
                matched_request_rows=("requestid", "size"),
            )
            .reset_index()
        )

    out = search_summary.merge(detail_summary, how="outer", on="ground_filter")
    out = out.merge(match_summary, how="left", on="ground_filter")
    for col in [
        "search_rows",
        "detail_rows",
        "unique_cases",
        "normalized_addresses",
        "addresses_with_apn",
        "matched_addresses",
        "matched_request_rows",
    ]:
        out[col] = out[col].fillna(0).astype(int)
    out["detail_rows_per_search_row"] = (
        out["detail_rows"] / out["search_rows"].where(out["search_rows"].ne(0), pd.NA)
    ).astype("Float64")
    out["matched_address_share"] = (
        out["matched_addresses"] / out["normalized_addresses"].where(out["normalized_addresses"].ne(0), pd.NA)
    ).astype("Float64")
    return out[columns].sort_values("ground_filter").reset_index(drop=True)


def match_rap_to_code_enforcement(
    rap_detail_df: pd.DataFrame,
    requests_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rap_summary = build_code_enforcement_address_summary(rap_detail_df)
    requests = requests_df.copy()
    request_matches = requests.merge(
        rap_summary[["normalized_address"]],
        how="inner",
        on="normalized_address",
    )
    request_summary = (
        request_matches.groupby("normalized_address", dropna=False)
        .agg(
            code_enforcement_requests=("requestid", "size"),
            first_request=("datetimeinit", "min"),
            last_request=("datetimeinit", "max"),
            latest_request_status=("status", "last"),
        )
        .reset_index()
    )
    address_summary = rap_summary.merge(
        request_summary,
        how="left",
        on="normalized_address",
    )
    address_summary["code_enforcement_requests"] = (
        address_summary["code_enforcement_requests"].fillna(0).astype(int)
    )
    overall_summary = pd.DataFrame(
        [
            {"metric": "rap_detail_rows", "value": int(len(rap_detail_df))},
            {"metric": "rap_unique_addresses", "value": int(rap_summary["normalized_address"].nunique())},
            {"metric": "code_enforcement_requests", "value": int(len(requests_df))},
            {"metric": "matched_request_rows", "value": int(len(request_matches))},
            {
                "metric": "matched_rap_addresses",
                "value": int(address_summary["code_enforcement_requests"].gt(0).sum()),
            },
        ]
    )
    return request_matches, address_summary, overall_summary


def summarize_ground_search(search_df: pd.DataFrame, detail_df: pd.DataFrame) -> pd.DataFrame:
    if search_df.empty:
        return pd.DataFrame(
            columns=[
                "ground_filter",
                "ground_value",
                "counter_message",
                "search_results_returned",
                "detail_cases_fetched",
            ]
        )
    detail_counts = (
        detail_df.groupby(["ground_filter", "ground_value"])
        .size()
        .rename("detail_cases_fetched")
        .reset_index()
        if not detail_df.empty
        else pd.DataFrame(columns=["ground_filter", "ground_value", "detail_cases_fetched"])
    )
    summary = (
        search_df.groupby(["ground_filter", "ground_value", "counter_message"], dropna=False)
        .size()
        .rename("search_results_returned")
        .reset_index()
    )
    summary = summary.merge(detail_counts, how="left", on=["ground_filter", "ground_value"])
    summary["detail_cases_fetched"] = summary["detail_cases_fetched"].fillna(0).astype(int)
    return summary.sort_values(["ground_filter"]).reset_index(drop=True)


def summarize_ground_status(search_df: pd.DataFrame) -> pd.DataFrame:
    if search_df.empty:
        return pd.DataFrame(
            columns=["ground_filter", "date_year", "case_status", "case_count"]
        )
    frame = search_df.copy()
    frame["date_year"] = frame["date_filed"].str[-4:]
    summary = (
        frame.groupby(["ground_filter", "date_year", "case_status"], dropna=False)
        .size()
        .rename("case_count")
        .reset_index()
        .sort_values(["ground_filter", "date_year", "case_status"])
        .reset_index(drop=True)
    )
    return summary


def summarize_rap_detail_by_year(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df.empty:
        return pd.DataFrame(
            columns=[
                "ground_filter",
                "date_year",
                "case_rows",
                "unique_cases",
                "unique_addresses",
                "addresses_with_apn",
            ]
        )
    frame = detail_df.copy()
    frame["date_year"] = pd.to_datetime(frame["date_filed"], errors="coerce").dt.year.astype("Int64")
    frame["normalized_address"] = frame["property_address"].map(normalize_address)
    frame["has_apn"] = frame["apn"].fillna("").astype(str).str.strip().ne("")
    summary = (
        frame.groupby(["ground_filter", "date_year"], dropna=False)
        .agg(
            case_rows=("case_number", "size"),
            unique_cases=("case_number", "nunique"),
            unique_addresses=("normalized_address", "nunique"),
            addresses_with_apn=("has_apn", "sum"),
        )
        .reset_index()
        .sort_values(["ground_filter", "date_year"])
        .reset_index(drop=True)
    )
    return summary


def summarize_rap_progress_activity(progress_df: pd.DataFrame) -> pd.DataFrame:
    if progress_df.empty:
        return pd.DataFrame(
            columns=["ground_filter", "activity", "status", "activity_count", "unique_cases"]
        )
    frame = progress_df.copy()
    summary = (
        frame.groupby(["ground_filter", "activity", "status"], dropna=False)
        .agg(
            activity_count=("case_number", "size"),
            unique_cases=("case_number", "nunique"),
        )
        .reset_index()
        .sort_values(["ground_filter", "activity_count", "activity"], ascending=[True, False, True])
        .reset_index(drop=True)
    )
    return summary


def summarize_rap_hearing_officers(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df.empty:
        return pd.DataFrame(
            columns=["ground_filter", "hearing_officer", "case_rows", "unique_cases", "unique_addresses"]
        )
    frame = detail_df.copy()
    frame["normalized_address"] = frame["property_address"].map(normalize_address)
    summary = (
        frame.groupby(["ground_filter", "hearing_officer"], dropna=False)
        .agg(
            case_rows=("case_number", "size"),
            unique_cases=("case_number", "nunique"),
            unique_addresses=("normalized_address", "nunique"),
        )
        .reset_index()
        .sort_values(["ground_filter", "case_rows", "hearing_officer"], ascending=[True, False, True])
        .reset_index(drop=True)
    )
    return summary


def summarize_rap_code_enforcement_by_year(
    detail_df: pd.DataFrame,
    request_matches_df: pd.DataFrame,
) -> pd.DataFrame:
    if detail_df.empty:
        return pd.DataFrame(
            columns=[
                "ground_filter",
                "date_year",
                "rap_case_rows",
                "matched_request_rows",
                "matched_addresses",
            ]
        )
    detail = detail_df.copy()
    detail["normalized_address"] = detail["property_address"].map(normalize_address)
    detail["date_year"] = pd.to_datetime(detail["date_filed"], errors="coerce").dt.year.astype("Int64")
    rap_year = (
        detail.groupby(["ground_filter", "date_year"], dropna=False)
        .agg(
            rap_case_rows=("case_number", "size"),
            matched_addresses=("normalized_address", "nunique"),
        )
        .reset_index()
    )
    if request_matches_df.empty:
        rap_year["matched_request_rows"] = 0
        return rap_year.sort_values(["ground_filter", "date_year"]).reset_index(drop=True)

    request_matches = request_matches_df.copy()
    request_matches["normalized_address"] = request_matches["normalized_address"].fillna("").astype(str)
    linked = detail.merge(
        request_matches[["normalized_address", "requestid"]],
        how="inner",
        on="normalized_address",
    )
    request_year = (
        linked.groupby(["ground_filter", "date_year"], dropna=False)
        .agg(matched_request_rows=("requestid", "size"))
        .reset_index()
    )
    out = rap_year.merge(request_year, how="left", on=["ground_filter", "date_year"])
    out["matched_request_rows"] = out["matched_request_rows"].fillna(0).astype(int)
    return out.sort_values(["ground_filter", "date_year"]).reset_index(drop=True)


def build_detail_join_key(frame: pd.DataFrame) -> pd.Series:
    data = frame.copy()
    ground = data.get("ground_filter", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    case_number = data.get("case_number", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    petition = data.get("petition", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    date_filed = data.get("date_filed", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    fallback = ground + "||PETITION||" + petition + "||" + date_filed
    primary = ground + "||CASE||" + case_number + "||" + date_filed
    return primary.where(case_number.ne(""), fallback)


def _split_address_unit(text: str) -> tuple[str, str]:
    parts = text.split()
    if len(parts) >= 3 and parts[-2] in _STREET_SUFFIXES:
        candidate = parts[-1]
        if re.fullmatch(r"[A-Z]|\d{1,4}[A-Z]?", candidate):
            return " ".join(parts[:-1]), candidate
    return text, ""


def _extract_label_value(html: str, label: str) -> str:
    patterns = [
        rf'>{re.escape(label)}</div\s*><div[^>]*>(.*?)</div',
        rf'>{re.escape(label)}</span></div\s*><div[^>]*>(.*?)</div',
    ]
    for pattern in patterns:
        match = re.search(pattern, html, re.S)
        if match is not None:
            return _clean_text(match.group(1))
    return ""


def _parse_grounds_table(html: str) -> list[str]:
    rows = _parse_table_rows(html, "wtTenantPetitionGroundsTable", expected_cells=2)
    return [row[0] for row in rows if row and row[0]]


def _parse_table_rows(html: str, table_id: str, *, expected_cells: int) -> list[list[str]]:
    table_re = re.compile(
        r'<table[^>]*id=\"[^\"]*%s[^\"]*\"[^>]*>.*?<tbody>(.*?)</tbody>'
        % re.escape(table_id),
        re.S,
    )
    match = table_re.search(html)
    if match is None:
        return []
    tbody = match.group(1)
    parsed_rows: list[list[str]] = []
    for row_html in _ROW_RE.findall(tbody):
        cells = [_clean_text(cell) for cell in _CELL_RE.findall(row_html)]
        if len(cells) < expected_cells:
            continue
        if cells[0].startswith("No items to show"):
            continue
        parsed_rows.append(cells[:expected_cells])
    return parsed_rows


def _extract_lines(html_fragment: str) -> list[str]:
    text = unescape(re.sub(r"(?i)<br\s*/?>", "\n", html_fragment))
    text = re.sub(r"<[^>]+>", " ", text)
    lines = []
    for raw in text.splitlines():
        line = _WS_RE.sub(" ", raw.replace("\xa0", " ")).strip()
        line = line.lstrip("- ").strip()
        if line:
            lines.append(line)
    return lines


def _clean_text(html_fragment: str) -> str:
    text = unescape(html_fragment)
    text = re.sub(r"(?i)<br\s*/?>", " ", text)
    text = re.sub(r"</div\s*>", " ", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("\xa0", " ")
    return _WS_RE.sub(" ", text).strip()
