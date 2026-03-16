from __future__ import annotations

from html import unescape
import json
import time
import re
import subprocess
from typing import Iterable
from urllib.parse import quote, urlencode

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


PROPERTY_ACTIVITY_URL = "https://housingapp.lacity.org/reportviolation/Pages/PropAtivityCases"
ASSESSOR_FEATURESERVER_URL = (
    "https://services.arcgis.com/RmCCgQtiZLDCtblq/arcgis/rest/services/"
    "Parcel_Data_2021_Table/FeatureServer/0/query"
)
ASSESSOR_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Referer": "https://services.arcgis.com/",
}
DEFAULT_ASSESSOR_FIELDS = [
    "AIN",
    "RollYear",
    "TaxRateArea_CITY",
    "PropertyLocation",
    "UseType",
    "UseCode",
    "YearBuilt",
    "EffectiveYearBuilt",
    "Units",
    "SQFTmain",
    "Bedrooms",
    "Bathrooms",
    "RecordingDate",
    "Roll_TotalValue",
]
PROPERTY_INFO_SPAN_IDS = {
    "apn": "lblAPN2",
    "total_units": "lblTotalPropUnits",
    "rent_registration_number": "lblRSU",
    "census_tract": "lblCT",
    "council_district": "lblCD",
    "official_address": "lblAddress",
    "total_exemption_units": "lblSCEPExemptions",
    "rent_office_id": "lblRentOfficeID",
    "code_regional_area": "lblCodeRegionalArea",
    "year_built": "lblYear",
}
_ASSESSOR_SESSION: requests.Session | None = None


def strip_html(text: str) -> str:
    return " ".join(unescape(re.sub(r"<[^>]+>", " ", text)).split())


def extract_input_value(html: str, input_id: str) -> str:
    match = re.search(rf'id="{re.escape(input_id)}"\s+value="([^"]*)"', html)
    if not match:
        raise ValueError(f"Could not find hidden input `{input_id}` in LA property activity HTML")
    return unescape(match.group(1))


def extract_table_html(html: str, table_id: str) -> str:
    match = re.search(rf'<table[^>]+id="{re.escape(table_id)}"[^>]*>.*?</table>', html, flags=re.S)
    if not match:
        raise ValueError(f"Could not find table `{table_id}` in LA property activity HTML")
    return match.group(0)


def extract_table_rows(table_html: str) -> list[list[str]]:
    rows: list[list[str]] = []
    for row_html in re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, flags=re.S):
        cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row_html, flags=re.S)
        cleaned = [strip_html(cell) for cell in cells]
        if cleaned:
            rows.append(cleaned)
    return rows


def parse_property_search_results(html: str) -> pd.DataFrame:
    table_html = extract_table_html(html, "dgProperty2")
    rows = extract_table_rows(table_html)
    if len(rows) <= 1:
        return pd.DataFrame(columns=["event_target", "apn", "address"])
    event_targets = re.findall(
        r'WebForm_PostBackOptions\(&quot;(dgProperty2\$ctl\d+\$lnkSelectProp)&quot;',
        table_html,
    )
    records: list[dict[str, str]] = []
    for idx, row in enumerate(rows[1:]):
        if len(row) < 3:
            continue
        records.append(
            {
                "event_target": event_targets[idx] if idx < len(event_targets) else "",
                "apn": row[1],
                "address": row[2],
            }
        )
    return pd.DataFrame(records)


def parse_property_info(html: str) -> dict[str, str]:
    info: dict[str, str] = {}
    for key, span_id in PROPERTY_INFO_SPAN_IDS.items():
        match = re.search(rf'<span[^>]+id="{re.escape(span_id)}"[^>]*>(.*?)</span>', html, flags=re.S)
        info[key] = strip_html(match.group(1)) if match else ""
    return info


def parse_property_cases(html: str) -> pd.DataFrame:
    table_html = extract_table_html(html, "dgPropCases2")
    rows = extract_table_rows(table_html)
    if len(rows) <= 1:
        return pd.DataFrame(columns=["case_type", "case_number", "date_closed"])
    records: list[dict[str, str]] = []
    for row in rows[1:]:
        if len(row) < 4:
            continue
        records.append(
            {
                "case_type": row[1],
                "case_number": row[2],
                "date_closed": row[3],
            }
        )
    return pd.DataFrame(records)


def fetch_property_search_html(
    *,
    session: requests.Session | None = None,
    street_no: str = "",
    street_name: str,
    timeout: int = 30,
) -> str:
    client = session or requests.Session()
    resp = client.get(
        PROPERTY_ACTIVITY_URL,
        params={"StreetNo": street_no, "StreetName": street_name, "Source": "ActivityReport"},
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.text


def fetch_property_detail_html(
    search_html: str,
    *,
    event_target: str,
    session: requests.Session | None = None,
    street_no: str = "",
    street_name: str,
    timeout: int = 30,
) -> str:
    client = session or requests.Session()
    data = {
        "__VIEWSTATE": extract_input_value(search_html, "__VIEWSTATE"),
        "__VIEWSTATEGENERATOR": extract_input_value(search_html, "__VIEWSTATEGENERATOR"),
        "__EVENTVALIDATION": extract_input_value(search_html, "__EVENTVALIDATION"),
        "__EVENTTARGET": event_target,
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
    }
    resp = client.post(
        PROPERTY_ACTIVITY_URL,
        params={"StreetNo": street_no, "StreetName": street_name, "Source": "ActivityReport"},
        data=data,
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.text


def build_property_activity_pilot(
    *,
    street_no: str = "",
    street_name: str,
    max_properties: int = 5,
    timeout: int = 30,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    session = requests.Session()
    search_html = fetch_property_search_html(session=session, street_no=street_no, street_name=street_name, timeout=timeout)
    properties = parse_property_search_results(search_html).head(max_properties).copy()
    property_records: list[dict[str, object]] = []
    case_frames: list[pd.DataFrame] = []

    for _, row in properties.iterrows():
        detail_html = fetch_property_detail_html(
            search_html,
            event_target=str(row["event_target"]),
            session=session,
            street_no=street_no,
            street_name=street_name,
            timeout=timeout,
        )
        info = parse_property_info(detail_html)
        info["search_apn"] = row["apn"]
        info["search_address"] = row["address"]
        property_records.append(info)

        cases = parse_property_cases(detail_html)
        if not cases.empty:
            cases["apn"] = info["apn"] or row["apn"]
            cases["official_address"] = info["official_address"] or row["address"]
            case_frames.append(cases)

    property_df = pd.DataFrame(property_records)
    case_df = pd.concat(case_frames, ignore_index=True) if case_frames else pd.DataFrame(columns=["case_type", "case_number", "date_closed", "apn", "official_address"])
    summary_df = summarize_property_activity(property_df, case_df, street_name=street_name, street_no=street_no)
    return property_df, case_df, summary_df


def summarize_property_activity(
    property_df: pd.DataFrame,
    case_df: pd.DataFrame,
    *,
    street_name: str,
    street_no: str = "",
) -> pd.DataFrame:
    metrics: list[dict[str, object]] = [
        {"metric": "search_street_name", "value": street_name.upper()},
        {"metric": "search_street_no", "value": street_no},
        {"metric": "properties_sampled", "value": int(len(property_df))},
        {"metric": "properties_with_rent_registration", "value": int(property_df["rent_registration_number"].astype(str).str.len().gt(0).sum()) if not property_df.empty else 0},
        {"metric": "properties_with_case_history", "value": int(case_df["apn"].nunique()) if not case_df.empty else 0},
        {"metric": "total_cases", "value": int(len(case_df))},
    ]
    if not property_df.empty:
        metrics.extend(
            [
                {"metric": "mean_total_units", "value": round(pd.to_numeric(property_df["total_units"], errors="coerce").mean(), 3)},
                {"metric": "median_year_built", "value": round(pd.to_numeric(property_df["year_built"], errors="coerce").median(), 3)},
            ]
        )
    case_type_counts = (
        case_df["case_type"].value_counts().to_dict() if not case_df.empty else {}
    )
    for case_type, count in sorted(case_type_counts.items()):
        slug = re.sub(r"[^a-z0-9]+", "_", case_type.lower()).strip("_")
        metrics.append({"metric": f"case_type_{slug}", "value": int(count)})
    return pd.DataFrame(metrics)


def case_type_counts(case_df: pd.DataFrame) -> pd.DataFrame:
    if case_df.empty:
        return pd.DataFrame(columns=["case_type", "case_count", "properties_with_case_type"])
    out = (
        case_df.groupby("case_type", as_index=False)
        .agg(
            case_count=("case_number", "count"),
            properties_with_case_type=("apn", "nunique"),
        )
        .sort_values(["case_count", "case_type"], ascending=[False, True])
        .reset_index(drop=True)
    )
    return out


def build_property_activity_sample(
    *,
    street_names: Iterable[str],
    max_properties_per_street: int = 10,
    timeout: int = 30,
    pause_seconds: float = 0.0,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build a broader bounded sample across multiple street-name queries."""
    session = requests.Session()
    property_frames: list[pd.DataFrame] = []
    case_frames: list[pd.DataFrame] = []
    query_log: list[dict[str, object]] = []

    for street_name in street_names:
        try:
            search_html = fetch_property_search_html(
                session=session,
                street_name=street_name,
                timeout=timeout,
            )
            search_results = parse_property_search_results(search_html).head(max_properties_per_street).copy()
            query_log.append(
                {
                    "street_name": street_name.upper(),
                    "search_results_sampled": int(len(search_results)),
                    "query_error": "",
                }
            )
        except Exception as exc:
            query_log.append(
                {
                    "street_name": street_name.upper(),
                    "search_results_sampled": 0,
                    "query_error": str(exc),
                }
            )
            continue
        for _, row in search_results.iterrows():
            try:
                detail_html = fetch_property_detail_html(
                    search_html,
                    event_target=str(row["event_target"]),
                    session=session,
                    street_name=street_name,
                    timeout=timeout,
                )
                info = parse_property_info(detail_html)
                info["query_street_name"] = street_name.upper()
                info["search_apn"] = row["apn"]
                info["search_address"] = row["address"]
                info["detail_error"] = ""
                property_frames.append(pd.DataFrame([info]))

                cases = parse_property_cases(detail_html)
                if not cases.empty:
                    cases["apn"] = info["apn"] or row["apn"]
                    cases["official_address"] = info["official_address"] or row["address"]
                    cases["query_street_name"] = street_name.upper()
                    case_frames.append(cases)
            except Exception as exc:
                property_frames.append(
                    pd.DataFrame(
                        [
                            {
                                "apn": row["apn"],
                                "official_address": row["address"],
                                "query_street_name": street_name.upper(),
                                "search_apn": row["apn"],
                                "search_address": row["address"],
                                "detail_error": str(exc),
                            }
                        ]
                    )
                )
            if pause_seconds > 0:
                time.sleep(pause_seconds)

    property_df = (
        pd.concat(property_frames, ignore_index=True)
        if property_frames
        else pd.DataFrame()
    )
    if not property_df.empty:
        property_df = (
            property_df.sort_values(["query_street_name", "official_address", "search_address"])
            .drop_duplicates(subset=["apn"], keep="first")
            .reset_index(drop=True)
        )
    case_df = (
        pd.concat(case_frames, ignore_index=True)
        if case_frames
        else pd.DataFrame(columns=["case_type", "case_number", "date_closed", "apn", "official_address", "query_street_name"])
    )
    query_df = pd.DataFrame(query_log)
    property_summary = summarize_property_activity_sample(property_df, case_df, query_df)
    return property_df, case_df, property_summary, query_df


def build_property_level_case_summary(case_df: pd.DataFrame) -> pd.DataFrame:
    if case_df.empty:
        return pd.DataFrame(
            columns=[
                "apn",
                "official_address",
                "total_cases",
                "complaint_cases",
                "scep_cases",
                "other_cases",
                "first_case_year",
                "last_case_year",
            ]
        )
    cases = case_df.copy()
    dates = pd.to_datetime(cases["date_closed"], errors="coerce")
    cases["case_year"] = dates.dt.year
    cases["case_type_norm"] = cases["case_type"].fillna("").str.lower()
    cases["is_complaint"] = cases["case_type_norm"].eq("complaint")
    cases["is_scep"] = cases["case_type_norm"].eq("systematic code enforcement program")
    summary = (
        cases.groupby(["apn", "official_address"], as_index=False)
        .agg(
            total_cases=("case_number", "count"),
            complaint_cases=("is_complaint", "sum"),
            scep_cases=("is_scep", "sum"),
            first_case_year=("case_year", "min"),
            last_case_year=("case_year", "max"),
        )
    )
    summary["other_cases"] = (
        summary["total_cases"] - summary["complaint_cases"] - summary["scep_cases"]
    )
    return summary


def summarize_property_activity_sample(
    property_df: pd.DataFrame,
    case_df: pd.DataFrame,
    query_df: pd.DataFrame,
) -> pd.DataFrame:
    property_level = property_df.copy()
    case_summary = build_property_level_case_summary(case_df)
    if not property_level.empty:
        property_level = property_level.merge(case_summary, how="left", on=["apn", "official_address"])
    for col in ["total_cases", "complaint_cases", "scep_cases", "other_cases"]:
        if col in property_level.columns:
            property_level[col] = property_level[col].fillna(0)
    registered = _registered_indicator(property_level) if not property_level.empty else pd.Series(dtype=bool)

    metrics: list[dict[str, object]] = [
        {"metric": "street_queries", "value": int(len(query_df))},
        {"metric": "properties_sampled", "value": int(len(property_df))},
        {"metric": "unique_apn", "value": int(property_df["apn"].nunique()) if not property_df.empty else 0},
        {"metric": "properties_with_rent_registration", "value": int(registered.sum()) if not property_level.empty else 0},
        {"metric": "properties_without_rent_registration", "value": int((~registered).sum()) if not property_level.empty else 0},
        {"metric": "properties_with_case_history", "value": int(case_df["apn"].nunique()) if not case_df.empty else 0},
        {"metric": "total_cases", "value": int(len(case_df))},
    ]

    if not property_level.empty:
        metrics.extend(
            [
                {"metric": "mean_total_units_registered", "value": _safe_mean(property_level.loc[registered, "total_units"])},
                {"metric": "mean_total_units_unregistered", "value": _safe_mean(property_level.loc[~registered, "total_units"])},
                {"metric": "mean_year_built_registered", "value": _safe_mean(property_level.loc[registered, "year_built"])},
                {"metric": "mean_year_built_unregistered", "value": _safe_mean(property_level.loc[~registered, "year_built"])},
                {"metric": "mean_cases_registered", "value": _safe_mean(property_level.loc[registered, "total_cases"])},
                {"metric": "mean_cases_unregistered", "value": _safe_mean(property_level.loc[~registered, "total_cases"])},
                {"metric": "mean_complaints_registered", "value": _safe_mean(property_level.loc[registered, "complaint_cases"])},
                {"metric": "mean_complaints_unregistered", "value": _safe_mean(property_level.loc[~registered, "complaint_cases"])},
                {"metric": "mean_scep_registered", "value": _safe_mean(property_level.loc[registered, "scep_cases"])},
                {"metric": "mean_scep_unregistered", "value": _safe_mean(property_level.loc[~registered, "scep_cases"])},
            ]
        )

    return pd.DataFrame(metrics)


def build_registration_comparison(property_df: pd.DataFrame, case_df: pd.DataFrame) -> pd.DataFrame:
    if property_df.empty:
        return pd.DataFrame(columns=["group", "properties", "mean_total_units", "mean_year_built", "mean_total_cases", "mean_complaint_cases", "mean_scep_cases"])
    property_level = property_df.copy()
    case_summary = build_property_level_case_summary(case_df)
    property_level = property_level.merge(case_summary, how="left", on=["apn", "official_address"])
    for col in ["total_cases", "complaint_cases", "scep_cases"]:
        property_level[col] = property_level[col].fillna(0)
    property_level["registered_group"] = _registered_indicator(property_level).map(
        {True: "registered", False: "not_registered"}
    )
    out = (
        property_level.groupby("registered_group", as_index=False)
        .agg(
            properties=("apn", "nunique"),
            mean_total_units=("total_units", lambda s: _safe_mean(s)),
            mean_year_built=("year_built", lambda s: _safe_mean(s)),
            mean_total_cases=("total_cases", "mean"),
            mean_complaint_cases=("complaint_cases", "mean"),
            mean_scep_cases=("scep_cases", "mean"),
        )
        .rename(columns={"registered_group": "group"})
    )
    return out


def build_query_coverage_summary(
    property_df: pd.DataFrame,
    case_df: pd.DataFrame,
    query_df: pd.DataFrame,
) -> pd.DataFrame:
    if query_df.empty:
        return pd.DataFrame(
            columns=[
                "query_street_name",
                "search_results_sampled",
                "sample_properties",
                "properties_with_case_history",
                "properties_with_rent_registration",
                "detail_errors",
                "query_error",
            ]
        )
    properties = property_df.copy()
    if not properties.empty:
        properties["has_case_history"] = properties["apn"].fillna("").astype(str).isin(
            case_df["apn"].fillna("").astype(str) if not case_df.empty else []
        )
        properties["has_rent_registration"] = _registered_indicator(properties)
        properties["has_detail_error"] = properties.get("detail_error", "").fillna("").astype(str).str.strip().ne("")
        property_summary = (
            properties.groupby("query_street_name", as_index=False)
            .agg(
                sample_properties=("apn", "nunique"),
                properties_with_case_history=("has_case_history", "sum"),
                properties_with_rent_registration=("has_rent_registration", "sum"),
                detail_errors=("has_detail_error", "sum"),
            )
        )
    else:
        property_summary = pd.DataFrame(
            columns=[
                "query_street_name",
                "sample_properties",
                "properties_with_case_history",
                "properties_with_rent_registration",
                "detail_errors",
            ]
        )
    query_summary = query_df.rename(columns={"street_name": "query_street_name"}).copy()
    out = query_summary.merge(property_summary, how="left", on="query_street_name")
    for col in [
        "search_results_sampled",
        "sample_properties",
        "properties_with_case_history",
        "properties_with_rent_registration",
        "detail_errors",
    ]:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).astype(int)
    out["query_error"] = out["query_error"].fillna("").astype(str)
    return out.sort_values("query_street_name").reset_index(drop=True)


def build_sample_strata_summary(
    merged_df: pd.DataFrame,
    case_df: pd.DataFrame,
) -> pd.DataFrame:
    if merged_df.empty:
        return pd.DataFrame(
            columns=[
                "sample_group",
                "properties",
                "share_of_sample",
                "properties_with_case_history",
                "mean_total_cases",
                "mean_units",
                "mean_year_built",
            ]
        )
    property_level = merged_df.copy()
    case_summary = build_property_level_case_summary(case_df)
    property_level["apn"] = property_level["apn"].fillna("").astype(str).str.strip()
    property_level["official_address"] = property_level["official_address"].fillna("").astype(str)
    case_summary["apn"] = case_summary["apn"].fillna("").astype(str).str.strip()
    case_summary["official_address"] = case_summary["official_address"].fillna("").astype(str)
    property_level = property_level.merge(case_summary, how="left", on=["apn", "official_address"])
    for col in ["total_cases", "complaint_cases", "scep_cases"]:
        if col in property_level.columns:
            property_level[col] = property_level[col].fillna(0)
    property_level["has_case_history"] = property_level["total_cases"].gt(0)
    property_level["has_rent_registration"] = _registered_indicator(property_level)
    property_level["is_rso_eligible_proxy"] = property_level["rso_eligible_proxy"].fillna(False).astype(bool)
    assessor_units = (
        pd.to_numeric(property_level["Units"], errors="coerce")
        if "Units" in property_level.columns
        else pd.Series(index=property_level.index, dtype=float)
    )
    sample_units = (
        pd.to_numeric(property_level["total_units"], errors="coerce")
        if "total_units" in property_level.columns
        else pd.Series(index=property_level.index, dtype=float)
    )
    assessor_year_built = (
        pd.to_numeric(property_level["YearBuilt"], errors="coerce")
        if "YearBuilt" in property_level.columns
        else pd.Series(index=property_level.index, dtype=float)
    )
    sample_year_built = (
        pd.to_numeric(property_level["year_built"], errors="coerce")
        if "year_built" in property_level.columns
        else pd.Series(index=property_level.index, dtype=float)
    )
    property_level["units_context"] = assessor_units.combine_first(sample_units)
    property_level["year_built_context"] = assessor_year_built.combine_first(sample_year_built)

    total_properties = max(int(property_level["apn"].nunique()), 1)
    group_defs = [
        ("all_sampled", pd.Series(True, index=property_level.index, dtype=bool)),
        ("with_case_history", property_level["has_case_history"]),
        ("with_rent_registration", property_level["has_rent_registration"]),
        ("without_rent_registration", ~property_level["has_rent_registration"]),
        ("rso_eligible_proxy", property_level["is_rso_eligible_proxy"]),
        ("not_rso_eligible_proxy", ~property_level["is_rso_eligible_proxy"]),
    ]
    rows: list[dict[str, object]] = []
    for label, mask in group_defs:
        subset = property_level.loc[mask].copy()
        rows.append(
            {
                "sample_group": label,
                "properties": int(subset["apn"].nunique()),
                "share_of_sample": round(int(subset["apn"].nunique()) / total_properties, 4),
                "properties_with_case_history": int(subset.loc[subset["has_case_history"], "apn"].nunique()),
                "mean_total_cases": _safe_mean(subset["total_cases"]),
                "mean_units": _safe_mean(subset["units_context"]),
                "mean_year_built": _safe_mean(subset["year_built_context"]),
            }
        )
    return pd.DataFrame(rows)


def _registered_indicator(df: pd.DataFrame) -> pd.Series:
    return df["rent_registration_number"].fillna("").astype(str).str.strip().ne("")


def _safe_mean(series: pd.Series) -> float:
    values = pd.to_numeric(series, errors="coerce")
    mean = values.mean()
    return round(float(mean), 3) if pd.notna(mean) else float("nan")


def fetch_latest_roll_year(*, timeout: int = 60) -> str:
    response = _assessor_get(
        {
            "f": "json",
            "where": "1=1",
            "returnDistinctValues": "true",
            "returnGeometry": "false",
            "outFields": "RollYear",
            "orderByFields": "RollYear DESC",
            "resultRecordCount": 20,
        },
        timeout=timeout,
    )
    payload = response
    years = sorted(
        {
            feature["attributes"].get("RollYear", "")
            for feature in payload.get("features", [])
            if feature["attributes"].get("RollYear")
        },
        reverse=True,
    )
    if not years:
        raise ValueError("Could not determine latest LA assessor roll year")
    return years[0]


def fetch_assessor_records_for_ains(
    ains: Iterable[str],
    *,
    roll_year: str,
    timeout: int = 120,
    out_fields: Iterable[str] | None = None,
) -> pd.DataFrame:
    ain_list = [str(ain).strip() for ain in ains if str(ain).strip()]
    if not ain_list:
        return pd.DataFrame(columns=DEFAULT_ASSESSOR_FIELDS)
    fields = list(out_fields or DEFAULT_ASSESSOR_FIELDS)
    frames: list[pd.DataFrame] = []
    seen_ains: set[str] = set()
    for ain_batch in _chunked(ain_list, 25):
        try:
            payload = _assessor_get(
                {
                    "f": "json",
                    "where": _build_ain_in_clause(ain_batch),
                    "returnGeometry": "false",
                    "outFields": ",".join(fields),
                    "resultRecordCount": 1000,
                },
                timeout=timeout,
            )
            rows = [feature["attributes"] for feature in payload.get("features", [])]
            if rows:
                frame = pd.DataFrame(rows)
                frames.append(frame)
                if "AIN" in frame.columns:
                    seen_ains.update(frame["AIN"].fillna("").astype(str).str.strip().tolist())
        except Exception:
            # Fall back to per-AIN lookups so one slow batch does not discard the whole sample.
            pass
    for ain in ain_list:
        if ain in seen_ains:
            continue
        payload = _fetch_assessor_rows_for_ain(ain, fields=fields, timeout=timeout)
        rows = [feature["attributes"] for feature in payload.get("features", [])]
        if rows:
            frames.append(pd.DataFrame(rows))
    if not frames:
        return pd.DataFrame(columns=fields)
    frame = pd.concat(frames, ignore_index=True)
    frame = _prepare_assessor_frame(frame)
    if "RollYear" in frame.columns:
        frame = frame.loc[frame["RollYear"].fillna("").astype(str).eq(str(roll_year))].copy()
    if "AIN" in frame.columns:
        frame["AIN"] = frame["AIN"].fillna("").astype(str).str.strip()
        frame = frame.drop_duplicates(subset=["AIN"], keep="last").reset_index(drop=True)
    return frame


def fetch_assessor_count(
    *,
    where: str,
    timeout: int = 120,
) -> int:
    payload = _assessor_get(
        {
            "f": "json",
            "where": where,
            "returnCountOnly": "true",
        },
        timeout=timeout,
    )
    return int(payload.get("count", 0))


def build_assessor_citywide_summary(
    *,
    roll_year: str,
    timeout: int = 120,
) -> pd.DataFrame:
    city = f"RollYear='{roll_year}' AND TaxRateArea_CITY='LOS ANGELES'"
    metrics = [
        {"metric": "roll_year", "value": roll_year},
        {"metric": "city_parcels", "value": fetch_assessor_count(where=city, timeout=timeout)},
        {
            "metric": "city_multifamily_proxy_parcels",
            "value": fetch_assessor_count(where=f"{city} AND Units >= 2", timeout=timeout),
        },
        {
            "metric": "city_rso_eligible_proxy_parcels",
            "value": fetch_assessor_count(
                where=f"{city} AND Units >= 2 AND YearBuilt <= '1978'",
                timeout=timeout,
            ),
        },
        {
            "metric": "city_post_1978_multifamily_proxy_parcels",
            "value": fetch_assessor_count(
                where=f"{city} AND Units >= 2 AND YearBuilt > '1978'",
                timeout=timeout,
            ),
        },
    ]
    return pd.DataFrame(metrics)


def classify_assessor_proxy(frame: pd.DataFrame) -> pd.DataFrame:
    data = frame.copy()
    if data.empty:
        data["city_of_los_angeles"] = pd.Series(dtype=bool)
        data["multifamily_proxy"] = pd.Series(dtype=bool)
        data["rso_eligible_proxy"] = pd.Series(dtype=bool)
        return data
    year_built = pd.to_numeric(data.get("YearBuilt"), errors="coerce")
    units = pd.to_numeric(data.get("Units"), errors="coerce")
    data["city_of_los_angeles"] = data.get("TaxRateArea_CITY", "").fillna("").astype(str).str.upper().eq("LOS ANGELES")
    data["multifamily_proxy"] = units.ge(2)
    data["rso_eligible_proxy"] = data["city_of_los_angeles"] & data["multifamily_proxy"] & year_built.le(1978)
    return data


def merge_sample_with_assessor(
    property_df: pd.DataFrame,
    assessor_df: pd.DataFrame,
) -> pd.DataFrame:
    properties = property_df.copy()
    properties["apn"] = properties["apn"].fillna("").astype(str).str.strip()
    assessor = classify_assessor_proxy(assessor_df)
    if not assessor.empty:
        assessor["AIN"] = assessor["AIN"].fillna("").astype(str).str.strip()
    merged = properties.merge(
        assessor,
        how="left",
        left_on="apn",
        right_on="AIN",
        suffixes=("", "_assessor"),
    )
    return merged


def summarize_sample_assessor_backbone(
    merged_df: pd.DataFrame,
    case_df: pd.DataFrame,
    *,
    roll_year: str,
) -> pd.DataFrame:
    if merged_df.empty:
        return pd.DataFrame(columns=["metric", "value"])
    metrics = [
        {"metric": "roll_year", "value": roll_year},
        {"metric": "sample_properties", "value": int(len(merged_df))},
        {"metric": "sample_assessor_matches", "value": int(merged_df["AIN"].fillna("").astype(str).str.len().gt(0).sum())},
        {"metric": "sample_rso_eligible_proxy", "value": int(merged_df["rso_eligible_proxy"].fillna(False).sum())},
        {"metric": "sample_multifamily_proxy", "value": int(merged_df["multifamily_proxy"].fillna(False).sum())},
        {"metric": "sample_properties_with_case_history", "value": int(case_df["apn"].nunique()) if not case_df.empty else 0},
    ]
    return pd.DataFrame(metrics)


def build_sample_assessor_group_comparison(
    merged_df: pd.DataFrame,
    case_df: pd.DataFrame,
) -> pd.DataFrame:
    if merged_df.empty:
        return pd.DataFrame(
            columns=[
                "group",
                "properties",
                "mean_units",
                "mean_year_built",
                "mean_total_cases",
                "mean_complaint_cases",
                "mean_scep_cases",
            ]
        )
    property_level = merged_df.copy()
    case_summary = build_property_level_case_summary(case_df)
    property_level["apn"] = property_level["apn"].fillna("").astype(str).str.strip()
    property_level["official_address"] = property_level["official_address"].fillna("").astype(str)
    case_summary["apn"] = case_summary["apn"].fillna("").astype(str).str.strip()
    case_summary["official_address"] = case_summary["official_address"].fillna("").astype(str)
    property_level = property_level.merge(case_summary, how="left", on=["apn", "official_address"])
    for col in ["total_cases", "complaint_cases", "scep_cases"]:
        if col in property_level.columns:
            property_level[col] = property_level[col].fillna(0)
    property_level["group"] = property_level["rso_eligible_proxy"].fillna(False).map(
        {True: "rso_eligible_proxy", False: "not_rso_eligible_proxy"}
    )
    out = (
        property_level.groupby("group", as_index=False)
        .agg(
            properties=("apn", "nunique"),
            mean_units=("Units", lambda s: _safe_mean(s)),
            mean_year_built=("YearBuilt", lambda s: _safe_mean(s)),
            mean_total_cases=("total_cases", "mean"),
            mean_complaint_cases=("complaint_cases", "mean"),
            mean_scep_cases=("scep_cases", "mean"),
        )
    )
    return out


def _prepare_assessor_frame(frame: pd.DataFrame) -> pd.DataFrame:
    data = frame.copy()
    for col in ["Units", "SQFTmain", "Bedrooms", "Bathrooms", "Roll_TotalValue"]:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce")
    if "RecordingDate" in data.columns:
        data["RecordingDate"] = pd.to_datetime(data["RecordingDate"], errors="coerce", unit="ms")
    return data


def _build_ain_window_clause(ain: str) -> str:
    normalized = str(ain).strip()
    if normalized.isdigit():
        value = int(normalized)
        lower = str(value - 1).zfill(len(normalized))
        upper = str(value + 1).zfill(len(normalized))
        return f"(AIN > '{lower}' AND AIN < '{upper}')"
    escaped = normalized.replace("'", "''")
    return f"(AIN >= '{escaped}' AND AIN <= '{escaped}')"


def _build_ain_exact_clause(ain: str) -> str:
    escaped = str(ain).strip().replace("'", "''")
    return f"AIN = '{escaped}'"


def _build_ain_in_clause(ains: Iterable[str]) -> str:
    escaped: list[str] = []
    for ain in ains:
        normalized = str(ain).strip()
        if not normalized:
            continue
        escaped.append("'" + normalized.replace("'", "''") + "'")
    if not escaped:
        return "1=0"
    return f"AIN IN ({','.join(escaped)})"


def _fetch_assessor_rows_for_ain(
    ain: str,
    *,
    fields: list[str],
    timeout: int,
) -> dict:
    exact_payload = _assessor_get(
        {
            "f": "json",
            "where": _build_ain_exact_clause(ain),
            "returnGeometry": "false",
            "outFields": ",".join(fields),
            "resultRecordCount": 25,
        },
        timeout=timeout,
    )
    if exact_payload.get("features"):
        return exact_payload
    return _assessor_get(
        {
            "f": "json",
            "where": _build_ain_window_clause(ain),
            "returnGeometry": "false",
            "outFields": ",".join(fields),
            "resultRecordCount": 500,
        },
        timeout=timeout,
    )


def _chunked(items: list[str], size: int) -> Iterable[list[str]]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _assessor_get(params: dict[str, object], *, timeout: int) -> dict:
    request_error: Exception | None = None
    session = _get_assessor_session()
    try:
        response = session.post(
            ASSESSOR_FEATURESERVER_URL,
            data=params,
            headers=ASSESSOR_HEADERS,
            timeout=(10, timeout),
        )
        response.raise_for_status()
        payload = response.json()
        if "error" in payload:
            raise ValueError(payload["error"])
        return payload
    except (requests.RequestException, ValueError, json.JSONDecodeError) as exc:
        request_error = exc

    try:
        response = session.get(
            ASSESSOR_FEATURESERVER_URL,
            params=params,
            headers=ASSESSOR_HEADERS,
            timeout=(10, timeout),
        )
        response.raise_for_status()
        payload = response.json()
        if "error" in payload:
            raise ValueError(payload["error"])
        return payload
    except (requests.RequestException, ValueError, json.JSONDecodeError) as exc:
        request_error = exc

    query = urlencode(params, quote_via=quote)
    try:
        result = subprocess.run(
            [
                "curl",
                "-sS",
                "-A",
                ASSESSOR_HEADERS["User-Agent"],
                "-H",
                f"Accept: {ASSESSOR_HEADERS['Accept']}",
                "-H",
                f"Referer: {ASSESSOR_HEADERS['Referer']}",
                "-X",
                "POST",
                "--data-raw",
                query,
                "--max-time",
                str(timeout),
                ASSESSOR_FEATURESERVER_URL,
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        payload = json.loads(result.stdout)
        if "error" in payload:
            raise ValueError(payload["error"])
        return payload
    except Exception as curl_exc:
        if request_error is not None:
            raise RuntimeError(
                f"LA assessor transport failed for params={params!r}; "
                f"requests_error={request_error}; curl_error={curl_exc}"
            ) from curl_exc
        raise


def _get_assessor_session() -> requests.Session:
    global _ASSESSOR_SESSION
    if _ASSESSOR_SESSION is not None:
        return _ASSESSOR_SESSION
    retry = Retry(
        total=2,
        connect=2,
        read=2,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    _ASSESSOR_SESSION = session
    return session
