from __future__ import annotations

import pandas as pd


TOP_CITY_ORDER = [
    "New York City",
    "Los Angeles",
    "Oakland",
    "Berkeley",
    "Washington",
    "Saint Paul",
]

CITY_REASON_MAP = {
    "New York City": "Best public quality and maintenance extension with official treated-stock and violations data.",
    "Los Angeles": "Best official enforcement, exits, and habitability dashboard package outside NYC.",
    "Oakland": "Best petition and enforcement channel with RAP cases and eviction-linked administration.",
    "Berkeley": "Best local legal-ceiling and registry-style mechanism extension.",
    "Washington": "Best public rent registry transparency extension with analyst-facing official database.",
    "Saint Paul": "Best modern city ordinance and amendment case with clean timing.",
}

CITY_NEXT_ACTION_MAP = {
    "New York City": "Audit RSBL file formats and HPD violation joins at building level.",
    "Los Angeles": "Audit dashboard export formats for RSO inventory, Ellis Act, THP, and code inspection series.",
    "Oakland": "Audit RAP case fields, eviction filing pathways, and registry/public export boundaries.",
    "Berkeley": "Audit registry extraction path and legal-ceiling fields before any panel design.",
    "Washington": "Audit RentRegistry fields and public query/export mechanics.",
    "Saint Paul": "Build ordinance timeline and amendment windows, then reuse federal outcome backbone.",
}


def _require_city_rows(df: pd.DataFrame, city: str, *, label: str) -> pd.DataFrame:
    rows = df.loc[df["city"] == city].copy()
    if rows.empty:
        raise ValueError(f"Missing `{city}` in {label}.")
    return rows


def build_priority_shortlist(
    city_catalog: pd.DataFrame,
    question_catalog: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for rank, city in enumerate(TOP_CITY_ORDER, start=1):
        city_row = _require_city_rows(city_catalog, city, label="city catalog").iloc[0]
        city_questions = _require_city_rows(question_catalog, city, label="question catalog")
        high_questions = city_questions[city_questions["feasibility"] == "high"]
        best_question = high_questions.iloc[0] if not high_questions.empty else city_questions.iloc[0]
        rows.append(
            {
                "priority_rank": rank,
                "city": city,
                "state_abbr": city_row["state_abbr"],
                "policy_status": city_row["policy_status"],
                "build_priority": city_row["build_priority"],
                "bulk_public_access": city_row["bulk_public_access"],
                "primary_question_family": best_question["question_family"],
                "best_first_question_id": best_question["question_id"],
                "best_first_question": best_question["question_text"],
                "why_now": CITY_REASON_MAP[city],
                "next_backend_action": CITY_NEXT_ACTION_MAP[city],
                "official_policy_source": city_row["official_policy_source"],
            }
        )
    return pd.DataFrame(rows)


def build_question_shortlist(question_catalog: pd.DataFrame) -> pd.DataFrame:
    missing = [city for city in TOP_CITY_ORDER if city not in set(question_catalog["city"])]
    if missing:
        raise ValueError(f"Question catalog missing top cities: {', '.join(missing)}")

    shortlist = question_catalog[question_catalog["city"].isin(TOP_CITY_ORDER)].copy()
    shortlist["priority_rank"] = shortlist["city"].map({city: idx for idx, city in enumerate(TOP_CITY_ORDER, start=1)})
    return shortlist.sort_values(["priority_rank", "feasibility", "question_id"]).reset_index(drop=True)
