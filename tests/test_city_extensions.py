from __future__ import annotations

import pandas as pd
import pytest

from rent_control_public.city_extensions import TOP_CITY_ORDER, build_priority_shortlist, build_question_shortlist


def test_build_priority_shortlist_uses_expected_top_city_order() -> None:
    city_catalog = pd.DataFrame(
        {
            "city": TOP_CITY_ORDER,
            "state_abbr": ["NY", "CA", "CA", "CA", "DC", "MN"],
            "policy_status": ["a", "b", "c", "d", "e", "f"],
            "build_priority": ["high"] * 6,
            "bulk_public_access": ["yes"] * 6,
            "official_policy_source": [f"https://example.com/{i}" for i in range(6)],
        }
    )
    question_catalog = pd.DataFrame(
        {
            "city": TOP_CITY_ORDER,
            "question_id": [f"Q{i}" for i in range(6)],
            "question_family": ["fam"] * 6,
            "question_text": [f"Question {i}" for i in range(6)],
            "feasibility": ["high"] * 6,
        }
    )

    out = build_priority_shortlist(city_catalog, question_catalog)

    assert list(out["city"]) == TOP_CITY_ORDER
    assert list(out["priority_rank"]) == [1, 2, 3, 4, 5, 6]


def test_build_question_shortlist_filters_to_top_cities() -> None:
    question_catalog = pd.DataFrame(
        {
            "city": TOP_CITY_ORDER + ["Other City"],
            "question_id": [f"Q{i}" for i in range(len(TOP_CITY_ORDER))] + ["OTHER_Q1"],
            "question_family": ["quality"] * len(TOP_CITY_ORDER) + ["other"],
            "question_text": [f"question {i}" for i in range(len(TOP_CITY_ORDER))] + ["c"],
            "feasibility": ["high", "medium", "medium", "medium", "medium", "medium", "high"],
        }
    )

    out = build_question_shortlist(question_catalog)

    assert list(out["city"]) == TOP_CITY_ORDER
    assert "OTHER_Q1" not in set(out["question_id"])


def test_build_priority_shortlist_raises_for_missing_city() -> None:
    city_catalog = pd.DataFrame(
        {
            "city": ["New York City"],
            "state_abbr": ["NY"],
            "policy_status": ["active"],
            "build_priority": ["high"],
            "bulk_public_access": ["yes"],
            "official_policy_source": ["https://example.com/nyc"],
        }
    )
    question_catalog = pd.DataFrame(
        {
            "city": ["New York City"],
            "question_id": ["NYC_Q1"],
            "question_family": ["quality"],
            "question_text": ["Question"],
            "feasibility": ["high"],
        }
    )

    with pytest.raises(ValueError, match="Missing `Los Angeles` in city catalog."):
        build_priority_shortlist(city_catalog, question_catalog)


def test_build_question_shortlist_raises_for_missing_top_city() -> None:
    question_catalog = pd.DataFrame(
        {
            "city": ["New York City", "Los Angeles"],
            "question_id": ["NYC_Q1", "LA_Q1"],
            "question_family": ["quality", "quality"],
            "question_text": ["a", "b"],
            "feasibility": ["high", "medium"],
        }
    )

    with pytest.raises(ValueError, match="Question catalog missing top cities"):
        build_question_shortlist(question_catalog)
