from __future__ import annotations

import pandas as pd

from rent_control_public.reporting import add_per_1000_metric, summarize_event_window_coefficients


def test_add_per_1000_metric_handles_zero_denominator() -> None:
    df = pd.DataFrame(
        {
            "permits": [50, 10],
            "renters": [2000, 0],
        }
    )

    out = add_per_1000_metric(
        df,
        numerator_col="permits",
        denominator_col="renters",
        output_col="permits_per_1000_renters",
    )

    assert out.loc[0, "permits_per_1000_renters"] == 25
    assert pd.isna(out.loc[1, "permits_per_1000_renters"])


def test_summarize_event_window_coefficients_by_state() -> None:
    coef = pd.DataFrame(
        {
            "state_abbr": ["CA", "CA", "CA", "OR", "OR", "OR"],
            "event_time": [-2, 0, 2, -3, 1, 3],
            "coef": [1.0, 2.0, 4.0, -1.5, 0.5, 1.5],
        }
    )

    summary = summarize_event_window_coefficients(coef, group_cols=["state_abbr"]).sort_values("state_abbr").reset_index(drop=True)

    assert list(summary["state_abbr"]) == ["CA", "OR"]
    assert summary.loc[0, "avg_pre_coef"] == 1.0
    assert summary.loc[0, "avg_post_coef"] == 3.0
    assert summary.loc[0, "first_post_coef"] == 2.0
    assert summary.loc[0, "last_post_coef"] == 4.0
    assert summary.loc[1, "max_abs_pre_coef"] == 1.5
