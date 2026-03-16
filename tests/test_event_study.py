import pandas as pd

from rent_control_public.event_study import EventStudyResult, extract_event_study_coefficients


def test_extract_event_study_coefficients_orders_terms():
    result = EventStudyResult(
        formula="y ~ evt_m2 + evt_p1",
        model_summary="summary",
        coefficient_table=pd.DataFrame(
            [
                {"term": "Intercept", "coef": 1.0, "std_err": 0.1, "t": 10.0, "p_value": 0.0},
                {"term": "evt_p1", "coef": 0.3, "std_err": 0.2, "t": 1.5, "p_value": 0.2},
                {"term": "evt_m2", "coef": -0.4, "std_err": 0.1, "t": -4.0, "p_value": 0.01},
            ]
        ),
    )

    out = extract_event_study_coefficients(result)

    assert out["event_time"].tolist() == [-2, 1]
    assert out["ci_low"].iloc[0] < out["coef"].iloc[0] < out["ci_high"].iloc[0]
