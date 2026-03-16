from __future__ import annotations

from dataclasses import dataclass
import pandas as pd
import statsmodels.formula.api as smf


@dataclass
class EventStudyResult:
    formula: str
    model_summary: str
    coefficient_table: pd.DataFrame


def _event_col_name(k: int) -> str:
    return f"evt_m{abs(k)}" if k < 0 else f"evt_p{k}"


def add_binned_event_time_dummies(
    df: pd.DataFrame,
    event_time_col: str,
    *,
    min_bin: int = -5,
    max_bin: int = 5,
    reference_period: int = -1,
) -> pd.DataFrame:
    out = df.copy()
    et = pd.to_numeric(out[event_time_col], errors="coerce")
    et = et.clip(lower=min_bin, upper=max_bin)
    out["_event_time_binned"] = et

    for k in range(min_bin, max_bin + 1):
        if k == reference_period:
            continue
        out[_event_col_name(k)] = (out["_event_time_binned"] == k).fillna(False).astype(int)

    return out


def fit_twfe_event_study(
    df: pd.DataFrame,
    *,
    outcome: str,
    unit_col: str,
    time_col: str,
    event_prefix: str = "evt_",
) -> EventStudyResult:
    event_cols = [c for c in df.columns if c.startswith(event_prefix)]
    if not event_cols:
        raise ValueError("No event-study dummies found. Run add_binned_event_time_dummies first.")

    rhs = " + ".join(event_cols + [f"C({unit_col})", f"C({time_col})"])
    formula = f"{outcome} ~ {rhs}"
    model = smf.ols(formula, data=df).fit(cov_type="HC1")
    coefficient_table = pd.DataFrame(
        {
            "term": model.params.index,
            "coef": model.params.values,
            "std_err": model.bse.values,
            "t": model.tvalues.values,
            "p_value": model.pvalues.values,
        }
    )
    try:
        model_summary = model.summary().as_text()
    except ValueError as exc:
        model_summary = "\n".join(
            [
                f"Formula: {formula}",
                f"Summary fallback: statsmodels summary() failed with `{exc}`.",
                "",
                coefficient_table.to_string(index=False),
            ]
        )
    return EventStudyResult(formula=formula, model_summary=model_summary, coefficient_table=coefficient_table)


def extract_event_study_coefficients(
    result: EventStudyResult,
    *,
    event_prefix: str = "evt_",
) -> pd.DataFrame:
    coef = result.coefficient_table.copy()
    coef = coef[coef["term"].str.startswith(event_prefix)].copy()
    if coef.empty:
        return coef

    def parse_event_time(term: str) -> int:
        suffix = term.removeprefix(event_prefix)
        if suffix.startswith("m"):
            return -int(suffix[1:])
        if suffix.startswith("p"):
            return int(suffix[1:])
        raise ValueError(f"Unexpected event-study term: {term}")

    coef["event_time"] = coef["term"].map(parse_event_time)
    coef["ci_low"] = coef["coef"] - 1.96 * coef["std_err"]
    coef["ci_high"] = coef["coef"] + 1.96 * coef["std_err"]
    return coef.sort_values("event_time").reset_index(drop=True)
