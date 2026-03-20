from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf


@dataclass
class EventStudyResult:
    formula: str
    model_summary: str
    coefficient_table: pd.DataFrame
    infer_method: str = "conventional_hc1"
    resample_count: int = 0


def _event_col_name(k: int) -> str:
    return f"evt_m{abs(k)}" if k < 0 else f"evt_p{k}"


def _parse_event_term(term: str, *, event_prefix: str = "evt_") -> int:
    suffix = term.removeprefix(event_prefix)
    if suffix.startswith("m"):
        return -int(suffix[1:])
    if suffix.startswith("p"):
        return int(suffix[1:])
    raise ValueError(f"Unexpected event-study term: {term}")


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


def _fit_ols(formula: str, df: pd.DataFrame):
    return smf.ols(formula, data=df).fit(cov_type="HC1")


def _event_column_bounds(event_cols: list[str], *, event_prefix: str) -> tuple[int, int]:
    event_times = [_parse_event_term(col, event_prefix=event_prefix) for col in event_cols]
    return min(event_times), max(event_times)


def _reference_period(event_cols: list[str], *, event_prefix: str) -> int:
    event_times = {_parse_event_term(col, event_prefix=event_prefix) for col in event_cols}
    for candidate in range(min(event_times), max(event_times) + 1):
        if candidate not in event_times:
            return candidate
    return -1


def _time_index(frame: pd.DataFrame, time_col: str) -> pd.Series:
    if pd.api.types.is_numeric_dtype(frame[time_col]):
        values = pd.to_numeric(frame[time_col], errors="coerce")
        if values.notna().all():
            return values.astype(int)
    ordered = pd.Index(pd.Series(frame[time_col]).dropna().astype(str).unique())
    time_lookup = {value: idx for idx, value in enumerate(sorted(ordered))}
    return frame[time_col].astype(str).map(time_lookup).astype("Int64")


def _treated_unit_timing_map(df: pd.DataFrame, *, unit_col: str, time_col: str, event_time_col: str) -> dict[object, int]:
    work = df[[unit_col, time_col, event_time_col]].copy()
    work = work.dropna(subset=[event_time_col])
    if work.empty:
        return {}
    work["_time_index"] = _time_index(work, time_col)
    work["_event_time"] = pd.to_numeric(work[event_time_col], errors="coerce").astype("Int64")
    work["_treat_index"] = work["_time_index"] - work["_event_time"]
    treated_map: dict[object, int] = {}
    for unit, frame in work.groupby(unit_col, dropna=False):
        treat_indexes = frame["_treat_index"].dropna().astype(int)
        if treat_indexes.empty:
            continue
        treated_map[unit] = int(treat_indexes.mode().iloc[0])
    return treated_map


def _permuted_event_frame(
    df: pd.DataFrame,
    *,
    unit_col: str,
    time_col: str,
    event_time_col: str,
    event_prefix: str,
    rng: np.random.Generator,
) -> pd.DataFrame:
    treated_timing = _treated_unit_timing_map(df, unit_col=unit_col, time_col=time_col, event_time_col=event_time_col)
    if not treated_timing:
        raise ValueError("Permutation inference requires at least one treated unit.")

    time_index = _time_index(df, time_col)
    units = pd.Index(df[unit_col].dropna().unique())
    if len(units) < len(treated_timing):
        raise ValueError("Permutation inference requires at least as many units as treated assignments.")

    permuted_units = list(rng.choice(units.to_numpy(), size=len(treated_timing), replace=False))
    treat_indexes = list(treated_timing.values())
    rng.shuffle(treat_indexes)
    permuted_assignment = dict(zip(permuted_units, treat_indexes))

    min_bin, max_bin = _event_column_bounds([c for c in df.columns if c.startswith(event_prefix)], event_prefix=event_prefix)
    reference_period = _reference_period([c for c in df.columns if c.startswith(event_prefix)], event_prefix=event_prefix)

    out = df.copy()
    out["_time_index"] = time_index
    out[event_time_col] = out[unit_col].map(permuted_assignment)
    out[event_time_col] = out["_time_index"] - pd.to_numeric(out[event_time_col], errors="coerce")
    out = add_binned_event_time_dummies(
        out,
        event_time_col,
        min_bin=min_bin,
        max_bin=max_bin,
        reference_period=reference_period,
    )
    return out.drop(columns=["_time_index"])


def _permutation_inference(
    df: pd.DataFrame,
    *,
    formula: str,
    event_cols: list[str],
    unit_col: str,
    time_col: str,
    event_time_col: str,
    event_prefix: str,
    resample_count: int,
    random_seed: int,
) -> pd.DataFrame:
    rng = np.random.default_rng(random_seed)
    rows: list[dict[str, object]] = []
    for _ in range(resample_count):
        try:
            permuted = _permuted_event_frame(
                df,
                unit_col=unit_col,
                time_col=time_col,
                event_time_col=event_time_col,
                event_prefix=event_prefix,
                rng=rng,
            )
            model = _fit_ols(formula, permuted)
        except Exception:
            continue
        row = {col: model.params.get(col, np.nan) for col in event_cols}
        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=event_cols)
    return pd.DataFrame(rows)


def fit_twfe_event_study(
    df: pd.DataFrame,
    *,
    outcome: str,
    unit_col: str,
    time_col: str,
    event_prefix: str = "evt_",
    event_time_col: str | None = None,
    resampled_inference: str | None = None,
    resample_count: int = 0,
    random_seed: int = 0,
) -> EventStudyResult:
    event_cols = [c for c in df.columns if c.startswith(event_prefix)]
    if not event_cols:
        raise ValueError("No event-study dummies found. Run add_binned_event_time_dummies first.")

    rhs = " + ".join(event_cols + [f"C({unit_col})", f"C({time_col})"])
    formula = f"{outcome} ~ {rhs}"
    model = _fit_ols(formula, df)
    coefficient_table = pd.DataFrame(
        {
            "term": model.params.index,
            "coef": model.params.values,
            "std_err": model.bse.values,
            "t": model.tvalues.values,
            "p_value": model.pvalues.values,
            "infer_method": "conventional_hc1",
            "p_value_resampled": pd.NA,
            "ci_low_resampled": pd.NA,
            "ci_high_resampled": pd.NA,
            "resample_count": 0,
        }
    )

    infer_method = "conventional_hc1"
    if resampled_inference == "permutation" and event_time_col is not None and resample_count > 0:
        permutation_draws = _permutation_inference(
            df,
            formula=formula,
            event_cols=event_cols,
            unit_col=unit_col,
            time_col=time_col,
            event_time_col=event_time_col,
            event_prefix=event_prefix,
            resample_count=resample_count,
            random_seed=random_seed,
        )
        if not permutation_draws.empty:
            infer_method = "permutation"
            for event_col in event_cols:
                term_mask = coefficient_table["term"].eq(event_col)
                observed = float(coefficient_table.loc[term_mask, "coef"].iloc[0])
                draws = pd.to_numeric(permutation_draws[event_col], errors="coerce").dropna()
                if draws.empty:
                    continue
                two_sided_p = (float((draws.abs() >= abs(observed)).sum()) + 1.0) / (float(len(draws)) + 1.0)
                coefficient_table.loc[term_mask, "infer_method"] = "permutation"
                coefficient_table.loc[term_mask, "p_value_resampled"] = two_sided_p
                coefficient_table.loc[term_mask, "ci_low_resampled"] = float(draws.quantile(0.025))
                coefficient_table.loc[term_mask, "ci_high_resampled"] = float(draws.quantile(0.975))
                coefficient_table.loc[term_mask, "resample_count"] = int(len(draws))

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

    model_summary = "\n".join(
        [
            "Inference note: conventional HC1 standard errors are retained for continuity; resampled inference columns are the headline uncertainty output when available.",
            f"Resampled inference: {infer_method} (draws={int(coefficient_table['resample_count'].max()) if not coefficient_table.empty else 0})",
            "",
            model_summary,
        ]
    )

    return EventStudyResult(
        formula=formula,
        model_summary=model_summary,
        coefficient_table=coefficient_table,
        infer_method=infer_method,
        resample_count=int(coefficient_table["resample_count"].max()) if not coefficient_table.empty else 0,
    )


def extract_event_study_coefficients(
    result: EventStudyResult,
    *,
    event_prefix: str = "evt_",
) -> pd.DataFrame:
    coef = result.coefficient_table.copy()
    coef = coef[coef["term"].str.startswith(event_prefix)].copy()
    if coef.empty:
        return coef

    coef["event_time"] = coef["term"].map(lambda term: _parse_event_term(term, event_prefix=event_prefix))
    coef["ci_low"] = coef["coef"] - 1.96 * coef["std_err"]
    coef["ci_high"] = coef["coef"] + 1.96 * coef["std_err"]
    return coef.sort_values("event_time").reset_index(drop=True)
