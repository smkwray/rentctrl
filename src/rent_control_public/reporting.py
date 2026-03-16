from __future__ import annotations

import pandas as pd


def add_per_1000_metric(
    df: pd.DataFrame,
    *,
    numerator_col: str,
    denominator_col: str,
    output_col: str,
) -> pd.DataFrame:
    out = df.copy()
    denominator = pd.to_numeric(out[denominator_col], errors="coerce")
    numerator = pd.to_numeric(out[numerator_col], errors="coerce")
    out[output_col] = numerator.div(denominator.where(denominator.ne(0))).mul(1000)
    return out


def summarize_event_window_coefficients(
    coef: pd.DataFrame,
    *,
    group_cols: list[str] | None = None,
) -> pd.DataFrame:
    if coef.empty:
        base_cols = group_cols or []
        return pd.DataFrame(columns=base_cols + ["avg_pre_coef", "max_abs_pre_coef", "avg_post_coef", "first_post_coef", "last_post_coef"])

    group_cols = group_cols or []
    work = coef.copy()
    work["event_time"] = pd.to_numeric(work["event_time"], errors="coerce")
    work["coef"] = pd.to_numeric(work["coef"], errors="coerce")

    if group_cols:
        grouped = work.groupby(group_cols, dropna=False)
    else:
        grouped = [((), work)]

    rows: list[dict[str, object]] = []
    for key, frame in grouped:
        row: dict[str, object] = {}
        if group_cols:
            if len(group_cols) == 1:
                if isinstance(key, tuple):
                    key = key[0]
                row[group_cols[0]] = key
            else:
                row.update(dict(zip(group_cols, key)))

        pre = frame[frame["event_time"] <= -2]
        post = frame[frame["event_time"] >= 0].sort_values("event_time")

        row["avg_pre_coef"] = pre["coef"].mean()
        row["max_abs_pre_coef"] = pre["coef"].abs().max()
        row["avg_post_coef"] = post["coef"].mean()
        row["first_post_coef"] = post.iloc[0]["coef"] if not post.empty else pd.NA
        row["last_post_coef"] = post.iloc[-1]["coef"] if not post.empty else pd.NA
        rows.append(row)

    return pd.DataFrame(rows)
