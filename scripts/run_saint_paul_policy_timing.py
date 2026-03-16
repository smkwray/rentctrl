from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from rent_control_public.bps import parse_county_ytd_file
from rent_control_public.qcew import fetch_area_slice
from rent_control_public.saint_paul import (
    PRIMARY_CONTROL_COUNTIES,
    RAMSEY_COUNTY_FIPS,
    TWIN_CITIES_MSA_CBSA,
    add_quarter_period,
    build_policy_event_table,
    label_pre_post,
    summarize_treated_vs_controls,
    summarize_by_period,
)

BPS_URL_TEMPLATE = 'https://www2.census.gov/econ/bps/County/co{year}a.txt'
BPS_START_YEAR = 2015
FHFA_START_YEAR = 2018
QCEW_START_YEAR = 2019
COUNTY_NAME_BY_FIPS = {
    RAMSEY_COUNTY_FIPS: 'Ramsey County',
    **{fips: name for name, fips in PRIMARY_CONTROL_COUNTIES.items()},
}


def ensure_dirs() -> tuple[Path, Path, Path, Path]:
    raw_bps = ROOT / 'data' / 'raw' / 'bps' / 'county'
    raw_qcew = ROOT / 'data' / 'raw' / 'qcew' / 'county'
    results_tables = ROOT / 'results' / 'tables'
    results_figures = ROOT / 'results' / 'figures'
    raw_bps.mkdir(parents=True, exist_ok=True)
    raw_qcew.mkdir(parents=True, exist_ok=True)
    results_tables.mkdir(parents=True, exist_ok=True)
    results_figures.mkdir(parents=True, exist_ok=True)
    return raw_bps, raw_qcew, results_tables, results_figures


def download_bps_county_file(year: int, raw_dir: Path) -> Path | None:
    path = raw_dir / f'co{year}a.txt'
    if path.exists():
        return path
    response = requests.get(BPS_URL_TEMPLATE.format(year=year), timeout=60)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    path.write_text(response.text)
    return path


def load_bps_county_panel(raw_dir: Path, *, start_year: int = BPS_START_YEAR, end_year: int = 2025) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    target_fips = set(COUNTY_NAME_BY_FIPS)
    for year in range(start_year, end_year + 1):
        path = download_bps_county_file(year, raw_dir)
        if path is None:
            continue
        df = parse_county_ytd_file(path)
        sample = df[(df['state_fips'] == '27') & (df['state_county_fips'].isin(target_fips))].copy()
        sample['county_fips_full'] = sample['state_county_fips']
        sample['county_name'] = sample['county_name'].astype(str).str.strip()
        sample['date'] = pd.to_datetime(sample['year'].astype(str) + '-01-01')
        frames.append(sample)
    if not frames:
        raise RuntimeError('No Saint Paul BPS county rows were loaded.')
    out = pd.concat(frames, ignore_index=True)
    return out.sort_values(['county_fips_full', 'year']).reset_index(drop=True)


def fetch_qcew_total_covered_county(year: int, quarter: int, area_fips: str, raw_dir: Path) -> pd.DataFrame:
    path = raw_dir / f'qcew_{area_fips}_{year}Q{quarter}.csv'
    if path.exists():
        df = pd.read_csv(path)
    else:
        df = fetch_area_slice(year, quarter, area_fips)
        path.write_text(df.to_csv(index=False))
    needed = (df['own_code'].astype(str) == '0') & (df['industry_code'].astype(str) == '10') & (df['agglvl_code'].astype(str) == '70')
    out = df.loc[needed].copy()
    if out.empty:
        raise RuntimeError(f'No county total-covered QCEW row for {area_fips} {year}Q{quarter}')
    for col in ['qtrly_estabs', 'month1_emplvl', 'month2_emplvl', 'month3_emplvl', 'total_qtrly_wages', 'avg_wkly_wage']:
        out[col] = pd.to_numeric(out[col], errors='coerce')
    out['county_fips_full'] = area_fips
    out['county_name'] = COUNTY_NAME_BY_FIPS[area_fips]
    out['quarter'] = quarter
    out['emplvl_mean'] = out[['month1_emplvl', 'month2_emplvl', 'month3_emplvl']].mean(axis=1)
    return out[[
        'county_fips_full', 'county_name', 'year', 'quarter', 'qtrly_estabs', 'emplvl_mean', 'total_qtrly_wages', 'avg_wkly_wage'
    ]]


def load_qcew_county_panel(raw_dir: Path, *, start_year: int = QCEW_START_YEAR, end_year: int = 2025) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for area_fips in COUNTY_NAME_BY_FIPS:
        for year in range(start_year, end_year + 1):
            for quarter in [1, 2, 3, 4]:
                try:
                    rows.append(fetch_qcew_total_covered_county(year, quarter, area_fips, raw_dir))
                except requests.HTTPError:
                    continue
    if not rows:
        raise RuntimeError('No Saint Paul QCEW county rows were loaded.')
    out = pd.concat(rows, ignore_index=True)
    out = add_quarter_period(out, year_col='year', quarter_col='quarter')
    return out.sort_values(['county_fips_full', 'year', 'quarter']).reset_index(drop=True)


def load_fhfa_panel(*, start_year: int = FHFA_START_YEAR) -> pd.DataFrame:
    fhfa = pd.read_csv(ROOT / 'data' / 'raw' / 'fhfa' / 'hpi_master.csv', dtype={'place_id': str})
    sample = fhfa[
        (fhfa['place_id'] == TWIN_CITIES_MSA_CBSA)
        & (fhfa['level'] == 'MSA')
        & (fhfa['frequency'] == 'quarterly')
        & (fhfa['hpi_flavor'] == 'all-transactions')
        & (fhfa['yr'] >= start_year)
    ].copy()
    sample['quarter'] = pd.to_numeric(sample['period'], errors='coerce')
    sample['year'] = pd.to_numeric(sample['yr'], errors='coerce')
    sample['index_value'] = pd.to_numeric(sample['index_sa'], errors='coerce')
    sample['index_value'] = sample['index_value'].fillna(pd.to_numeric(sample['index_nsa'], errors='coerce'))
    sample = sample.dropna(subset=['quarter', 'year', 'index_value']).copy()
    sample = add_quarter_period(sample, year_col='year', quarter_col='quarter')
    sample['place_name'] = sample['place_name'].astype(str)
    return sample[['place_name', 'year', 'quarter', 'calendar_period', 'date', 'index_value']].sort_values(['year', 'quarter'])


def build_control_gap_panel(df: pd.DataFrame, *, value_col: str) -> pd.DataFrame:
    treated = df[df['county_name'] == 'Ramsey County'][['date', value_col]].rename(columns={value_col: 'treated_value'})
    controls = (
        df[df['county_name'].isin(PRIMARY_CONTROL_COUNTIES)]
        .groupby('date', as_index=False)[value_col]
        .mean()
        .rename(columns={value_col: 'control_mean'})
    )
    out = treated.merge(controls, on='date', how='inner').sort_values('date')
    out['gap'] = out['treated_value'] - out['control_mean']
    out['value_col'] = value_col
    return out


def write_policy_lines(ax, events: pd.DataFrame) -> None:
    for _, row in events.iterrows():
        if row['event'] not in {'ballot_adoption', 'ordinance_effective', 'amendment_2023'}:
            continue
        ax.axvline(row['date'], color='gray', linestyle=':', linewidth=1)
        ax.text(row['date'], ax.get_ylim()[1], row['event'], rotation=90, va='top', ha='right', fontsize=8)


def write_county_plot(df: pd.DataFrame, *, value_col: str, title: str, path: Path) -> None:
    events = build_policy_event_table()
    plt.figure(figsize=(10.5, 5.5))
    for county_name, county_df in df.groupby('county_name'):
        plt.plot(county_df['date'], county_df[value_col], marker='o', linewidth=2, label=county_name)
    ax = plt.gca()
    write_policy_lines(ax, events)
    plt.title(title)
    plt.xlabel('Date')
    plt.ylabel(title)
    plt.legend()
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def write_gap_plot(df: pd.DataFrame, *, title: str, path: Path) -> None:
    events = build_policy_event_table()
    plt.figure(figsize=(10.5, 5.5))
    plt.axhline(0, color='black', linewidth=1)
    plt.plot(df['date'], df['gap'], marker='o', linewidth=2, color='#1f4e79')
    ax = plt.gca()
    write_policy_lines(ax, events)
    plt.title(title)
    plt.xlabel('Date')
    plt.ylabel('Ramsey minus control mean')
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def write_hpi_plot(df: pd.DataFrame, *, title: str, path: Path) -> None:
    events = build_policy_event_table()
    plt.figure(figsize=(10.5, 5.5))
    plt.plot(df['date'], df['index_value'], marker='o', linewidth=2, color='#8b2e16')
    ax = plt.gca()
    write_policy_lines(ax, events)
    plt.title(title)
    plt.xlabel('Date')
    plt.ylabel(title)
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def write_note(path: Path, *, bps_summary: pd.DataFrame, qcew_summary: pd.DataFrame, fhfa: pd.DataFrame) -> None:
    text = f"""# Saint Paul Policy Timing Note

This local extension treats Saint Paul's rent stabilization policy as a timing case, not a building-level registry design.

## Policy dates

- 2021-11-02: ballot adoption
- 2022-05-01: ordinance effective
- 2023-01-01: first major amendment package effective
- 2025-06-13: later amendment package effective

## Geographic backbone

- Ramsey County: treated local county proxy for permits and labor outcomes
- Hennepin County and Dakota County: comparison counties
- Minneapolis-St. Paul-Bloomington MSA: price-series geography from FHFA

## Current results

- BPS permits sample: {int(bps_summary['treated_n'].max())} annual Ramsey observations in the pre/post summary
- QCEW sample: {int(qcew_summary['treated_n'].max())} quarterly Ramsey observations in the pre/post summary
- FHFA quarterly coverage: {fhfa['calendar_period'].min()} through {fhfa['calendar_period'].max()}

## Limits

- This is a county/MSA aggregate study, not a city registry or parcel design.
- Ramsey County includes jurisdictions beyond Saint Paul.
- FHFA is MSA-level and cannot isolate Saint Paul alone.
- The 2025 amendment date is too late for a meaningful post window in the current local panel.
"""
    path.write_text(text)


def run() -> None:
    raw_bps, raw_qcew, results_tables, results_figures = ensure_dirs()
    events = build_policy_event_table()
    events.to_csv(results_tables / 'saint_paul_policy_events.csv', index=False)

    bps = load_bps_county_panel(raw_bps)
    bps.to_csv(ROOT / 'data' / 'processed' / 'saint_paul_bps_county_panel.csv', index=False)
    bps_summary = summarize_treated_vs_controls(
        bps,
        value_col='permits_units_total',
        group_col='county_name',
        treated_group='Ramsey County',
        control_groups=list(PRIMARY_CONTROL_COUNTIES),
        event='ordinance_effective',
    )
    bps_summary['outcome'] = 'permits_units_total'
    bps_gap = build_control_gap_panel(bps, value_col='permits_units_total')

    qcew = load_qcew_county_panel(raw_qcew)
    qcew.to_csv(ROOT / 'data' / 'processed' / 'saint_paul_qcew_county_panel.csv', index=False)
    qcew_emp_summary = summarize_treated_vs_controls(
        qcew,
        value_col='emplvl_mean',
        group_col='county_name',
        treated_group='Ramsey County',
        control_groups=list(PRIMARY_CONTROL_COUNTIES),
        event='ordinance_effective',
    )
    qcew_emp_summary['outcome'] = 'qcew_total_covered_emplvl'
    qcew_wage_summary = summarize_treated_vs_controls(
        qcew,
        value_col='avg_wkly_wage',
        group_col='county_name',
        treated_group='Ramsey County',
        control_groups=list(PRIMARY_CONTROL_COUNTIES),
        event='ordinance_effective',
    )
    qcew_wage_summary['outcome'] = 'qcew_avg_wkly_wage'
    qcew_gap = build_control_gap_panel(qcew, value_col='emplvl_mean')
    qcew_wage_gap = build_control_gap_panel(qcew, value_col='avg_wkly_wage')

    fhfa = load_fhfa_panel()
    fhfa.to_csv(ROOT / 'data' / 'processed' / 'saint_paul_fhfa_msa_panel.csv', index=False)
    fhfa_period = fhfa[['date', 'index_value']].copy()
    fhfa_period['series'] = 'fhfa_hpi'
    fhfa_period = label_pre_post(fhfa_period, 'date', event='ordinance_effective')
    fhfa_summary = summarize_by_period(fhfa_period, 'index_value')
    fhfa_summary['outcome'] = 'fhfa_hpi'

    combined_summary = pd.concat([bps_summary, qcew_emp_summary, qcew_wage_summary], ignore_index=True)
    combined_summary.to_csv(results_tables / 'saint_paul_policy_timing_summary.csv', index=False)
    fhfa_summary.to_csv(results_tables / 'saint_paul_fhfa_policy_summary.csv', index=False)
    bps_gap.to_csv(results_tables / 'saint_paul_permits_gap.csv', index=False)
    qcew_gap.to_csv(results_tables / 'saint_paul_qcew_employment_gap.csv', index=False)
    qcew_wage_gap.to_csv(results_tables / 'saint_paul_qcew_wage_gap.csv', index=False)

    write_county_plot(
        bps,
        value_col='permits_units_total',
        title='Saint Paul local proxy: county permits',
        path=results_figures / 'saint_paul_permits_county_trends.png',
    )
    write_gap_plot(
        bps_gap,
        title='Saint Paul local proxy: permits gap vs controls',
        path=results_figures / 'saint_paul_permits_gap.png',
    )
    write_county_plot(
        qcew,
        value_col='emplvl_mean',
        title='Saint Paul local proxy: covered employment',
        path=results_figures / 'saint_paul_qcew_employment_trends.png',
    )
    write_gap_plot(
        qcew_gap,
        title='Saint Paul local proxy: employment gap vs controls',
        path=results_figures / 'saint_paul_qcew_employment_gap.png',
    )
    write_county_plot(
        qcew,
        value_col='avg_wkly_wage',
        title='Saint Paul local proxy: average weekly wage',
        path=results_figures / 'saint_paul_qcew_wage_trends.png',
    )
    write_gap_plot(
        qcew_wage_gap,
        title='Saint Paul local proxy: wage gap vs controls',
        path=results_figures / 'saint_paul_qcew_wage_gap.png',
    )
    write_hpi_plot(
        fhfa,
        title='Twin Cities FHFA HPI',
        path=results_figures / 'saint_paul_fhfa_hpi.png',
    )
    write_note(ROOT / 'results' / 'saint_paul_policy_timing.md', bps_summary=bps_summary, qcew_summary=qcew_emp_summary, fhfa=fhfa)

    print(f'wrote Saint Paul tables to {results_tables}')
    print(f'wrote Saint Paul figures to {results_figures}')


if __name__ == '__main__':
    run()
