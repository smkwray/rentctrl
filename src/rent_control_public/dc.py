from __future__ import annotations

from io import StringIO

import pandas as pd
import requests

ITSPE_QUERY_URL = (
    'https://services.arcgis.com/neT9SoYxizqTHZPH/arcgis/rest/services/'
    'ITSPE_View_07082025/FeatureServer/0/query'
)
CONDO_QUERY_URL = (
    'https://maps2.dcgis.dc.gov/dcgis/rest/services/'
    'DCGIS_DATA/Property_and_Land_WebMercator/FeatureServer/24/query'
)

RESIDENTIAL_USECODES = [
    '011', '012', '013', '014', '015', '016', '017', '018', '019',
    '021', '022', '023', '024', '025', '026', '027', '028', '029',
    '116', '117', '126', '127', '214', '216', '217', '316', '516',
]
_USECODE_SQL = ",".join(f"'{code}'" for code in RESIDENTIAL_USECODES)
RESIDENTIAL_WHERE = (
    f"USECODE IN ({_USECODE_SQL}) OR CLASSTYPE IN ('1A','1B')"
)
DEFAULT_OUT_FIELDS = [
    'SSL',
    'PROPTYPE',
    'USECODE',
    'CLASSTYPE',
    'HSTDCODE',
    'PREMISEADD',
    'NBHD',
    'PRMS_WARD',
    'OLDTOTAL',
    'NEWTOTAL',
    'SALEPRICE',
    'SALEDATE',
]


class DCFetchError(RuntimeError):
    """Raised when the ITSPE FeatureServer returns an unexpected payload."""


def fetch_itspe_records(
    *,
    where: str = RESIDENTIAL_WHERE,
    out_fields: list[str] | None = None,
    page_size: int = 2000,
    limit: int | None = None,
    timeout: int = 60,
) -> pd.DataFrame:
    fields = out_fields or DEFAULT_OUT_FIELDS
    rows: list[dict[str, object]] = []
    offset = 0
    session = requests.Session()
    while True:
        batch_size = page_size
        if limit is not None:
            remaining = limit - len(rows)
            if remaining <= 0:
                break
            batch_size = min(batch_size, remaining)
        params = {
            'f': 'json',
            'where': where,
            'returnGeometry': 'false',
            'outFields': ','.join(fields),
            'resultOffset': offset,
            'resultRecordCount': batch_size,
        }
        response = session.get(ITSPE_QUERY_URL, params=params, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
        if 'features' not in payload:
            raise DCFetchError(f'ITSPE response missing features: {payload}')
        features = payload['features']
        if not features:
            break
        rows.extend(feature['attributes'] for feature in features)
        if len(features) < batch_size:
            break
        offset += len(features)
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    for column in ['OLDTOTAL', 'NEWTOTAL', 'SALEPRICE']:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors='coerce')
    if 'SALEDATE' in frame.columns:
        frame['SALEDATE'] = pd.to_datetime(frame['SALEDATE'], errors='coerce')
    return frame


def add_homestead_proxy_columns(frame: pd.DataFrame) -> pd.DataFrame:
    data = frame.copy()
    hstd = data.get('HSTDCODE')
    if hstd is None:
        data['homestead_present'] = False
    else:
        data['homestead_present'] = hstd.fillna('').astype(str).str.strip().ne('')
    data['owner_occupied_proxy'] = data['homestead_present']
    data['rental_probable_proxy'] = ~data['homestead_present']
    data['assessment_change'] = data['NEWTOTAL'] - data['OLDTOTAL']
    data['assessment_pct_change'] = data['assessment_change'] / data['OLDTOTAL'].where(data['OLDTOTAL'].gt(0))
    return data


def summarize_by_ward(frame: pd.DataFrame) -> pd.DataFrame:
    data = add_homestead_proxy_columns(frame)
    summary = (
        data.groupby(['PRMS_WARD', 'owner_occupied_proxy'], dropna=False)
        .agg(
            parcel_count=('SSL', 'nunique'),
            mean_newtotal=('NEWTOTAL', 'mean'),
            mean_oldtotal=('OLDTOTAL', 'mean'),
            mean_assessment_change=('assessment_change', 'mean'),
            median_assessment_pct_change=('assessment_pct_change', 'median'),
        )
        .reset_index()
        .sort_values(['PRMS_WARD', 'owner_occupied_proxy'])
        .reset_index(drop=True)
    )
    return summary


def summarize_by_property_type(frame: pd.DataFrame) -> pd.DataFrame:
    data = add_homestead_proxy_columns(frame)
    summary = (
        data.groupby(['PROPTYPE', 'owner_occupied_proxy'], dropna=False)
        .agg(
            parcel_count=('SSL', 'nunique'),
            mean_newtotal=('NEWTOTAL', 'mean'),
            median_newtotal=('NEWTOTAL', 'median'),
        )
        .reset_index()
        .sort_values(['parcel_count', 'PROPTYPE'], ascending=[False, True])
        .reset_index(drop=True)
    )
    return summary


def summarize_overall(frame: pd.DataFrame) -> pd.DataFrame:
    data = add_homestead_proxy_columns(frame)
    return pd.DataFrame(
        [
            {'metric': 'residential_parcels', 'value': int(data['SSL'].nunique())},
            {'metric': 'owner_occupied_proxy_parcels', 'value': int(data.loc[data['owner_occupied_proxy'], 'SSL'].nunique())},
            {'metric': 'rental_probable_proxy_parcels', 'value': int(data.loc[data['rental_probable_proxy'], 'SSL'].nunique())},
            {'metric': 'wards_covered', 'value': int(data['PRMS_WARD'].dropna().nunique())},
        ]
    )


def fetch_cama_condo_records(
    *,
    out_fields: list[str] | None = None,
    page_size: int = 2000,
    limit: int | None = None,
    timeout: int = 60,
) -> pd.DataFrame:
    fields = out_fields or [
        'SSL', 'AYB', 'YR_RMDL', 'EYB', 'ROOMS', 'BEDRM', 'BATHRM',
        'PRICE', 'SALEDATE', 'LIVING_GBA', 'USECODE',
    ]
    rows: list[dict[str, object]] = []
    offset = 0
    session = requests.Session()
    while True:
        batch_size = page_size
        if limit is not None:
            remaining = limit - len(rows)
            if remaining <= 0:
                break
            batch_size = min(batch_size, remaining)
        params = {
            'f': 'json',
            'where': '1=1',
            'returnGeometry': 'false',
            'outFields': ','.join(fields),
            'resultOffset': offset,
            'resultRecordCount': batch_size,
        }
        response = session.get(CONDO_QUERY_URL, params=params, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
        if 'features' not in payload:
            raise DCFetchError(f'Condo response missing features: {payload}')
        features = payload['features']
        if not features:
            break
        rows.extend(feature['attributes'] for feature in features)
        if len(features) < batch_size:
            break
        offset += len(features)
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    for column in ['AYB', 'YR_RMDL', 'EYB', 'ROOMS', 'BEDRM', 'BATHRM', 'PRICE', 'LIVING_GBA']:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors='coerce')
    if 'SALEDATE' in frame.columns:
        frame['SALEDATE'] = pd.to_datetime(frame['SALEDATE'], errors='coerce')
    return frame


def enrich_with_condo_characteristics(
    itspe_df: pd.DataFrame,
    condo_df: pd.DataFrame,
) -> pd.DataFrame:
    if itspe_df.empty:
        return itspe_df.copy()
    condo = condo_df.copy()
    condo['SSL'] = condo['SSL'].astype(str).str.strip()
    condo_summary = (
        condo.groupby('SSL', as_index=False)
        .agg(
            condo_records=('SSL', 'size'),
            condo_mean_bedrooms=('BEDRM', 'mean'),
            condo_mean_bathrooms=('BATHRM', 'mean'),
            condo_mean_living_gba=('LIVING_GBA', 'mean'),
            condo_mean_year_built=('AYB', 'mean'),
            condo_recent_sale_price=('PRICE', 'max'),
        )
    )
    data = itspe_df.copy()
    data['SSL'] = data['SSL'].astype(str).str.strip()
    out = data.merge(condo_summary, how='left', on='SSL')
    out['condo_match'] = out['condo_records'].fillna(0).gt(0)
    return out


def summarize_ward_rental_share(frame: pd.DataFrame) -> pd.DataFrame:
    data = add_homestead_proxy_columns(frame)
    counts = (
        data.groupby(['PRMS_WARD', 'owner_occupied_proxy'], dropna=False)
        .agg(parcel_count=('SSL', 'nunique'))
        .reset_index()
    )
    ward_totals = counts.groupby('PRMS_WARD')['parcel_count'].transform('sum')
    counts['rental_share'] = counts['parcel_count'] / ward_totals
    counts['ward_total'] = ward_totals
    return counts.sort_values(['PRMS_WARD', 'owner_occupied_proxy']).reset_index(drop=True)


def summarize_ward_assessment_gap(frame: pd.DataFrame) -> pd.DataFrame:
    data = add_homestead_proxy_columns(frame)
    data['tenure_proxy'] = data['owner_occupied_proxy'].map(
        {True: 'owner_occupied', False: 'rental_probable'}
    )
    return (
        data.groupby(['PRMS_WARD', 'tenure_proxy'], dropna=False)
        .agg(
            parcel_count=('SSL', 'nunique'),
            mean_assessment_change=('assessment_change', 'mean'),
            median_assessment_pct_change=('assessment_pct_change', 'median'),
            mean_newtotal=('NEWTOTAL', 'mean'),
        )
        .reset_index()
        .sort_values(['PRMS_WARD', 'tenure_proxy'])
        .reset_index(drop=True)
    )


def summarize_condo_ward_tenure(enriched_df: pd.DataFrame) -> pd.DataFrame:
    data = add_homestead_proxy_columns(enriched_df)
    data = data[data.get('condo_match', pd.Series(dtype=bool))].copy()
    if data.empty:
        return pd.DataFrame()
    data['tenure_proxy'] = data['owner_occupied_proxy'].map(
        {True: 'owner_occupied', False: 'rental_probable'}
    )
    return (
        data.groupby(['PRMS_WARD', 'tenure_proxy'], dropna=False)
        .agg(
            parcel_count=('SSL', 'nunique'),
            median_bedrooms=('condo_mean_bedrooms', 'median'),
            median_bathrooms=('condo_mean_bathrooms', 'median'),
            median_living_gba=('condo_mean_living_gba', 'median'),
            median_year_built=('condo_mean_year_built', 'median'),
        )
        .reset_index()
        .sort_values(['PRMS_WARD', 'tenure_proxy'])
        .reset_index(drop=True)
    )


def summarize_ward_property_type_heatmap(frame: pd.DataFrame) -> pd.DataFrame:
    data = add_homestead_proxy_columns(frame)
    cross = (
        data.groupby(['PRMS_WARD', 'PROPTYPE'], dropna=False)
        .agg(
            parcel_count=('SSL', 'nunique'),
            rental_count=('rental_probable_proxy', 'sum'),
        )
        .reset_index()
    )
    cross['rental_share'] = cross['rental_count'] / cross['parcel_count'].where(cross['parcel_count'].gt(0))
    return cross.sort_values(['PRMS_WARD', 'parcel_count'], ascending=[True, False]).reset_index(drop=True)


def summarize_nbhd_assessment_disparity(
    frame: pd.DataFrame,
    *,
    min_parcels: int = 30,
) -> pd.DataFrame:
    """Per-neighborhood rental vs owner-occupied assessment gap.

    Returns one row per neighborhood with columns for each tenure group's
    mean assessment change and a ``disparity_ratio`` (rental / owner).
    Neighborhoods with fewer than *min_parcels* in either group are dropped.
    """
    data = add_homestead_proxy_columns(frame)
    data['tenure_proxy'] = data['owner_occupied_proxy'].map(
        {True: 'owner_occupied', False: 'rental_probable'}
    )
    agg = (
        data.groupby(['NBHD', 'tenure_proxy'], dropna=False)
        .agg(
            parcel_count=('SSL', 'nunique'),
            mean_assessment_change=('assessment_change', 'mean'),
            median_assessment_pct_change=('assessment_pct_change', 'median'),
            mean_newtotal=('NEWTOTAL', 'mean'),
        )
        .reset_index()
    )
    # Pivot to one row per NBHD
    owner = agg[agg['tenure_proxy'] == 'owner_occupied'].set_index('NBHD')
    rental = agg[agg['tenure_proxy'] == 'rental_probable'].set_index('NBHD')
    both = owner.index.intersection(rental.index)
    rows = []
    for nbhd in sorted(both):
        o = owner.loc[nbhd]
        r = rental.loc[nbhd]
        if int(o['parcel_count']) < min_parcels or int(r['parcel_count']) < min_parcels:
            continue
        o_change = float(o['mean_assessment_change'])
        r_change = float(r['mean_assessment_change'])
        ratio = (r_change / o_change) if o_change != 0 else float('nan')
        rows.append({
            'NBHD': nbhd,
            'owner_parcels': int(o['parcel_count']),
            'rental_parcels': int(r['parcel_count']),
            'owner_mean_change': round(o_change, 2),
            'rental_mean_change': round(r_change, 2),
            'disparity_ratio': round(ratio, 3),
            'rental_median_pct_change': round(float(r['median_assessment_pct_change']), 4),
            'owner_median_pct_change': round(float(o['median_assessment_pct_change']), 4),
        })
    result = pd.DataFrame(rows)
    if not result.empty:
        result = result.sort_values('disparity_ratio', ascending=False, na_position='last').reset_index(drop=True)
    return result


def summarize_condo_enrichment(enriched_df: pd.DataFrame) -> pd.DataFrame:
    if enriched_df.empty:
        return pd.DataFrame(columns=['metric', 'value'])
    data = add_homestead_proxy_columns(enriched_df)
    return pd.DataFrame(
        [
            {'metric': 'residential_parcels', 'value': int(data['SSL'].nunique())},
            {'metric': 'condo_matched_parcels', 'value': int(data.loc[data['condo_match'], 'SSL'].nunique())},
            {'metric': 'condo_matched_owner_proxy', 'value': int(data.loc[data['condo_match'] & data['owner_occupied_proxy'], 'SSL'].nunique())},
            {'metric': 'condo_matched_rental_proxy', 'value': int(data.loc[data['condo_match'] & data['rental_probable_proxy'], 'SSL'].nunique())},
        ]
    )
