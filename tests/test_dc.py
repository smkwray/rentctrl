import pandas as pd

from rent_control_public.dc import (
    add_homestead_proxy_columns,
    enrich_with_condo_characteristics,
    summarize_by_ward,
    summarize_condo_enrichment,
    summarize_nbhd_assessment_disparity,
    summarize_overall,
    summarize_ward_rental_share,
    summarize_ward_assessment_gap,
    summarize_condo_ward_tenure,
    summarize_ward_property_type_heatmap,
)


def test_add_homestead_proxy_columns():
    frame = pd.DataFrame(
        {
            'SSL': ['1', '2'],
            'HSTDCODE': ['1', None],
            'OLDTOTAL': [100000, 200000],
            'NEWTOTAL': [110000, 180000],
        }
    )
    out = add_homestead_proxy_columns(frame)
    assert out['owner_occupied_proxy'].tolist() == [True, False]
    assert out['rental_probable_proxy'].tolist() == [False, True]
    assert out['assessment_change'].tolist() == [10000, -20000]


def test_summarize_by_ward():
    frame = pd.DataFrame(
        {
            'SSL': ['1', '2', '3'],
            'PRMS_WARD': ['1', '1', '2'],
            'HSTDCODE': ['1', None, None],
            'OLDTOTAL': [100000, 200000, 300000],
            'NEWTOTAL': [110000, 210000, 330000],
        }
    )
    summary = summarize_by_ward(frame)
    assert set(summary['PRMS_WARD']) == {'1', '2'}
    assert int(summary['parcel_count'].sum()) == 3


def test_summarize_overall():
    frame = pd.DataFrame(
        {
            'SSL': ['1', '2', '3'],
            'PRMS_WARD': ['1', '1', '2'],
            'HSTDCODE': ['1', None, None],
            'OLDTOTAL': [100000, 200000, 300000],
            'NEWTOTAL': [110000, 210000, 330000],
        }
    )
    summary = summarize_overall(frame)
    metrics = dict(zip(summary['metric'], summary['value']))
    assert metrics['residential_parcels'] == 3
    assert metrics['owner_occupied_proxy_parcels'] == 1
    assert metrics['rental_probable_proxy_parcels'] == 2


def test_enrich_with_condo_characteristics():
    itspe = pd.DataFrame(
        {
            'SSL': ['0015    2197', '9999 0001'],
            'HSTDCODE': ['1', None],
            'OLDTOTAL': [100000, 200000],
            'NEWTOTAL': [110000, 220000],
        }
    )
    condo = pd.DataFrame(
        {
            'SSL': ['0015    2197'],
            'BEDRM': [1],
            'BATHRM': [1],
            'LIVING_GBA': [374],
            'AYB': [1939],
            'PRICE': [230000],
        }
    )
    enriched = enrich_with_condo_characteristics(itspe, condo)
    row = enriched.loc[enriched['SSL'].str.contains('2197')].iloc[0]
    assert bool(row['condo_match']) is True
    assert row['condo_mean_bedrooms'] == 1


def test_summarize_condo_enrichment():
    enriched = pd.DataFrame(
        {
            'SSL': ['1', '2'],
            'HSTDCODE': ['1', None],
            'OLDTOTAL': [1, 1],
            'NEWTOTAL': [2, 2],
            'condo_match': [True, False],
        }
    )
    summary = summarize_condo_enrichment(enriched)
    metrics = dict(zip(summary['metric'], summary['value']))
    assert metrics['condo_matched_parcels'] == 1
    assert metrics['condo_matched_owner_proxy'] == 1


def _sample_frame():
    return pd.DataFrame({
        'SSL': ['1', '2', '3', '4'],
        'PRMS_WARD': ['1', '1', '2', '2'],
        'PROPTYPE': ['Residential-Single Family (Row', 'Residential-Condominium (Verti', 'Residential-Single Family (Row', 'Residential-Single Family (Row'],
        'HSTDCODE': ['1', None, None, '1'],
        'OLDTOTAL': [100000, 200000, 300000, 400000],
        'NEWTOTAL': [110000, 220000, 330000, 390000],
    })


def test_summarize_ward_rental_share():
    share = summarize_ward_rental_share(_sample_frame())
    assert 'rental_share' in share.columns
    assert 'ward_total' in share.columns
    ward1_rental = share[(share['PRMS_WARD'] == '1') & (~share['owner_occupied_proxy'])]
    assert ward1_rental.iloc[0]['rental_share'] == 0.5


def test_summarize_ward_assessment_gap():
    gap = summarize_ward_assessment_gap(_sample_frame())
    assert set(gap['tenure_proxy']) == {'owner_occupied', 'rental_probable'}
    assert 'mean_assessment_change' in gap.columns
    assert len(gap) == 4  # 2 wards × 2 tenure


def test_summarize_condo_ward_tenure():
    enriched = pd.DataFrame({
        'SSL': ['1', '2', '3'],
        'PRMS_WARD': ['1', '1', '2'],
        'HSTDCODE': ['1', None, None],
        'OLDTOTAL': [1, 1, 1],
        'NEWTOTAL': [2, 2, 2],
        'condo_match': [True, True, False],
        'condo_mean_bedrooms': [2.0, 1.0, None],
        'condo_mean_bathrooms': [1.0, 1.0, None],
        'condo_mean_living_gba': [800.0, 600.0, None],
        'condo_mean_year_built': [1960.0, 1980.0, None],
    })
    result = summarize_condo_ward_tenure(enriched)
    assert len(result) == 2  # ward 1 has both tenure types matched
    assert 'median_bedrooms' in result.columns


def test_summarize_nbhd_assessment_disparity():
    frame = pd.DataFrame({
        'SSL': [f's{i}' for i in range(120)],
        'NBHD': ['A'] * 60 + ['B'] * 60,
        'PRMS_WARD': ['1'] * 120,
        'PROPTYPE': ['Row'] * 120,
        'HSTDCODE': (['1'] * 30 + [None] * 30) * 2,
        'OLDTOTAL': [100000] * 30 + [200000] * 30 + [150000] * 30 + [150000] * 30,
        'NEWTOTAL': [110000] * 30 + [250000] * 30 + [160000] * 30 + [180000] * 30,
    })
    result = summarize_nbhd_assessment_disparity(frame, min_parcels=10)
    assert len(result) == 2
    assert 'disparity_ratio' in result.columns
    # NBHD A: owner change=10k, rental change=50k, ratio=5
    # NBHD B: owner change=10k, rental change=30k, ratio=3
    top = result.iloc[0]
    assert top['NBHD'] == 'A'
    assert top['disparity_ratio'] == 5.0


def test_summarize_nbhd_assessment_disparity_min_parcels():
    frame = pd.DataFrame({
        'SSL': ['s1', 's2', 's3'],
        'NBHD': ['X', 'X', 'X'],
        'PRMS_WARD': ['1', '1', '1'],
        'PROPTYPE': ['Row', 'Row', 'Row'],
        'HSTDCODE': ['1', None, None],
        'OLDTOTAL': [100000, 200000, 200000],
        'NEWTOTAL': [110000, 220000, 220000],
    })
    result = summarize_nbhd_assessment_disparity(frame, min_parcels=30)
    assert len(result) == 0  # too few parcels


def test_summarize_ward_property_type_heatmap():
    heatmap = summarize_ward_property_type_heatmap(_sample_frame())
    assert 'rental_share' in heatmap.columns
    assert heatmap['parcel_count'].sum() == 4
