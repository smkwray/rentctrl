# NYC Additional Question Shortlist

Date:
- 2026-03-16

Scope:
- follow-on NYC question package using the landed RSBL + HPD + MapPLUTO + MDR backend
- immediate artifacts answer Questions 1-4 from saved panels
- Questions 5-6 now include a narrowed live extension using grouped HPD pulls since 2025

## Ranked questions and artifacts

1. **Selection and coverage**
   - Main artifact: `results/tables/nyc_treated_selection_coverage_overall_since_2019.csv`
   - Supporting artifacts:
     - `results/tables/nyc_treated_selection_coverage_by_borough_since_2019.csv`
     - `results/tables/nyc_treated_selection_coverage_by_communityboard_since_2019.csv`
     - `results/tables/nyc_treated_selection_coverage_by_yearbuilt_bin_since_2019.csv`
     - `results/tables/nyc_treated_selection_coverage_by_units_bin_since_2019.csv`
   - Current read: the landed treated-stock universe contains `47256` RSBL buildings, of which `32951` are HPD-observed, `32793` survive the richer stratified design, and `28746` survive the refined community-board matcher.

2. **Geographic concentration of the gap**
   - Main artifacts:
     - `results/tables/nyc_geography_gap_by_borough_year_since_2019.csv`
     - `results/tables/nyc_geography_gap_by_communityboard_year_since_2019.csv`
   - Borough concentration artifact: `results/tables/nyc_geography_gap_by_borough_year_since_2019.csv` (largest 2025 treated-control gap in `BRONX` at `10.796` violations per building).

3. **Building-type heterogeneity**
   - Main artifacts:
     - `results/tables/nyc_building_type_gap_by_yearbuilt_bin_year_since_2019.csv`
     - `results/tables/nyc_building_type_gap_by_units_bin_year_since_2019.csv`
     - `results/tables/nyc_building_type_gap_by_bldgclass_year_since_2019.csv`
   - Building-type artifact: `results/tables/nyc_building_type_gap_by_yearbuilt_bin_year_since_2019.csv` (largest 2025 age-bin gap in `1970_1999` at `6.386`).

4. **Extensive versus intensive margin**
   - Main artifacts:
     - `results/tables/nyc_margin_summary_since_2019.csv`
     - `results/tables/nyc_margin_gap_summary_since_2019.csv`
   - Current read: by `2025`, the treated-control gap is `-0.124` on the any-violation rate and `9.605` on positive-only counts.

5. **Registration lifecycle and enforcement timing**
   - Main artifacts:
     - `results/tables/nyc_registration_gap_by_count_bin_year_since_2019.csv`
     - `results/tables/nyc_registration_gap_by_recency_bin_year_since_2019.csv`
     - `results/tables/nyc_registration_gap_by_end_bin_year_since_2019.csv`
   - Current read: by `2025`, the largest registration-recency gap is in `2025_plus` at `5.109`; monthly timing artifacts now live in `results/tables/nyc_timing_monthly_gap_summary_since_2025.csv` and `results/tables/nyc_timing_monthly_gap_by_borough_since_2025.csv`.

6. **Resolution dynamics**
   - Main artifacts:
     - `results/tables/nyc_status_top_counts_since_2025.csv`
     - `results/tables/nyc_status_family_summary_since_2025.csv`
     - `results/tables/nyc_status_family_gap_summary_since_2025.csv`
   - Current read: in `2025`, `open_reinspection` carries a treated-control mean gap of `4.229` and a treated-control status-share gap of `-0.041`.

## Interpretation guardrails

- These follow-on artifacts are descriptive support for the existing NYC package.
- The geography and building-type outputs use the richer restricted comparison universe, not the refined matched estimator.
- The shortlist is designed to explain where the broader positive differential-growth pattern appears strongest and where treated-stock attrition is concentrated before any stronger causal framing.
