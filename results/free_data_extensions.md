# Free Data Extension Note

This package closes out the remaining useful phase-1 questions that can be answered from the current free public data stack.

## Question coverage

The coverage matrix is written to `results\tables\free_data_question_coverage.csv`.

## Question 7: Higher baseline burden

- California's 2015-2018 pre-policy rent burden mean is `55.23%`.
- Oregon's 2015-2018 pre-policy rent burden mean is `50.95%`.
- Use `results\tables\state_specific_effect_summary.csv` to compare CA-only and OR-only event-study response patterns.

Interpretation rule:
- Treat this as a CA-versus-OR contrast, not a fully powered heterogeneity design. There are only two treated states.

## Question 8: Supply constraint

- California's 2015-2018 permits per 1,000 renter households mean is `18.05`.
- Oregon's 2015-2018 permits per 1,000 renter households mean is `31.96`.
- Lower baseline permit intensity implies California is the more supply-constrained treated state in this public-data setup.

Interpretation rule:
- Use the same state-specific contrast table, alongside the pre-policy profile table in `results\tables\prepolicy_state_profiles.csv`.

## Question 9: Cross-outcome comparison

The pooled comparison table is written to `results\tables\domain_comparison_summary.csv` and should be read outcome by outcome, not as a single scalar ranking.

Outcome families included:
- `rent_level`: Median gross rent
- `affordability`: Rent burden 30%+
- `stability`: Moved from different state, Moved last year, Same house one year ago
- `supply`: 5+ unit permits, Multifamily permit share, Permitted units
- `capitalization`: FHFA HPI (SA mean)
- `labor`: QCEW average weekly wage, QCEW covered employment

Reporting posture:
- `FHFA` and `BPS` remain the cleanest headline outcome families.
- ACS rent, burden, and mobility outcomes are informative but coverage-limited.
- QCEW employment and wage outcomes remain exploratory because the annual and quarterly state slices start in 2014.
