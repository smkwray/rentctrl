# Identification and methods

## Baseline annual specification

The current baseline is a transparent two-way fixed-effects event study at the state-year level:

- unit fixed effects: state
- time fixed effects: year
- event-time leads/lags around treatment
- baseline event-time binning from the preferred annual timing definition
- parallel annual timing-sensitivity run from the alternative Oregon timing definition

Outcomes:
- ACS rent and burden measures
- BPS annual permit measures
- FHFA annual HPI
- annualized QCEW measures

## Preferred quarterly specification

Use quarterly data for:

- FHFA HPI
- QCEW

Quarter-level treatment timing should be conservative:

- Oregon baseline: 2019Q2
- California baseline: 2020Q1
- Washington descriptive extension: 2025Q3

## Robustness checks

The current build implements:

1. separate California-only and Oregon-only models,
2. pooled model with state-specific event interactions,
3. donor-pool restrictions,
4. placebo dates,
5. leave-one-donor-out checks,
6. pre-trend visualization,
7. quarterly preferred-versus-alternative timing comparisons.

## Recommended interpretations

Interpret outcomes as evidence on:

- incumbent rents,
- affordability,
- residential stability,
- supply response,
- capitalization,
- labor-market spillovers.

Do not interpret phase-1 results as a global answer to “rent control” in every regime.

## Inference package

For each main outcome the public package now keeps:

- pre-trend plot,
- coefficient table,
- conventional HC1 standard errors for continuity,
- permutation-based p-values and interval columns when available,
- annual timing-sensitivity summary for preferred versus alternative treatment timing,
- treatment-timing note,
- donor-pool note,
- plain-language interpretation,
- limitations paragraph.

Interpretation rule:

- treat the resampled inference columns as the headline uncertainty layer,
- treat the HC1 layer as descriptive / continuity output,
- keep Washington descriptive only in statewide reporting.
