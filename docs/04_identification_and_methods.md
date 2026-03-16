# Identification and methods

## Baseline annual specification

The first baseline should be a transparent two-way fixed-effects event study at the state-year level:

- unit fixed effects: state
- time fixed effects: year
- event-time leads/lags around treatment

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

Codex should implement at least:

1. separate California-only and Oregon-only models,
2. pooled model with state-specific event interactions,
3. donor-pool restrictions,
4. placebo dates,
5. leave-one-donor-out checks,
6. pre-trend visualization,
7. optional synthetic-control comparison.

## Recommended interpretations

Interpret outcomes as evidence on:

- incumbent rents,
- affordability,
- residential stability,
- supply response,
- capitalization,
- labor-market spillovers.

Do not interpret phase-1 results as a global answer to “rent control” in every regime.

## Minimum viable inference package

For each main outcome:

- pre-trend plot,
- coefficient table,
- treatment-timing note,
- donor-pool note,
- plain-language interpretation,
- limitations paragraph.
