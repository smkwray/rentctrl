# Credibility Interpretation

This note interprets the current phase-1 credibility outputs in `results/tables/` and `results/figures/`.

## Scope

- Core treated states: California and Oregon
- Descriptive extension excluded from causal baseline: Washington
- Annual rent outcomes: ACS coverage is limited to 2015-2019 and 2021-2024
- Annual labor-market outcomes: QCEW coverage is limited to 2014-2024

## Outcome-by-outcome assessment

### Permits (`permits_units_total`)

Status:
- usable as a directional phase-1 outcome

Why:
- The pooled annual baseline does not show strong pooled pre-trend failure in the lead coefficients.
- The pooled post-treatment path turns negative, with the largest decline at event time `+4`.
- Placebo timing still generates negative post-placebo coefficients, so the estimate is not fully robust.
- State-specific interaction results show heterogeneity and some significant pre-treatment coefficients for both California and Oregon.

Interpretation:
- Treat the permits result as suggestive evidence of weaker permitted-unit growth after treatment, not as a settled causal headline.

### House prices (`index_sa_mean`)

Status:
- usable as a secondary phase-1 outcome

Why:
- The pooled annual baseline has relatively mild pre-trend issues compared with rent and labor-market outcomes.
- The annual post path is negative, but most coefficients are imprecise until the far post period.
- State-specific interactions indicate the negative post pattern is much stronger in California than in Oregon.
- Quarterly preferred-vs-alternative treatment timing produces similar qualitative paths, which is a good sign for timing robustness.

Interpretation:
- Use FHFA as a secondary capitalization outcome with explicit caution that California appears to drive most of the pooled signal.

### Labor market (`qcew_total_covered_emplvl`)

Status:
- not ready for a causal headline

Why:
- The annual baseline has a borderline negative lead at `-4` and a negative contemporaneous coefficient.
- The placebo specification shows a large false lead at `-4`.
- Quarterly preferred and alternative treatment timing both show negative pre-period movement at the far lead and sizable negative post coefficients in the early post window.
- State-specific interaction results are highly uneven and indicate substantial heterogeneity.

Interpretation:
- Keep QCEW in the output package as an exploratory spillover outcome, but do not use it as headline causal evidence.

### Rent (`DP04_0134E`)

Status:
- not ready for a causal headline

Why:
- The annual baseline has strongly negative and statistically significant lead coefficients at `-4` and `-3`.
- The placebo specification is also contaminated by pre-treatment movement.
- State-specific interaction results show pre-treatment movement for both California and Oregon.
- The ACS sample window is narrow and has a break in 2020, which increases comparability risk.

Interpretation:
- Present rent as an informative descriptive series for now, not as the main causal estimate.

## Cross-check summary

Most credible current outcome:
- FHFA HPI, as a secondary outcome

Potentially usable with caution:
- permits

Not yet credible as headline causal estimates:
- median gross rent
- QCEW employment

Stable design choices so far:
- Washington remains excluded from causal baseline interpretation
- quarterly preferred vs alternative treatment timing does not materially change the broad FHFA and QCEW quarterly patterns
- donor-pool perturbations were run and saved, so sensitivity is no longer an untested gap

## Recommended reporting posture

If reporting this phase-1 build now:
- lead with the fact that the state-level public-data pipeline is operational and reproducible
- present permits and FHFA as provisional baseline outcomes
- present rent and labor-market outcomes as exploratory because pre-trend and placebo checks are not clean enough
- state the ACS and QCEW source-coverage limits explicitly in every narrative summary

## Recommended next decision

Do not spend the next turn trying to rescue the rent outcome with ad hoc source changes.

Preferred next move:
- keep the current rent limitation explicit,
- interpret the existing outputs carefully,
- and only then decide whether a separate official-source investigation for pre-2015 and 2020 ACS rent coverage is worth the added comparability risk.
