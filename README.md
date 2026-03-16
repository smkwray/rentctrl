# Statewide Rent Caps and Housing Markets

An empirical evaluation of statewide rent-regulation policies in Oregon and California using free, reproducible public data.

**[View the interactive report](https://smkwray.github.io/rentctrl/)**

---

## What this project does

This project estimates the effects of Oregon's SB 608 (effective February 2019) and California's AB 1482 (effective January 2020) on housing supply, home prices, rents, affordability, residential mobility, and labor-market conditions. Every data source is free, every download is scripted, and every result can be reproduced from scratch.

Washington's HB 1217 (effective May 2025) is tracked for descriptive comparison only. It does not appear in the causal estimates.

Beyond statewide estimates, the project extends to local-level analysis across twelve cities with legacy or modern rent-regulation systems. Nine cities have completed result packages using city administrative data: NYC (building-level violations), San Francisco (rental inventory), West Hollywood (protected-stock linkage), Los Angeles (bounded comparative pilot), Oakland (petition panel), Berkeley, Washington DC, Saint Paul, and San Jose.

## Research question

**Did statewide rent caps in Oregon and California measurably change housing supply, price dynamics, affordability, or residential stability?**

The project addresses ten specific sub-questions across six outcome domains, graded by the strength of the evidence the available public data can provide. See [Question map](#question-map) below.

## Why Oregon and California

These two states enacted the clearest statewide rent-cap legislation in recent U.S. history:

| State | Law | Effective date | Cap structure |
|-------|-----|---------------|---------------|
| Oregon | SB 608 | 2019-02-28 | CPI + 7 pp on most units > 15 years old |
| California | AB 1482 | 2020-01-01 | CPI + 5 pp on most units > 15 years old |

Both policies have clean effective dates, apply at the state level, and generate enough post-treatment data for event-study analysis against a donor pool of eleven comparison states without statewide caps (AZ, CO, FL, GA, ID, NC, NV, TN, TX, UT, VA).

Washington's HB 1217 took effect in May 2025. With almost no post-policy outcome data available, it is included for descriptive tracking only.

## What the public data can and cannot answer

<details>
<summary><strong>What it can answer</strong></summary>

- Whether permitted housing units changed after statewide caps took effect (Census Building Permits Survey, 2010-2024).
- Whether state-level house price indexes shifted relative to comparison states (FHFA HPI, 2010-2025, annual and quarterly).
- Whether ACS-measured rents, cost burden, and mobility moved differently in treated states (ACS 1-year profiles, 2010-2019 and 2021-2024).
- Whether state-level employment and wages diverged from comparison states (QCEW, 2014-2024, annual and quarterly).
- Whether California and Oregon responded similarly enough to pool in a single model.

</details>

<details>
<summary><strong>What it cannot answer</strong></summary>

- How asking rents (as opposed to occupied rents) responded. The ACS measures what tenants currently pay, not what landlords list.
- Effects on individual housing units, buildings, or neighborhoods. All estimates are at the state level.
- General-equilibrium welfare effects or landlord profitability.
- Effects of legacy local rent-control systems (e.g., NYC, Berkeley, San Francisco). Those require separate data and designs.
- Powered subgroup heterogeneity. With only two treated states, comparisons between California and Oregon are descriptive contrasts, not statistical interaction tests.
- Effects during the ACS survey gap year of 2020. The Census Bureau did not release 1-year ACS profiles for 2020.

</details>

---

## Findings

Evidence strength varies across outcome domains. Each finding is graded by how clean the pre-treatment trends and placebo checks are.

### Evidence grading

| Grade | Meaning |
|-------|---------|
| **Answered** | Full coverage window, clean or near-clean pre-trends, placebo and donor checks run |
| **Answered with limits** | Informative estimates exist but coverage gaps or pre-trend issues reduce confidence |
| **Exploratory** | Short pre-period, contaminated placebo, or other design concerns; directional evidence only |

<details>
<summary><strong>Supply outcomes (permits) — Answered</strong></summary>

**Source:** Census Building Permits Survey, 2010-2024 (annual)

The pooled event study for total permitted units shows weaker permitted-unit growth in treated states after policy adoption. The post-treatment path turns negative, with the largest magnitude at event time +4.

- Pre-trend coefficients do not show strong pooled pre-trend failure.
- Placebo timing (treatment shifted two years early) still generates some negative post-placebo coefficients, so the estimate is suggestive rather than fully robust.
- State-specific interactions show heterogeneity: both California and Oregon contribute, but with different magnitudes.

Outcomes examined: total permitted units, 5+ unit permits, multifamily share.

**Interpretation.** Suggestive evidence of weaker permitted-unit growth after statewide caps. Direction consistent across permit measures. Not a settled causal headline due to placebo sensitivity.

</details>

<details>
<summary><strong>Capitalization outcomes (FHFA HPI) — Answered</strong></summary>

**Source:** FHFA Purchase-Only House Price Index, 2010-2025 (annual and quarterly)

The pooled annual baseline shows a negative post-treatment path for house price indexes relative to the donor pool. Pre-trend issues are relatively mild compared to other outcome families.

- State-specific interactions show the negative post-treatment pattern is much stronger in California than Oregon.
- Quarterly preferred-vs-alternative treatment timing (Oregon 2019Q2 vs. 2019Q1) produces qualitatively similar paths.
- Donor-pool sensitivity and leave-one-donor-out checks have been run.

**Interpretation.** Strongest secondary outcome in the current build. Negative post-treatment direction is consistent across specifications, but driven more by California.

</details>

<details>
<summary><strong>Rent, burden, and mobility outcomes (ACS) — Answered with limits</strong></summary>

**Source:** ACS 1-Year Profiles, 2010-2019 and 2021-2024

**Median gross rent:** The annual baseline has strongly negative lead coefficients at event times -4 and -3, indicating pre-treatment divergence. The placebo specification is also contaminated. Present as a descriptive series, not a causal estimate.

**Rent burden (30%+ of income):** Subject to the same ACS window constraints. California pre-policy mean: 55.2%. Oregon: 51.0%. Informative for context, not a stand-alone causal headline.

**Mobility:** Same-house, moved-last-year, and moved-from-different-state rates share the ACS coverage limits. The narrow window limits causal confidence.

**Key limitation:** The ACS window is 2010-2019 and 2021-2024. The 2020 profile was not released, creating a gap at the start of California's treatment period. The pre-period is still interrupted by that missing year.

</details>

<details>
<summary><strong>Labor-market outcomes (QCEW) — Exploratory</strong></summary>

**Source:** QCEW Area Slices, 2014-2024 (annual and quarterly)

Employment and wage outcomes have a short pre-treatment window (starts 2014). The employment baseline has borderline negative leads and the placebo check shows a false lead. Quarterly timing sensitivity produces similar qualitative patterns but with pre-period movement.

**Interpretation.** Retain as exploratory spillover evidence. Do not use as headline causal estimates.

</details>

### Cross-outcome comparison

Pooled baseline coefficients normalized to treated pre-policy means:

| Domain | Outcome | Avg post (% of pre-policy mean) | Grade |
|--------|---------|--------------------------------|-------|
| Supply | Total permitted units | -24.7% | Answered |
| Supply | 5+ unit permits | -21.1% | Answered |
| Supply | Multifamily share | -8.9% | Answered |
| Capitalization | FHFA HPI | -7.1% | Answered |
| Rent level | Median gross rent | -0.7% | Answered with limits |
| Affordability | Rent burden 30%+ | -1.0% | Answered with limits |
| Stability | Same house 1yr ago | +0.4% | Answered with limits |
| Stability | Moved last year | -2.1% | Answered with limits |
| Stability | Moved different state | +31.7% | Answered with limits |
| Labor | QCEW weekly wage | +3.8% | Exploratory |
| Labor | QCEW employment | -2.3% | Exploratory |

> These normalized magnitudes are from the pooled baseline and should not be compared as a single scalar ranking across domains. Coverage windows and evidence quality differ.

<details>
<summary><strong>State-specific contrasts (CA vs. OR)</strong></summary>

With only two treated states, CA-vs-OR comparisons are descriptive contrasts, not powered heterogeneity estimates.

| Outcome | California direction | Oregon direction |
|---------|---------------------|-----------------|
| FHFA HPI | Strongly negative post | Near zero post |
| Total permits | Negative (~-13% of pre-mean) | Strongly negative (~-96% of pre-mean) |
| Median gross rent | Slightly positive post | Negative post |
| QCEW employment | Mildly negative | Strongly negative |

These contrasts reflect differences in pre-policy baselines (California has higher rents, more renters, and lower per-renter permit rates), policy timing, and broader state economic conditions.

</details>

---

## Local city extensions

Beyond the statewide analysis, this project extends to twelve cities with legacy or modern rent-regulation systems. Six Tier 1 cities have completed audits and active research questions:

| City | Question Family | Key Question | Status |
|------|----------------|-------------|--------|
| New York City | Quality & maintenance | HPD violation trends in stabilized buildings | Strongest local package |
| San Francisco | Inventory / compliance | Stock composition, occupancy, rent-band reporting | Inventory / compliance result |
| West Hollywood | Administrative linkage | Protected-stock overlap with buyouts, seismic, appeals | Linked administrative result |
| Los Angeles | Quality & maintenance | RSO exposure and code-enforcement activity | Bounded comparative pilot |
| Oakland | Petition activity | 3,805 RAP case rows with 311 code-enforcement linkage | Administrative panel |
| Berkeley | Legal ceiling vs. market | Registered ceilings vs. market rent trends | Core mechanism question |
| Washington, DC | Registry inventory | RentRegistry protected stock and turnover | Administrative transparency |
| Saint Paul | Policy case study | 2021 ordinance effects on permits, prices, employment | Clean modern timing |

Results are design-sensitive and should not be read as settled causal claims. Additional audited cities: Santa Monica, East Palo Alto, Mountain View.

<details>
<summary><strong>New York City — Building-level violation analysis (strongest local package)</strong></summary>

The NYC extension joins the **Rent Stabilized Building List** (RGB, 2024) to **HPD housing violations** at the building level using borough-block-lot identifiers. HPD violations are code-enforcement records filed when inspectors find conditions like heat/hot-water failures, lead paint, pests, or structural defects.

- **89,100** buildings (32,793 stabilized, 56,307 non-stabilized)
- **623,700** building-year observations, 2019–2025
- **6** distinct modeling approaches

| Year | Building FE coefficient | Stabilized mean | Non-stabilized mean |
|------|------------------------|-----------------|---------------------|
| 2019 | 0.00 (baseline) | 1.33 | 0.76 |
| 2020 | +0.18 | 1.22 | 0.48 |
| 2021 | +0.44 | 2.24 | 1.23 |
| 2022 | +0.36 | 2.49 | 1.57 |
| 2023 | +1.01 | 3.17 | 1.59 |
| 2024 | +2.14 | 4.65 | 1.94 |
| 2025 | +4.20 | 7.27 | 2.51 |

All building FE coefficients significant at p<0.001. The widening gap suggests stabilized buildings accumulated violations faster than non-stabilized controls. However, the refined within-community-board match (28,746 well-balanced pairs) shows effects near zero by 2025. The result depends heavily on the comparison strategy — these are design-sensitive, not settled causal estimates.

</details>

<details>
<summary><strong>Oakland — Administrative petition panel</strong></summary>

Oakland's Rent Adjustment Program (RAP) tracks tenant petitions for rent adjustments, decreased-services complaints, and code-related grievances. This analysis extracted the complete public RAP search universe for three petition grounds and linked addresses to Oakland's 311 code-enforcement request database (30,164 total requests).

- **3,805** RAP case rows (3,678 unique case numbers)
- **2,977** linked addresses
- **387** matched 311 code-enforcement requests at **206** addresses

| Petition ground | Detail rows | Unique cases | 311 matches |
|-----------------|-------------|--------------|-------------|
| Code violation | 69 | 65 | 14 |
| Decrease in services (pre-2021) | 2,996 | 2,988 | 275 |
| Fewer housing services (post-2021) | 734 | 672 | 151 |

This is a near-full local administrative panel for tenant-service and code-related petitions, with a linked 311 enforcement signal. It is not a causal estimate of citywide rent control effects.

</details>

<details>
<summary><strong>Saint Paul — Modern ordinance case study</strong></summary>

Saint Paul voters approved a **3% annual rent stabilization cap** by ballot initiative in November 2021, effective May 2022. This provides the cleanest modern policy case study in the project. The analysis compares Ramsey County (treated) to Hennepin and Dakota Counties (controls) using a simple difference-in-differences design.

| Outcome | Treated change | Control change | DiD |
|---------|---------------|----------------|-----|
| Building permits (annual units) | −459 | −979 | +520 |
| Covered employment (quarterly) | +7,084 | +13,390 | −6,306 |
| Average weekly wage (quarterly) | +$144 | +$171 | −$27 |

Permits declined less in Saint Paul than in neighboring counties. Employment grew more slowly. Wage effects were negligible. With one treated county and two controls, these are descriptive differences, not powered causal estimates.

</details>

<details>
<summary><strong>San Francisco — Inventory / compliance result</strong></summary>

San Francisco's public rental inventory documents **533,205** unit rows across submission years **2022–2026**, covering **4,277** census blocks, **41** neighborhoods, and **11** supervisor districts. Median rent midpoint: **$2,375.50**. Non-owner-occupied rows: **468,062**.

| Year | Unit Rows | Blocks | Neighborhoods | Non-Owner Share |
|------|-----------|--------|---------------|-----------------|
| 2022 | 66,744 | 1,226 | 37 | 88.8% |
| 2023 | 107,886 | 3,753 | 38 | 86.7% |
| 2024 | 111,512 | 3,809 | 40 | 85.9% |
| 2025 | 132,088 | 3,854 | 41 | 88.0% |
| 2026 | 114,975 | 3,764 | 41 | 89.8% |

This is an inventory/compliance package supporting stock composition, geographic concentration, occupancy mix, and rent-band reporting. Block-anonymized and owner-reported — not a treatment-effects estimate.

</details>

<details>
<summary><strong>West Hollywood — Linked administrative result</strong></summary>

The West Hollywood package links **17,175** protected RSO stock rows (5,959 addresses, 2,108 parcels) to three public administrative surfaces.

| Surface | Matched | Total | Match Rate |
|---------|---------|-------|------------|
| Buyouts | 71 | 206 | 34.5% |
| Seismic retrofit | 705 | 851 | 82.8% |
| Commission appeals | 14 | 34 | 41.2% |

The seismic surface shows strong linkage. Buyout and appeal surfaces are sparser but track real administrative activity. The hearings layer is bounded to the currently collected public archive. Not a treatment-effects estimate.

</details>

<details>
<summary><strong>Los Angeles — Bounded comparative pilot</strong></summary>

The Los Angeles pilot samples **150** properties across **8** fixed street names in central Los Angeles, linking LAHD property-activity records to LA County Assessor parcel context.

| Street | Sampled Rows | Properties | Rent Registered |
|--------|-------------|------------|-----------------|
| Main | 22 | 16 | 11 |
| Broadway | 25 | 19 | 9 |
| Spring | 25 | 19 | 3 |
| Olympic | 25 | 20 | 14 |
| Figueroa | 25 | 21 | 9 |
| Vermont | 22 | 14 | 11 |
| Western | 25 | 22 | 15 |
| Alvarado | 25 | 19 | 12 |

Of the 150 properties, **84** have rent registration numbers and **2,367** total case-history rows are recorded.

| Group | Properties | Mean Cases | Mean Units | Mean Year Built |
|-------|-----------|------------|------------|-----------------|
| Registered | 84 | 12.31 | 10.32 | 1930.9 |
| Not registered | 66 | 20.20 | 65.23 | 1959.5 |

Assessor context: **806,127** city parcels, **114,856** pre-1979 multifamily proxies. Sample-level assessor matches remain sparse (2 of 150). This is a bounded pilot, not a citywide estimate.

</details>

---

## Data sources

All data are from official government agencies or publicly accessible city portals. No proprietary, scraped, or paywalled data are used.

**Statewide sources**

| Source | Agency | Frequency | Coverage |
|--------|--------|-----------|----------|
| Building Permits Survey | Census Bureau | Annual | 2010-2024 |
| House Price Index (HPI) | FHFA | Annual + Quarterly | 2010-2025 |
| ACS 1-Year Profiles | Census Bureau | Annual | 2010-2019, 2021-2024 |
| QCEW Area Slices | BLS | Annual + Quarterly | 2014-2024 |

<details>
<summary><strong>City-level sources</strong></summary>

| City | Source | Agency / Portal | Records |
|------|--------|----------------|---------|
| New York City | Rent Stabilized Building List | Rent Guidelines Board (RGB) | 32,793 buildings (2024) |
| New York City | HPD Housing Violations | NYC Dept. of Housing Preservation & Development | 623,700 building-years |
| San Francisco | Rental Inventory | SF Rent Board Portal | 533,205 unit rows (2022–2026) |
| West Hollywood | RSO Address List | City of West Hollywood | 17,175 unit rows |
| West Hollywood | Buyout Tracking, Seismic Retrofit, Commission Appeals | City of West Hollywood | 1,091 total records |
| Los Angeles | LAHD Property Activity | LA Housing Dept. (LAHD) | 2,367 case rows |
| Los Angeles | Assessor Parcel Data | LA County Assessor (ArcGIS) | 806,127 city parcels |
| Oakland | RAP Petition Cases | Oakland Rent Adjustment Program | 3,805 case rows |
| Oakland | 311 Code Enforcement | City of Oakland Open Data | 30,164 requests |
| Saint Paul | BPS, QCEW, FHFA | Census Bureau, BLS, FHFA | County-level DiD |
| Washington, DC | Assessment Records | DC Office of Tax and Revenue | 130,934 residential parcels |

</details>

<details>
<summary><strong>Coverage timeline</strong></summary>

```
Source          2010  11  12  13  14  15  16  17  18  19  20  21  22  23  24  25
BPS             |---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
FHFA HPI        |---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
ACS profiles    |---|---|---|---|---|---|---|---|---|---|       |---|---|---|---|
QCEW                            |---|---|---|---|---|---|---|---|---|---|---|

Treatment:                                           OR              CA
                                                     2019             2020
```

</details>

<details>
<summary><strong>States in the analysis</strong></summary>

| Role | States |
|------|--------|
| Core treated | California (CA), Oregon (OR) |
| Descriptive only | Washington (WA) |
| Donor pool | AZ, CO, FL, GA, ID, NC, NV, TN, TX, UT, VA |

All 13 states (2 treated + 11 donors) appear in every outcome panel. Washington is retained in the data but excluded from all causal specifications.

</details>

<details>
<summary><strong>ACS coverage notes</strong></summary>

- The Census Bureau did not release 1-year ACS profiles for 2020 due to data-collection disruptions.
- ACS rent and burden variable codes shift between 2010-2014 and 2015+ vintages. The project applies year-aware harmonization.
- ACS mobility variable codes shift between 2015-2018 and 2019+ vintages. The project applies year-aware harmonization.

</details>

---

## Method summary

<details>
<summary><strong>Baseline specification</strong></summary>

**Two-way fixed-effects (TWFE) event study** at the state-year level:

- **Unit fixed effects:** state
- **Time fixed effects:** year
- **Event-time indicators:** leads and lags around each state's treatment year

Estimated as a pooled model with both California and Oregon as treated states and the eleven-state donor pool as controls.

</details>

<details>
<summary><strong>Treatment timing</strong></summary>

| State | Law | Effective date | Annual treat year | Quarterly preferred | Quarterly alt |
|-------|-----|---------------|-------------------|--------------------|----|
| Oregon | SB 608 | 2019-02-28 | 2019 | 2019Q2 | 2019Q1 |
| California | AB 1482 | 2020-01-01 | 2020 | 2020Q1 | 2020Q1 |
| Washington | HB 1217 | 2025-05-07 | Descriptive only | -- | -- |

Oregon's effective date falls at the end of February 2019. The preferred quarterly treatment period is 2019Q2 (first fully exposed quarter). An alternative using 2019Q1 is run as sensitivity.

California's January 1, 2020 effective date produces clean alignment between annual and quarterly treatment definitions.

</details>

<details>
<summary><strong>Robustness battery</strong></summary>

Every outcome is run through:

1. **CA-only and OR-only models** — tests whether pooled result is driven by one state.
2. **State-specific event-time interactions** — reveals whether the two states show different response patterns.
3. **Placebo timing** — treatment shifted two years early. If placebo shows significant effects, the original is less credible.
4. **Western-only donor pool** — restricted to AZ, CO, ID, NV, UT.
5. **Leave-one-donor-out** — AZ, CO, and FL each dropped in turn.
6. **Quarterly timing sensitivity** — FHFA and QCEW run under both preferred and alternative quarterly treatment definitions.

</details>

<details>
<summary><strong>Pre-trend assessment</strong></summary>

| Outcome family | Pre-trend status |
|---------------|-----------------|
| Permits | No strong pooled failure. Usable. |
| FHFA HPI | Relatively mild issues. Usable with caution. |
| Median gross rent | Strongly negative leads at -4, -3. Not clean. |
| QCEW employment | Borderline negative lead at -4. Not clean. |
| Rent burden, mobility | Compressed by ACS coverage limits. |

</details>

<details>
<summary><strong>What the design does not do</strong></summary>

- No synthetic-control methods (noted as optional extension).
- No city, county, or unit-level estimation.
- No general-equilibrium or welfare modeling.
- No claim about "rent control" as a general policy category — estimates are specific to Oregon SB 608 and California AB 1482.

</details>

---

## Limitations

<details>
<summary><strong>ACS coverage window</strong></summary>

Coverage: 2010-2019 and 2021-2024. The 2020 ACS 1-year profile was not released. ACS rent and burden variables require year-aware harmonization because official profile codes shift between 2010-2014 and 2015+. The missing 2020 still interrupts the treatment-era path for California.

</details>

<details>
<summary><strong>QCEW start year</strong></summary>

Area-slice coverage begins in 2014, giving Oregon five and California six pre-treatment annual observations. Short pre-periods increase the risk that pre-trends are uninformative. Placebo checks have even fewer clean observations.

</details>

<details>
<summary><strong>Two treated states</strong></summary>

With only California and Oregon, heterogeneity questions (Q7 and Q8) are **treated-state contrasts**, not powered interaction estimates. If one state dominates the pooled signal (as California does for FHFA HPI), the pooled estimate may not generalize.

</details>

<details>
<summary><strong>Occupied vs. asking rents</strong></summary>

ACS measures rent paid by current occupants, not listing prices. Rent-cap policies constrain renewal prices; ACS reflects a mix of stabilized, market-rate, new, and renewal leases. Asking rents could move differently.

</details>

<details>
<summary><strong>State-level aggregation</strong></summary>

All estimates are at the state level. Within-state heterogeneity across cities, unit types, or tenant demographics is not identified.

</details>

<details>
<summary><strong>Washington</strong></summary>

HB 1217 took effect May 2025. Included for descriptive comparison only. Almost no post-policy outcome data exist. Future updates can incorporate it as data accumulate.

</details>

<details>
<summary><strong>Non-claims</strong></summary>

This project does not claim to:
- Measure the effect of "rent control" as a general policy category.
- Determine optimal rent-cap levels or policy design.
- Estimate general-equilibrium welfare effects.
- Identify effects of legacy local systems (NYC, Berkeley, San Francisco, etc.).
- Measure landlord profitability, maintenance spending, or informal side payments.
- Capture eviction changes (phase-2 extension).
- Provide a causal estimate for any outcome where pre-trend or placebo checks fail.

</details>

---

## Question map

Ten research questions mapped to evidence status:

| # | Question | Domain | Status | Source | Coverage |
|---|----------|--------|--------|--------|----------|
| Q1 | Did caps reduce occupied-rent growth? | Rent levels | Answered with limits | ACS | 2015-19, 2021-24 |
| Q2 | Did caps reduce renter cost burden? | Affordability | Answered with limits | ACS | 2015-19, 2021-24 |
| Q3 | Did caps increase residential stability? | Mobility | Answered with limits | ACS | 2015-19, 2021-24 |
| Q4 | Did caps reduce housing supply? | Supply | **Answered** | BPS | 2010-2024 |
| Q5 | Did caps change housing prices? | Capitalization | **Answered** | FHFA | 2010-2025 |
| Q6 | Did caps change labor-market outcomes? | Labor | Answered with limits | QCEW | 2014-2024 |
| Q7 | Larger effects where burden was higher? | Heterogeneity | Suggestive | ACS + event study | CA vs. OR contrast |
| Q8 | Larger effects where supply was constrained? | Heterogeneity | Suggestive | BPS + event study | CA vs. OR contrast |
| Q9 | Did impacts differ across outcome domains? | Cross-domain | Answered (summary) | All | Varies |
| Q10 | Were CA and OR similar enough to pool? | Poolability | **Answered** | All | Full |

<details>
<summary><strong>Notes on Q7 and Q8</strong></summary>

Q7 and Q8 compare California to Oregon on pre-policy characteristics:
- **Q7 (burden):** CA pre-policy rent burden 55.2% vs. OR 51.0%.
- **Q8 (supply constraint):** CA permits per 1,000 renter households 18.0 vs. OR 32.0.

With only two treated states, these are descriptive contrasts. They indicate which state was more burdened or supply-constrained before treatment, but cannot isolate burden or supply constraint as a causal moderator.

</details>

---

## Repository structure

```
rentctrl/
  config/           Source inventory, policy metadata, state metadata, project settings
  data/             Raw inputs and processed panels
  docs/             Static site (GitHub Pages) and research design notes
  results/          Output tables, figures, and interpretation notes
  scripts/          Reproducible command-line entry points
  src/              Python package: statewide parsers, city modules (12 cities), analysis utilities
  tests/            Automated test suite
```

## Reproducing the results

```bash
# set up the environment
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# bootstrap seed data from included raw files
python -B scripts/bootstrap_seed_data.py

# download ACS state profiles
python -B scripts/download_acs_state_profile.py

# download QCEW state quarter files
python -B scripts/download_qcew_state_quarters.py

# build the merged state panel
python -B scripts/build_core_state_panel.py

# run baseline event studies and credibility checks
python -B scripts/run_baseline_event_study.py

# run tests
python -B -m pytest -q
```

Alternative setup using [uv](https://github.com/astral-sh/uv):

```bash
source .env
uv pip install -e '.[dev]' --python "$UV_PROJECT_ENVIRONMENT/bin/python"
uv run python -B scripts/bootstrap_seed_data.py
# (same scripts as above, prefixed with uv run)
```

## License

See [LICENSE](LICENSE).
