# rentctrl: Statewide Rent Caps and Housing Markets

**[View the interactive report](https://smkwray.github.io/rentctrl/)**

rentctrl estimates how Oregon's and California's statewide rent caps affected housing supply, house prices, rents, affordability, residential mobility and labor markets. It uses free federal data and an event-study design that compares the two states with 11 states that have no statewide cap. It also includes local extensions that analyze administrative records in 12 cities with rent regulation. The estimates are informative but not settled causal effects: only two states are treated, several outcomes fail pre-trend or placebo checks, and permutation inference does not distinguish any estimate from zero.

| State | Law | Effective | Cap |
|---|---|---|---|
| Oregon | SB 608 | 2019-02-28 | Consumer Price Index (CPI) + 7 percentage points, most units over 15 years old |
| California | AB 1482 | 2020-01-01 | CPI + 5 percentage points, most units over 15 years old |

Washington's HB 1217 (effective May 2025) is tracked for description only and is excluded from all estimates.

## Headline findings

Average post-policy shift in the pooled event study, as a percentage of the treated states' pre-policy mean:

| Domain | Outcome | Average post shift | Evidence grade |
|---|---|---|---|
| Supply | Total permitted units | −24.7% | Answered |
| Supply | 5+ unit permits | −21.1% | Answered |
| Supply | Multifamily share of permits | −8.9% | Answered |
| House prices | FHFA House Price Index | −7.1% | Answered |
| Rent | Median gross rent | −0.9% | Answered with limits |
| Affordability | Rent burden 30%+ of income | −1.0% | Answered with limits |
| Stability | Same house one year ago | +0.4% | Answered with limits |
| Stability | Moved in the last year | −2.0% | Answered with limits |
| Stability | Moved from a different state | +31.2% | Answered with limits |
| Tenure | Renter share | +0.8% | Answered with limits |
| Labor | Average weekly wage | +3.8% | Exploratory |
| Labor | Covered employment | −2.3% | Exploratory |

"Answered" means full coverage, near-clean pre-trends and all checks run; "Answered with limits" means coverage gaps or pre-trend problems; "Exploratory" means only the direction is informative. Coverage and evidence quality differ, so the magnitudes are not a ranking across domains.

- Permitted units grew more slowly in the treated states, consistently across permit measures, but a placebo that moves treatment two years earlier still produces some negative coefficients.
- The house-price decline has relatively mild pre-trend problems and comes mostly from California.
- Median gross rent shows strongly negative leads at event times −4 and −3, so the rent series is descriptive. Rent burden and mobility share the American Community Survey (ACS) coverage gap.
- Labor outcomes from the Quarterly Census of Employment and Wages (QCEW) start in 2014, and employment shows borderline negative leads and a false placebo lead.

The site has the event-study plots, state contrasts and uncertainty table.

## City extensions

The project audits 12 cities. Six have result packages (New York City, Oakland, Saint Paul, San Francisco, San Jose and West Hollywood) and six are pilots (Los Angeles, Berkeley, Washington DC, Santa Monica, East Palo Alto and Mountain View). The largest package links New York City's Rent Stabilized Building List to Department of Housing Preservation and Development (HPD) violation records for 89,100 buildings from 2019 to 2025. A building fixed-effects model shows stabilized buildings accumulating violations faster (+4.20 per building by 2025), but a refined within-community-board match of 28,746 pairs puts the 2025 gap near zero, so the result depends on the comparison design. City results are descriptive or design-sensitive; the site's Cities page and the notes in `results/` give the details.

## Method

The baseline is a two-way fixed-effects (TWFE) event study at the state-year level, with state and year fixed effects and event-time leads and lags around each state's treatment year. California and Oregon are pooled against a donor pool of Arizona, Colorado, Florida, Georgia, Idaho, Nevada, North Carolina, Tennessee, Texas, Utah and Virginia. Robustness checks run each outcome for each state alone, with state-specific interactions, with a placebo treatment two years early, with a western-only donor pool, and leaving out Arizona, Colorado or Florida; house prices and labor outcomes are also run quarterly under two Oregon treatment quarters. Uncertainty is reported with conventional HC1 standard errors and with permutation inference (250 draws).

## Data sources

| Source | Agency | Frequency | Coverage |
|---|---|---|---|
| Building Permits Survey | Census Bureau | Annual | 2010–2024 |
| Purchase-only House Price Index | Federal Housing Finance Agency (FHFA) | Annual and quarterly | 2010–2025 |
| ACS 1-year state profiles | Census Bureau | Annual | 2010–2019, 2021–2024 |
| QCEW area slices | Bureau of Labor Statistics | Annual and quarterly | 2014–2024 |

The Census Bureau did not release 2020 ACS 1-year profiles. City extensions use public administrative data from city and county portals, such as New York City's Rent Guidelines Board and HPD, the San Francisco Rent Board and Oakland's Rent Adjustment Program.

## Reproduce

Requires Python 3.10 or newer and network access to the four federal sources.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

make reproduce          # or: python3 -B scripts/reproduce_public_baseline.py
python3 -B -m pytest -q
```

`make reproduce` downloads the permit, house-price, ACS and QCEW data, builds the policy and state panels, runs the baseline event studies and the robustness checks, and writes tables to `results/tables/`. Only the statewide baseline rebuilds end to end. The city results are committed in `results/`; the city scripts in `scripts/` rebuild parts of them, and the New York City scripts need a processed building-year panel whose builder is not included.

## Outputs and layout

```
config/    policy dates, state metadata, source and outcome catalogs, city audit status
src/       Python package (rent_control_public): statewide parsers, event study, city modules
scripts/   download, panel-building, estimation and city-extension entry points
results/   committed tables and interpretation notes
docs/      source of the interactive report
tests/     unit and integration tests
data/      downloaded and processed data (not committed)
```

## Limitations

- With two treated states, permutation p-values for all 12 headline outcomes are above 0.1, and California-versus-Oregon differences are descriptive contrasts rather than powered interaction estimates.
- The ACS measures rent paid by current occupants, not asking rents, and the missing 2020 release interrupts the series at the start of California's treatment period.
- QCEW coverage starts in 2014, leaving five pre-treatment years for Oregon and six for California.
- All statewide estimates are at the state level and say nothing about cities, unit types or tenant groups within a state. They apply to SB 608 and AB 1482, not to rent control in general.
- Evictions, landlord profitability, maintenance spending and welfare effects are not measured.

## License

MIT. See [LICENSE](LICENSE).
