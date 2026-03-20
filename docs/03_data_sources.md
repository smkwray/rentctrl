# Core data sources

## Core, phase-1 sources

### ACS 1-year state profile API
Use for:
- median gross rent,
- rent burden,
- renter share,
- mobility,
- income.

Why core:
- official,
- stable,
- easy to automate.

Status in starter:
- variable mapping included,
- full panel downloader implemented,
- processed panel assembled under a strict coverage manifest,
- 2020 remains intentionally absent because the ACS 1-year release does not exist.

### Census Building Permits Survey
Use for:
- total permitted units,
- structure mix,
- multifamily activity.

Why core:
- direct flat files,
- official,
- already staged for 2010-2024 state annual files.

### FHFA HPI master file
Use for:
- state and MSA house price indexes,
- annual and quarterly aggregation.

Why core:
- one stable official file,
- long run,
- easy to subset.

### BLS QCEW open data area slices
Use for:
- state employment,
- wages,
- establishments,
- labor-market side effects.

Why core:
- official,
- reproducible URL structure,
- enough detail without requiring paid access.

## Current build contract

The public statewide build now expects:

- annual baseline domains: policy, ACS, BPS, FHFA, QCEW
- quarterly baseline domains: policy, FHFA, QCEW

The merged panel builder writes `data/processed/data_coverage_manifest.csv` on every run and can fail in strict mode if any required domain is missing, malformed, or non-ready for baseline use.

This means downstream reporting no longer silently treats partial panels as successful baseline builds.

---

## Deferred, phase-2 sources

### Eviction Lab
Useful for:
- formal eviction filings.
Deferred because:
- not required for first-pass state panel.

### AHS
Useful for:
- housing quality and unit-level characteristics.
Deferred because:
- more survey-design handling and metro logic.

### Local registries and municipal open data
Useful for:
- mechanism work,
- legal ceilings,
- maintenance and code enforcement.
Deferred because:
- high complexity per jurisdiction.

### Urban multi-city reform panel
Useful for:
- exploratory coverage,
- validation of reform inventory.
Deferred because:
- should not anchor the core treatment assignment.

## Data reliability hierarchy used in this starter

1. official federal / state sources,
2. official municipal sources,
3. broad public academic sources,
4. machine-learning-compiled or press-derived references.
