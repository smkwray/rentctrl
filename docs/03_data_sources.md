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
- one small sample extract staged,
- full panel downloader left for Codex.

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
