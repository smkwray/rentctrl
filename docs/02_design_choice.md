# Design choice

## Chosen design

**Statewide rent-cap event study with a phased build.**

Core treated states:
- Oregon
- California

Descriptive extension:
- Washington

## Why not start with a multi-city national reform panel?

Because the public Urban Institute multi-city reform dataset is useful as a reference but explicitly warns that many reforms were likely missed and that it should be used with caution for all-city reform analysis.

Implication:
- use it as a validation aid or exploratory catalog,
- do not make it the main treatment source for the first build.

## Why not start with local registries?

Because the most visible local public sources create major treatment-assignment frictions:

- NYC rent-stabilized lists are building-level, not apartment-level.
- Berkeley and other local systems are rich but bespoke.
- Legacy regimes are hard to compare as a single binary treatment.

## Why statewide caps first?

Statewide caps have:

- clear official effective dates,
- simpler treatment coding,
- outcomes available at state level,
- data sources that are easy to automate.

## Why not use Washington as a core treated unit yet?

Its law is too recent for a strong post-period as of this starter's build date. Washington should be part of the monitoring and descriptive package, not the first causal headline.

## Recommended empirical sequence

1. annual state panel,
2. quarterly state panel for FHFA and QCEW,
3. baseline DID / event study,
4. donor-pool sensitivity,
5. placebo intervention dates,
6. optional synthetic-control extensions,
7. only then local case studies.
