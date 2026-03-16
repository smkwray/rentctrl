# Project overview

## Goal

Estimate the effects of statewide rent-regulation policies using only free public data and a build sequence that is realistic for an automated coding agent.

## Primary recommendation

Start with a **state-level event-study / DID build** centered on:

- Oregon (2019)
- California (2020)

Treat Washington (2025) as a **descriptive extension**, not a core causal estimate yet.

## Why this is the right first build

This design is the best balance of:

- policy clarity,
- public-data availability,
- automation friendliness,
- low treatment-mapping burden,
- expansion potential.

## What this starter already solves

- project structure,
- source inventory,
- core outcome catalog,
- policy timing metadata,
- parser scaffolding,
- real seed raw files,
- processed starter outputs,
- Codex task sequencing.

## Deliverables Codex should produce next

1. a reproducible ACS state-profile downloader,
2. a reproducible QCEW state-quarter downloader,
3. a merged annual core panel,
4. a baseline event-study output package,
5. placebo / donor sensitivity checks,
6. optional second-stage local extensions.

## What not to do first

Do **not** begin by scraping many city portals or trying to harmonize multiple legacy local rent-control regimes.
