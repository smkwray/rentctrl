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
- downloader entry points,
- strict panel build with coverage manifest,
- baseline event-study output package,
- placebo / donor sensitivity checks,
- test fixtures and integration checks.

## Deliverables the build should produce next

1. a fully validated public reproduce smoke run,
2. refreshed checked-in results artifacts from the latest pipeline,
3. one extension package once the statewide pipeline is stable,
4. doc maintenance as inference and artifact outputs evolve.

## What not to do first

Do **not** begin by scraping many city portals or trying to harmonize multiple legacy local rent-control regimes.
