# Los Angeles Public Reporting Note

Los Angeles currently supports a bounded comparative pilot built from LAHD property activity records, linked to LA County Assessor parcel context.

- Sampled properties: 150
- Properties with assessor matches: 2
- Properties with rent registration numbers: 84
- Assessor `rso_eligible_proxy` properties: 2
- Total case-history rows: 2,367

Interpretation:
- This is a deterministic bounded pilot across a fixed street tranche, not a citywide estimate.
- The assessor layer adds parcel age and unit-count context where the public ArcGIS service returns sample matches.
- The package should be presented as a comparative pilot rather than a treatment-effects design.
