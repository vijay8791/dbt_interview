# Salesforce Analytics — dbt Project

## Overview

Full dimensional model built on top of 14 Salesforce CRM staging sources
using dbt Core 1.11 + DuckDB. Demonstrates production-grade dbt patterns
across all layers.

## Project Structure
```
models/
├── staging/          — Views on raw Salesforce sources. Rename only.
├── intermediate/     — Business logic, joins, enrichment. Tables.
└── marts/
    ├── dimensions/   — Conformed dimensions with surrogate keys.
    └── facts/        — Fact tables with window metrics and audit columns.

seeds/                — Static lookup data.
snapshots/            — SCD Type 2 tracking.
macros/               — Shared Jinja helpers.
docs/                 — Doc blocks referenced in YAML schema files.
analyses/             — Ad-hoc version-controlled SQL queries.
tests/                — Singular data tests.
```

## Features Demonstrated

| Feature | Detail |
|---|---|
| Surrogate keys | `dbt_utils.generate_surrogate_key` on all dims + facts |
| Incremental models | `fct_opportunity` + `fct_opportunity_stage_history` — merge strategy |
| SCD Type 2 snapshots | `snapshot_opportunity`, `snapshot_account` — check strategy |
| Seeds | `seed_lead_status_map`, `seed_opportunity_stage_map` |
| Custom macros | `safe_divide`, `current_timestamp_utc`, `generate_audit_columns` (Jinja loop) |
| Custom generic tests | `test_is_positive`, `test_is_between` |
| All 6 test types | Column, model, source, singular, generic, project level |
| Window functions | ROW_NUMBER, LAG, running SUM in all fact tables |
| Hooks | on-run-start, on-run-end, pre-hook, post-hook |
| Tags | staging, intermediate, marts, dimensions, facts, daily, critical |
| Source freshness | All 14 Salesforce sources with loaded_at_field |
| store_failures | Failing rows saved to dbt_test__audit |
| Doc blocks | docs/salesforce_docs.md referenced via doc() |
| persist_docs | Column descriptions written to DuckDB as comments |
| Exposures | 3 exposures — dashboards and ML model |
| Analyses | pipeline_summary, lead_funnel |
| codegen | generate_model_yaml demonstrated |
| dbt_date | dim_date generated 2018-2030 |
| run-operation | validate_sources, grant_select |

## Run Order
```bash
dbt deps
dbt seed
dbt run --select staging
dbt run --select intermediate
dbt run --select marts.dimensions
dbt run --select marts.facts
dbt snapshot
```

Or simply:
```bash
dbt build
dbt snapshot
```

## Design Decisions

**Staging as views** — Pure rename layer. No logic. If source changes,
only staging needs updating.

**Intermediate as tables** — Pre-computed enrichment so mart queries
join to fast sets rather than re-running joins on raw views.

**Incremental for fct_opportunity** — Highest volume object, changes
frequently. Incremental reduces daily compute cost significantly.

**Snapshots use check strategy** — Salesforce systemmodstamp gets
touched by system events. Check strategy tracks only business-relevant
column changes accurately.

**Seeds for lookup data** — Lead status and opportunity stage maps are
version-controlled as CSV seeds. Single place to update, shows in DAG.

**store_failures on marts** — Failing test rows saved to
dbt_test__audit for debugging. Teams can query exactly which rows
failed instead of just knowing a test failed.
