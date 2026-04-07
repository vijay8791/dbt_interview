# dbt Analytics Engineering Assessment — Vijay Sundaram

## Overview

Full dimensional model built on top of 14 Salesforce CRM staging sources
using dbt Core 1.11 + DuckDB. Demonstrates production-grade dbt patterns
across staging, intermediate, and marts layers.

## Getting Started

This project uses [Dev Containers](https://containers.dev/) for a zero-setup environment.

1. Clone the repository
2. Open in VS Code with the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) installed
3. Click **Reopen in Container** when prompted (or `Ctrl+Shift+P` → "Dev Containers: Reopen in Container")

Docker builds the environment and installs all dependencies automatically. Then:

```bash
cd transformation
dbt build       # builds all models + runs all 188 tests
```

Expected result: `PASS=221 WARN=1 ERROR=0`

The 1 WARN is intentional — `hours_to_resolve > 8760` (cases open > 1 year) is a data quality signal configured with `severity: warn`.

---

## Project Structure

```
transformation/
├── models/
│   ├── staging/          — 14 views on raw Salesforce sources. Rename only, no logic.
│   ├── intermediate/     — Business logic, joins, seed enrichment. Tables.
│   └── marts/
│       ├── dimensions/   — 6 conformed dimensions with surrogate keys.
│       └── facts/        — 4 fact tables with window metrics and audit columns.
├── seeds/                — Static lookup tables (lead status, opportunity stage).
├── snapshots/            — SCD tracking for account and opportunity.
├── macros/               — Reusable Jinja helpers and custom generic tests.
├── docs/                 — Doc blocks referenced via {{ doc() }} in schema YAML.
├── analyses/             — Ad-hoc version-controlled SQL queries.
└── tests/                — Singular data quality assertions.
```

---

## Models

### Staging (14 views)
Rename-only layer. One model per Salesforce source table.
`stg_salesforce__account`, `stg_salesforce__opportunity`, `stg_salesforce__lead`,
`stg_salesforce__case`, `stg_salesforce__contact`, `stg_salesforce__user`,
`stg_salesforce__campaign`, `stg_salesforce__product_2`, `stg_salesforce__pricebook_entry`,
`stg_salesforce__opportunity_history`, `stg_salesforce__case_history_2`,
`stg_salesforce__record_type`, `stg_salesforce__solution`, `stg_salesforce__user_role`

### Intermediate (4 tables)
Business logic and enrichment layer. All join to seeds for lookup enrichment.
`int_opportunity_enriched`, `int_opportunity_stage_history`,
`int_case_enriched`, `int_lead_enriched`

### Dimensions (6 tables)
`dim_account`, `dim_contact`, `dim_user`, `dim_campaign`, `dim_product`, `dim_date`

### Facts (4 models)
| Model | Materialization | Key Features |
|---|---|---|
| `fct_opportunity` | incremental | Window metrics, deal size, running revenue |
| `fct_opportunity_stage_history` | incremental | Stage transition tracking, days_in_stage |
| `fct_case` | table | SLA tracking, resolution speed, hours_to_resolve |
| `fct_lead` | table | Lifecycle stage, conversion tracking, owner metrics |

---

## dbt Features Demonstrated

| Feature | Detail |
|---|---|
| Surrogate keys | `dbt_utils.generate_surrogate_key` on all dims + facts |
| Incremental models | `fct_opportunity` + `fct_opportunity_stage_history` — append strategy |
| SCD snapshots | `snapshot_opportunity`, `snapshot_account` — check strategy |
| Seeds | `seed_lead_status_map`, `seed_opportunity_stage_map` |
| Custom macros | `generate_audit_columns` (Jinja loop), `generate_schema_name` |
| Custom generic tests | `test_is_positive`, `test_is_between` |
| Test types | Column, model, source, singular, generic |
| Window functions | ROW_NUMBER, LAG, running SUM across all fact tables |
| Tags | `staging`, `intermediate`, `marts`, `facts`, `daily`, `critical` |
| store_failures | Failing rows saved to `dbt_test__audit` for debugging |
| Doc blocks | `docs/salesforce_docs.md` — referenced via `{{ doc() }}` |
| persist_docs | Column descriptions written to DuckDB as comments |
| Exposures | 3 — sales dashboard, support report, lead conversion ML |
| Analyses | `pipeline_summary`, `lead_funnel` |
| dbt_date | `dim_date` generated 2018–2030 |
| Hooks | `on-run-start` / `on-run-end` logging |
| severity | `warn` on derived/nullable fields, `error` on critical PK/FK |

---

## Design Decisions

**Staging as views** — Pure rename layer with no logic. If a source column name changes, only the staging model needs updating, not downstream models.

**Intermediate as tables** — Pre-computed enrichment so mart queries join to fast materialised sets rather than re-running joins on raw views every time.

**Incremental for fct_opportunity** — Highest volume object in Salesforce, changes frequently. Incremental strategy reduces daily compute significantly in production.

**Snapshots use check strategy** — Salesforce `systemmodstamp` gets touched by internal system events unrelated to business changes. Check strategy tracks only explicitly specified business-relevant columns accurately.

**Seeds for lookup data** — Lead status and opportunity stage maps are version-controlled as CSV seeds. Single place to update, visible in the DAG, testable like any other model.

**store_failures on marts** — Failing test rows persisted to `dbt_test__audit` schema. Teams can query exactly which rows failed rather than just knowing a count.

**Custom generic tests over singular** — `test_is_positive` and `test_is_between` are reusable across columns and models. Singular tests reserved for cross-model business rules that generic tests can't express.

---

## Test Results

```
dbt build --no-partial-parse

Done. PASS=221  WARN=1  ERROR=0  SKIP=0  NO-OP=3  TOTAL=225
```
