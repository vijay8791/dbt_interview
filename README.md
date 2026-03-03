# dbt Analytics Engineering Assessment

This project evaluates your dbt and SQL skills using an embedded [DuckDB](https://duckdb.org/) database with sample Salesforce CRM data. No cloud accounts or credentials needed.

## Getting Started

This project requires [Dev Containers](https://containers.dev/).

1. Clone the repository
2. Open it in VS Code with the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) installed
3. When prompted, click **Reopen in Container** (or run the command manually via `Ctrl+Shift+P` → "Dev Containers: Reopen in Container")

The devcontainer builds a Docker image and installs all dependencies automatically. Once inside, verify:

```bash
cd transformation
dbt run     # should build all 14 staging views successfully
```

## What's Provided

- **14 staging models** in `models/staging/` — views on raw Salesforce data (accounts, opportunities, leads, cases, contacts, users, campaigns, products, etc.)
- **Source definition** in `models/staging/_src__salesforce.yml`
- **Pre-configured schemas** in `dbt_project.yml`: `staging` (views), `intermediate` (tables), `marts` (tables)
- **Packages**: `dbt_utils`, `dbt_date`, `codegen`

## Your Task

**This is a technical skills demonstration.** The goal is to showcase as many dbt features and best practices as you can. We will **not** evaluate whether the data in your models makes business sense — we are looking at how you use dbt as a tool.

Using the staging models as input, build a dimensional model. The more dbt capabilities you demonstrate, the better.

### Minimum Expected

- Dimension and fact models in `models/marts/`
- dbt tests in YAML schema files
- Proper use of `{{ ref() }}` and materializations

### What Will Impress Us

The list below is not exhaustive — use your experience to decide what's appropriate:

- **Project structure** — clear layering (staging → intermediate → marts), consistent naming conventions (`dim_`, `fct_`, `int_` prefixes), logical file organization
- **SQL style** — CTE-based queries (no nested subqueries), meaningful CTE names, consistent column ordering
- **Schema YAML files** — model and column descriptions, tests co-located with models
- **Testing** — primary key tests (unique + not_null), referential integrity between facts and dimensions, accepted_values for enums, composite key tests
- **Materializations** — appropriate choice per layer, understanding of when to use views vs tables
- **dbt packages** — `dbt_utils` macros (e.g., `generate_surrogate_key`, `unique_combination_of_columns`), `dbt_date` for date dimensions
- **Custom macros** — reusable Jinja logic where it reduces repetition
- **Tags** — organizing models for selective runs
- **Seeds** — static lookup data where useful
- **Snapshots** — SCD Type 2 tracking (timestamp or check strategy)
- **Incremental models** — if you see a good use case for it
- **Window functions** — ROW_NUMBER, LAG/LEAD, running totals
- **Jinja** — loops, conditionals, dynamic SQL generation
- **Documentation** — `persist_docs`, doc blocks, or a brief design rationale

## Interview Format

You will present your solution in a live screen-sharing session. Be prepared to:

- Walk through your project and explain your design decisions
- Demonstrate running `dbt build` and show passing tests
- Discuss trade-offs and alternative approaches
- Answer follow-up questions about dbt concepts and patterns

Good luck!
