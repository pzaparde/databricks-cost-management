# Cost Management Accelerator

Company-agnostic Databricks + cloud cost analytics. Deploy with config only - no code edits.

What you get: a Lakeflow pipeline (bronze/silver/gold) over `system.billing`, an optional cloud-bill (AWS Cost Explorer / generic CSV) ingestion, data-quality reconciliation, identity (user / service principal) attribution, budget tracking, a 4-page AI/BI dashboard, a daily job and a freshness alert.

## Components
| Path | What |
| --- | --- |
| `install` | **One-click installer notebook** (no CLI / bundle needed) |
| `databricks.yml` | Bundle variables + targets (`demo`, `customer_template`). |
| `targets/*.yml` | Your company-specific targets (git-ignored - never committed) |
| `resources/cost_management.pipeline.yml` | Lakeflow Spark Declarative Pipeline definition |
| `resources/cost_management.job.yml` | Daily job: `setup` -> `refresh_pipeline` |
| `resources/cost_management.dashboard.yml` | AI/BI dashboard (catalog/schema injected at deploy) |
| `resources/cost_management_freshness.alert.yml` | SQL alert: stale billing data or missing monthly cloud file |
| `src/setup/00_setup.sql` + `run_setup` | Idempotent bootstrap: creates the schema if missing, landing volume, config tables, optional sample data |
| `src/pipeline/transformations/` | Bronze / silver / gold / DQ pipeline code |
| `src/dashboards/Cost Management Accelerator.lvdash.json` | Dashboard **template** (unqualified table names). The catalog/schema saved inside it are placeholders - `install` and the bundle always replace them with yours. |
| `config/cloud_sources_seed.csv` | Default cloud source folders, loaded once if `config_cloud_source` is empty |
| `config/cloud_cost_template.csv` | Generic cloud cost upload template (any provider) |
| `docs/DATA_CONTRACT.md` | Inputs, rules, outputs, attribution limits |
| `sample_data/` | Anonymized dummy cloud cost CSVs for demo/testing (see `sample_data/README.md`) |

## Prerequisites (one-time, per workspace)
1. Unity Catalog workspace with **serverless** jobs, notebooks and pipelines enabled.
2. The target **catalog** exists. The **schema** is created by setup only if it does not exist yet (`CREATE SCHEMA IF NOT EXISTS`); an existing schema is never altered or dropped, and a catalog is never created.
3. The installing / run-as identity has either `USE CATALOG` + `CREATE SCHEMA` on the catalog (so setup can create the schema - you then own it), or, for an existing schema, `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `CREATE MATERIALIZED VIEW`, `CREATE VOLUME` on it.
4. The `system.billing` and `system.access` schemas are enabled and the identity has `SELECT` on `system.billing.usage`, `system.billing.list_prices`, `system.access.workspaces_latest` (and optionally `system.billing.account_prices` for negotiated prices). The setup preflight checks this and stops with a clear message if not.
5. A SQL warehouse you can use (the installer auto-selects one; the bundle path needs its ID).
6. Bundle path only: Databricks CLI >= 0.281.0 (laptop or the workspace **web terminal**).

## Option A - Quick install, no CLI (recommended for demos)
1. Workspace -> **Create -> Git folder** -> paste the GitHub URL of this repo.
2. Open the `install` notebook in the repo root.
3. Every widget has a default, so normally you just press **Run all**. Change a widget only to override:

   | Widget | Default | Notes |
   | --- | --- | --- |
   | `catalog` | blank = the workspace's default catalog | Must be a Unity Catalog catalog. If the workspace default is the legacy `hive_metastore`, the installer stops and lists the catalogs you can pick. |
   | `schema` | `cost_management` | Created by setup if it does not exist. |
   | `warehouse_id` | blank = auto-pick | Prefers a serverless, running SQL warehouse you can use. |
   | `history_start_date` | `2025-01-01` | First run loads billing history from this date. |
   | `workspace_filter` | `ALL` | Every workspace in the account, discovered from billing data. Comma-separated IDs narrow the scope. |
   | `pricing_mode` | `account_with_list_fallback` | Negotiated price from `system.billing.account_prices` per SKU; list price when there is none (or when `account_prices` is not readable). SKUs with no price at all are labelled `UNPRICED`. |
   | `load_sample_data` | `false` | `true` copies demo cloud cost files into the landing volume. |
   | `create_job` | `false` | `true` creates the daily refresh job. |
   | `name_prefix` | blank | Optional prefix for pipeline / job / dashboard names. |
4. **Run all**. It first prints a **preflight report** covering Unity Catalog, catalog/schema existence, your privileges, system tables, SQL warehouse, serverless pipelines, existing-table ownership and SCIM access. Every check is run and listed as PASS / WARN / FAIL with the exact fix; the install stops before creating anything if any check FAILs. A missing schema is a WARN ("setup will create it") when you can create it, and a FAIL with the exact `GRANT` statement when you cannot. Then it runs, in order: setup (creates the schema if needed) -> creates the pipeline -> first pipeline run (loads history; 10-30 min) -> imports + publishes the dashboard -> (optional) daily job. The last cell prints the dashboard URL.
   - The published dashboard is created in the installer's home folder (`/Users/<you>/Cost Management Accelerator`). It is built from the template in `src/dashboards/`; re-run with `steps = dashboard` after pulling dashboard changes from Git.
5. Re-running is safe (existing pipeline / dashboard / job with the same name are updated). `dry_run = true` validates everything without creating anything. `steps` lets you run a subset, e.g. `dashboard`.

Note: the freshness alert is created only by the bundle path (Option B).

## Option B - Declarative Automation Bundle (CI/CD, production)
1. Clone the repo (laptop, or a Git folder + web terminal). `databricks auth login --host <workspace-url>`.
2. Demo / first try - no file edits:
   `databricks bundle deploy -t demo --var="catalog=<catalog>" --var="warehouse_id=<id>"` (`schema` defaults to `cost_management`, `history_start_date` to `2025-01-01`; add `--var="schema=<schema>"` to override)
   then `databricks bundle run cost_management_refresh -t demo ...same --var flags...`
   (development mode: names prefixed `[dev <you>]`, schedules paused, sample cloud data loaded).
3. Production: copy `customer_template` from `databricks.yml` into `targets/<company>.yml` (git-ignored), fill `host`, `catalog`, `schema`, `warehouse_id`, `alert_email(s)`, optionally `run_as` (service principal), `workspace_filter`, `history_start_date`, discounts. Then:
   `databricks bundle validate -t <company>` -> `databricks bundle deploy -t <company>` -> `databricks bundle run cost_management_refresh -t <company>`.
4. This creates the pipeline, job (setup -> refresh_pipeline), dashboard and freshness alert.
   - `setup` creates the schema if it does not exist, then the landing volume + config tables, and seeds workspace environments and default tag mappings.
   - `refresh_pipeline` loads billing history from `history_start_date` on the first run, then re-reads `lookback_days` on every later run.
5. To force a full history reload later, set `force_full_reload: "true"` for one deploy + run, then set it back.

## After install - verify (5 min)
1. Pipeline update is **COMPLETED**; `gold_dbx_cost_daily` and `gold_total_cost_monthly` have rows.
2. `dq_reconciliation` latest row shows status PASS on the dashboard's *Data Quality & Budget* page.
3. Dashboard *Overview* KPIs show Month-to-Date / YTD values and *Latest Data* is within ~2 days of today.
4. With sample data, *Cloud Costs* shows AWS EC2 / S3 rows for the sample month.

## Tune config (optional, never overwritten by later runs)
- `config_workspace_env`: environments are guessed from workspace names; fix any `UNASSIGNED`, set `include_flag = false` to exclude a workspace.
- `config_tag_mapping`: point standard attributes (product, sub_product, cost_center, team, owner, project) at your tag keys.
- `config_cloud_source`: cloud folders (seeded from `config/cloud_sources_seed.csv` on first run only).
- `config_budget`: monthly budgets (see Budget).
- `config_identity.manual_name`: friendly names for unresolved identities.

## Where cloud costs are stored

Cloud cost CSV files are uploaded to the **landing volume**:

```
/Volumes/<catalog>/<schema>/cloud_cost_landing/<provider>/<source_folder>/
```

Each `<provider>/<source_folder>` must match a row in `config_cloud_source` (e.g. `aws/prod`, `aws/test`, `generic/manual`).
The pipeline reads all `.csv` files in those folders and deduplicates by (provider, source_folder, cost_date, granularity), keeping the latest file.

Anonymized sample files are provided in `sample_data/` for demo and testing. Set `load_sample_data = true` (install widget, or the bundle variable - the `demo` target does this) and setup copies them into the volume. **Delete them from the volume before loading real exports.** See `sample_data/README.md`.

## Budget

The default assumption is an **annual budget of 2,000,000 USD** (Databricks + cloud combined, current calendar year).

* **Dashboard viewers** can change the budget on the fly via the **"Annual Budget (USD)"** input on the *Data Quality & Budget* dashboard page (dataset parameter `annual_budget_usd`, default `2000000`).
* **To change the default for everyone**, edit the `annual_budget_usd` parameter default in the dashboard definition and redeploy.
* **Budget vs Actual table**: for each month of the current year it uses the `config_budget` row for that month (with `environment` and `product` NULL, `cost_scope` = `TOTAL`); if `config_budget` has no such row it falls back to **annual budget / 12**. The `Source` column shows which was used. Leaving `config_budget` empty is fine.
* **For explicit monthly budgets**, insert rows into `config_budget`:
  | Column | Description |
  | --- | --- |
  | `budget_month` | First day of the month (e.g. `2026-01-01`) |
  | `environment` | `NULL` = all environments (only NULL rows are shown today) |
  | `product` | `NULL` = all products (only NULL rows are shown today) |
  | `cost_scope` | `TOTAL` (shown), `DATABRICKS` or `CLOUD` (stored for future use) |
  | `budget_amount` | Budget for that month |
  | `currency` | e.g. `USD` |

  Example: `INSERT INTO config_budget VALUES (DATE'2026-11-01', NULL, NULL, 'TOTAL', 180000, 'USD', current_timestamp())`

## Cost by user / service principal

Databricks usage is attributed to the identity that ran each workload, using `identity_metadata.run_as` from `system.billing.usage`, falling back to `owned_by` then `created_by`.

Identities are classified as:
* **User** — identified by email address; display name resolved from the workspace SCIM directory
* **Service principal** — identified by application UUID; display name resolved from the workspace SCIM directory (e.g. `terraform_service_principal`)
* **Shared compute** — no identity assigned (e.g. classic all-purpose clusters with no single owner)
* **Unknown** — the identity exists in billing but was not found in this workspace (typically runs in another workspace covered by `workspace_filter`). Needs account-level SCIM access or a manual override to resolve.

This attribution is shown on the **Products & Workloads** dashboard page (current month + previous 2 months).

### Identity lookup table (`config_identity`)

The setup task (`run_setup`) automatically refreshes `config_identity` on every run by calling the workspace SCIM API for users and service principals. Each row stores:

| Column | Description |
| --- | --- |
| `identity_id` | Email (users) or application UUID (service principals) |
| `identity_type` | `User`, `Service principal`, or `Unknown` |
| `display_name` | Name from SCIM (e.g. `Jamie Butler`, `terraform_service_principal`) |
| `manual_name` | Optional override — set this for identities the API cannot resolve |
| `active` | Whether the identity is currently active in the workspace (NULL for Unknown) |
| `last_refreshed` | When setup last refreshed the row |

The dashboard shows `manual_name` first, then `display_name`, then the raw ID as a fallback. On a brand-new install, billing-only (Unknown) identities appear after the second setup run (the billing table does not exist before the first pipeline run).

**Resolving "Unknown" identities** (identities that appear in billing but not in this workspace, e.g. from other workspaces in `workspace_filter`):
1. **(Preferred)** Grant the run-as principal account-level read access to users and service principals, so the setup task can resolve identities across all workspaces.
2. **(Manual)** Run `UPDATE config_identity SET manual_name = '<friendly name>' WHERE identity_id = '<id>'` for each unknown identity.

## Monthly AWS upload runbook (no AWS API access needed)
1. AWS Console -> Cost Explorer -> Granularity **Daily**, Group by **Service**, filter **Linked account** = the environment's account.
2. Optional (recommended): filter **Tag `Vendor` = `Databricks`** for Databricks-only EC2 (requires the tag to be activated as a cost allocation tag) and register the folder with `allocation_basis = TAGGED_DATABRICKS`.
3. Download CSV.
4. Catalog Explorer -> `<catalog>.<schema>.cloud_cost_landing` -> `aws/<source_folder>/` -> Upload. File name does not matter.
5. The pipeline picks the file up on the next run; re-uploading the same month replaces it (no double counting).
6. A freshness alert fires if last month's file is missing by `freshness_due_day`.

## Troubleshooting
| Symptom | Cause / fix |
| --- | --- |
| Setup stops at **Preflight checks** with `Cannot read [...]` | System schemas not enabled or no SELECT grant - ask an account / metastore admin. |
| `Schema <catalog>.<schema> does not exist and could not be created` | You lack `CREATE SCHEMA` on the catalog. Ask an admin to run the `GRANT USE CATALOG, CREATE SCHEMA ON CATALOG ...` shown in the message, or to create the schema, then re-run. |
| `This workspace's default catalog is the legacy hive_metastore` | The workspace has no Unity Catalog default catalog. Set the `catalog` widget to one of the catalogs listed in the message. |
| Install stops: `Tables ... are owned by another pipeline` | That schema already holds accelerator tables from a different pipeline. Use another schema, or rename that pipeline to the expected name to adopt it. |
| Dashboard tiles empty right after install | First pipeline run not finished yet - check the pipeline update status. |
| Many `UNASSIGNED` environments | Workspace names don't contain prod/dev/test/qa/uat/cert - edit `config_workspace_env`. |
| `Unknown` identities | See *Resolving Unknown identities* above. |
| Cloud cost file not showing | Check `cloud_cost_quarantine` (reason column) and that the folder is registered in `config_cloud_source`. |
| `databricks` CLI says it only runs in the web terminal | Run bundle commands from the workspace web terminal or a laptop - or use Option A (`install`). |

## Uninstall
- Bundle: `databricks bundle destroy -t <target>` (removes pipeline, job, dashboard, alert).
- Install notebook: delete the pipeline `cost-management-accelerator`, job `cost-management-accelerator-refresh` and dashboard `Cost Management Accelerator` (prefixed if you used `name_prefix`).
- Tables, config tables, the landing volume and the schema (even if setup created it) stay until you drop them yourself.
