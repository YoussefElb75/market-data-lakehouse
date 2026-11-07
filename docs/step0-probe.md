# Step 0: Free Edition workspace probe (2026-11-07)

Before any design work, the probe in `probe/` checked what a Databricks Free
Edition workspace actually allows. Everything the probe created was destroyed
afterwards: the bundle, the `mdl_probe` catalog and the service principal.

| Capability | Result | Design consequence |
|---|---|---|
| `CREATE CATALOG` (default managed storage) | Works | One catalog per environment: `mdl_dev`, `mdl_prod` |
| Schemas, managed tables, UC Volumes | Work | Landing zone is a UC Volume per environment |
| Table and column comments, table and column tags | Work; readable from `information_schema.table_tags` | Tags `layer`, `source`, `license`, `pii` on every table |
| Workspace-local groups (SCIM) | Can be created, but **not usable as UC principals** (`PRINCIPAL_DOES_NOT_EXIST`) | Roles are service principals, plus the built-in `account users` group |
| Service principals | Can be created and granted (`USE CATALOG`, `USE SCHEMA`, `SELECT`) | `mdl-pipeline` (writer) and `mdl-analyst` (reader) service principals; prod jobs `run_as` the pipeline service principal |
| Serverless egress | `data-api.ecb.europa.eu`, `www.ecb.europa.eu`, `pypi.org`, `github.com` and `example.com` all returned HTTP 200 | ECB ingestion runs inside the workspace; the local upload path is kept only as a fallback |
| Serverless job environment `6` | Python 3.12.3, Spark 4.2.0 | Matches the local `.venv` (Python 3.12, pyspark 4.2.0) |
| Serverless job environment `4` | Python 3.12.3, pyspark 4.0.0 | Not used |
| Lakeflow pipeline, channel `CURRENT` | Databricks Runtime 18.3 (Spark 4.1) | Pipeline code must run on Spark 4.1 and 4.2; CI tests both |
| Auto Loader (`cloudFiles`) on a Volume, `_metadata.file_path` | Works | Bronze is all streaming tables fed by Auto Loader |
| Expectations (`expect`, `expect_or_drop`) | Work; per-rule pass/fail counts appear in `event_log()` | DQ KPIs in gold are computed from the event log |
| Quarantine table (inverse predicate) | Works | `ops.quarantine_*` tables |
| `create_auto_cdc_flow(..., stored_as_scd_type=2)` | Works; `__START_AT` and `__END_AT` are set correctly on an update | SCD2 for reference data |
| Watermarked streaming aggregate | Works; a window is emitted only after the watermark passes it | 1-minute OHLCV bars for the synthetic ticks |
| Two triggered pipelines updating at the same time | Both completed, with no quota error | Dev and prod pipelines can coexist; runs are still kept sequential to save quota |
| Job with a file-arrival trigger on a Volume (paused) | Deploys | Optional trigger for the tick landing path |
| Lineage REST API (`lineage-tracking/table-lineage`) | Returns table and pipeline lineage within minutes | Lineage is documented from the API and Catalog Explorer |
| `system.access.table_lineage`, `system.billing.usage`, `system.lakeflow.jobs` | Readable, still empty after about 20 minutes (ingestion lag) | Rechecked in milestone 5; cost notes use them if they fill |
| `bundle deploy`, `run`, `destroy` with an OAuth CLI profile | Work | Host stays in the local profile; `databricks.yml` holds no host |
| `bundle validate` with no workspace | Fails: it calls `/api/2.0/preview/scim/v2/Me` | CI runs `validate --strict` against `scripts/stub_workspace.py`, a local stub, with no secret |
| `bundle validate --strict` | Fails on type warnings; unresolved `${resources...}` references are **not** reported | CI adds a pytest check on the rendered `validate -o json` |

## Found during milestone 4 (first dev deploy)

| Finding | Consequence |
|---|---|
| The bundle `catalogs` resource (catalogs REST API) fails: "Metastore storage root URL does not exist. Default Storage is enabled", while SQL `CREATE CATALOG` works | `scripts/uc_bootstrap.py` creates the catalog once per environment with SQL; schemas and the Volume stay bundle resources (`prevent_destroy` in prod) |
| `mode: development` prefixes every UC schema with `dev_<user>_`, and an empty `presets.name_prefix` does not turn it off | The dev target sets the presets it needs (pipeline development mode, paused triggers, one concurrent run) without `mode: development`; environments are isolated by catalog |
| `--editable ${workspace.file_path}` as a serverless dependency installs without error, but `import mdl` then fails | A regular install of the synced project folder (`${workspace.file_path}`) |
| `system.access.table_lineage` filled within about 40 minutes (68 rows for `mdl_dev`); `system.billing.usage` still had no rows for the day | Lineage from system tables is usable; cost notes cannot rely on same-day billing rows |
