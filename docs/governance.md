# Unity Catalog governance

Everything here is declared in code: the bundle (`resources/uc.yml`,
`databricks.yml`), the data contracts (`src/mdl/contracts/*.yml`) and two small
bootstrap scripts for the parts Free Edition's APIs cannot manage declaratively.

## Layout

One catalog per environment (`mdl_dev`, `mdl_prod`) and one schema per layer:

| Schema | Content | Managed by |
|---|---|---|
| `landing` | Managed Volume `raw`: payloads, `_manifests/`, `_state/` | bundle |
| `bronze` | Auto Loader streaming tables, raw strings plus ingestion metadata | pipeline |
| `silver` | Typed and validated tables; AUTO CDC SCD1 facts and SCD2 reference data | pipeline |
| `gold` | Contract-bound marts | pipeline, plus `dq_expectations_daily` written by `post_checks` |
| `ops` | `quarantine` (pipeline) and `contract_results` (post_checks) | pipeline / job |
| `scratch` (dev only, transient) | Tables and files of the serverless test job | created at the start of each `mdl-tests` run, dropped at the end |

The catalogs themselves come from `scripts/uc_bootstrap.py`. On Free Edition
the catalogs REST API, and therefore the bundle `catalogs` resource, cannot
create Default Storage catalogs, while SQL can.

## Roles and grants

Free Edition has no account console and no SCIM sync. Workspace-local groups
can be created, but Unity Catalog rejects them as grantees
(`PRINCIPAL_DOES_NOT_EXIST`). Roles are therefore **service principals**,
created by `scripts/uc_principals.py`. Their application ids are never
written to the repository: the bundle resolves them by display name at
deploy time (`lookup: service_principal`).

| Role | Principal | Privileges |
|---|---|---|
| Pipeline / prod runtime | `mdl-pipeline` | `USE CATALOG`; `USE_SCHEMA, CREATE_TABLE, CREATE_MATERIALIZED_VIEW, MODIFY, SELECT` on bronze, silver, gold, ops; `USE_SCHEMA` on landing; `READ_VOLUME, WRITE_VOLUME` on `landing.raw`; `CAN_MANAGE` on the prod bundle |
| Analyst (read only) | `mdl-analyst` | `USE CATALOG`; `USE_SCHEMA, SELECT` on gold only; `CAN_VIEW` on the prod bundle |
| Deployer | the human admin running `bundle deploy` | owner of the schemas; needs `servicePrincipal.user` on `mdl-pipeline` for the prod `run_as` binding |

Schema and volume grants are bundle resources. Bundle grants are
authoritative, so a grant added by hand is removed on the next deploy.
`USE CATALOG` is granted by `scripts/uc_principals.py`, because the catalog
is not a bundle resource.

Check what is in force:

```sql
SELECT schema_name, grantee, privilege_type
FROM mdl_prod.information_schema.schema_privileges
ORDER BY 1, 2, 3;
```

## Tags

Applied after every pipeline update by the `governance` job task
(`mdl.governance`, idempotent `ALTER TABLE ... SET TAGS`):

| Tag | Values | On |
|---|---|---|
| `layer` | bronze, silver, gold, ops | every table |
| `source` | ecb, synthetic, manifests, event_log, mixed, metadata | every table |
| `license` | ecb-statistics-reuse, synthetic-mit, project-metadata | every table |
| `pii` | false | every table |
| `domain` | fx, rates, market_microstructure, data_quality | gold |
| `derived` | true | gold tables computed from ECB values (crosses, slopes) |
| `contract` | `gold.<table>@<version>` | gold |
| `role` (column tag) | primary_key | primary-key columns of gold tables |

```sql
SELECT table_schema, table_name, tag_name, tag_value
FROM mdl_prod.information_schema.table_tags ORDER BY 1, 2, 3;
```

## Comments and data contracts

* Table comments: bronze and silver in the pipeline code; gold from the
  contract (`description`, plus contract id, version and owner).
* Gold column comments: from the contract schema, passed as the pipeline
  `schema=` DDL (`mdl.governance.column_ddl`), so names, types and
  descriptions have a single source.
* Each gold table has a data contract (`src/mdl/contracts/<table>.yml`):
  columns, types, nullability, primary key, freshness SLA, quality rules,
  owner, license and tags. It is enforced three times:
  1. unit tests check every gold transform's output against its contract;
  2. the `post_checks` job task checks the live tables after every update and
     fails the run on a violation;
  3. results are kept in `ops.contract_results`, MERGEd on
     `(job_run_id, contract)`, so a task retry is idempotent.

## Lineage

Unity Catalog records lineage for pipeline and job writes. The README graph
is generated from `system.access.table_lineage` by `scripts/make_charts.py`.
Table-level lineage is also available through the REST API:

```bash
databricks api get "/api/2.0/lineage-tracking/table-lineage?table_name=mdl_prod.gold.fx_daily&include_entity_lineage=true" -p mdl
```

`system.access.table_lineage` filled within about 40 minutes of the first
runs. `system.billing.usage` had no rows on the day of the first runs.
