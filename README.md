# market-data-lakehouse

A production-style market data lakehouse on **Databricks Free Edition**:
medallion architecture on Delta Lake, Lakeflow Declarative Pipelines with
data-quality expectations, Lakeflow Jobs, Unity Catalog governance and
Declarative Automation Bundles (formerly Asset Bundles).

> Work in progress. The full README (architecture diagram, data-quality and
> freshness charts, cost notes, Free Edition limits) arrives with milestone 6.

- Data sources and their terms: [DATA.md](DATA.md). No data is stored in this repository.
- Workspace capability probe: [docs/step0-probe.md](docs/step0-probe.md).

## Local development

Requirements: Python 3.12, JDK 21 (Temurin) and the Databricks CLI.

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -e ".[spark,dev]"   # .venv/bin/python on Linux/macOS
git config core.hooksPath .githooks                      # enable the secret check
cp .env.example .env                                     # fill in local-only values
ruff check . && ruff format --check . && mypy && pytest
```

Spark's local file I/O on Windows needs Hadoop's `winutils`, which this
project does not install. Unit tests therefore build DataFrames in memory
with the same reader options as the pipeline. Tests marked `localfs` (real
file reads with `_metadata`) or `delta` (Delta Lake writes, MERGE, time
travel) are skipped on Windows and run in Linux CI.

The workspace host lives only in your local Databricks CLI profile
(`databricks auth login --host https://<workspace-host> --profile mdl`).
It is never written to this repository.

## Deploy (dev)

```bash
databricks auth login --host https://<workspace-host> --profile mdl   # once
python scripts/uc_bootstrap.py --profile mdl --env dev                # once: creates mdl_dev
databricks bundle deploy -t dev -p mdl
databricks bundle run daily -t dev -p mdl
```

## License

MIT, see [LICENSE](LICENSE). Data sources keep their own terms, see [DATA.md](DATA.md).
