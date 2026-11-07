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

Delta Lake write tests are marked `delta`. They run in Linux CI and are
skipped on Windows, which would need Hadoop's `winutils`.

The workspace host lives only in your local Databricks CLI profile
(`databricks auth login --host https://<workspace-host> --profile mdl`).
It is never written to this repository.

## License

MIT, see [LICENSE](LICENSE). Data sources keep their own terms, see [DATA.md](DATA.md).
