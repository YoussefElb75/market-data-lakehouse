"""Unity Catalog governance as code: table tags, key-column tags and column DDL.

* Gold column comments come from the data contracts and are declared through
  the pipeline ``schema=`` DDL (``column_ddl``), so the contract is the single
  source for names, types and descriptions.
* Table tags (layer, source, license, pii, domain) and ``role=primary_key``
  column tags are applied after each pipeline update by the ``governance`` job
  task. ``SET TAGS`` overwrites, so the task is idempotent.
"""

from __future__ import annotations

from mdl import contracts

ECB_LICENSE = "ecb-statistics-reuse"
SYNTH_LICENSE = "synthetic-mit"

# Non-gold tables: (layer, table) -> source. Gold tables take their tags from the contracts.
LAYER_TABLES: dict[tuple[str, str], str] = {
    ("bronze", "ecb_fx_reference_rates"): "ecb",
    ("bronze", "ecb_yield_curve_aaa"): "ecb",
    ("bronze", "ecb_yield_curve_all"): "ecb",
    ("bronze", "ecb_estr"): "ecb",
    ("bronze", "ecb_currency_codelist"): "ecb",
    ("bronze", "ingest_manifests"): "manifests",
    ("bronze", "synth_trades"): "synthetic",
    ("bronze", "synth_book_snapshots"): "synthetic",
    ("silver", "fx_rates"): "ecb",
    ("silver", "yield_curve"): "ecb",
    ("silver", "estr"): "ecb",
    ("silver", "currencies"): "ecb",
    ("silver", "series_attributes"): "ecb",
    ("silver", "trades"): "synthetic",
    ("silver", "book_top"): "synthetic",
    ("ops", "quarantine"): "mixed",
    ("ops", "contract_results"): "metadata",
}

_TYPES = {"date": "DATE", "string": "STRING", "double": "DOUBLE", "bigint": "BIGINT",
          "timestamp": "TIMESTAMP", "boolean": "BOOLEAN", "int": "INT"}  # fmt: skip


def sql_string(text: str) -> str:
    """A single-quoted SQL string literal."""
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def column_ddl(contract: contracts.Contract) -> str:
    """``name TYPE COMMENT '...'`` list for a pipeline dataset ``schema=`` argument."""
    parts = []
    for col in contract.columns:
        sql_type = _TYPES.get(col.type)
        if sql_type is None:
            raise ValueError(f"{contract.id}: no DDL type for {col.type}")
        parts.append(f"{col.name} {sql_type} COMMENT {sql_string(col.description)}")
    return ", ".join(parts)


def _license(source: str) -> str:
    return {"ecb": ECB_LICENSE, "synthetic": SYNTH_LICENSE}.get(source, "project-metadata")


def _tags_sql(table: str, tags: dict[str, str]) -> str:
    body = ", ".join(f"{sql_string(k)} = {sql_string(v)}" for k, v in sorted(tags.items()))
    return f"ALTER TABLE {table} SET TAGS ({body})"


def table_tags(catalog: str) -> dict[str, dict[str, str]]:
    """Fully qualified table name -> tags, for every managed table of the lakehouse."""
    out: dict[str, dict[str, str]] = {}
    for (layer, name), source in LAYER_TABLES.items():
        out[f"{catalog}.{layer}.{name}"] = {
            "layer": layer,
            "source": source,
            "license": _license(source),
            "pii": "false",
        }
    for name, contract in contracts.load_all().items():
        tags = dict(contract.tags)
        tags.setdefault("license", _license(tags.get("source", "")))
        tags["contract"] = f"{contract.id}@{contract.version}"
        out[f"{catalog}.gold.{name}"] = tags
    return out


def statements(catalog: str) -> list[str]:
    """Idempotent SQL that applies every table tag and primary-key column tag."""
    stmts = [_tags_sql(table, tags) for table, tags in sorted(table_tags(catalog).items())]
    for name, contract in sorted(contracts.load_all().items()):
        for key in contract.primary_key:
            stmts.append(
                f"ALTER TABLE {catalog}.gold.{name} ALTER COLUMN {key} SET TAGS ('role' = 'primary_key')"
            )
    return stmts
