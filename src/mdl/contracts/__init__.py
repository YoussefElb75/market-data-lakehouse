"""Data contracts for gold tables: load, and check a schema or a DataFrame.

A contract (``<table>.yml`` next to this file) fixes the published interface
of a gold table: columns, types, nullability, primary key, freshness SLA,
quality rules, owner, license and tags. The pipeline uses it for table
comments and tags, CI checks transforms against it, and a job task checks
the live tables.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from importlib import resources
from typing import Any

import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

CONTRACT_PACKAGE = "mdl.contracts"


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool
    description: str


@dataclass(frozen=True)
class QualityRule:
    name: str
    sql: str


@dataclass(frozen=True)
class Contract:
    id: str
    version: str
    owner: str
    description: str
    source: str
    license: str
    columns: tuple[Column, ...]
    primary_key: tuple[str, ...]
    freshness_dataset: str
    max_lag_hours: float
    quality: tuple[QualityRule, ...]
    tags: dict[str, str] = field(default_factory=dict)

    @property
    def table(self) -> str:
        return self.id.split(".", 1)[1]

    @property
    def layer(self) -> str:
        return self.id.split(".", 1)[0]

    def column(self, name: str) -> Column:
        return next(c for c in self.columns if c.name == name)


def _parse(doc: dict[str, Any]) -> Contract:
    if doc.get("kind") != "DataContract" or doc.get("apiVersion") != "mdl/v1":
        raise ValueError(f"not an mdl/v1 DataContract: {doc.get('id')}")
    columns = tuple(
        Column(c["name"], c["type"], bool(c["nullable"]), str(c.get("description", "")))
        for c in doc["schema"]
    )
    names = [c.name for c in columns]
    if len(set(names)) != len(names):
        raise ValueError(f"{doc['id']}: duplicate column names")
    pk = tuple(doc["primary_key"])
    missing_pk = [k for k in pk if k not in names]
    if missing_pk:
        raise ValueError(f"{doc['id']}: primary key columns not in schema: {missing_pk}")
    nullable_pk = [c.name for c in columns if c.name in pk and c.nullable]
    if nullable_pk:
        raise ValueError(f"{doc['id']}: primary key columns must be non-nullable: {nullable_pk}")
    return Contract(
        id=doc["id"],
        version=str(doc["version"]),
        owner=doc["owner"],
        description=" ".join(str(doc["description"]).split()),
        source=str(doc["source"]),
        license=str(doc["license"]),
        columns=columns,
        primary_key=pk,
        freshness_dataset=doc["freshness"]["dataset"],
        max_lag_hours=float(doc["freshness"]["max_lag_hours"]),
        quality=tuple(QualityRule(q["name"], str(q["sql"])) for q in doc.get("quality", [])),
        tags={str(k): str(v) for k, v in doc.get("tags", {}).items()},
    )


def load(table: str) -> Contract:
    text = resources.files(CONTRACT_PACKAGE).joinpath(f"{table}.yml").read_text(encoding="utf-8")
    return _parse(yaml.safe_load(text))


def load_all() -> dict[str, Contract]:
    out: dict[str, Contract] = {}
    for entry in sorted(resources.files(CONTRACT_PACKAGE).iterdir(), key=lambda e: e.name):
        if entry.name.endswith(".yml"):
            contract = _parse(yaml.safe_load(entry.read_text(encoding="utf-8")))
            out[contract.table] = contract
    return out


def schema_violations(contract: Contract, schema: StructType) -> list[str]:
    """Missing, unexpected or mistyped columns (nullability is checked on data)."""
    actual = {f.name: f.dataType.simpleString() for f in schema.fields}
    expected = {c.name: c.type for c in contract.columns}
    problems = [f"missing column {n}" for n in expected if n not in actual]
    problems += [f"unexpected column {n}" for n in actual if n not in expected]
    problems += [
        f"column {n}: expected {t}, got {actual[n]}"
        for n, t in expected.items()
        if n in actual and actual[n] != t
    ]
    if [n for n in actual if n in expected] != [n for n in expected if n in actual]:
        problems.append("column order differs from the contract")
    return problems


def data_violations(contract: Contract, df: DataFrame) -> dict[str, int]:
    """Counts of rows breaking non-null columns, the primary key or quality rules."""
    checks = [
        F.sum(F.when(F.col(c.name).isNull(), 1).otherwise(0)).alias(f"not_null:{c.name}")
        for c in contract.columns
        if not c.nullable
    ]
    checks += [
        F.sum(F.when(F.coalesce(F.expr(q.sql), F.lit(False)), 0).otherwise(1)).alias(f"rule:{q.name}")
        for q in contract.quality
    ]
    counts: dict[str, int] = {}
    if checks:
        row = df.agg(*checks).collect()[0].asDict()
        counts = {k: int(v or 0) for k, v in row.items()}
    dupes = df.groupBy(*contract.primary_key).count().where("count > 1").count()
    counts["primary_key:duplicates"] = int(dupes)
    return {k: v for k, v in counts.items() if v}


def table_comment(contract: Contract) -> str:
    return f"{contract.description} [contract {contract.id} v{contract.version}; owner {contract.owner}]"
