from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from mdl import quality
from mdl.quality import Action, Rule

pytestmark = pytest.mark.spark

RULES = (
    Rule("positive", "x > 0", Action.DROP),
    Rule("has_name", "name IS NOT NULL", Action.DROP),
    Rule("small", "x < 100", Action.WARN),
    Rule("structural", "id IS NOT NULL", Action.FAIL),
)


def test_expectation_dicts_by_action() -> None:
    assert quality.expectations(RULES, Action.DROP) == {"positive": "x > 0", "has_name": "name IS NOT NULL"}
    assert quality.expectations(RULES, Action.WARN) == {"small": "x < 100"}
    assert quality.expectations(RULES, Action.FAIL) == {"structural": "id IS NOT NULL"}


def test_clean_and_quarantine_partition_rows_null_safely(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [
            (1, 5, "a", "f1"),
            (2, -1, "b", "f1"),
            (3, None, "c", "f2"),
            (4, 7, None, "f2"),
            (5, 500, "e", "f3"),
        ],
        "id INT, x INT, name STRING, _source_file STRING",
    )
    clean = quality.clean(df, RULES)
    quarantined = quality.quarantine(df, RULES, "silver.t")
    # Every row lands in exactly one of the two: no silent loss when a constraint is NULL.
    assert sorted(r.id for r in clean.collect()) == [1, 5]  # warn rules do not drop
    rows = {r.record: r for r in quarantined.collect()}
    assert len(rows) == 3
    assert clean.count() + quarantined.count() == df.count()
    by_id = {int(r.record.split('"id":')[1].split(",")[0]): r.failed_rules for r in rows.values()}
    assert by_id == {2: ["positive"], 3: ["positive"], 4: ["has_name"]}
    first = next(iter(rows.values()))
    assert first.source_table == "silver.t"
    assert first._source_file in {"f1", "f2"}
    assert first.quarantined_at is not None


def test_failed_rules_lists_every_failure(spark: SparkSession) -> None:
    df = spark.createDataFrame([(-1, None)], "x INT, name STRING")
    out = df.select(quality.failed_rules(RULES[:3]).alias("f")).collect()[0].f
    assert out == ["positive", "has_name"]


@pytest.mark.parametrize(
    "rules",
    [quality.FX_RULES, quality.YIELD_RULES, quality.ESTR_RULES, quality.TRADE_RULES, quality.BOOK_RULES],
)
def test_rule_sets_have_unique_names(rules: tuple[Rule, ...]) -> None:
    names = [r.name for r in rules]
    assert len(names) == len(set(names))
