"""Data-quality rules as data, shared by Lakeflow expectations and local tests.

Each rule is a SQL boolean expression with an action:

* ``warn``: keep the row, count the failure (``@dp.expect_all``);
* ``drop``: drop the row from the clean table and route it to quarantine
  (``@dp.expect_all_or_drop``);
* ``fail``: stop the update (``@dp.expect_all_or_fail``), for structural errors.

A filter drops rows whose constraint is NULL, so the quarantine predicate
wraps every rule in ``coalesce(rule, false)``. Each rule checks one concern and
lets NULLs through unless NULL is the concern, so a bad value is counted
against exactly one rule. Without that, a NULL result
would be dropped from the clean table *and* missed by quarantine.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from enum import StrEnum

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


class Action(StrEnum):
    WARN = "warn"
    DROP = "drop"
    FAIL = "fail"


@dataclass(frozen=True)
class Rule:
    name: str
    constraint: str
    action: Action
    description: str = ""


def expectations(rules: Iterable[Rule], action: Action) -> dict[str, str]:
    """``{name: constraint}`` for one action, as ``dp.expect_all*`` expects."""
    return {r.name: r.constraint for r in rules if r.action is action}


def _passes(rule: Rule) -> Column:
    return F.coalesce(F.expr(rule.constraint), F.lit(False))


def failed_rules(rules: Sequence[Rule]) -> Column:
    """Array of the names of the rules a row fails (null-safe)."""
    flags = [F.when(~_passes(r), F.lit(r.name)) for r in rules]
    if not flags:
        return F.array().cast("array<string>")
    return F.filter(F.array(*flags), lambda x: x.isNotNull())


def drop_condition(rules: Sequence[Rule]) -> Column:
    """True when a row passes every ``drop`` rule (null-safe)."""
    cond = F.lit(True)
    for rule in rules:
        if rule.action is Action.DROP:
            cond = cond & _passes(rule)
    return cond


def clean(df: DataFrame, rules: Sequence[Rule]) -> DataFrame:
    """Local equivalent of ``expect_all_or_drop``: keep rows passing all drop rules."""
    return df.where(drop_condition(rules))


def quarantine(df: DataFrame, rules: Sequence[Rule], table: str) -> DataFrame:
    """Rows failing at least one drop rule, with the failed rule names and context."""
    drop_rules = [r for r in rules if r.action is Action.DROP]
    payload_cols = [c for c in df.columns if not c.startswith("_")]
    return (
        df.where(~drop_condition(drop_rules))
        .select(
            F.lit(table).alias("source_table"),
            failed_rules(drop_rules).alias("failed_rules"),
            F.to_json(F.struct(*[F.col(f"`{c}`") for c in payload_cols])).alias("record"),
            *[F.col(c) for c in df.columns if c.startswith("_")],
        )
        .withColumn("quarantined_at", F.current_timestamp())
    )


# --- rule sets per silver table ----------------------------------------------------------------

ECB_OBS_RULES: tuple[Rule, ...] = (
    Rule("series_key_present", "series_key IS NOT NULL AND series_key <> ''", Action.FAIL),
    Rule("valid_date", "obs_date IS NOT NULL", Action.DROP, "TIME_PERIOD parses as a date"),
    Rule("numeric_value", "obs_value IS NOT NULL", Action.DROP, "OBS_VALUE parses as a number"),
    Rule(
        "date_not_in_future", "obs_date IS NULL OR obs_date <= current_date() + INTERVAL 1 DAY", Action.DROP
    ),
    Rule("known_status", "obs_status IN ('A', 'P', 'E', 'F', 'M', 'L', 'B')", Action.WARN),
)

FX_RULES: tuple[Rule, ...] = (
    *ECB_OBS_RULES,
    Rule("positive_rate", "obs_value IS NULL OR obs_value > 0", Action.DROP),
    Rule("iso_currency", "currency RLIKE '^[A-Z]{3}$'", Action.DROP),
)

YIELD_RULES: tuple[Rule, ...] = (
    *ECB_OBS_RULES,
    Rule("plausible_yield", "obs_value IS NULL OR obs_value BETWEEN -5 AND 25", Action.DROP, "percent"),
    Rule("known_tenor", "tenor_years IS NOT NULL", Action.DROP),
)

ESTR_RULES: tuple[Rule, ...] = (
    *ECB_OBS_RULES,
    Rule("known_measure", "measure IN ('rate_pct', 'volume')", Action.DROP),
    Rule(
        "plausible_rate",
        "measure <> 'rate_pct' OR obs_value IS NULL OR obs_value BETWEEN -2 AND 15",
        Action.DROP,
    ),
    Rule("positive_volume", "measure <> 'volume' OR obs_value IS NULL OR obs_value > 0", Action.DROP),
)

CURRENCY_RULES: tuple[Rule, ...] = (
    Rule("code_present", "code IS NOT NULL AND code <> ''", Action.DROP),
    Rule("name_present", "name IS NOT NULL AND name <> ''", Action.WARN),
)

TRADE_RULES: tuple[Rule, ...] = (
    Rule("event_id_present", "event_id IS NOT NULL AND event_id <> ''", Action.DROP),
    Rule("symbol_present", "symbol IS NOT NULL AND symbol <> ''", Action.DROP),
    Rule("valid_event_ts", "event_ts IS NOT NULL", Action.DROP),
    Rule("positive_price", "price IS NOT NULL AND price > 0", Action.DROP),
    Rule("positive_qty", "qty IS NOT NULL AND qty > 0", Action.DROP),
    Rule("known_side", "side IN ('buy', 'sell')", Action.DROP),
    Rule("not_late", "emit_ts IS NULL OR emit_ts <= event_ts + INTERVAL 2 MINUTES", Action.WARN),
)

BOOK_RULES: tuple[Rule, ...] = (
    Rule("symbol_present", "symbol IS NOT NULL AND symbol <> ''", Action.DROP),
    Rule("valid_event_ts", "event_ts IS NOT NULL", Action.DROP),
    Rule("two_sided", "best_bid IS NOT NULL AND best_ask IS NOT NULL", Action.DROP),
    Rule("not_crossed", "best_bid < best_ask", Action.DROP),
)
