from __future__ import annotations

from mdl import contracts, governance


def test_column_ddl_follows_contract_with_escaped_comments() -> None:
    ddl = governance.column_ddl(contracts.load("fx_daily"))
    assert ddl.startswith("obs_date DATE COMMENT 'Reference date (ECB TIME_PERIOD)', currency STRING")
    assert ddl.count(" COMMENT ") == len(contracts.load("fx_daily").columns)
    assert governance.sql_string("it's") == "'it\\'s'"


def test_every_table_is_tagged_with_layer_source_license() -> None:
    tags = governance.table_tags("mdl_dev")
    gold = {t for t in tags if ".gold." in t}
    assert gold == {f"mdl_dev.gold.{n}" for n in contracts.load_all()}
    for table, t in tags.items():
        assert {"layer", "source", "license", "pii"} <= set(t), table
        assert t["layer"] == table.split(".")[1]
    assert tags["mdl_dev.gold.fx_daily"]["license"] == "ecb-statistics-reuse"
    assert tags["mdl_dev.gold.ohlcv_1m"]["license"] == "synthetic-mit"
    assert tags["mdl_dev.gold.fx_daily"]["contract"] == "gold.fx_daily@1.0.0"


def test_statements_are_tag_sql_only() -> None:
    stmts = governance.statements("mdl_prod")
    assert all(s.startswith("ALTER TABLE mdl_prod.") and "SET TAGS" in s for s in stmts)
    key_tags = [s for s in stmts if "ALTER COLUMN" in s]
    assert len(key_tags) == sum(len(c.primary_key) for c in contracts.load_all().values())
