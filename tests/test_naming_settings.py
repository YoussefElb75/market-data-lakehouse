from __future__ import annotations

from pathlib import Path

import pytest

from mdl.naming import Layer, Names
from mdl.settings import get_setting, read_env_file


def test_names_per_environment() -> None:
    dev = Names("dev")
    assert dev.catalog == "mdl_dev"
    assert dev.schema(Layer.GOLD) == "mdl_dev.gold"
    assert dev.table(Layer.SILVER, "fx_rates") == "mdl_dev.silver.fx_rates"
    assert Names("prod").volume_path("raw", "ecb", "EXR") == "/Volumes/mdl_prod/landing/raw/ecb/EXR"


@pytest.mark.parametrize("bad", ["FX", "fx-rates", "1fx", "fx;drop", ""])
def test_rejects_bad_identifiers(bad: str) -> None:
    with pytest.raises(ValueError, match="identifier"):
        Names("dev").table(Layer.GOLD, bad)


def test_rejects_unknown_env_and_path_traversal() -> None:
    with pytest.raises(ValueError, match="environment"):
        Names("staging")
    with pytest.raises(ValueError, match="segment"):
        Names("dev").volume_path("raw", "..")


def test_env_file_and_precedence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env = tmp_path / ".env"
    env.write_text("# comment\nA=1\nB = 'two'\nmalformed\nEMPTY=\n", encoding="utf-8")
    assert read_env_file(env) == {"A": "1", "B": "two", "EMPTY": ""}
    monkeypatch.delenv("A", raising=False)
    assert get_setting("A", env) == "1"
    monkeypatch.setenv("A", "from-env")
    assert get_setting("A", env) == "from-env"
    assert get_setting("EMPTY", env) is None
    assert read_env_file(tmp_path / "missing.env") == {}
