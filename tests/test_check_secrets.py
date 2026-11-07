"""The secret check must catch real-looking identifiers and allow placeholders.

Sample values are assembled at runtime so this file does not trip the check.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("check_secrets", ROOT / "scripts" / "check_secrets.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_secrets"] = module
    spec.loader.exec_module(module)
    return module


cs = _load()
DOMAIN = "cloud." + "databricks.com"


def kinds(text: str, literals: tuple[str, ...] = ()) -> set[str]:
    return {f.kind for f in cs.scan_text(text, "x", literals)}


@pytest.mark.parametrize(
    ("text", "kind"),
    [
        ("host: https://dbc-" + "0123abcd-ef01." + DOMAIN, "databricks workspace host"),
        ("slug dbc-" + "0123abcd-ef01", "databricks workspace slug"),
        ("token dapi" + "0" * 32, "databricks personal access token"),
        ("bearer eyJ" + "a" * 12 + "." + "b" * 12 + ".sig", "jwt / oauth token"),
        ("mail someone" + "@" + "mail.com", "e-mail address"),
    ],
)
def test_detects(text: str, kind: str) -> None:
    assert kind in kinds(text)


@pytest.mark.parametrize(
    "text",
    [
        "host: https://<workspace-host>",
        "author 1+someone" + "@users.noreply.github.com",
        "ci-user" + "@example.com",
        "see https://docs.databricks.com/aws/en/getting-started",
    ],
)
def test_allows_placeholders(text: str) -> None:
    assert kinds(text) == set()


def test_forbidden_literals_case_insensitive(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MDL_FORBIDDEN", raising=False)
    env = tmp_path / ".env"
    env.write_text("MDL_FORBIDDEN=Secret-Value,abc,1234567\n", encoding="utf-8")
    literals = cs.forbidden_literals(env)
    assert literals == ["Secret-Value", "1234567"]  # too-short values are ignored
    assert "forbidden literal from MDL_FORBIDDEN" in kinds("x secret-value y", tuple(literals))


def test_tracked_tree_is_clean(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(ROOT)
    assert cs.main(["--all"]) == 0
