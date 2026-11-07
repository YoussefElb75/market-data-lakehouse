"""The local stub that lets CI run `databricks bundle validate` with no workspace."""

from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.request
from collections.abc import Iterator
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PORT = 8791


@pytest.fixture(scope="module")
def stub() -> Iterator[str]:
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "scripts" / "stub_workspace.py"), str(PORT)], stderr=subprocess.DEVNULL
    )
    base = f"http://127.0.0.1:{PORT}"
    for _ in range(50):
        try:
            urllib.request.urlopen(f"{base}/api/2.0/preview/scim/v2/Me", timeout=1)
            break
        except OSError:
            time.sleep(0.1)
    yield base
    proc.terminate()
    proc.wait(timeout=5)


def _get(url: str) -> dict[str, object]:
    with urllib.request.urlopen(url, timeout=5) as resp:
        body: dict[str, object] = json.loads(resp.read())
        return body


def test_answers_current_user_and_principal_lookup(stub: str) -> None:
    assert _get(f"{stub}/api/2.0/preview/scim/v2/Me")["userName"] == "ci-user@example.com"
    found = _get(f"{stub}/api/2.0/preview/scim/v2/ServicePrincipals?filter=displayName+eq+%27mdl-pipeline%27")
    resources = found["Resources"]
    assert isinstance(resources, list)
    assert resources[0]["displayName"] == "mdl-pipeline"
    assert resources[0]["applicationId"] == "00000000-0000-0000-0000-000000000000"


def test_unknown_endpoint_is_a_clear_404(stub: str) -> None:
    with pytest.raises(urllib.error.HTTPError) as info:
        urllib.request.urlopen(f"{stub}/api/2.0/unknown", timeout=5)
    assert info.value.code == 404
