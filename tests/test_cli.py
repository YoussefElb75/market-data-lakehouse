from __future__ import annotations

import json
from pathlib import Path

import pytest

from mdl import cli
from mdl.sources import ecb
from tests.fakes import FAKE_EXR_CSV, FakeResponse, FakeSession


def test_datasets_lists_all(capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(["datasets"]) == 0
    out = capsys.readouterr().out
    assert all(name in out for name in ecb.DATASETS)


def test_synth_writes_files(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code = cli.main(
        [
            "synth",
            "--landing",
            str(tmp_path),
            "--seconds",
            "10",
            "--bursts",
            "2",
            "--start",
            "2030-01-02T09:00Z",
        ]
    )
    assert code == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["bursts"] == 2
    assert summary["events"] > 0
    assert list((tmp_path / "synth" / "trades").rglob("*.json"))


def test_ingest_unknown_dataset(tmp_path: Path) -> None:
    assert cli.main(["ingest-ecb", "--landing", str(tmp_path), "--dataset", "nope"]) == 2


def test_ingest_one_dataset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    session = FakeSession([FakeResponse(200, FAKE_EXR_CSV)])
    real_ingest = ecb.ingest

    def fake_ingest(*args: object, **kwargs: object) -> object:
        return real_ingest(*args, session=session, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(ecb, "ingest", fake_ingest)
    assert (
        cli.main(
            [
                "ingest-ecb",
                "--landing",
                str(tmp_path),
                "--dataset",
                "fx_reference_rates",
                "--max-windows",
                "1",
            ]
        )
        == 0
    )
    line = json.loads(capsys.readouterr().out)
    assert line["row_count"] == 3
    assert line["dataset"] == "fx_reference_rates"
    assert line["mode"] == "backfill"
