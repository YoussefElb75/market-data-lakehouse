from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from mdl import synth
from mdl.sources.landing import Landing

START = datetime(2030, 1, 2, 9, 0, tzinfo=UTC)


def _cfg(**kwargs: object) -> synth.SynthConfig:
    params: dict[str, object] = {"start": START, "seconds": 120, "seed": 11}
    params.update(kwargs)
    return synth.SynthConfig(**params)  # type: ignore[arg-type]


def test_deterministic_for_a_seed() -> None:
    a, b = synth.generate(_cfg()), synth.generate(_cfg())
    assert a.events == b.events
    assert a.truth == b.truth
    assert synth.generate(_cfg(seed=12)).events != a.events


def test_volumes_and_shapes() -> None:
    batch = synth.generate(_cfg(anomalies=synth.Anomalies(0, 0, 0, 0)))
    trades, books = batch.stream("trade"), batch.stream("book")
    n_inst = len(synth.DEFAULT_INSTRUMENTS)
    assert len(books) == n_inst * 120
    assert 0.7 * n_inst * 5 * 120 < len(trades) < 1.3 * n_inst * 5 * 120
    assert batch.truth == {}
    book = books[0]
    assert len(book["bids"]) == len(book["asks"]) == 10
    assert book["bids"][0][0] < book["asks"][0][0]
    assert all(t["price"] > 0 and t["qty"] >= 1 and t["side"] in ("buy", "sell") for t in trades)
    ids = [e["event_id"] for e in batch.events]
    assert len(ids) == len(set(ids))


def test_prices_stay_on_tick_grid() -> None:
    fx = next(i for i in synth.DEFAULT_INSTRUMENTS if i.symbol == "SYN-FX1")
    trades = [t for t in synth.generate(_cfg()).stream("trade") if t["symbol"] == "SYN-FX1"]
    trades = [t for t in trades if t["price"] is not None]
    assert all(abs(t["price"] / fx.tick - round(t["price"] / fx.tick)) < 1e-6 for t in trades)


def test_injected_anomalies_are_present_and_recorded() -> None:
    rates = synth.Anomalies(duplicate=0.05, out_of_order=0.05, late=0.05, malformed=0.05)
    batch = synth.generate(_cfg(anomalies=rates))
    kinds = Counter(k.split(":")[0] for ks in batch.truth.values() for k in ks)
    assert {"duplicate", "out_of_order", "late", "malformed"} <= set(kinds)

    id_counts = Counter(e["event_id"] for e in batch.events)
    assert {i for i, c in id_counts.items() if c > 1} == {
        i for i, ks in batch.truth.items() if "duplicate" in ks
    }

    late_ids = {i for i, ks in batch.truth.items() if "late" in ks}
    for e in batch.events:
        if e["event_id"] in late_ids and e["event_ts"] != "not-a-timestamp":
            emit = datetime.fromisoformat(e["emit_ts"].replace("Z", "+00:00"))
            event = datetime.fromisoformat(e["event_ts"].replace("Z", "+00:00"))
            assert emit - event >= timedelta(seconds=180)

    malformed = {i for i, ks in batch.truth.items() if any(k.startswith("malformed") for k in ks)}
    for e in batch.events:
        if e["event_id"] in malformed:
            assert (
                e["symbol"] == ""
                or e["event_ts"] == "not-a-timestamp"
                or e.get("price") is None
                or e.get("qty", 1) < 0
            )


def test_write_bursts_splits_streams(tmp_path: Path) -> None:
    batch = synth.generate(_cfg(seconds=30))
    paths = synth.write_bursts(batch, Landing(tmp_path), burst_size=200)
    trade_files = [p for p in paths if p.parts[-3] == "trades"]
    book_files = [p for p in paths if p.parts[-3] == "book_snapshots"]
    assert trade_files
    assert book_files
    lines = [json.loads(line) for p in trade_files for line in p.read_text(encoding="utf-8").splitlines()]
    assert len(lines) == len(batch.stream("trade"))
    assert all(len(p.read_text(encoding="utf-8").splitlines()) <= 200 for p in paths)


def test_config_validation() -> None:
    with pytest.raises(ValueError, match="timezone"):
        synth.SynthConfig(start=datetime(2030, 1, 1))
    with pytest.raises(ValueError, match="positive"):
        _cfg(seconds=0)
    with pytest.raises(ValueError, match="burst_size"):
        synth.write_bursts(synth.SynthBatch([], {}), Landing(Path()), burst_size=0)
