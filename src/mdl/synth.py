"""Synthetic trades and order-book snapshots for made-up instruments.

Mid prices follow geometric Brownian motion on a one-second grid. Trades arrive
as a Poisson process and print at the touch, and 10-level book snapshots are
taken at a fixed interval. Everything is seeded and reproducible.

To exercise data quality, the generator injects, at configurable rates:

* duplicates: the same event delivered twice;
* out-of-order: two neighbouring events swapped in delivery order;
* late: delivered minutes after its event time;
* malformed: null price, negative quantity, blank symbol or unparsable time.

The ground truth (which event got which anomaly) is returned beside the events
and never written into the payload, so tests can check that the pipeline
catches what was injected.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np

from mdl.sources.landing import Landing

SOURCE = "synth"
SECONDS_PER_YEAR = 252 * 8 * 3600  # trading-time convention for the GBM step


@dataclass(frozen=True)
class Instrument:
    symbol: str
    s0: float
    vol_annual: float
    tick: float
    spread_ticks: int = 2

    def round_tick(self, price: float) -> float:
        decimals = max(0, -math.floor(math.log10(self.tick)) + 1)
        return round(round(price / self.tick) * self.tick, decimals)


DEFAULT_INSTRUMENTS = (
    Instrument("SYN-EQ1", 100.0, 0.25, 0.01),
    Instrument("SYN-EQ2", 42.0, 0.40, 0.01),
    Instrument("SYN-FX1", 1.1, 0.08, 0.0001),
    Instrument("SYN-RT1", 98.5, 0.05, 0.005),
)


@dataclass(frozen=True)
class Anomalies:
    duplicate: float = 0.01
    out_of_order: float = 0.02
    late: float = 0.005
    malformed: float = 0.003
    late_delay_s: tuple[int, int] = (180, 1800)


@dataclass(frozen=True)
class SynthConfig:
    start: datetime
    seconds: int = 60
    seed: int = 7
    trade_rate_hz: float = 5.0
    book_interval_s: float = 1.0
    depth: int = 10
    instruments: tuple[Instrument, ...] = DEFAULT_INSTRUMENTS
    anomalies: Anomalies = field(default_factory=Anomalies)

    def __post_init__(self) -> None:
        if self.start.tzinfo is None:
            raise ValueError("start must be timezone-aware")
        if self.seconds <= 0 or self.trade_rate_hz <= 0 or self.book_interval_s <= 0 or self.depth <= 0:
            raise ValueError("seconds, rates, interval and depth must be positive")


@dataclass
class SynthBatch:
    events: list[dict[str, Any]]
    truth: dict[str, list[str]]  # event_id -> injected anomalies

    def stream(self, event_type: str) -> list[dict[str, Any]]:
        return [e for e in self.events if e.get("event_type") == event_type]


def _iso(at: datetime) -> str:
    return at.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _mid_path(inst: Instrument, seconds: int, rng: np.random.Generator) -> np.ndarray:
    dt = 1.0 / SECONDS_PER_YEAR
    shocks = rng.standard_normal(seconds)
    log_steps = -0.5 * inst.vol_annual**2 * dt + inst.vol_annual * math.sqrt(dt) * shocks
    path: np.ndarray = inst.s0 * np.exp(np.concatenate([[0.0], np.cumsum(log_steps)]))
    return path


def _clean_events(cfg: SynthConfig, rng: np.random.Generator) -> list[tuple[datetime, dict[str, Any]]]:
    out: list[tuple[datetime, dict[str, Any]]] = []
    for inst in cfg.instruments:
        mids = _mid_path(inst, cfg.seconds, rng)
        half_spread = inst.spread_ticks * inst.tick / 2
        n_trades = int(rng.poisson(cfg.trade_rate_hz * cfg.seconds))
        offsets = np.sort(rng.uniform(0.0, cfg.seconds, n_trades))
        sides = rng.integers(0, 2, n_trades)
        sizes = np.maximum(1, np.round(rng.lognormal(2.0, 0.8, n_trades)))
        n_books = int(cfg.seconds / cfg.book_interval_s)
        book_offsets = np.arange(n_books) * cfg.book_interval_s
        timeline = sorted(
            [(float(t), "trade", i) for i, t in enumerate(offsets)]
            + [(float(t), "book", i) for i, t in enumerate(book_offsets)]
        )
        for seq, (offset, kind, idx) in enumerate(timeline, start=1):
            at = cfg.start + timedelta(seconds=offset)
            mid = float(mids[min(int(offset), cfg.seconds)])
            if kind == "trade":
                side = "buy" if sides[idx] == 1 else "sell"
                px = inst.round_tick(mid + half_spread if side == "buy" else mid - half_spread)
                event: dict[str, Any] = {
                    "event_type": "trade",
                    "event_id": f"{inst.symbol}-T{seq:09d}",
                    "symbol": inst.symbol,
                    "seq": seq,
                    "event_ts": _iso(at),
                    "price": px,
                    "qty": float(sizes[idx]),
                    "side": side,
                    "venue": "SYNTH",
                }
            else:
                levels = np.arange(cfg.depth)
                qty = np.round(rng.lognormal(3.0, 0.6, (2, cfg.depth)))
                event = {
                    "event_type": "book",
                    "event_id": f"{inst.symbol}-B{seq:09d}",
                    "symbol": inst.symbol,
                    "seq": seq,
                    "event_ts": _iso(at),
                    "bids": [
                        [inst.round_tick(mid - half_spread - lv * inst.tick), float(q)]
                        for lv, q in zip(levels, qty[0], strict=True)
                    ],
                    "asks": [
                        [inst.round_tick(mid + half_spread + lv * inst.tick), float(q)]
                        for lv, q in zip(levels, qty[1], strict=True)
                    ],
                    "venue": "SYNTH",
                }
            out.append((at, event))
    return out


def _malform(event: dict[str, Any], rng: np.random.Generator) -> str:
    if event["event_type"] == "trade":
        choice = ("null_price", "negative_qty", "blank_symbol", "bad_timestamp")[int(rng.integers(0, 4))]
    else:
        choice = ("blank_symbol", "bad_timestamp")[int(rng.integers(0, 2))]
    if choice == "null_price":
        event["price"] = None
    elif choice == "negative_qty":
        event["qty"] = -abs(event["qty"])
    elif choice == "blank_symbol":
        event["symbol"] = ""
    else:
        event["event_ts"] = "not-a-timestamp"
    return f"malformed:{choice}"


def generate(cfg: SynthConfig) -> SynthBatch:
    """Generate one batch of events in delivery order, with injected anomalies."""
    rng = np.random.default_rng(cfg.seed)
    an = cfg.anomalies
    truth: dict[str, list[str]] = {}
    delivered: list[tuple[datetime, int, dict[str, Any]]] = []
    for order, (at, event) in enumerate(_clean_events(cfg, rng)):
        latency = timedelta(milliseconds=float(rng.exponential(40.0)))
        emit_at = at + latency
        if rng.random() < an.late:
            emit_at += timedelta(seconds=int(rng.integers(an.late_delay_s[0], an.late_delay_s[1])))
            truth.setdefault(event["event_id"], []).append("late")
        if rng.random() < an.malformed:
            truth.setdefault(event["event_id"], []).append(_malform(event, rng))
        event["emit_ts"] = _iso(emit_at)
        delivered.append((emit_at, order, event))
    delivered.sort(key=lambda item: (item[0], item[1]))
    events = [e for _, _, e in delivered]

    i = 0
    while i < len(events) - 1:
        if rng.random() < an.out_of_order and events[i]["symbol"] == events[i + 1]["symbol"]:
            events[i], events[i + 1] = events[i + 1], events[i]
            truth.setdefault(events[i]["event_id"], []).append("out_of_order")
            i += 2
            continue
        i += 1
    final: list[dict[str, Any]] = []
    for event in events:
        final.append(event)
        if rng.random() < an.duplicate:
            final.append(dict(event))
            truth.setdefault(event["event_id"], []).append("duplicate")
    return SynthBatch(final, truth)


def _jsonl(events: Iterable[dict[str, Any]]) -> bytes:
    return ("\n".join(json.dumps(e, separators=(",", ":")) for e in events) + "\n").encode("utf-8")


def write_bursts(batch: SynthBatch, landing: Landing, *, burst_size: int = 5000) -> list[Path]:
    """Write trades and book snapshots as JSON-lines files of at most ``burst_size`` events."""
    if burst_size <= 0:
        raise ValueError("burst_size must be positive")
    written: list[Path] = []
    for stream, dataset in (("trade", "trades"), ("book", "book_snapshots")):
        rows = batch.stream(stream)
        for start in range(0, len(rows), burst_size):
            chunk = rows[start : start + burst_size]
            stamp = datetime.fromisoformat(chunk[-1]["emit_ts"].replace("Z", "+00:00"))
            path, _ = landing.write_payload(SOURCE, dataset, _jsonl(chunk), "json", stamp)
            written.append(path)
    return written
