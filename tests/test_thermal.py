from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest

from mdl.thermal import Budget, ThermalAbortError, ThermalGuard, read_latest

HEADER = 'Date,Time,"Core 0 VID [V]","CPU (Tctl/Tdie) [°C]","Core0 [°C]"'


def _row(at: datetime, temp: float) -> str:
    return f"{at.day}.{at.month}.{at.year},{at:%H:%M:%S}.{at.microsecond // 1000:03d},0.92,{temp},{temp - 5}"


def _write(path: Path, temps: list[float], at: datetime | None = None, partial_tail: bool = False) -> None:
    at = at or datetime.now()
    lines = [HEADER] + [_row(at - timedelta(seconds=len(temps) - i), t) for i, t in enumerate(temps)]
    text = "﻿" + "\n".join(lines) + "\n"
    if partial_tail:
        text += f"{at.day}.{at.month}.{at.year},06:00"
    path.write_text(text, encoding="utf-8")


def test_read_latest_parses_hwinfo_locale_and_bom(tmp_path: Path) -> None:
    csv_path = tmp_path / "hwinfo.csv"
    _write(csv_path, [55.0, 61.5], partial_tail=True)
    reading = read_latest(csv_path)
    assert reading.celsius == 61.5
    assert reading.column == "CPU (Tctl/Tdie) [°C]"
    assert reading.age_s() < 30


def test_disabled_guard_is_noop() -> None:
    guard = ThermalGuard(None)
    assert not guard.enabled
    assert guard.check() is None


def test_stale_log_is_ignored(tmp_path: Path) -> None:
    _write(tmp_path / "hwinfo.csv", [99.0], at=datetime.now() - timedelta(hours=1))
    assert ThermalGuard(tmp_path).reading() is None


def test_hot_cpu_pauses_until_cool(tmp_path: Path) -> None:
    csv_path = tmp_path / "hwinfo.csv"
    _write(csv_path, [93.0])
    temps = iter([88.0, 79.0])
    sleeps: list[float] = []

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        _write(csv_path, [next(temps)])

    guard = ThermalGuard(tmp_path, poll_s=5, sleep=fake_sleep)
    assert guard.check() == 79.0
    assert sleeps == [5, 5]


def test_hot_cpu_aborts_after_max_wait(tmp_path: Path) -> None:
    csv_path = tmp_path / "hwinfo.csv"
    _write(csv_path, [95.0])
    guard = ThermalGuard(tmp_path, poll_s=10, max_wait_s=30, sleep=lambda _s: _write(csv_path, [94.0]))
    with pytest.raises(ThermalAbortError):
        guard.check()


def test_invalid_thresholds() -> None:
    with pytest.raises(ValueError, match="resume_c"):
        ThermalGuard(None, limit_c=80, resume_c=85)


def test_budget() -> None:
    now = [100.0]
    budget = Budget(60, clock=lambda: now[0])
    assert budget.elapsed == 0.0
    now[0] = 161.0
    assert budget.exhausted
    assert budget.elapsed == pytest.approx(61.0)
