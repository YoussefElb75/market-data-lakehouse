"""CPU thermal guard for local runs, driven by an HWiNFO CSV sensor log.

HWiNFO can append sensor readings to a CSV file. The guard reads the last
row, finds the CPU temperature column and, when the CPU is too hot, pauses
until it cools down (or gives up with ``ThermalAbortError``). When no log folder
is configured (``MDL_HWINFO_LOG_DIR``), as in CI, the guard is a no-op.
"""

from __future__ import annotations

import csv
import io
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from mdl.settings import get_setting

log = logging.getLogger(__name__)

# Preferred CPU temperature columns, most specific first (matched as substrings).
CPU_COLUMNS = ("CPU (Tctl/Tdie)", "CPU Package", "CPU (Tctl)", "Core Max")
_TAIL_BYTES = 64 * 1024


class ThermalAbortError(RuntimeError):
    """Raised when the CPU stays above the limit for longer than allowed."""


@dataclass(frozen=True)
class Reading:
    celsius: float
    at: datetime
    column: str

    def age_s(self, now: datetime | None = None) -> float:
        return ((now or datetime.now()) - self.at).total_seconds()


def _parse_timestamp(day: str, clock: str) -> datetime:
    """HWiNFO writes the local date and time, e.g. ``7.11.2026`` and ``6:20:42.849``."""
    for date_fmt in ("%d.%m.%Y", "%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(f"{day} {clock}", f"{date_fmt} %H:%M:%S.%f")
        except ValueError:
            continue
    raise ValueError(f"unrecognised HWiNFO timestamp: {day!r} {clock!r}")


def _pick_column(header: list[str]) -> int:
    for wanted in CPU_COLUMNS:
        for idx, name in enumerate(header):
            if wanted.lower() in name.lower() and "°C" in name:
                return idx
    raise ValueError("no CPU temperature column found in the HWiNFO header")


def read_latest(csv_path: Path) -> Reading:
    """Read the CPU temperature from the last complete row of an HWiNFO CSV."""
    with csv_path.open("rb") as fh:
        header_line = fh.readline().decode("utf-8-sig", errors="replace")
        fh.seek(0, io.SEEK_END)
        size = fh.tell()
        fh.seek(max(0, size - _TAIL_BYTES))
        tail = fh.read().decode("utf-8", errors="replace")
    header = next(csv.reader([header_line]))
    rows = [line for line in tail.splitlines() if line.strip()]
    # The last line may be partially written; walk back to the first row that parses.
    for line in reversed(rows):
        row = next(csv.reader([line]))
        if len(row) != len(header) or row[0] == header[0]:
            continue
        idx = _pick_column(header)
        try:
            return Reading(float(row[idx]), _parse_timestamp(row[0], row[1]), header[idx])
        except ValueError:
            continue
    raise ValueError(f"no complete data row in {csv_path}")


class ThermalGuard:
    """Pause while the CPU is above ``limit_c``; resume below ``resume_c``."""

    def __init__(
        self,
        log_dir: Path | None,
        *,
        limit_c: float = 90.0,
        resume_c: float = 80.0,
        max_stale_s: float = 120.0,
        poll_s: float = 10.0,
        max_wait_s: float = 300.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if resume_c >= limit_c:
            raise ValueError("resume_c must be below limit_c")
        self.log_dir = log_dir
        self.limit_c = limit_c
        self.resume_c = resume_c
        self.max_stale_s = max_stale_s
        self.poll_s = poll_s
        self.max_wait_s = max_wait_s
        self._sleep = sleep

    @classmethod
    def from_env(cls, *, limit_c: float = 90.0, resume_c: float = 80.0) -> ThermalGuard:
        folder = get_setting("MDL_HWINFO_LOG_DIR")
        return cls(Path(folder) if folder else None, limit_c=limit_c, resume_c=resume_c)

    @property
    def enabled(self) -> bool:
        return self.log_dir is not None

    def _csv(self) -> Path | None:
        if self.log_dir is None or not self.log_dir.is_dir():
            return None
        files = sorted(self.log_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
        return files[0] if files else None

    def reading(self) -> Reading | None:
        """Latest fresh reading, or ``None`` if disabled, missing or stale."""
        path = self._csv()
        if path is None:
            return None
        try:
            reading = read_latest(path)
        except (OSError, ValueError) as exc:
            log.warning("thermal guard: cannot read %s: %s", path.name, exc)
            return None
        if reading.age_s() > self.max_stale_s:
            log.warning(
                "thermal guard: HWiNFO log is stale (%.0f s old); is logging running?", reading.age_s()
            )
            return None
        return reading

    def check(self) -> float | None:
        """Block while too hot. Returns the last temperature seen, if any."""
        first = self.reading()
        if first is None or first.celsius < self.limit_c:
            return None if first is None else first.celsius
        log.warning("thermal guard: CPU %.1f °C >= %.1f °C, pausing", first.celsius, self.limit_c)
        waited = 0.0
        while waited < self.max_wait_s:
            self._sleep(self.poll_s)
            waited += self.poll_s
            current = self.reading()
            if current is None or current.celsius <= self.resume_c:
                return None if current is None else current.celsius
        raise ThermalAbortError(f"CPU above {self.resume_c} °C for {self.max_wait_s:.0f} s; aborting")


class Budget:
    """Wall-clock budget for foreground runs (default: under 10 minutes)."""

    def __init__(self, seconds: float = 570.0, clock: Callable[[], float] = time.monotonic) -> None:
        self.seconds = seconds
        self._clock = clock
        self._start = clock()

    @property
    def elapsed(self) -> float:
        return self._clock() - self._start

    @property
    def exhausted(self) -> bool:
        return self.elapsed >= self.seconds
