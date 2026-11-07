"""Write raw payloads to a landing folder (local disk or a UC Volume).

Layout under ``root`` (Auto Loader watches the data folders only)::

    <source>/<dataset>/ingest_date=YYYY-MM-DD/<dataset>_<UTC ts>_<sha8>.<ext>   payloads
    _manifests/<source>/<dataset>/ingest_date=YYYY-MM-DD/<batch_id>.json          one per batch
    _state/<source>/<dataset>.json                                               incremental state
    _tmp/                                                                        staging for atomic writes

Files are written to ``_tmp`` and then renamed into place, so a stream never
sees a half-written file. UC Volumes are mounted under ``/Volumes`` on
Databricks compute, so the same code works locally and in a job.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def utc_now() -> datetime:
    return datetime.now(UTC)


def _stamp(at: datetime) -> str:
    return at.astimezone(UTC).strftime("%Y%m%dT%H%M%S%fZ")


def atomic_write(root: Path, target: Path, data: bytes) -> Path:
    """Write ``data`` to ``target`` via a staging file under ``root/_tmp``."""
    tmp_dir = root / "_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = tmp_dir / f"{uuid.uuid4().hex}.part"
    tmp.write_bytes(data)
    tmp.replace(target)
    return target


@dataclass(frozen=True)
class Manifest:
    """Ingestion metadata for one fetched batch; becomes a row in bronze."""

    batch_id: str
    source: str
    dataset: str
    fetched_at: str
    request_url: str
    request_params: dict[str, str]
    http_status: int
    bytes: int
    sha256: str
    row_count: int
    payload_path: str | None
    first_period: str | None = None
    last_period: str | None = None
    attempts: int = 1
    extra: dict[str, Any] = field(default_factory=dict)


class Landing:
    """Landing zone rooted at a local folder or a ``/Volumes/...`` path."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)

    def write_payload(
        self, source: str, dataset: str, payload: bytes, ext: str, fetched_at: datetime
    ) -> tuple[Path, str]:
        digest = hashlib.sha256(payload).hexdigest()
        day = fetched_at.astimezone(UTC).strftime("%Y-%m-%d")
        name = f"{dataset}_{_stamp(fetched_at)}_{digest[:8]}.{ext}"
        target = self.root / source / dataset / f"ingest_date={day}" / name
        atomic_write(self.root, target, payload)
        return target, digest

    def write_manifest(self, manifest: Manifest) -> Path:
        day = manifest.fetched_at[:10]
        target = (
            self.root
            / "_manifests"
            / manifest.source
            / manifest.dataset
            / f"ingest_date={day}"
            / f"{manifest.batch_id}.json"
        )
        body = json.dumps(asdict(manifest), sort_keys=True).encode("utf-8")
        return atomic_write(self.root, target, body)

    def _state_path(self, source: str, dataset: str) -> Path:
        return self.root / "_state" / source / f"{dataset}.json"

    def read_state(self, source: str, dataset: str) -> dict[str, Any]:
        path = self._state_path(source, dataset)
        if not path.is_file():
            return {}
        loaded = json.loads(path.read_text(encoding="utf-8"))
        return loaded if isinstance(loaded, dict) else {}

    def write_state(self, source: str, dataset: str, state: dict[str, Any]) -> None:
        body = json.dumps(state, sort_keys=True, indent=2).encode("utf-8")
        atomic_write(self.root, self._state_path(source, dataset), body)


def new_batch_id() -> str:
    return uuid.uuid4().hex
