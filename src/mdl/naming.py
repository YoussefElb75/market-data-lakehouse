"""Unity Catalog naming: one catalog per environment, one schema per layer."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum

_IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")
ENVIRONMENTS = ("dev", "prod")


class Layer(StrEnum):
    LANDING = "landing"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    OPS = "ops"


def _check_ident(value: str) -> str:
    if not _IDENT.match(value):
        raise ValueError(f"invalid Unity Catalog identifier: {value!r}")
    return value


@dataclass(frozen=True)
class Names:
    """Fully qualified names for one environment."""

    env: str

    def __post_init__(self) -> None:
        if self.env not in ENVIRONMENTS:
            raise ValueError(f"unknown environment {self.env!r}; expected one of {ENVIRONMENTS}")

    @property
    def catalog(self) -> str:
        return f"mdl_{self.env}"

    def schema(self, layer: Layer) -> str:
        return f"{self.catalog}.{layer.value}"

    def table(self, layer: Layer, name: str) -> str:
        return f"{self.catalog}.{layer.value}.{_check_ident(name)}"

    def volume_path(self, volume: str = "raw", *parts: str) -> str:
        """POSIX path of a landing Volume, e.g. ``/Volumes/mdl_dev/landing/raw/ecb``."""
        base = f"/Volumes/{self.catalog}/{Layer.LANDING.value}/{_check_ident(volume)}"
        for part in parts:
            if not part or "/" in part or part in (".", ".."):
                raise ValueError(f"invalid path segment: {part!r}")
            base = f"{base}/{part}"
        return base
