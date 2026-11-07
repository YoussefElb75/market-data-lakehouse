"""Local settings read from the environment, with an optional git-ignored ``.env``.

Only non-secret, machine-local values live here (thermal log folder, CLI
profile name). Workspace hosts and credentials stay in the Databricks CLI
profile and are never read or stored by this package.
"""

from __future__ import annotations

import os
from pathlib import Path


def read_env_file(path: Path) -> dict[str, str]:
    """Parse ``KEY=VALUE`` lines; ignores blanks, comments and malformed lines."""
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def get_setting(name: str, env_file: Path | None = None) -> str | None:
    """Environment first, then ``.env`` in the working directory; empty means unset."""
    value = os.environ.get(name)
    if value is None:
        value = read_env_file(env_file or Path.cwd() / ".env").get(name)
    return value or None
