"""Fail if text about to be committed contains workspace identifiers or secrets.

Two layers:

* generic patterns (always on, also in CI): Databricks workspace hosts,
  personal access tokens, OAuth/JWT tokens, and e-mail addresses other than
  GitHub noreply and example domains;
* exact literals listed in ``MDL_FORBIDDEN`` (comma-separated) in the
  git-ignored ``.env`` file or the environment, for values that have no
  generic shape (account e-mail, user id, workspace id, local user path).

Usage:
    python scripts/check_secrets.py --staged          # pre-commit hook
    python scripts/check_secrets.py --message FILE    # commit-msg hook
    python scripts/check_secrets.py --all             # whole tree (CI)
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

GENERIC_PATTERNS: dict[str, re.Pattern[str]] = {
    "databricks workspace host": re.compile(
        r"\b[a-z0-9][a-z0-9-]*\.(?:cloud\.databricks\.com|azuredatabricks\.net|gcp\.databricks\.com)\b",
        re.IGNORECASE,
    ),
    "databricks workspace slug": re.compile(r"\bdbc-[0-9a-f]{8}-[0-9a-f]{4}\b", re.IGNORECASE),
    "databricks personal access token": re.compile(r"\bdapi[0-9a-f]{32}\b"),
    "jwt / oauth token": re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]*"),
    "e-mail address": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
}

ALLOWED_EMAIL_SUFFIXES = ("@users.noreply.github.com", "@example.com", "@example.org")
SKIP_SUFFIXES = (".png", ".jpg", ".jpeg", ".gif", ".ico", ".pdf")


@dataclass(frozen=True)
class Finding:
    where: str
    line: int
    kind: str

    def __str__(self) -> str:
        return f"{self.where}:{self.line}: {self.kind}"


def forbidden_literals(env_file: Path = Path(".env")) -> list[str]:
    """Exact values from MDL_FORBIDDEN (environment wins over .env)."""
    raw = os.environ.get("MDL_FORBIDDEN", "")
    if not raw and env_file.is_file():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            key, _, value = line.partition("=")
            if key.strip() == "MDL_FORBIDDEN":
                raw = value.strip().strip('"').strip("'")
    return [v.strip() for v in raw.split(",") if len(v.strip()) >= 4]


def scan_text(text: str, where: str, literals: Iterable[str]) -> Iterator[Finding]:
    lowered_literals = [(lit, lit.lower()) for lit in literals]
    for lineno, line in enumerate(text.splitlines(), start=1):
        for kind, pattern in GENERIC_PATTERNS.items():
            for match in pattern.finditer(line):
                if kind == "e-mail address" and match.group(0).lower().endswith(ALLOWED_EMAIL_SUFFIXES):
                    continue
                yield Finding(where, lineno, kind)
        low = line.lower()
        for _, lit in lowered_literals:
            if lit in low:
                yield Finding(where, lineno, "forbidden literal from MDL_FORBIDDEN")


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], capture_output=True, text=True, encoding="utf-8", errors="replace", check=True
    ).stdout


def staged_files() -> Iterator[tuple[str, str]]:
    names = _git("diff", "--cached", "--name-only", "--diff-filter=ACMR", "-z").split("\0")
    for name in filter(None, names):
        if name.lower().endswith(SKIP_SUFFIXES):
            continue
        yield name, _git("show", f":{name}")


def tracked_files() -> Iterator[tuple[str, str]]:
    for name in filter(None, _git("ls-files", "-z").split("\0")):
        path = Path(name)
        if name.lower().endswith(SKIP_SUFFIXES) or not path.is_file():
            continue
        yield name, path.read_text(encoding="utf-8", errors="replace")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--staged", action="store_true")
    mode.add_argument("--all", action="store_true")
    mode.add_argument("--message", type=Path)
    args = parser.parse_args(argv)

    literals = forbidden_literals()
    if args.message is not None:
        sources: Iterable[tuple[str, str]] = [
            ("<commit message>", args.message.read_text(encoding="utf-8", errors="replace"))
        ]
    elif args.staged:
        sources = staged_files()
    else:
        sources = tracked_files()

    findings: list[Finding] = []
    for where, text in sources:
        findings.extend(scan_text(text, where, literals))
        if not where.startswith("<"):
            findings.extend(scan_text(where, f"<path> {where}", literals))
    if findings:
        print("check_secrets: blocked, remove these before committing:", file=sys.stderr)
        for finding in findings:
            print(f"  {finding}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
