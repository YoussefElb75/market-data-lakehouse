"""Small HTTP GET helper with retries, backoff and an injectable session."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Protocol, cast

import requests

log = logging.getLogger(__name__)

RETRY_STATUS = frozenset({429, 500, 502, 503, 504})
USER_AGENT = "market-data-lakehouse/0.1 (+https://github.com/YoussefElb75/market-data-lakehouse)"


class ResponseLike(Protocol):
    status_code: int
    content: bytes
    headers: Mapping[str, str]


class SessionLike(Protocol):
    def get(
        self, url: str, *, params: Mapping[str, str] | None, timeout: float, headers: Mapping[str, str]
    ) -> ResponseLike: ...


@dataclass(frozen=True)
class HttpResult:
    url: str
    params: dict[str, str]
    status: int
    content: bytes
    headers: dict[str, str] = field(default_factory=dict)
    attempts: int = 1


class HttpError(RuntimeError):
    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


def get(
    url: str,
    params: Mapping[str, str] | None = None,
    *,
    session: SessionLike | None = None,
    accept: str = "*/*",
    timeout: float = 60.0,
    retries: int = 4,
    backoff_s: float = 2.0,
    max_backoff_s: float = 60.0,
    ok_status: frozenset[int] = frozenset({200}),
    sleep: Callable[[float], None] = time.sleep,
) -> HttpResult:
    """GET with exponential backoff on 429/5xx and connection errors.

    Statuses in ``ok_status`` are returned; other non-retryable statuses raise.
    """
    sess = session if session is not None else cast(SessionLike, requests.Session())
    headers = {"User-Agent": USER_AGENT, "Accept": accept}
    query = dict(params or {})
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = sess.get(url, params=query, timeout=timeout, headers=headers)
        except requests.RequestException as exc:
            if attempt > retries:
                raise HttpError(f"GET {url} failed after {attempt} attempts: {exc}") from exc
            delay = min(max_backoff_s, backoff_s * 2 ** (attempt - 1))
            log.warning("GET %s: %s; retrying in %.0f s", url, type(exc).__name__, delay)
            sleep(delay)
            continue
        if resp.status_code in ok_status:
            return HttpResult(url, query, resp.status_code, resp.content, dict(resp.headers), attempt)
        if resp.status_code in RETRY_STATUS and attempt <= retries:
            delay = min(max_backoff_s, backoff_s * 2 ** (attempt - 1))
            retry_after = resp.headers.get("Retry-After", "")
            if retry_after.isdigit():
                delay = min(max_backoff_s, float(retry_after))
            log.warning("GET %s: HTTP %d; retrying in %.0f s", url, resp.status_code, delay)
            sleep(delay)
            continue
        snippet = resp.content[:300].decode("utf-8", errors="replace")
        raise HttpError(f"GET {url} returned HTTP {resp.status_code}: {snippet}", resp.status_code)
