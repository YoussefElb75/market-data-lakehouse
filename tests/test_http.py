from __future__ import annotations

import pytest

from mdl.sources import http
from tests.fakes import FakeResponse, FakeSession, connection_error


def test_retries_on_5xx_and_connection_errors_then_succeeds() -> None:
    session = FakeSession([connection_error(), FakeResponse(503), FakeResponse(200, b"ok")])
    sleeps: list[float] = []
    result = http.get("https://x.test/a", {"q": "1"}, session=session, backoff_s=1, sleep=sleeps.append)
    assert result.content == b"ok"
    assert result.attempts == 3
    assert sleeps == [1, 2]
    assert session.calls[0] == ("https://x.test/a", {"q": "1"})


def test_honours_retry_after() -> None:
    session = FakeSession([FakeResponse(429, headers={"Retry-After": "7"}), FakeResponse(200)])
    sleeps: list[float] = []
    http.get("https://x.test", session=session, sleep=sleeps.append)
    assert sleeps == [7.0]


def test_gives_up_after_retries() -> None:
    session = FakeSession([FakeResponse(500)] * 3)
    with pytest.raises(http.HttpError) as info:
        http.get("https://x.test", session=session, retries=2, sleep=lambda _s: None)
    assert info.value.status == 500


def test_non_retryable_status_raises_immediately() -> None:
    session = FakeSession([FakeResponse(400, b"bad request")])
    with pytest.raises(http.HttpError, match="400"):
        http.get("https://x.test", session=session, sleep=lambda _s: None)
    assert len(session.calls) == 1


def test_extra_ok_status_is_returned() -> None:
    session = FakeSession([FakeResponse(404, b"nothing")])
    result = http.get("https://x.test", session=session, ok_status=frozenset({200, 404}))
    assert result.status == 404
