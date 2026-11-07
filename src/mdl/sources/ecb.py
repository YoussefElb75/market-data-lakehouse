"""ECB Data Portal client (SDMX REST) and incremental landing.

Source: ECB statistics (https://data.ecb.europa.eu). Reuse terms are in DATA.md:
payloads are landed byte for byte and never modified.

Incremental strategy (state per dataset in the landing zone):

* backfill (first run, or ``full=True``): windows of ``window_days`` from the
  backfill start (``startPeriod``/``endPeriod``), at most ``max_windows`` per
  run. The cursor survives between runs, so a long history is loaded in small
  pieces that stay within the compute fair-use quota;
* once caught up: ``updatedAfter=<anchor - margin>``, which returns new *and
  revised* observations, so late corrections reach silver too. The anchor of
  the first incremental run is the backfill start time, so revisions published
  while the backfill ran are not missed;
* "no data" is HTTP 404 with an SDMX "No Series/No Results" body, treated as an
  empty batch: a manifest is still written for freshness monitoring.
"""

from __future__ import annotations

import csv
import io
import json
import xml.etree.ElementTree as ET
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import Any

from mdl.sources import http
from mdl.sources.landing import Landing, Manifest, new_batch_id, utc_now

BASE_URL = "https://data-api.ecb.europa.eu/service"
SOURCE = "ecb"
ATTRIBUTION = "Source: ECB statistics"

FX_CURRENCIES = (
    "USD", "JPY", "GBP", "CHF", "CNY", "AUD", "CAD", "SEK", "NOK", "DKK",
    "PLN", "CZK", "HUF", "KRW", "INR", "BRL", "MXN", "ZAR", "SGD", "HKD",
)  # fmt: skip
YC_TENORS = (
    "3M", "6M", "9M", "1Y", "2Y", "3Y", "4Y", "5Y", "6Y", "7Y",
    "8Y", "9Y", "10Y", "15Y", "20Y", "25Y", "30Y",
)  # fmt: skip

_NO_DATA_MARKERS = (b"No Series was returned", b"No Results", b"NoRecordsFound")
_NS = {
    "mes": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    "str": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
    "com": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
}
_XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"


class Kind(StrEnum):
    DATA = "data"
    CODELIST = "codelist"


@dataclass(frozen=True)
class EcbDataset:
    name: str
    kind: Kind
    resource: str  # dataflow id (data) or codelist id (codelist)
    key: str = ""
    description: str = ""

    @property
    def url(self) -> str:
        if self.kind is Kind.CODELIST:
            return f"{BASE_URL}/codelist/ECB/{self.resource}"
        return f"{BASE_URL}/data/{self.resource}/{self.key}"


def _yc_key(issuer: str) -> str:
    return f"B.U2.EUR.4F.{issuer}.SV_C_YM." + "+".join(f"SR_{t}" for t in YC_TENORS)


DATASETS: dict[str, EcbDataset] = {
    ds.name: ds
    for ds in (
        EcbDataset(
            "fx_reference_rates",
            Kind.DATA,
            "EXR",
            "D." + "+".join(FX_CURRENCIES) + ".EUR.SP00.A",
            "Daily euro foreign exchange reference rates",
        ),
        EcbDataset("yield_curve_aaa", Kind.DATA, "YC", _yc_key("G_N_A"), "Euro area AAA spot curve"),
        EcbDataset("yield_curve_all", Kind.DATA, "YC", _yc_key("G_N_C"), "Euro area all-issuer spot curve"),
        EcbDataset("estr", Kind.DATA, "EST", "B.EU000A2X2A25.WT+TT", "Euro short-term rate and volume"),
        EcbDataset("currency_codelist", Kind.CODELIST, "CL_CURRENCY", description="ECB currency codes"),
    )
}


def is_no_data(status: int, content: bytes) -> bool:
    return status == 404 and any(marker in content for marker in _NO_DATA_MARKERS)


def parse_csv(payload: bytes) -> list[dict[str, str]]:
    """Parse an SDMX ``csvdata`` payload into rows (all values stay strings)."""
    text = payload.decode("utf-8-sig")
    return list(csv.DictReader(io.StringIO(text)))


def period_range(rows: list[dict[str, str]]) -> tuple[str | None, str | None]:
    periods = sorted(r["TIME_PERIOD"] for r in rows if r.get("TIME_PERIOD"))
    return (periods[0], periods[-1]) if periods else (None, None)


def parse_codelist(payload: bytes) -> list[dict[str, str]]:
    """Codes and English names from an SDMX-ML 2.1 codelist message."""
    root = ET.fromstring(payload)
    rows: list[dict[str, str]] = []
    for codelist in root.iterfind(".//str:Codelist", _NS):
        cl_id = codelist.get("id", "")
        version = codelist.get("version", "")
        for code in codelist.iterfind("str:Code", _NS):
            names = {n.get(_XML_LANG, ""): (n.text or "") for n in code.iterfind("com:Name", _NS)}
            rows.append(
                {
                    "codelist_id": cl_id,
                    "codelist_version": version,
                    "code": code.get("id", ""),
                    "name": names.get("en") or next(iter(names.values()), ""),
                }
            )
    return rows


def _iso_utc(at: datetime) -> str:
    return at.astimezone(UTC).isoformat(timespec="seconds")


def _fetch(
    dataset: EcbDataset,
    landing: Landing,
    params: dict[str, str],
    *,
    session: http.SessionLike | None,
    now: Callable[[], datetime],
    sleep: Callable[[float], None] | None,
    mode: str,
) -> Manifest:
    """One request: land the raw payload (if any) and write its manifest."""
    fetched_at = now()
    accept = "application/xml" if dataset.kind is Kind.CODELIST else "text/csv"
    kwargs: dict[str, Any] = {"session": session, "accept": accept, "ok_status": frozenset({200, 404})}
    if sleep is not None:
        kwargs["sleep"] = sleep
    result = http.get(dataset.url, params, **kwargs)
    if result.status == 404 and not is_no_data(result.status, result.content):
        raise http.HttpError(f"GET {dataset.url}: HTTP 404 that is not an SDMX 'no data' answer", 404)

    payload_path: str | None = None
    digest = ""
    row_count = 0
    first = last = None
    extra: dict[str, Any] = {"attribution": ATTRIBUTION, "mode": mode}
    if result.status == 200:
        if dataset.kind is Kind.CODELIST:
            rows = parse_codelist(result.content)
            row_count = len(rows)
            path, digest = landing.write_payload(SOURCE, dataset.name, result.content, "xml", fetched_at)
            # Same content, one JSON object per code, for Auto Loader (format change only).
            jsonl = "\n".join(_json_line(r) for r in rows).encode("utf-8") + b"\n"
            rows_path, _ = landing.write_payload(SOURCE, f"{dataset.name}_rows", jsonl, "json", fetched_at)
            extra["rows_path"] = str(rows_path)
        else:
            rows = parse_csv(result.content)
            row_count = len(rows)
            first, last = period_range(rows)
            path, digest = landing.write_payload(SOURCE, dataset.name, result.content, "csv", fetched_at)
        payload_path = str(path)

    manifest = Manifest(
        batch_id=new_batch_id(),
        source=SOURCE,
        dataset=dataset.name,
        fetched_at=_iso_utc(fetched_at),
        request_url=dataset.url,
        request_params=dict(result.params),
        http_status=result.status,
        bytes=len(result.content) if result.status == 200 else 0,
        sha256=digest,
        row_count=row_count,
        payload_path=payload_path,
        first_period=first,
        last_period=last,
        attempts=result.attempts,
        extra=extra,
    )
    landing.write_manifest(manifest)
    return manifest


def ingest(
    dataset: EcbDataset,
    landing: Landing,
    *,
    backfill_start: str = "2020-01-01",
    window_days: int = 366,
    max_windows: int = 8,
    margin: timedelta = timedelta(hours=6),
    full: bool = False,
    today: date | None = None,
    session: http.SessionLike | None = None,
    now: Callable[[], datetime] = utc_now,
    sleep: Callable[[float], None] | None = None,
) -> list[Manifest]:
    """Advance one dataset: backfill windows while behind, else one incremental fetch."""
    if window_days <= 0 or max_windows <= 0:
        raise ValueError("window_days and max_windows must be positive")
    fetch_kwargs: dict[str, Any] = {"session": session, "now": now, "sleep": sleep}
    if dataset.kind is Kind.CODELIST:
        manifest = _fetch(dataset, landing, {}, mode="snapshot", **fetch_kwargs)
        landing.write_state(SOURCE, dataset.name, {"last_success_at": manifest.fetched_at})
        return [manifest]

    state = landing.read_state(SOURCE, dataset.name)
    if full or not state:
        state = {"backfill_next": backfill_start, "anchor": None, "last_period": None}
    end_day = today or now().date()
    manifests: list[Manifest] = []

    if state.get("backfill_next"):
        while state["backfill_next"] and len(manifests) < max_windows:
            window_start = date.fromisoformat(state["backfill_next"])
            window_end = min(window_start + timedelta(days=window_days - 1), end_day)
            params = {
                "format": "csvdata",
                "startPeriod": window_start.isoformat(),
                "endPeriod": window_end.isoformat(),
            }
            manifest = _fetch(dataset, landing, params, mode="backfill", **fetch_kwargs)
            manifests.append(manifest)
            state["anchor"] = state.get("anchor") or manifest.fetched_at
            state["last_period"] = max(
                filter(None, [state.get("last_period"), manifest.last_period]), default=None
            )
            next_start = window_end + timedelta(days=1)
            state["backfill_next"] = next_start.isoformat() if next_start <= end_day else None
            landing.write_state(SOURCE, dataset.name, state)
        return manifests

    anchor = datetime.fromisoformat(str(state.get("anchor") or state["last_success_at"]))
    params = {"format": "csvdata", "updatedAfter": _iso_utc(anchor - margin)}
    manifest = _fetch(dataset, landing, params, mode="incremental", **fetch_kwargs)
    state.update(
        {
            "backfill_next": None,
            "anchor": manifest.fetched_at,
            "last_success_at": manifest.fetched_at,
            "last_period": max(filter(None, [state.get("last_period"), manifest.last_period]), default=None),
        }
    )
    landing.write_state(SOURCE, dataset.name, state)
    return [manifest]


def _json_line(row: dict[str, str]) -> str:
    return json.dumps(row, ensure_ascii=False, sort_keys=True)
