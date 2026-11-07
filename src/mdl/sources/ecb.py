"""ECB Data Portal client (SDMX REST) and incremental landing.

Source: ECB statistics (https://data.ecb.europa.eu). Reuse terms are in DATA.md:
payloads are landed byte for byte and never modified.

Incremental strategy:

* first run (or ``full=True``): ``startPeriod=<backfill start>``;
* later runs: ``updatedAfter=<last success - margin>``, which returns new
  *and revised* observations, so late corrections reach silver too;
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
from datetime import UTC, datetime, timedelta
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


def request_params(
    dataset: EcbDataset, state: dict[str, Any], *, backfill_start: str, margin: timedelta, full: bool
) -> dict[str, str]:
    if dataset.kind is Kind.CODELIST:
        return {}
    params = {"format": "csvdata"}
    last_success = state.get("last_success_at")
    if full or not last_success:
        params["startPeriod"] = backfill_start
    else:
        since = datetime.fromisoformat(str(last_success)) - margin
        params["updatedAfter"] = _iso_utc(since)
    return params


def ingest(
    dataset: EcbDataset,
    landing: Landing,
    *,
    backfill_start: str = "2020-01-01",
    margin: timedelta = timedelta(hours=6),
    full: bool = False,
    session: http.SessionLike | None = None,
    now: Callable[[], datetime] = utc_now,
    sleep: Callable[[float], None] | None = None,
) -> Manifest:
    """Fetch one dataset, land the raw payload and its manifest, update the state."""
    state = landing.read_state(SOURCE, dataset.name)
    params = request_params(dataset, state, backfill_start=backfill_start, margin=margin, full=full)
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
    extra: dict[str, Any] = {"attribution": ATTRIBUTION}
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
    prev_last = state.get("last_period")
    landing.write_state(
        SOURCE,
        dataset.name,
        {
            "last_success_at": _iso_utc(fetched_at),
            "last_batch_id": manifest.batch_id,
            "last_period": max(filter(None, [prev_last, last]), default=None),
        },
    )
    return manifest


def _json_line(row: dict[str, str]) -> str:
    return json.dumps(row, ensure_ascii=False, sort_keys=True)
