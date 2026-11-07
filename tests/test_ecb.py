from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from mdl.sources import ecb, http
from mdl.sources.landing import Landing
from tests.fakes import FAKE_CODELIST_XML, FAKE_EXR_CSV, FAKE_NO_DATA, FakeResponse, FakeSession

T0 = datetime(2030, 1, 4, 16, 0, tzinfo=UTC)


def test_dataset_urls_and_keys() -> None:
    fx = ecb.DATASETS["fx_reference_rates"]
    assert fx.url.startswith("https://data-api.ecb.europa.eu/service/data/EXR/D.USD+JPY+")
    assert fx.url.endswith(".EUR.SP00.A")
    yc = ecb.DATASETS["yield_curve_aaa"]
    assert ".G_N_A.SV_C_YM.SR_3M+SR_6M+" in yc.url
    assert yc.url.endswith("SR_30Y")
    assert ecb.DATASETS["currency_codelist"].url.endswith("/codelist/ECB/CL_CURRENCY")


def test_parse_csv_and_period_range() -> None:
    rows = ecb.parse_csv(b"\xef\xbb\xbf" + FAKE_EXR_CSV)
    assert len(rows) == 3
    assert rows[0]["KEY"] == "EXR.D.USD.EUR.SP00.A"
    assert ecb.period_range(rows) == ("2030-01-02", "2030-01-03")
    assert ecb.period_range([]) == (None, None)


def test_parse_codelist_prefers_english() -> None:
    rows = ecb.parse_codelist(FAKE_CODELIST_XML)
    assert rows == [
        {"codelist_id": "CL_CURRENCY", "codelist_version": "1.0", "code": "AAA", "name": "Fake currency A"},
        {"codelist_id": "CL_CURRENCY", "codelist_version": "1.0", "code": "BBB", "name": "Nur deutsch"},
    ]


def test_no_data_detection() -> None:
    assert ecb.is_no_data(404, FAKE_NO_DATA)
    assert not ecb.is_no_data(404, b"<html>gateway</html>")
    assert not ecb.is_no_data(200, FAKE_NO_DATA)


def test_backfill_window_then_incremental_uses_updated_after(tmp_path: Path) -> None:
    landing = Landing(tmp_path)
    ds = ecb.DATASETS["fx_reference_rates"]
    session = FakeSession([FakeResponse(200, FAKE_EXR_CSV), FakeResponse(404, FAKE_NO_DATA)])

    (first,) = ecb.ingest(
        ds, landing, backfill_start="2029-12-01", today=T0.date(), session=session, now=lambda: T0
    )
    assert session.calls[0][1] == {
        "format": "csvdata",
        "startPeriod": "2029-12-01",
        "endPeriod": "2030-01-04",
    }
    assert first.extra["mode"] == "backfill"
    assert first.row_count == 3
    assert first.first_period == "2030-01-02"
    assert first.last_period == "2030-01-03"
    assert first.payload_path is not None
    payload = Path(first.payload_path)
    assert payload.read_bytes() == FAKE_EXR_CSV  # landed byte for byte
    assert first.sha256 == hashlib.sha256(FAKE_EXR_CSV).hexdigest()
    assert payload.parent.name == "ingest_date=2030-01-04"
    assert landing.read_state("ecb", ds.name)["backfill_next"] is None

    later = T0 + timedelta(days=1)
    (second,) = ecb.ingest(ds, landing, margin=timedelta(hours=6), session=session, now=lambda: later)
    # Anchored at the backfill start, so revisions published during the backfill are covered.
    assert session.calls[1][1] == {"format": "csvdata", "updatedAfter": "2030-01-04T10:00:00+00:00"}
    assert second.extra["mode"] == "incremental"
    assert second.row_count == 0
    assert second.payload_path is None
    assert second.http_status == 404

    state = landing.read_state("ecb", ds.name)
    assert state["last_success_at"] == "2030-01-05T16:00:00+00:00"
    assert state["anchor"] == "2030-01-05T16:00:00+00:00"
    assert state["last_period"] == "2030-01-03"  # an empty batch keeps the high-water mark
    manifests = sorted((tmp_path / "_manifests" / "ecb" / ds.name).rglob("*.json"))
    assert len(manifests) == 2
    assert not list((tmp_path / "_tmp").iterdir())  # staging area is left empty


def test_backfill_resumes_across_runs_in_small_windows(tmp_path: Path) -> None:
    landing = Landing(tmp_path)
    ds = ecb.DATASETS["estr"]
    session = FakeSession([FakeResponse(404, FAKE_NO_DATA)] * 4)
    kwargs = {"backfill_start": "2030-01-01", "window_days": 10, "max_windows": 2, "today": date(2030, 1, 25)}
    run1 = ecb.ingest(ds, landing, session=session, now=lambda: T0, **kwargs)  # type: ignore[arg-type]
    assert [m.request_params["startPeriod"] for m in run1] == ["2030-01-01", "2030-01-11"]
    assert landing.read_state("ecb", ds.name)["backfill_next"] == "2030-01-21"
    run2 = ecb.ingest(ds, landing, session=session, now=lambda: T0, **kwargs)  # type: ignore[arg-type]
    assert [(m.request_params["startPeriod"], m.request_params["endPeriod"]) for m in run2] == [
        ("2030-01-21", "2030-01-25")
    ]
    assert landing.read_state("ecb", ds.name)["backfill_next"] is None
    run3 = ecb.ingest(ds, landing, session=session, now=lambda: T0, **kwargs)  # type: ignore[arg-type]
    assert "updatedAfter" in run3[0].request_params
    with pytest.raises(ValueError, match="positive"):
        ecb.ingest(ds, landing, window_days=0)


def test_full_reload_restarts_backfill(tmp_path: Path) -> None:
    landing = Landing(tmp_path)
    landing.write_state("ecb", "estr", {"backfill_next": None, "anchor": "2030-01-01T00:00:00+00:00"})
    session = FakeSession([FakeResponse(200, FAKE_EXR_CSV)])
    ecb.ingest(
        ecb.DATASETS["estr"], landing, full=True, backfill_start="2030-01-01", today=date(2030, 1, 5),
        session=session,
    )  # fmt: skip
    assert session.calls[0][1]["startPeriod"] == "2030-01-01"


def test_codelist_lands_raw_xml_and_json_rows(tmp_path: Path) -> None:
    landing = Landing(tmp_path)
    session = FakeSession([FakeResponse(200, FAKE_CODELIST_XML)])
    (manifest,) = ecb.ingest(ecb.DATASETS["currency_codelist"], landing, session=session, now=lambda: T0)
    assert session.calls[0][1] == {}
    assert manifest.row_count == 2
    assert manifest.payload_path is not None
    assert Path(manifest.payload_path).read_bytes() == FAKE_CODELIST_XML
    rows_path = Path(manifest.extra["rows_path"])
    lines = [json.loads(line) for line in rows_path.read_text(encoding="utf-8").splitlines()]
    assert [r["code"] for r in lines] == ["AAA", "BBB"]
    assert manifest.extra["attribution"] == "Source: ECB statistics"


def test_unexpected_404_raises(tmp_path: Path) -> None:
    session = FakeSession([FakeResponse(404, b"<html>not here</html>")])
    with pytest.raises(http.HttpError, match="not an SDMX"):
        ecb.ingest(ecb.DATASETS["estr"], Landing(tmp_path), session=session)
    assert not (tmp_path / "_state").exists()
