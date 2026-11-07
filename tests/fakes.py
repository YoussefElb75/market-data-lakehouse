"""Test doubles and handcrafted payloads.

The payloads mimic the *format* of ECB SDMX responses with made-up values;
they are not ECB data.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

import requests

FAKE_EXR_CSV = (
    b"KEY,FREQ,CURRENCY,CURRENCY_DENOM,EXR_TYPE,EXR_SUFFIX,TIME_PERIOD,OBS_VALUE,OBS_STATUS,OBS_CONF,TITLE\n"
    b"EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2030-01-02,1.2000,A,F,Fake title\n"
    b"EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2030-01-03,1.2100,A,F,Fake title\n"
    b"EXR.D.JPY.EUR.SP00.A,D,JPY,EUR,SP00,A,2030-01-02,150.00,A,F,Fake title\n"
)

FAKE_NO_DATA = b'{"title":"Not Found","status":404,"detail":"No Series was returned for the query"}'

FAKE_CODELIST_XML = b"""<?xml version='1.0' encoding='UTF-8'?>
<mes:Structure xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"
  xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"
  xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">
 <mes:Structures><str:Codelists>
  <str:Codelist id="CL_CURRENCY" agencyID="ECB" version="1.0">
   <com:Name xml:lang="en">Currency code list</com:Name>
   <str:Code id="AAA"><com:Name xml:lang="en">Fake currency A</com:Name></str:Code>
   <str:Code id="BBB"><com:Name xml:lang="de">Nur deutsch</com:Name></str:Code>
  </str:Codelist>
 </str:Codelists></mes:Structures>
</mes:Structure>"""


@dataclass
class FakeResponse:
    status_code: int
    content: bytes = b""
    headers: Mapping[str, str] = field(default_factory=dict)


@dataclass
class FakeSession:
    """Returns queued responses (or raises queued exceptions) and records calls."""

    queue: list[FakeResponse | Exception]
    calls: list[tuple[str, dict[str, str]]] = field(default_factory=list)

    def get(
        self, url: str, *, params: Mapping[str, str] | None, timeout: float, headers: Mapping[str, str]
    ) -> FakeResponse:
        self.calls.append((url, dict(params or {})))
        item = self.queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def connection_error() -> Exception:
    return requests.ConnectionError("boom")
