"""Step 0 probe: which public hosts can serverless compute reach?"""

import sys

import requests

TARGETS = {
    "ecb_api": "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?lastNObservations=1&format=csvdata",
    "ecb_www": "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml",
    "pypi": "https://pypi.org/simple/requests/",
    "github": "https://github.com",
    "example": "https://example.com",
}

for name, url in TARGETS.items():
    try:
        resp = requests.get(url, timeout=20)
        print(f"EGRESS {name}: HTTP {resp.status_code}, {len(resp.content)} bytes")
    except Exception as exc:  # noqa: BLE001 - probe reports every failure
        print(f"EGRESS {name}: FAILED {type(exc).__name__}: {str(exc)[:200]}")
print("python", sys.version)
try:
    import pyspark

    print("pyspark", pyspark.__version__)
except ImportError:
    print("pyspark not importable")
try:
    from pyspark.sql import SparkSession

    print("spark", SparkSession.builder.getOrCreate().version)
except Exception as exc:  # noqa: BLE001
    print("spark session failed", exc)
