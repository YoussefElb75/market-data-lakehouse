# Data sources and terms

This repository contains **no data**. Every dataset is fetched at run time
into the Databricks workspace, or into a git-ignored local folder for
development. Raw files, Delta tables, Parquet and CSV extracts are all
excluded by `.gitignore`, and the pre-commit check blocks workspace
identifiers. The README charts show only derived counts (data-quality pass
rates, quarantine counts, freshness lag), never observations.

Terms were checked on 2026-11-07. Check them again before any change of use.

## 1. ECB Data Portal: used

| Dataset | Dataflow and key | Content |
|---|---|---|
| Euro FX reference rates | `EXR/D.<CCY>.EUR.SP00.A` | Daily reference rate of each currency against the euro |
| Euro area yield curves | `YC/B.U2.EUR.4F.G_N_A.SV_C_YM.<TENOR>` (AAA), `YC/B.U2.EUR.4F.G_N_C.SV_C_YM.<TENOR>` (all issuers) | Spot rates, 3 months to 30 years |
| Euro short-term rate | `EST/B.EU000A2X2A25.WT` (rate), `EST/B.EU000A2X2A25.TT` (total volume) | €STR, published each TARGET2 business day |
| Code lists | `codelist/ECB/CL_CURRENCY` | Currency codes and names (reference data, kept as SCD2) |

- **Endpoint:** `https://data-api.ecb.europa.eu/service/data/{flow}/{key}` (SDMX REST, `format=csvdata`). Incremental loads use `startPeriod`, and `updatedAfter` picks up revised and late observations.
- **Terms:** [ECB statistics usage policy](https://www.ecb.europa.eu/stats/ecb_statistics/governance_and_quality_framework/html/usage_policy.en.html). The ESCB follows "a policy of free access and free reuse" of publicly released statistics, "irrespective of any subsequent commercial or non-commercial use", provided that:
  - **the source is quoted:** this project and every gold table comment carry *"Source: ECB statistics"*;
  - **the statistics (including metadata) are not modified:** bronze keeps each payload byte for byte, with a SHA-256 in the ingestion manifest. Silver and gold are typed, deduplicated *derived* views, and their comments say so. Values are never adjusted. Derived measures such as cross rates are new columns, not edits of ECB values;
  - **no third-party data is reused without the originator's permission:** see the yield-curve note below.
- **Yield curves:** the ECB estimates them from "bond and price information provided by MTS Markets" with "ratings provided by Fitch Ratings" ([source page](https://www.ecb.europa.eu/stats/financial_markets_and_interest_rates/euro_area_yield_curves/html/index.en.html)). This project uses only the curve *estimates* the ECB publishes as statistics in the `YC` dataflow, never the underlying MTS prices or Fitch ratings.
- **€STR:** the ECB says it does not charge for the €STR or license its use ([€STR overview](https://www.ecb.europa.eu/stats/financial_markets_and_interest_rates/euro_short-term_rate/html/eurostr_overview.en.html)). "€STR" is an ECB mark and is used here only to name the series.
- **FX reference rates:** these are published for information only. The ECB does not intend them for transaction purposes, and neither does this project.
- **Continuity:** the ECB does not guarantee that a series continues. Ingestion treats a missing series as a data-quality event, not as a crash.

## 2. US Treasury daily par yield curve: not used

I looked for current reuse terms on home.treasury.gov on 2026-11-07. The *Site Policies and Notices* and privacy pages say nothing about copyright or reuse. The only explicit statement I found is an [archived 2009 legal notice](https://webarchive.library.unt.edu/eot2008/20090109020154mp_/http://treasury.gov/privacy.html). US federal works are generally not copyrighted (17 U.S.C. § 105), but the curve is fitted to market quotes the Federal Reserve Bank of New York collects. Without a clear current reuse statement the source is left out. It can come back if Treasury publishes explicit terms.

## 3. Synthetic market data: used

`mdl.synth` generates seeded, reproducible trades, quotes and 10-level order-book snapshots for made-up instruments (`SYN-*`). Prices follow geometric Brownian motion and trades arrive as a Poisson process. The generator deliberately injects duplicates, out-of-order and late events, and malformed rows, so that the data-quality rules have something to catch. No real market data, parameters or calibration from any other source is involved.

## 4. Excluded on purpose

- Exchange or vendor market data whose terms restrict storage or redistribution (for example crypto exchange APIs, or Bloomberg and Refinitiv extracts).
- Any code, data, parameters or documentation from a current or former employer.
- FRED series: many are third-party with their own restrictions, and the ECB already covers the euro data needed.
