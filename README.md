# dbx-invest-analytics

Analytical and predictive data engineering project, assessing the impact of market, macroeconomic, and sentiment factors on the technology sector and QQQ ETF.

Built entirely on Databricks Free Edition with Unity Catalog, Delta Lake, and deterministic ETL pipelines following the Medallion architecture.

---

## Technology Stack

- Databricks Free Edition (Unity Catalog, Delta Lake)
- Python (PySpark, pandas)
- External APIs: FRED, yfinance, Alpha Vantage
- GitHub (version control, environments: dev / test / prod)
- Databricks Jobs & Pipelines (scheduling, orchestration)

---

## Architecture

The project follows a **Modular Python Project Structure** with strict separation of concerns.

### Core Principles

- Pipeline is the only entrypoint and controls I/O and execution order.
- ETL modules are pure, deterministic, and testable with no side effects on import.
- Each module has a single responsibility: extraction, transformation, validation, cleaning, enrichment, or write.
- Data contracts are enforced via schema definitions and runtime validation.
- All pipelines are idempotent and safe to re-run without duplicating data.

### Medallion Architecture

**Bronze** - raw data ingested from external sources (APIs, CSV files). Minimal processing, stored as Delta Tables.

**Silver** - cleaned, validated, and standardized data. Deduplication, type casting, canonical key formatting (lowercase, trimmed). Domain rule validation enforced before write.

**Gold** - business-level aggregations, enrichments, and analytical tables. Joins across data sources, calculated metrics (returns, sentiment scores, sector aggregations).

---

## Data Sources

### 1. QQQ ETF Constituents

Source: Invesco QQQ ETF constituents CSV (manual download from Barchart).
Scope: company list, names, percent holdings.
Pipeline: CSV → Bronze → Silver. 

The source file must be placed manually in `/Volumes/workspace/bronze/raw/`.

### 2. QQQ ETF Categories

Source: yfinance API (sector and industry metadata per symbol).
Scope: sector, industry classification for each QQQ constituent.
Pipeline: API → Bronze → Silver.

### 3. Market Data (OHLCV)

Source: yfinance API.
Scope: daily OHLCV (open, high, low, close, adj_close, volume) for ~100 QQQ constituents + 5 benchmark symbols (^VIX, QQQ, SPY, TLT, GLD).
Date range: from 2010-01-01, with 12-month incremental refresh window.
Pipeline: API → Bronze (upsert) → Silver (upsert).

### 4. Macroeconomic Data (FRED)

Source: FRED API (Federal Reserve Economic Data), XML format.
Indicators: CPIAUCSL, CPILFESL, T10YIE, FEDFUNDS, DGS10, DGS2, UNRATE, INDPRO.
Frequency: daily or monthly depending on indicator.
Pipeline: API → Bronze (overwrite per run with 12-month refresh window) → Silver (upsert). Full history on first run, incremental thereafter.
Metadata: unit and frequency extracted as JSON, transformed to DataFrame, canonical frequency mapping (daily→d, monthly→m, quarterly→q, annual→a).

### 5. News Sentiment (Alpha Vantage)

Source: Alpha Vantage NEWS_SENTIMENT API.
Scope: per-ticker sentiment scores for top ~41 technology symbols from QQQ, filtered by technology topic.
Rate limits: 25 requests/day, 5 requests/min (free tier).
Pipelines: latest sentiment API → Bronze (upsert), historical backfill API → Bronze (upsert), Bronze → Silver (upsert with deduplication).

---

## Pipeline Inventory

### Bronze Pipelines (API/File → Bronze)

| Pipeline | Source | Target Table | Write Mode |
|---|---|---|---|
| `run_qqq_entities_raw_to_bronze` | CSV file | `bronze.qqq_etf_constituents` | overwrite |
| `run_qqq_categories_api_to_bronze` | yfinance API | `bronze.qqq_etf_categories` | overwrite |
| `run_ohlcv_api_to_bronze` | yfinance API | `bronze.ohlcv_indicators` | upsert |
| `run_fred_api_to_bronze` | FRED API | `bronze.fred_macro_indicators` + `bronze.fred_macro_metadata_indicators` | overwrite |
| `run_av_sentiment_api_to_bronze` | Alpha Vantage API | `bronze.av_sentiment` | upsert |
| `run_av_sentiment_history_api_to_bronze` | Alpha Vantage API | `bronze.av_sentiment` | upsert |

### Silver Pipelines (Bronze → Silver)

| Pipeline | Source Table | Target Table | Write Mode |
|---|---|---|---|
| `run_qqq_entities_bronze_to_silver` | `bronze.qqq_etf_constituents` | `silver.qqq_entities` | overwrite |
| `run_qqq_categories_bronze_to_silver` | `bronze.qqq_etf_categories` | `silver.qqq_etf_categories` | overwrite |
| `run_ohlcv_bronze_to_silver` | `bronze.ohlcv_indicators` | `silver.ohlcv_indicators` | upsert |
| `run_fred_bronze_to_silver` | `bronze.fred_macro_indicators` + metadata | `silver.fred_macro_indicators` + `silver.fred_macro_metadata_indicators` | upsert |
| `run_av_sentiment_bronze_to_silver` | `bronze.av_sentiment` | `silver.av_sentiment` | upsert |

### Gold Pipelines (Silver → Gold)

| Pipeline | Source Tables | Target Table | Write Mode |
|---|---|---|---|
| `run_gold_ohlcv_with_dimension` | silver.ohlcv + silver.qqq_entities + silver.qqq_etf_categories | `gold.ohlcv_with_dimension` | overwrite |
| `run_gold_fred_with_dimension` | silver.fred_macro_indicators + silver.fred_macro_metadata_indicators | `gold.fred_with_dimension` | overwrite |
| `run_gold_sentiment_daily_agg` | silver.av_sentiment | `gold.av_sentiment_aggregated` | overwrite |
| `run_gold_daily_sentiment_per_sector` | gold.av_sentiment_aggregated + silver.qqq_etf_categories | `gold.av_sentiment_sector_daily` | overwrite |
| `run_gold_sentiment_vs_returns` | silver.ohlcv + gold.av_sentiment_aggregated | `gold.sentiment_vs_returns` | overwrite |
| `run_gold_sentiment_lead_lag` | silver.ohlcv + gold.av_sentiment_aggregated | `gold.sentiment_lead_lag` | overwrite |
| `run_gold_macro_impact_on_tech` | silver.ohlcv (QQQ only) + silver.fred_macro_indicators | `gold.macro_impact_on_tech` | overwrite |

### Maintenance Pipelines

| Pipeline | Purpose |
|---|---|
| `run_vacuum` | Delta VACUUM on all configured tables (7-day retention) |

---

## Pipeline Execution Order

Pipelines have hard dependencies. The correct execution order is:

**Phase 1 Bronze (independent, can run in parallel)**

1. `run_qqq_entities_raw_to_bronze`
2. `run_fred_api_to_bronze`

**Phase 2 Bronze (depends on Phase 1)**

3. `run_qqq_entities_bronze_to_silver`
4. `run_qqq_categories_api_to_bronze` (needs `bronze.qqq_etf_constituents`)
5. `run_fred_bronze_to_silver`

**Phase 3 Silver + OHLCV Bronze**

6. `run_qqq_categories_bronze_to_silver`
7. `run_ohlcv_api_to_bronze` (needs `silver.qqq_entities` for symbol list)
8. `run_ohlcv_bronze_to_silver`

**Phase 4 Gold OHLCV + Sentiment Bronze**

9. `run_gold_ohlcv_with_dimension` (needs silver.ohlcv + silver.qqq_entities + silver.qqq_etf_categories)
10. `run_av_sentiment_api_to_bronze` (needs `gold.ohlcv_with_dimension` for tech symbol list)
11. `run_av_sentiment_history_api_to_bronze` (needs `gold.ohlcv_with_dimension` + `bronze.av_sentiment`)

**Phase 5 Sentiment Silver + Gold**

12. `run_av_sentiment_bronze_to_silver`
13. `run_gold_fred_with_dimension`
14. `run_gold_sentiment_daily_agg`

**Phase 6 Final Gold (depends on Phase 5)**

15. `run_gold_sentiment_vs_returns`
16. `run_gold_sentiment_lead_lag`
17. `run_gold_daily_sentiment_per_sector`
18. `run_gold_macro_impact_on_tech`

**Maintenance (run after all pipelines)**

19. `run_vacuum`

---

## Project Structure

```
dbx-invest-analytics/
├── src/
│   ├── config/
│   │   ├── config_loader.py          # YAML config loader (env-aware)
│   │   ├── logger.py                 # Structured logging setup
│   │   ├── dev.yaml                  # Dev environment config
│   │   ├── test.yaml                 # Test environment config
│   │   └── prod.yaml                 # Prod environment config
│   ├── etl/
│   │   ├── cleaning/                 # Data standardization (lowercase, trim, canonical forms)
│   │   │   ├── av_sentiment_cleaning.py
│   │   │   ├── fred_cleaning.py
│   │   │   ├── fred_metadata_cleaning.py
│   │   │   ├── ohlcv_cleaning.py
│   │   │   ├── qqq_categories_cleaning.py
│   │   │   └── qqq_entities_cleaning.py
│   │   ├── enrichment/               # Joins and data merging
│   │   │   ├── fred_dimension_enrichment.py
│   │   │   ├── macro_return_enrichment.py
│   │   │   ├── ohlcv_dimension_enrichment.py
│   │   │   ├── qqq_entities_enrichment.py
│   │   │   ├── sentiment_return_enrichment.py
│   │   │   └── sentiment_sector_enrichment.py
│   │   ├── extraction/               # Data reading and API clients
│   │   │   ├── alpha_vantage/
│   │   │   │   ├── av_sentiment_client.py
│   │   │   │   └── av_sentiment_history_client.py
│   │   │   ├── fred/
│   │   │   │   ├── fred_client.py
│   │   │   │   └── fred_run_metadata.py
│   │   │   ├── yahoo_finance/
│   │   │   │   └── yahoo_ohlcv_extractor.py
│   │   │   ├── delta_table_extractor.py
│   │   │   ├── file_utils.py
│   │   │   ├── qqq_categories_extraction.py
│   │   │   └── qqq_entities_extraction.py
│   │   ├── monitoring/               # Pipeline run tracking
│   │   │   └── pipeline_run_logger.py
│   │   ├── retention/                # Delta table maintenance
│   │   │   └── delta_vacuum.py
│   │   ├── schema/                   # Data contracts and schema definitions
│   │   │   ├── av_schema.py
│   │   │   ├── fred_schema.py
│   │   │   ├── fred_metadata_schema.py
│   │   │   ├── macro_impact_schema.py
│   │   │   ├── ohlcv_schema.py
│   │   │   ├── pipeline_logger_schema.py
│   │   │   ├── qqq_categories_schema.py
│   │   │   ├── qqq_entities_schema.py
│   │   │   ├── sentiment_lead_lag_schema.py
│   │   │   ├── sentiment_sector_schema.py
│   │   │   └── sentiment_vs_returns_schema.py
│   │   ├── transformations/          # Pure data transformations (no I/O)
│   │   │   ├── av_json_parser.py
│   │   │   ├── av_sentiment_aggregation.py
│   │   │   ├── fred_macro_transformation.py
│   │   │   ├── fred_xml_parser.py
│   │   │   ├── ohlcv_monthly_aggregation.py
│   │   │   ├── ohlcv_return_calculation.py
│   │   │   ├── ohlcv_transformation.py
│   │   │   ├── qqq_entities_transformation.py
│   │   │   └── sentiment_sector_aggregation.py
│   │   ├── utils/                    # Shared validation helpers
│   │   │   └── validation_helper.py
│   │   ├── validation/               # Domain-specific validators
│   │   │   ├── av_sentiment_validation.py
│   │   │   ├── fred_metadata_validation.py
│   │   │   ├── fred_validation.py
│   │   │   ├── gold_fred_validation.py
│   │   │   ├── gold_macro_impact_validation.py
│   │   │   ├── gold_ohlcv_validation.py
│   │   │   ├── gold_sentiment_lead_lag_validation.py
│   │   │   ├── gold_sentiment_returns_validation.py
│   │   │   ├── gold_sentiment_sector_validation.py
│   │   │   ├── gold_sentiment_validation.py
│   │   │   ├── ohlcv_validation.py
│   │   │   ├── qqq_categories_validation.py
│   │   │   └── qqq_entities_validation.py
│   │   └── write/                    # Delta table writers (overwrite, append, upsert)
│   │       └── delta_table_writer.py
│   └── pipelines/                    # Orchestration entrypoints
│       ├── run_av_sentiment_api_to_bronze.py
│       ├── run_av_sentiment_bronze_to_silver.py
│       ├── run_av_sentiment_history_api_to_bronze.py
│       ├── run_fred_api_to_bronze.py
│       ├── run_fred_bronze_to_silver.py
│       ├── run_gold_daily_sentiment_per_sector.py
│       ├── run_gold_fred_with_dimension.py
│       ├── run_gold_macro_impact_on_tech.py
│       ├── run_gold_ohlcv_with_dimension.py
│       ├── run_gold_sentiment_daily_agg.py
│       ├── run_gold_sentiment_lead_lag.py
│       ├── run_gold_sentiment_vs_returns.py
│       ├── run_ohlcv_api_to_bronze.py
│       ├── run_ohlcv_bronze_to_silver.py
│       ├── run_qqq_categories_api_to_bronze.py
│       ├── run_qqq_categories_bronze_to_silver.py
│       ├── run_qqq_entities_bronze_to_silver.py
│       ├── run_qqq_entities_raw_to_bronze.py
│       └── run_vacuum.py
├── tests/
│   └── etl/
│       ├── cleaning/
│       │   ├── test_av_sentiment_cleaning.py
│       │   ├── test_fred_cleaning.py
│       │   ├── test_fred_metadata_cleaning.py
│       │   └── test_ohlcv_cleaning.py
│       ├── transformations/
│       │   ├── test_av_json_parser.py
│       │   ├── test_av_sentiment_aggregation.py
│       │   ├── test_fred_xml_parser.py
│       │   ├── test_ohlcv_return_calculation.py
│       │   └── test_sentiment_sector_aggregation.py
│       └── test_fred_client.py
├── notebooks/
│   └── eda/                          # Exploratory data analysis notebooks
│       ├── 01_data_completness.sql.ipynb
│       ├── 02_fred_macro_quality.sql.ipynb
│       ├── 03_ohlcv_data_quality.sql.ipynb
│       ├── 04_sentiment_quality.sql.ipynb
│       ├── 05_gold_readiness.sql.ipynb
│       └── 06_cross_source_coverage.sql.ipynb
├── debug/                            # Debug notebooks (not for production)
├── conftest.py                       # Pytest SparkSession fixture
├── requirements.txt
└── README.md
```

---

## Configuration and Security

- Secrets (FRED API Key, Alpha Vantage API Key) stored in **Databricks Secrets** (`my-scope`).
- Paths, table names, and parameters loaded via `config_loader` from environment-specific YAML files (`dev.yaml`, `test.yaml`, `prod.yaml`).
- No secrets or credentials in code or config files.

---

## Monitoring

Every pipeline logs execution metadata to `ops.pipeline_runs` Delta table:
- run_id (UUID), pipeline_name, layer, status (SUCCESS/FAILURE)
- started_at, finished_at, duration_seconds
- input_rows, output_rows, rows_rejected
- source_table, target_table, config_params, error_message

---

## Data Retention

Delta VACUUM runs on all configured tables with 7-day retention (168 hours). Configured in `dev.yaml` under `vacuum.tables`.

---

## Dependencies

Runtime dependencies listed in `requirements.txt`:
- `yfinance==1.0` - market data extraction
- `lxml` - XML parsing
- `html5lib` - HTML parsing fallback

Additional dependencies available in Databricks runtime: `pyspark`, `delta-spark`, `requests`, `pyyaml`.

Some dependencies are installed directly at the Databricks Job/Pipeline level.

---

## Known Limitations

- Alpha Vantage free tier: 25 requests/day, limiting sentiment data freshness and backfill speed.
- QQQ ETF constituents CSV requires manual download and placement.
- Sentiment coverage is uneven across symbols (NVDA: 387 articles vs ARM: 3 articles).
- Sectors with <5 symbols have insufficient data for reliable sector-level aggregations.
- Benchmark symbols (^VIX, QQQ, SPY, TLT, GLD) intentionally lack dimension metadata (sector, industry).

---

## Planned

- Integration of news and geopolitical data sources
- Sector scoring model
- Predictive models and AI-driven investment insights
- CI/CD pipeline (GitHub Actions)

---

## Links

- [Invesco QQQ ETF constituents (CSV)](https://www.barchart.com/etfs-funds/quotes/QQQ/constituents)
- [yfinance documentation](https://ranaroussi.github.io/yfinance/reference/api/yfinance.download.html#yfinance.download)
- [FRED API documentation](https://fred.stlouisfed.org/docs/api/fred/)
- [Alpha Vantage API documentation](https://www.alphavantage.co/documentation/)

# dbx-invest-analytics - PL

Projekt analityczno-predykcyjny z zakresu inżynierii danych i oceny wpływu czynników rynkowych, makroekonomicznych i sentymentu na sektor technologiczny oraz ETF QQQ.

Zbudowany w całości na Databricks Free Edition z Unity Catalog, Delta Lake i deterministycznymi pipeline'ami ETL w architekturze Medallion.

---

## Stos technologiczny

- Databricks Free Edition (Unity Catalog, Delta Lake)
- Python (PySpark, pandas)
- Zewnętrzne API: FRED, yfinance, Alpha Vantage
- GitHub (kontrola wersji, środowiska: dev / test / prod)
- Databricks Jobs & Pipelines (scheduling, orkiestracja)

---

## Architektura

Projekt realizuje **Modular Python Project Structure** ze ścisłym rozdziałem odpowiedzialności.

### Zasady

- Pipeline jest jedynym entrypointem i kontroluje I/O i kolejność wykonania.
- Moduły ETL są czyste, deterministyczne i testowalne, brak efektów ubocznych przy imporcie.
- Każdy moduł ma jedną odpowiedzialność: ekstrakcja, transformacja, walidacja, czyszczenie, wzbogacenie lub zapis.
- Kontrakty danych wymuszane przez definicje schematów i walidacje runtime.
- Wszystkie pipeline'y są idempotentne i bezpieczne do ponownego uruchomienia bez duplikacji danych.

### Architektura Medallion

**Bronze** - surowe dane pobrane ze źródeł zewnętrznych (API, pliki CSV). Minimalne przetwarzanie, zapis jako Delta Tables.

**Silver** - dane oczyszczone, zwalidowane i ustandaryzowane. Deduplikacja, rzutowanie typów, kanonizacja kluczy (lowercase, trim). Walidacja reguł domenowych przed zapisem.

**Gold** - agregacje biznesowe, wzbogacenia i tabele analityczne. Złączenia między źródłami danych, obliczone metryki (stopy zwrotu, wyniki sentymentu, agregacje sektorowe).

---

## Źródła danych

### 1. Podmioty QQQ ETF

Źródło: Invesco QQQ ETF constituents CSV (ręczne pobranie z Barchart).
Zakres: lista spółek, nazwy, udziały procentowe.
Pipeline: CSV → Bronze → Silver.

Plik źródłowy musi być umieszczony ręcznie w `/Volumes/workspace/bronze/raw/`.

### 2. Kategorie QQQ ETF

Źródło: yfinance API (metadane sektora i branży per symbol).
Zakres: klasyfikacja sektorowa i branżowa dla każdego podmiotu QQQ.
Pipeline: API → Bronze → Silver.

### 3. Dane rynkowe (OHLCV)

Źródło: yfinance API.
Zakres: dzienne OHLCV (open, high, low, close, adj_close, volume) dla ~100 podmiotów QQQ + 5 symboli benchmarkowych (^VIX, QQQ, SPY, TLT, GLD).
Zakres dat: od 2010-01-01, z 12-miesięcznym oknem inkrementalnego odświeżania.
Pipeline: API → Bronze (upsert) → Silver (upsert).

### 4. Dane makroekonomiczne (FRED)

Źródło: FRED API (Federal Reserve Economic Data), format XML.
Wskaźniki: CPIAUCSL, CPILFESL, T10YIE, FEDFUNDS, DGS10, DGS2, UNRATE, INDPRO.
Częstotliwość: dzienna lub miesięczna w zależności od wskaźnika.
Pipeline: API → Bronze (overwrite z 12-miesięcznym oknem odświeżania) → Silver (upsert). Pełna historia przy pierwszym uruchomieniu, inkrementacja przy kolejnych.
Metadane: unit i frequency wyodrębnione jako JSON, przekształcone do DataFrame, kanonizacja częstotliwości (daily→d, monthly→m, quarterly→q, annual→a).

### 5. Sentyment newsów (Alpha Vantage)

Źródło: Alpha Vantage NEWS_SENTIMENT API.
Zakres: wyniki sentymentu per ticker dla ~41 symboli technologicznych z QQQ, filtrowane po temacie technology.
Limity API: 25 requestów/dzień, 5 requestów/min (darmowy tier).
Pipeline'y: najnowszy sentyment API → Bronze (upsert), historyczny backfill API → Bronze (upsert), Bronze → Silver (upsert z deduplikacją).

---

## Rejestr pipeline'ów

### Pipeline'y Bronze (API/Plik → Bronze)

| Pipeline | Źródło | Tabela docelowa | Tryb zapisu |
|---|---|---|---|
| `run_qqq_entities_raw_to_bronze` | plik CSV | `bronze.qqq_etf_constituents` | overwrite |
| `run_qqq_categories_api_to_bronze` | yfinance API | `bronze.qqq_etf_categories` | overwrite |
| `run_ohlcv_api_to_bronze` | yfinance API | `bronze.ohlcv_indicators` | upsert |
| `run_fred_api_to_bronze` | FRED API | `bronze.fred_macro_indicators` + `bronze.fred_macro_metadata_indicators` | overwrite |
| `run_av_sentiment_api_to_bronze` | Alpha Vantage API | `bronze.av_sentiment` | upsert |
| `run_av_sentiment_history_api_to_bronze` | Alpha Vantage API | `bronze.av_sentiment` | upsert |

### Pipeline'y Silver (Bronze → Silver)

| Pipeline | Tabela źródłowa | Tabela docelowa | Tryb zapisu |
|---|---|---|---|
| `run_qqq_entities_bronze_to_silver` | `bronze.qqq_etf_constituents` | `silver.qqq_entities` | overwrite |
| `run_qqq_categories_bronze_to_silver` | `bronze.qqq_etf_categories` | `silver.qqq_etf_categories` | overwrite |
| `run_ohlcv_bronze_to_silver` | `bronze.ohlcv_indicators` | `silver.ohlcv_indicators` | upsert |
| `run_fred_bronze_to_silver` | `bronze.fred_macro_indicators` + metadata | `silver.fred_macro_indicators` + `silver.fred_macro_metadata_indicators` | upsert |
| `run_av_sentiment_bronze_to_silver` | `bronze.av_sentiment` | `silver.av_sentiment` | upsert |

### Pipeline'y Gold (Silver → Gold)

| Pipeline | Tabele źródłowe | Tabela docelowa | Tryb zapisu |
|---|---|---|---|
| `run_gold_ohlcv_with_dimension` | silver.ohlcv + silver.qqq_entities + silver.qqq_etf_categories | `gold.ohlcv_with_dimension` | overwrite |
| `run_gold_fred_with_dimension` | silver.fred_macro_indicators + silver.fred_macro_metadata_indicators | `gold.fred_with_dimension` | overwrite |
| `run_gold_sentiment_daily_agg` | silver.av_sentiment | `gold.av_sentiment_aggregated` | overwrite |
| `run_gold_daily_sentiment_per_sector` | gold.av_sentiment_aggregated + silver.qqq_etf_categories | `gold.av_sentiment_sector_daily` | overwrite |
| `run_gold_sentiment_vs_returns` | silver.ohlcv + gold.av_sentiment_aggregated | `gold.sentiment_vs_returns` | overwrite |
| `run_gold_sentiment_lead_lag` | silver.ohlcv + gold.av_sentiment_aggregated | `gold.sentiment_lead_lag` | overwrite |
| `run_gold_macro_impact_on_tech` | silver.ohlcv (tylko QQQ) + silver.fred_macro_indicators | `gold.macro_impact_on_tech` | overwrite |

### Pipeline'y utrzymaniowe

| Pipeline | Cel |
|---|---|
| `run_vacuum` | Delta VACUUM na wszystkich skonfigurowanych tabelach (retencja 7 dni) |

---

## Kolejność uruchamiania pipeline'ów

Pipeline'y mają twarde zależności. Poprawna kolejność uruchamiania:

**Faza 1 - Bronze (niezależne, mogą działać równolegle)**

1. `run_qqq_entities_raw_to_bronze`
2. `run_fred_api_to_bronze`

**Faza 2 - Bronze (zależy od Fazy 1)**

3. `run_qqq_entities_bronze_to_silver`
4. `run_qqq_categories_api_to_bronze` (wymaga `bronze.qqq_etf_constituents`)
5. `run_fred_bronze_to_silver`

**Faza 3 - Silver + OHLCV Bronze**

6. `run_qqq_categories_bronze_to_silver`
7. `run_ohlcv_api_to_bronze` (wymaga `silver.qqq_entities` do listy symboli)
8. `run_ohlcv_bronze_to_silver`

**Faza 4 - Gold OHLCV + Sentiment Bronze**

9. `run_gold_ohlcv_with_dimension` (wymaga silver.ohlcv + silver.qqq_entities + silver.qqq_etf_categories)
10. `run_av_sentiment_api_to_bronze` (wymaga `gold.ohlcv_with_dimension` do listy symboli tech)
11. `run_av_sentiment_history_api_to_bronze` (wymaga `gold.ohlcv_with_dimension` + `bronze.av_sentiment`)

**Faza 5 - Sentiment Silver + Gold**

12. `run_av_sentiment_bronze_to_silver`
13. `run_gold_fred_with_dimension`
14. `run_gold_sentiment_daily_agg`

**Faza 6 - Końcowe Gold (zależy od Fazy 5)**

15. `run_gold_sentiment_vs_returns`
16. `run_gold_sentiment_lead_lag`
17. `run_gold_daily_sentiment_per_sector`
18. `run_gold_macro_impact_on_tech`

**Utrzymanie (uruchamiaj po wszystkich pipeline'ach)**

19. `run_vacuum`

---

## Struktura projektu

```
dbx-invest-analytics/
├── src/
│   ├── config/
│   │   ├── config_loader.py          # Loader konfiguracji YAML (per środowisko)
│   │   ├── logger.py                 # Konfiguracja logowania
│   │   ├── dev.yaml                  # Konfiguracja środowiska dev
│   │   ├── test.yaml                 # Konfiguracja środowiska test
│   │   └── prod.yaml                 # Konfiguracja środowiska prod
│   ├── etl/
│   │   ├── cleaning/                 # Standaryzacja danych (lowercase, trim, formy kanoniczne)
│   │   │   ├── av_sentiment_cleaning.py
│   │   │   ├── fred_cleaning.py
│   │   │   ├── fred_metadata_cleaning.py
│   │   │   ├── ohlcv_cleaning.py
│   │   │   ├── qqq_categories_cleaning.py
│   │   │   └── qqq_entities_cleaning.py
│   │   ├── enrichment/               # Złączenia i łączenie danych
│   │   │   ├── fred_dimension_enrichment.py
│   │   │   ├── macro_return_enrichment.py
│   │   │   ├── ohlcv_dimension_enrichment.py
│   │   │   ├── qqq_entities_enrichment.py
│   │   │   ├── sentiment_return_enrichment.py
│   │   │   └── sentiment_sector_enrichment.py
│   │   ├── extraction/               # Odczyt danych i klienci API
│   │   │   ├── alpha_vantage/
│   │   │   │   ├── av_sentiment_client.py
│   │   │   │   └── av_sentiment_history_client.py
│   │   │   ├── fred/
│   │   │   │   ├── fred_client.py
│   │   │   │   └── fred_run_metadata.py
│   │   │   ├── yahoo_finance/
│   │   │   │   └── yahoo_ohlcv_extractor.py
│   │   │   ├── delta_table_extractor.py
│   │   │   ├── file_utils.py
│   │   │   ├── qqq_categories_extraction.py
│   │   │   └── qqq_entities_extraction.py
│   │   ├── monitoring/               # Śledzenie uruchomień pipeline'ów
│   │   │   └── pipeline_run_logger.py
│   │   ├── retention/                # Utrzymanie tabel Delta
│   │   │   └── delta_vacuum.py
│   │   ├── schema/                   # Kontrakty danych i definicje schematów
│   │   │   ├── av_schema.py
│   │   │   ├── fred_schema.py
│   │   │   ├── fred_metadata_schema.py
│   │   │   ├── macro_impact_schema.py
│   │   │   ├── ohlcv_schema.py
│   │   │   ├── pipeline_logger_schema.py
│   │   │   ├── qqq_categories_schema.py
│   │   │   ├── qqq_entities_schema.py
│   │   │   ├── sentiment_lead_lag_schema.py
│   │   │   ├── sentiment_sector_schema.py
│   │   │   └── sentiment_vs_returns_schema.py
│   │   ├── transformations/          # Czyste transformacje danych (bez I/O)
│   │   │   ├── av_json_parser.py
│   │   │   ├── av_sentiment_aggregation.py
│   │   │   ├── fred_macro_transformation.py
│   │   │   ├── fred_xml_parser.py
│   │   │   ├── ohlcv_monthly_aggregation.py
│   │   │   ├── ohlcv_return_calculation.py
│   │   │   ├── ohlcv_transformation.py
│   │   │   ├── qqq_entities_transformation.py
│   │   │   └── sentiment_sector_aggregation.py
│   │   ├── utils/                    # Wspólne helpery walidacyjne
│   │   │   └── validation_helper.py
│   │   ├── validation/               # Walidatory domenowe
│   │   │   ├── av_sentiment_validation.py
│   │   │   ├── fred_metadata_validation.py
│   │   │   ├── fred_validation.py
│   │   │   ├── gold_fred_validation.py
│   │   │   ├── gold_macro_impact_validation.py
│   │   │   ├── gold_ohlcv_validation.py
│   │   │   ├── gold_sentiment_lead_lag_validation.py
│   │   │   ├── gold_sentiment_returns_validation.py
│   │   │   ├── gold_sentiment_sector_validation.py
│   │   │   ├── gold_sentiment_validation.py
│   │   │   ├── ohlcv_validation.py
│   │   │   ├── qqq_categories_validation.py
│   │   │   └── qqq_entities_validation.py
│   │   └── write/                    # Writery tabel Delta (overwrite, append, upsert)
│   │       └── delta_table_writer.py
│   └── pipelines/                    # Entrypointy orkiestracji
│       ├── run_av_sentiment_api_to_bronze.py
│       ├── run_av_sentiment_bronze_to_silver.py
│       ├── run_av_sentiment_history_api_to_bronze.py
│       ├── run_fred_api_to_bronze.py
│       ├── run_fred_bronze_to_silver.py
│       ├── run_gold_daily_sentiment_per_sector.py
│       ├── run_gold_fred_with_dimension.py
│       ├── run_gold_macro_impact_on_tech.py
│       ├── run_gold_ohlcv_with_dimension.py
│       ├── run_gold_sentiment_daily_agg.py
│       ├── run_gold_sentiment_lead_lag.py
│       ├── run_gold_sentiment_vs_returns.py
│       ├── run_ohlcv_api_to_bronze.py
│       ├── run_ohlcv_bronze_to_silver.py
│       ├── run_qqq_categories_api_to_bronze.py
│       ├── run_qqq_categories_bronze_to_silver.py
│       ├── run_qqq_entities_bronze_to_silver.py
│       ├── run_qqq_entities_raw_to_bronze.py
│       └── run_vacuum.py
├── tests/
│   └── etl/
│       ├── cleaning/
│       │   ├── test_av_sentiment_cleaning.py
│       │   ├── test_fred_cleaning.py
│       │   ├── test_fred_metadata_cleaning.py
│       │   └── test_ohlcv_cleaning.py
│       ├── transformations/
│       │   ├── test_av_json_parser.py
│       │   ├── test_av_sentiment_aggregation.py
│       │   ├── test_fred_xml_parser.py
│       │   ├── test_ohlcv_return_calculation.py
│       │   └── test_sentiment_sector_aggregation.py
│       └── test_fred_client.py
├── notebooks/
│   └── eda/                          # Notebooki eksploracyjnej analizy danych
│       ├── 01_data_completness.sql.ipynb
│       ├── 02_fred_macro_quality.sql.ipynb
│       ├── 03_ohlcv_data_quality.sql.ipynb
│       ├── 04_sentiment_quality.sql.ipynb
│       ├── 05_gold_readiness.sql.ipynb
│       └── 06_cross_source_coverage.sql.ipynb
├── debug/                            # Notebooki debugowe (nie do produkcji)
├── conftest.py                       # Fixture SparkSession dla pytest
├── requirements.txt
└── README.md
```

---

## Konfiguracja i bezpieczeństwo

- Sekrety (FRED API Key, Alpha Vantage API Key) przechowywane w **Databricks Secrets** (`my-scope`).
- Ścieżki, nazwy tabel i parametry ładowane przez `config_loader` z plików YAML per środowisko (`dev.yaml`, `test.yaml`, `prod.yaml`).
- Brak sekretów i poświadczeń w kodzie lub plikach konfiguracyjnych.

---

## Monitoring

Każdy pipeline loguje metadane uruchomienia do tabeli Delta `ops.pipeline_runs`:
- run_id (UUID), pipeline_name, layer, status (SUCCESS/FAILURE)
- started_at, finished_at, duration_seconds
- input_rows, output_rows, rows_rejected
- source_table, target_table, config_params, error_message

---

## Retencja danych

Delta VACUUM uruchamiany na wszystkich skonfigurowanych tabelach z retencją 7 dni (168 godzin). Konfiguracja w `dev.yaml` pod kluczem `vacuum.tables`.

---

## Zależności

Zależności runtime w `requirements.txt`:
- `yfinance==1.0` - ekstrakcja danych rynkowych
- `lxml` - parsowanie XML
- `html5lib` - fallback parsowania HTML

Dodatkowe zależności dostępne w Databricks runtime: `pyspark`, `delta-spark`, `requests`, `pyyaml`.

Część zależności instalowana bezpośrednio na poziomie Databricks Job/Pipeline.

---

## Znane ograniczenia

- Alpha Vantage darmowy tier: 25 requestów/dzień - ogranicza świeżość danych sentymentu i prędkość backfillu.
- Plik CSV podmiotów QQQ ETF wymaga ręcznego pobrania i umieszczenia.
- Pokrycie sentymentem jest nierównomierne między symbolami (NVDA: 387 artykułów vs ARM: 3 artykuły).
- Sektory z <5 symbolami mają niewystarczające dane do wiarygodnych agregacji sektorowych.
- Symbole benchmarkowe (^VIX, QQQ, SPY, TLT, GLD) celowo nie posiadają metadanych wymiarowych (sector, industry).

---

## Planowane

- Integracja źródeł newsowych i danych geopolitycznych
- Model scoringu sektorowego
- Modele predykcyjne i AI-driven investment insights
- Pipeline CI/CD (GitHub Actions)

---

## Linki

- [Invesco QQQ ETF constituents (CSV)](https://www.barchart.com/etfs-funds/quotes/QQQ/constituents)
- [Dokumentacja yfinance](https://ranaroussi.github.io/yfinance/reference/api/yfinance.download.html#yfinance.download)
- [Dokumentacja FRED API](https://fred.stlouisfed.org/docs/api/fred/)
- [Dokumentacja Alpha Vantage API](https://www.alphavantage.co/documentation/)