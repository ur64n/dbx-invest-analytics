# dbx-invest-analytics EN

## Project Description

`dbx-invest-analytics` is an analytical and predictive project whose goal is to assess the impact of market, macroeconomic, and geopolitical factors on the technology sector and technology ETFs (especially QQQ).

The project integrates data:
- market (ETF → companies),
- macroeconomic,
- in later stages: news, political decisions, and geopolitical data,

and then uses analytical techniques and AI to generate investment insights.

The project implements **deterministic ETL pipelines** with a clear separation of responsibilities (extraction / transformation / validation / cleaning / orchestration).

---

## Technology Stack

The project is built entirely using:

- **Databricks Free Edition**
- **Unity Catalog**
- **Delta Lake**
- **Python (Spark, SQL)**
- **GitHub (dev / test / prod)**
- **Databricks Jobs & Pipelines**

---

## Current Data Scope

### 1. Invesco QQQ ETF Constituents

- Source: Invesco QQQ ETF constituents (CSV)
- Scope:
  - list of companies,
  - sectors / business categories,
- Status:
  - data processed into the **Silver layer**,
  - validation and cleaning completed,
  - stored as **Delta Tables**.

**Input:**
The source QQQ ETF file must be downloaded **manually** and placed in:  
`/Volumes/workspace/bronze/raw`

---

### 2. Macroeconomic Data – FRED API

Macroeconomic data is fetched from the **FRED API** in XML format.

Currently used indicators:
- **CPIAUCSL** – inflation (CPI)
- **CPILFESL** – core inflation (Core CPI)
- **T10YIE** – 10-year inflation expectations
- **FEDFUNDS** – Federal Funds Rate
- **DGS10** – 10-year Treasury yield
- **DGS2** – 2-year Treasury yield
- **UNRATE** – unemployment rate
- **INDPRO** – industrial production

Data frequency:
- daily or monthly (depending on the indicator)

Status:
- data ingested into the **Bronze layer** as raw XML files,
- the first run loads full historical data,
- subsequent runs operate **incrementally** and refresh:
  - new data,
  - revisions from the last **6 months**,
  - overwrite Bronze files per indicator,
- parsing, validation, normalization, and persistence are handled in the **Silver layer**.
- Extraction of metadata (unit and frequency) in json format.
- Transformation of metadata to dataframe, standardization, and validation.
- Synchronization of metadata (UPSERT)
- Enrichment of fact tables with metadata (JOIN)
- Canonization of indicator_id key (lower+trim)
- Canonization of frequency daily, monthly, quarterly, annual -> (d, m, q, a)

---

## Project Architecture

The project follows a **Modular Python Project Structure** with clear responsibility contracts.

**Main directories:**
- `src/pipelines` — orchestration (entrypoints),
- `src/etl/extraction` — data extraction and reading (I/O),
-  `src/etl/enrichment` — data enrichment and merging,
- `src/etl/transformations` — transformations without I/O,
- `src/etl/validation` — data contract validation,
- `src/etl/cleaning` — data standardization,
- `src/etl/schema` — data schemas,
- `src/config` — configuration and logging,
- `src/utils` — helper utilities,
- `tests/` — unit tests.

**Principles:**
- the pipeline is the only entrypoint,
- only the pipeline performs I/O and controls execution order,
- ETL modules are pure, deterministic, and testable,
- no side effects on import.

The project is based on the Medallion architecture (Bronze / Silver / Gold).

---

## Configuration and Security

- secrets (e.g. FRED API Key) are stored in **Databricks Secrets**,
- paths and parameters are loaded via `config_loader`,
- dependencies are defined in the `requirements` file.

---

## Dependencies

- required libraries are listed in the `requirements` file,
- some dependencies are installed directly at the **Databricks Job / Pipeline** level.

---

## Project Status

**Completed:**
- Silver pipeline for ETF QQQ (Bronze → Silver),
- macroeconomic FRED pipeline + metadata (Bronze → Silver),
- data contract validations,
- data cleaning and normalization,
- deterministic orchestration,
- clear separation of module responsibilities.

**Planned:**
- Gold layer (aggregations, analytics),
- integration of news and geopolitical data,
- sector scoring,
- predictive models and AI-driven insights.

---

## Links

- Invesco QQQ ETF constituents (CSV):  
  https://www.barchart.com/etfs-funds/quotes/QQQ/constituents
- yfinance (download) docs
  https://ranaroussi.github.io/yfinance/reference/api/yfinance.download.html#yfinance.download

# dbx-invest-analytics PL

## Opis Projektu

`dbx-invest-analytics` to projekt analityczno-predykcyjny, którego celem jest ocena wpływu czynników rynkowych, makroekonomicznych oraz geopolitycznych na sektor technologiczny oraz ETF-y technologiczne (w szczególności QQQ).

Projekt integruje dane:
- rynkowe (ETF → spółki),
- makroekonomiczne,
- w dalszych etapach: newsy, decyzje polityczne i dane geopolityczne,

a następnie wykorzystuje techniki analityczne i AI do generowania wniosków inwestycyjnych.

Projekt realizuje **deterministyczne pipeline’y ETL** z wyraźnym rozdziałem odpowiedzialności (ekstrakcja / transformacja / walidacja / czyszczenie / orkiestracja).

---

## Technology Stack

Projekt jest budowany w całości w:

- **Databricks Free Edition**
- **Unity Catalog**
- **Delta Lake**
- **Python (Spark, SQL)**
- **GitHub (dev / test / prod)**
- **Databricks Jobs & Pipelines**

---

## Obecny zakres danych

### 1. Dane podmiotów QQQ ETF Invesco

- Źródło: Invesco QQQ ETF constituents (CSV)
- Zakres:
  - lista spółek,
  - sektory / kategorie działalności,
- Status:
  - dane przetworzone do **warstwy Silver**,
  - walidacja i czyszczenie wykonane,
  - zapis w formacie **Delta Table**.

**Input:**
Plik źródłowy ETF QQQ musi być pobierany **ręcznie** i umieszczony w:
/Volumes/workspace/bronze/raw

---

### 2. Dane makroekonomiczne - FRED API

Dane makroekonomiczne pobierane są iteracyjnie z **FRED API** w formacie XML.

Aktualnie wykorzystywane wskaźniki:
- **CPIAUCSL** – inflacja (CPI)
- **CPILFESL** – inflacja bazowa (Core CPI)
- **T10YIE** – oczekiwania inflacyjne (10Y)
- **FEDFUNDS** – stopa procentowa FED
- **DGS10** – rentowność obligacji 10Y
- **DGS2** – rentowność obligacji 2Y
- **UNRATE** – stopa bezrobocia
- **INDPRO** – produkcja przemysłowa

Częstotliwość danych:
- dzienna lub miesięczna (zależnie od wskaźnika)

Status:
- dane pobierane do **warstwy Bronze** jako surowe pliki XML,
- pierwszy run pobiera pełną historię źródła,
- kolejne runy działają **inkrementacyjnie** i odświeżają:
  - nowe dane,
  - rewizje z ostatnich **12 miesięcy**.
  - nadpisują pliki w warstwie Bronze per wskaźnik,
- parsowanie, walidacja, normalizacja i zapis realizowane w warstwie **Silver**.
- Extrakcja metadanych (unit i frequency) w formacie json.
- Transformacja metadanych do dataframe, standaryzacja i walidacja.
- Synchronizacja metadanych (UPSERT)
- Wzbogacenie tabeli faktów o metadane (JOIN)
- Kanonizacja klucza indicator_id (lower+trim)
- Kanonizacja frequency daily,monthly,quarterly,annual -> (d,m,q,a)

---

## Architektura projektu

Projekt wykorzystuje **Modular Python Project Structure** z kontraktami odpowiedzialności.

**Główne katalogi:**
- `src/pipelines` — orkiestracja (entrypointy),
- `src/etl/extraction` — ekstrakcja i odczyt danych (I/O),
- `src/etl/enrichment` — wzbogacenie i złączenia danych,
- `src/etl/transformations` — transformacje bez I/O,
- `src/etl/validation` — walidacja kontraktów danych,
- `src/etl/cleaning` — standaryzacja danych,
- `src/etl/schema` — schematy danych,
- `src/config` — konfiguracja i logowanie,
- `src/utils` — narzędzia pomocnicze,
- `tests/` — testy jednostkowe.

**Zasady:**
- pipeline jest jedynym entrypointem,
- tylko pipeline wykonuje I/O i steruje kolejnością kroków,
- moduły ETL są czyste, deterministyczne i testowalne,
- brak efektów ubocznych przy imporcie.

Projekt oparty jest o architekturę Medallion (Bronze / Silver / Gold)

---

## Konfiguracja i bezpieczeństwo

- sekrety (np. FRED API Key) przechowywane w **Databricks Secrets**,
- ścieżki i parametry w config ładowane przez `config_loader`,
- zależności w pliku `requirements`.

---

## Zaleznosci

- Wymagane biblioteki znajdują się w pliku `requirements`.
- Część zależności instalowana jest bezpośrednio na poziomie **Databricks Job / Pipeline**.

---

## Status projektu

**Zrealizowane:**
- pipeline Silver dla ETF QQQ, (Bronze → Silver),
- pipeline makroekonomiczny FRED + metadane (Bronze → Silver),
- walidacje kontraktów danych,
- czyszczenie i normalizacja,
- deterministyczna orkiestracja,
- rozdzielenie odpowiedzialności modułów.

**W planach:**
- warstwa Gold (agregacje, analizy),
- integracja newsów i danych geopolitycznych,
- scoring sektorowy,
- modele predykcyjne i AI-driven insights.

---

## Linki
- Invesco QQQ ETF constituents (CSV):  
  https://www.barchart.com/etfs-funds/quotes/QQQ/constituents
  
- yfinance (download) dokumentacja
  https://ranaroussi.github.io/yfinance/reference/api/yfinance.download.html#yfinance.download
