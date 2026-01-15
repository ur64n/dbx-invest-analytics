# dbx-invest-analytics EN

## Project Overview

`dbx-invest-analytics` is an analytical and predictive project aimed at assessing the impact of market, macroeconomic, and geopolitical factors on the technology sector and technology-focused ETFs (with a primary focus on QQQ).

The project integrates:
- market data (ETF → companies),
- macroeconomic indicators,
- and in later stages: news, political decisions, and geopolitical events,

and applies analytical techniques and AI to generate investment-related insights.

---

## Technology Stack

The project is built entirely using:

- **Databricks Free Edition**
- **Unity Catalog**
- **Delta Lake**
- **Python (Spark, SQL)**
- **GitHub (dev / test / prod)**

The data architecture follows the **Medallion Architecture (Bronze → Silver → Gold)**.  
Process orchestration is handled via **Databricks Jobs & Pipelines**.

---

## Current Data Scope

### 1. Market Data – ETF QQQ

- Source: Invesco QQQ ETF constituents (CSV)
- Scope:
  - list of companies,
  - sectors / business categories,
- Status:
  - data transformed into the **Silver layer**,
  - validation and cleaning completed,
  - stored as **Delta Tables**.

The source QQQ ETF file must be downloaded **manually** and placed in:
/Volumes/workspace/bronze/raw


---

### 2. Macro-Economic Data – FRED API

Macroeconomic data is extracted from the **FRED API** in XML format.

Currently used indicators:
- **CPIAUCSL** – Consumer Price Index (inflation)
- **CPILFESL** – Core Consumer Price Index
- **T10YIE** – 10-Year Breakeven Inflation Rate
- **FEDFUNDS** – Federal Funds Effective Rate
- **DGS10** – 10-Year Treasury Yield
- **DGS2** – 2-Year Treasury Yield
- **UNRATE** – Unemployment Rate
- **INDPRO** – Industrial Production Index

Data frequency:
- daily or monthly (depending on the indicator)

Status:
- data ingested into the **Bronze layer** as raw XML files,
- the first run retrieves the full available history,
- subsequent runs operate **incrementally**, refreshing:
  - newly available data,
  - historical revisions within the last **6 months**.

Time-range filtering and normalization are handled later in the **Silver layer**.

---

## Project Structure

The project follows a **Modular Python Project Structure**.

Main directories: 
- src/etl (extraction/transformation/cleaning...)
config files 
utils



The codebase is:
- modular,
- encapsulated,
- class- and method-based,
- designed for extensibility and scalability.

---

## Configuration & Secrets

- Secrets (e.g., FRED API key) are managed via **Databricks Secrets**.
- Paths and runtime parameters are defined in the `config` module.

---

## Dependencies

- Required libraries are listed in the `requirements` file.
- Some dependencies are installed directly at the **Databricks Job / Pipeline** level.

---

## Project Status

**Completed:**
- project structure,
- configuration and validations,
- Silver layer for ETF QQQ,
- macro data extraction from FRED API.

**Planned:**
- Silver transformations for macro data,
- integration of news and geopolitical data,
- sector-level scoring,
- predictive models and AI-driven insights.

---

## Useful Links

- Invesco QQQ ETF constituents (CSV):  
  https://www.barchart.com/etfs-funds/quotes/QQQ/constituents

# dbx-invest-analytics PL

## Project Overview

`dbx-invest-analytics` to projekt analityczno-predykcyjny, którego celem jest ocena wpływu czynników rynkowych, makroekonomicznych oraz geopolitycznych na sektor technologiczny oraz ETF-y technologiczne (w szczególności QQQ).

Projekt integruje dane:
- rynkowe (ETF → spółki),
- makroekonomiczne,
- w dalszych etapach: newsy, decyzje polityczne i dane geopolityczne,

a następnie wykorzystuje techniki analityczne i AI do generowania wniosków inwestycyjnych.

---

## Technology Stack

Projekt jest budowany w całości w:

- **Databricks Free Edition**
- **Unity Catalog**
- **Delta Lake**
- **Python (Spark, SQL)**
- **GitHub (dev / test / prod)**

Architektura danych oparta jest o **Medallion Architecture (Bronze → Silver → Gold)**.  
Orkiestracja procesów realizowana jest przy użyciu **Databricks Jobs & Pipelines**.

---

## Current Data Scope

### 1. Market Data – ETF QQQ

- Źródło: Invesco QQQ ETF constituents (CSV)
- Zakres:
  - lista spółek,
  - sektory / kategorie działalności,
- Status:
  - dane przetworzone do **warstwy Silver**,
  - walidacja i czyszczenie wykonane,
  - zapis w formacie **Delta Table**.

Plik źródłowy ETF QQQ musi być pobierany **ręcznie** i umieszczony w:
/Volumes/workspace/bronze/raw


---


---

### 2. Macro-Economic Data – FRED API

Dane makroekonomiczne pobierane są z **FRED API** w formacie XML.

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
  - rewizje z ostatnich **6 miesięcy**.

Ograniczenie zakresu czasowego oraz dalsza normalizacja danych realizowana będzie w **warstwie Silver**.

---

## Project Structure

Projekt wykorzystuje **Modular Python Project Structure**.

Główne katalogi:

src/etl (extraction/transformation/cleaning...)
config files 
utils


Kod jest:
- modularny,
- enkapsulowany,
- oparty o klasy i metody,
- przygotowany pod dalszą rozbudowę pipeline’ów.

---

## Configuration & Secrets

- Sekrety (np. FRED API key) przechowywane są w **Databricks Secrets**.
- Konfiguracja ścieżek i parametrów znajduje się w module `config`.

---

## Dependencies

- Wymagane biblioteki znajdują się w pliku `requirements`.
- Część zależności instalowana jest bezpośrednio na poziomie **Databricks Job / Pipeline**.

---

## Project Status

**Zrealizowane:**
- struktura projektu,
- konfiguracja i walidacje,
- Silver layer dla ETF QQQ,
- ekstrakcja danych makro z FRED API.

**W planach:**
- transformacje Silver dla danych makro,
- integracja newsów i danych geopolitycznych,
- scoring sektorowy,
- modele predykcyjne i AI-driven insights.

---

## Useful Links

- Invesco QQQ ETF constituents (CSV):  
  https://www.barchart.com/etfs-funds/quotes/QQQ/constituents
