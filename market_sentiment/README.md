# FRED Data Pipeline

Federal Reserve Economic Data (FRED) scraping and data pipeline for client-requested economic indicators.

## Overview

This pipeline collects economic data from the Federal Reserve Bank of St. Louis FRED API, focusing on consumer confidence, sentiment, and inflation expectations indicators.

## Project Structure

```
FRED/
├── README.md                 # This file
├── docs/                     # Documentation
│   ├── API_DOCUMENTATION.md           # Complete FRED API reference
│   ├── CLIENT_DATA_SOURCES.md         # Detailed info on each client data source
│   └── FINAL_DATA_SOURCES.md          # Quick reference summary of all sources
├── exploration/              # Research and exploration scripts
│   ├── fred_explorer.py               # General FRED API explorer
│   ├── explore_client_data.py         # Client-specific data exploration
│   ├── check_release_series.py        # Check series in releases
│   └── check_oecd_series.py           # Check OECD series details
├── umich_scraper.py           # ✅ Production UMich scraper
├── UMICH_SCRAPER_README.md    # Usage guide for UMich scraper
├── data/                      # Output data files
│   ├── umich_data_combined.csv         # Combined output
│   ├── umich_sentiment_raw.csv         # Raw sentiment data
│   ├── umich_components_raw.csv        # Raw components data
│   └── umich_inflation_raw.csv         # Raw inflation data
└── (FRED API integration - TO BE ADDED)
```

## Data Sources

The pipeline collects data from 8 sources:

| # | Source | Series/Release ID | Frequency |
|---|--------|-------------------|-----------|
| 1 | Surveys of Consumers | Release `91` | Monthly |
| 2 | University of Michigan Consumer Sentiment | `UMCSENT` | Monthly |
| 3 | 1-Year Expected Inflation | `EXPINF1YR` | Monthly |
| 4 | 10-Year Breakeven Inflation Rate | `T10YIE` | **Daily** |
| 5 | OECD Composite Consumer Confidence | `USACSCICP02STSAM` | Monthly |
| 6 | OECD Consumer Confidence Amplitude Adjusted | `CSCICP03USM665S` | Monthly |
| 7 | 10-Year Expected Inflation | `EXPINF10YR` | Monthly |
| 8 | Surveys of Consumers - Table Data | Release `91` tables | Monthly |

## Setup

1. **API Key**: Your FRED API key is stored in `.env`:
   ```
   FRED_API_KEY=39f68bda70f9e7933910743125870553
   ```

2. **Rate Limits**: 120 requests per minute

3. **Dependencies**:
   - `requests` - HTTP library
   - `python-dotenv` - Environment variable management

## Quick Start

### Explore the API
```bash
python exploration/fred_explorer.py
```

### Explore Client Data Sources
```bash
python exploration/explore_client_data.py
```

## Documentation

- **[API_DOCUMENTATION.md](docs/API_DOCUMENTATION.md)** - Complete FRED API reference with all endpoints, parameters, and usage examples
- **[CLIENT_DATA_SOURCES.md](docs/CLIENT_DATA_SOURCES.md)** - Detailed information about each of the 8 client-requested data sources
- **[FINAL_DATA_SOURCES.md](docs/FINAL_DATA_SOURCES.md)** - Quick reference summary table of all sources

## Quick Start

### Run All Scrapers:

**1. UMich Scraper** (5 fields from University of Michigan):
```bash
python FRED/umich_scraper.py
```
**Output:** `FRED/data/umich_data_combined.csv` | **Docs:** [docs/umich/UMICH_SCRAPER_README.md](docs/umich/UMICH_SCRAPER_README.md)

**2. FRED Breakeven Inflation** (2 series):
```bash
python FRED/fred_breakeven_scraper.py
```
**Output:** `FRED/data/fred_breakeven_inflation.csv` | **Docs:** [docs/BREAKEVEN_SCRAPER_README.md](docs/BREAKEVEN_SCRAPER_README.md)

**3. FRED OECD Confidence** (2 series):
```bash
python FRED/fred_oecd_scraper.py
```
**Output:** `FRED/data/fred_oecd_confidence.csv` | **Docs:** [docs/OECD_SCRAPER_README.md](docs/OECD_SCRAPER_README.md)

**4. FRED Cleveland Fed Inflation** (2 series):
```bash
python FRED/fred_cleveland_scraper.py
```
**Output:** `FRED/data/fred_cleveland_inflation.csv` | **Docs:** [docs/CLEVELAND_SCRAPER_README.md](docs/CLEVELAND_SCRAPER_README.md)

**5. FRED UMCSENT** (1 series + metadata):
```bash
python FRED/fred_umcsent_scraper.py
```
**Output:** `FRED/data/fred_umcsent.csv` | **Docs:** [docs/UMCSENT_SCRAPER_README.md](docs/UMCSENT_SCRAPER_README.md)

**6. DG ECFIN Surveys** (5 EU indicators):
```bash
python FRED/dg_ecfin_scraper.py
```
**Output:** `FRED/data/dg_ecfin_surveys.csv` | **Docs:** [docs/DG_ECFIN_SCRAPER_README.md](docs/DG_ECFIN_SCRAPER_README.md)

---

## ✅ Completed - All 6 Scrapers Production Ready

### 1. UMich Scraper ✅
- 5 University of Michigan fields
- 667 monthly observations (1952-2025)
- 100% production ready

### 2. FRED Breakeven Inflation ✅
- T10YIE, T5YIFR
- 5,738 daily observations (2003-2025)
- All metadata captured

### 3. FRED OECD Confidence ✅
- USACSCICP02STSAM, CSCICP03USM665S
- 790 monthly observations (1960-2025)
- All metadata captured

### 4. FRED Cleveland Fed Inflation ✅
- EXPINF1YR, EXPINF10YR
- 526 monthly observations (1982-2025)
- All metadata captured

### 5. FRED UMCSENT ✅
- UMCSENT series + table metadata
- 666 monthly observations (1952-2025)
- All requested fields

### 6. DG ECFIN Surveys ✅
- 5 EU/EA indicators (ESI, EEI, Consumer Confidence)
- 491 monthly observations (1985-2025)
- ZIP download + Excel parsing

## 📊 Total Data Coverage

**17 Series | 6 Scrapers | 100% Complete**

## API Access

Base URL: `https://api.stlouisfed.org/fred`

### Example: Get Series Observations
```python
import requests
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv('FRED_API_KEY')

response = requests.get(
    'https://api.stlouisfed.org/fred/series/observations',
    params={
        'api_key': api_key,
        'series_id': 'UMCSENT',
        'file_type': 'json'
    }
)
data = response.json()
```

## Citation Requirements

Some data sources require citation:
- **UMCSENT**: "Surveys of Consumers, University of Michigan"
- **T10YIE**: Federal Reserve Bank of St. Louis
- **OECD Series**: "OECD (year), (dataset name), https://data-explorer.oecd.org/"
