# DG ECFIN Data Exploration - Findings

**Date:** December 10, 2025

---

## Summary

DG ECFIN (European Commission Directorate-General for Economic and Financial Affairs) publishes **Business and Consumer Surveys** monthly since 1961.

---

## Data Access

### **No Direct API** ❌
- DG ECFIN does not provide a direct REST API
- Data is available as **ZIP file downloads** containing Excel files

### Download URL
**Main Indicators (Seasonally Adjusted):**
```
https://ec.europa.eu/economy_finance/db_indicators/surveys/documents/series/nace2_ecfin_2511/main_indicators_sa_nace2.zip
```

### File Structure
- **Format:** ZIP containing Excel (.xlsx) file
- **Excel file:** `main_indicators_nace2.xlsx`
- **Sheets:**
  - `Index` - Country codes reference
  - `INFO` - Methodology and updates
  - **`MONTHLY`** - Main data sheet with time series

---

## Available Indicators (EU & Euro Area)

From the MONTHLY sheet, key indicators include:

### 1. **ESI - Economic Sentiment Indicator**
- Column: `EU.ESI` (European Union) or `EA.ESI` (Euro Area)
- Latest (Nov 2025): 96.8 (EU)
- Description: Composite indicator from industry, services, retail, construction, consumer surveys

### 2. **EEI - Employment Expectations Indicator**
- Column: `EU.EEI` (European Union) or `EA.EEI` (Euro Area)
- Latest (Nov 2025): 98.8 (EU)
- Description: Employment expectations across sectors

### 3. **Consumer Confidence**
- Column: `EU.CONS` (European Union) or `EA.CONS` (Euro Area)
- Latest (Nov 2025): -13.6 (EU), -14.2 (Euro area)
- Description: Consumer confidence indicator

### Other Available Indicators:
- **INDU** - Industry confidence
- **SERV** - Services confidence
- **RETA** - Retail trade confidence
- **BUIL** - Construction/Building confidence

---

## Data Coverage

- **Start Date:** January 1985
- **End Date:** November 2025 (updated monthly)
- **Frequency:** Monthly
- **Countries:** EU, EA (Euro area), plus 27+ individual countries
- **Seasonal Adjustment:** Both SA (seasonally adjusted) and NSA (non-seasonally adjusted) available

---

## Data Format

The Excel file "MONTHLY" sheet structure:
```
Column 0: Date (datetime)
Column 1: Empty separator
Column 2: EU.INDU (Industry)
Column 3: EU.SERV (Services)
Column 4: EU.CONS (Consumer)
Column 5: EU.RETA (Retail)
Column 6: EU.BUIL (Construction)
Column 7: EU.ESI (Economic Sentiment Indicator)
Column 8: EU.EEI (Employment Expectations)
... (then repeats for EA, BE, BG, CZ, etc.)
```

Example data:
```
Date         EU.ESI  EU.EEI  EU.CONS
1985-01-31   95.0    81.5    -10.2
1985-02-28   93.7    79.3    -10.6
...
2025-11-30   96.8    98.8    -13.6
```

---

## Alternative Access Methods

### 1. ECB Data Portal
- URL: https://data.ecb.europa.eu/data/datasets/ECS
- Provides same ECFIN data through ECB interface
- May have API access through ECB (not explored yet)

### 2. Eurostat
- Some BCS data available through Eurostat database
- URL: https://ec.europa.eu/eurostat/data/database

### 3. European Open Data Portal
- URL: https://data.europa.eu/euodp/data/publisher/ecfin
- Provides various datasets including BCS

---

## Update Schedule

- **Flash Consumer Confidence:** Mid-month (e.g., Dec 19, 2025)
- **Full Survey Results:** End of month
- **Next Update:** December 19, 2025

---

## Citation

When using this data:
```
European Commission, Directorate-General for Economic and Financial Affairs
Business and Consumer Surveys
Joint Harmonised EU Programme of Business and Consumer Surveys
https://economy-finance.ec.europa.eu/economic-forecast-and-surveys/business-and-consumer-surveys_en
```

---

## Scraper Implementation Options

### Option A: ZIP Download + Excel Parsing ⭐ (Recommended)
- Download ZIP file
- Extract Excel file
- Parse MONTHLY sheet with pandas
- Extract specific columns (EU.ESI, EU.EEI, EU.CONS)
- Save as CSV

**Pros:**
- Simple, reliable
- No API dependencies
- Gets all historical data

**Cons:**
- Requires downloading full file (~700KB)
- Requires openpyxl for Excel parsing

### Option B: ECB Data Portal API
- Explore ECB's API for ECFIN data
- May provide REST API access

**Pros:**
- True API access
- More flexible queries

**Cons:**
- Requires additional exploration
- May not have all fields
- More complex authentication

---

## Recommended Fields to Scrape

Based on typical requirements:

1. **EU.ESI** - Economic Sentiment Indicator
2. **EU.EEI** - Employment Expectations Indicator
3. **EU.CONS** - Consumer Confidence
4. **EA.ESI** - Euro Area ESI
5. **EA.EEI** - Euro Area EEI
6. **EA.CONS** - Euro Area Consumer Confidence

Or specific country if needed (e.g., US has no data, this is EU only)

---

## Next Steps

1. Confirm with user which specific indicators/countries needed
2. Build scraper using Option A (ZIP + Excel parsing)
3. Extract time series data
4. Save to CSV with metadata

---

## Sources

- [DG ECFIN Business and Consumer Surveys](https://economy-finance.ec.europa.eu/economic-forecast-and-surveys/business-and-consumer-surveys_en)
- [Time Series Downloads](https://economy-finance.ec.europa.eu/economic-forecast-and-surveys/business-and-consumer-surveys/download-business-and-consumer-survey-data/time-series_en)
- [Latest Surveys](https://economy-finance.ec.europa.eu/economic-forecast-and-surveys/business-and-consumer-surveys/latest-business-and-consumer-surveys_en)
- [ECB Data Portal - ECFIN Surveys](https://data.ecb.europa.eu/data/datasets/ECS)
