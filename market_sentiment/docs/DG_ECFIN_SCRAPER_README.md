# DG ECFIN Business and Consumer Surveys Scraper - Usage Guide

## ✅ Status: PRODUCTION READY

The DG ECFIN scraper is **fully tested and working**. All 5 requested fields are successfully scraped.

---

## Quick Start

### Run the scraper:
```bash
python FRED/dg_ecfin_scraper.py
```

Output files:
- `FRED/data/dg_ecfin_surveys.csv` - All survey data
- `FRED/data/dg_ecfin_metadata.json` - Complete metadata

---

## Data Retrieved

### All 5 Requested Fields:

1. **ESI (EU) - Economic Sentiment Indicator (European Union)**
   - Latest: 96.8 (Nov 2025)
   - Range: 57.7 - 119.0
   - Coverage: 100%

2. **ESI (Euro Area) - Economic Sentiment Indicator (Euro Area)**
   - Latest: 97.0 (Nov 2025)
   - Range: 58.6 - 119.6
   - Coverage: 100%

3. **EEI (EU) - Employment Expectations Indicator (European Union)**
   - Latest: 98.8 (Nov 2025)
   - Range: 51.1 - 116.4
   - Coverage: 100%

4. **EEI (Euro Area) - Employment Expectations Indicator (Euro Area)**
   - Latest: 97.8 (Nov 2025)
   - Range: 50.7 - 116.2
   - Coverage: 100%

5. **Flash Consumer Confidence (Euro Area)**
   - Latest: -14.2 (Nov 2025)
   - Range: -28.8 to -1.8
   - Coverage: 100%

---

## Output Files

### CSV File (`dg_ecfin_surveys.csv`)
```csv
date,year,month,esi_eu,esi_ea,eei_eu,eei_ea,flash_consumer_confidence_ea
1985-01-31,1985,1,95.0,94.1,81.5,80.4,-10.4
1985-02-28,1985,2,93.7,92.8,79.3,77.0,-10.8
...
2025-11-30,2025,11,96.8,97.0,98.8,97.8,-14.2
```

**Columns:**
- `date` - Observation date
- `year` - Year
- `month` - Month number
- `esi_eu` - Economic Sentiment Indicator (EU)
- `esi_ea` - Economic Sentiment Indicator (Euro Area)
- `eei_eu` - Employment Expectations Indicator (EU)
- `eei_ea` - Employment Expectations Indicator (Euro Area)
- `flash_consumer_confidence_ea` - Flash Consumer Confidence (Euro Area)

### Metadata File (`dg_ecfin_metadata.json`)
Contains complete metadata for all fields including:
- Source information
- Download URL
- Date ranges
- Coverage statistics
- Latest values

---

## Data Coverage

- **Total Observations:** 491 monthly records
- **Date Range:** Jan 1985 - Nov 2025 (40+ years)
- **Coverage:** 100% for all 5 fields
- **Frequency:** Monthly
- **Seasonal Adjustment:** Seasonally adjusted

---

## How It Works

1. **Download:** Fetches ZIP file from DG ECFIN website
2. **Extract:** Extracts Excel file from ZIP
3. **Parse:** Reads MONTHLY sheet and extracts specific columns
4. **Transform:** Converts to clean CSV with proper date parsing
5. **Validate:** Checks coverage and calculates statistics
6. **Save:** Outputs CSV data + JSON metadata

---

## Features

### ✅ All Requested Fields
- ESI (EU) ✓
- ESI (Euro Area) ✓
- EEI (EU) ✓
- EEI (Euro Area) ✓
- Flash Consumer Confidence (Euro Area) ✓

### ✅ Robust & Reliable
- Retry logic (3 attempts)
- Automatic ZIP download/extract
- Excel parsing with pandas/openpyxl
- Date validation and cleaning
- 100% coverage on all fields

---

## Update Schedule

- **Frequency:** Monthly
- **Flash Consumer Confidence:** Mid-month (e.g., Dec 19, 2025)
- **Full Results:** End of month
- **Data Updates:** Monthly from European Commission

---

## Validation Results

From test run (December 10, 2025):

```
✓ ESI (EU): 100.0% coverage, latest 96.8 (Nov 2025)
✓ ESI (Euro Area): 100.0% coverage, latest 97.0 (Nov 2025)
✓ EEI (EU): 100.0% coverage, latest 98.8 (Nov 2025)
✓ EEI (Euro Area): 100.0% coverage, latest 97.8 (Nov 2025)
✓ Flash Consumer Confidence (EA): 100.0% coverage, latest -14.2 (Nov 2025)
```

---

## Performance

- **Runtime:** ~1.8 seconds
- **Download size:** ~690KB ZIP
- **Output size:** ~30KB CSV
- **Memory:** ~10-15MB

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

## Dependencies

```bash
pip install pandas openpyxl requests
```

---

## All Requested Fields ✅

- [x] ESI (EU)
- [x] ESI (Euro Area)
- [x] EEI (EU)
- [x] EEI (Euro Area)
- [x] Flash Consumer Confidence (Euro Area)

---

## 🎉 Status: IN THE BAG! 🎉

All 5 DG ECFIN fields are:
- ✅ Successfully scraped
- ✅ 100% coverage
- ✅ Validated and tested
- ✅ Ready for production use
