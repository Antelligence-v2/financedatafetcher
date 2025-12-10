# FRED OECD Consumer Confidence Scraper - Usage Guide

## ✅ Status: PRODUCTION READY

The FRED OECD consumer confidence scraper is **fully tested and working**. Both series are successfully scraped with all requested fields.

---

## Quick Start

### Run the scraper:
```bash
python FRED/fred_oecd_scraper.py
```

Output files:
- `FRED/data/fred_oecd_confidence.csv` - Combined data
- `FRED/data/fred_oecd_metadata.json` - Metadata with all fields

---

## Data Retrieved

### 1. USACSCICP02STSAM - OECD Composite Consumer Confidence
**All Requested Fields:**
- ✅ **Value:** In CSV column `oecd_composite_confidence`
- ✅ **Units:** Percentage balance (in metadata)
- ✅ **Frequency:** Monthly (in metadata)
- ✅ **Last Update:** 2025-11-17 12:04:38-06 (in metadata)
- ✅ **Series ID:** USACSCICP02STSAM (in metadata)

**Latest Value:** 57.67 (Oct 2025)

### 2. CSCICP03USM665S - OECD Consumer Confidence Amplitude Adjusted
**All Requested Fields:**
- ✅ **Value:** In CSV column `oecd_amplitude_adjusted`
- ✅ **Units:** Normalised (Normal=100) (in metadata)
- ✅ **Frequency:** Monthly (in metadata)
- ✅ **Last Update:** 2025-11-17 14:44:15-06 (in metadata)
- ✅ **Series ID:** CSCICP03USM665S (in metadata)

**Latest Value:** 98.91 (Jan 2024)

---

## Output Files

### CSV File (`fred_oecd_confidence.csv`)
```csv
date,year,month,oecd_composite_confidence,oecd_amplitude_adjusted
1960-01-01,1960,1,107.5944,101.6328
1960-02-01,1960,2,105.1914,101.3749
...
2025-10-01,2025,10,57.67058,
```

**Columns:**
- `date` - Observation date (YYYY-MM-DD)
- `year` - Year
- `month` - Month number
- `oecd_composite_confidence` - USACSCICP02STSAM values
- `oecd_amplitude_adjusted` - CSCICP03USM665S values

### Metadata File (`fred_oecd_metadata.json`)
```json
{
  "composite_confidence": {
    "series_id": "USACSCICP02STSAM",
    "title": "Consumer Opinion Surveys: Composite Consumer Confidence for United States",
    "units": "Percentage balance",
    "frequency": "Monthly",
    "last_updated": "2025-11-17 12:04:38-06",
    ...
  },
  "amplitude_adjusted": {
    "series_id": "CSCICP03USM665S",
    "title": "Composite Leading Indicators: Composite Consumer Confidence Amplitude Adjusted for United States",
    "units": "Normalised (Normal=100)",
    "frequency": "Monthly",
    "last_updated": "2025-11-17 14:44:15-06",
    ...
  }
}
```

---

## Data Coverage

- **Total Observations:** 790 monthly records
- **Date Range:** Jan 1960 - Oct 2025
- **Coverage:**
  - Composite Confidence: 100% (790/790)
  - Amplitude Adjusted: 97.3% (769/790) - data ends Jan 2024

### Coverage by Series:

| Series | Coverage | Range | Latest |
|--------|----------|-------|--------|
| USACSCICP02STSAM | 100% (790/790) | 53.8 - 120.5 | 57.67 (Oct 2025) |
| CSCICP03USM665S | 97.3% (769/790) | 96.2 - 102.8 | 98.91 (Jan 2024) |

**Note:** Amplitude Adjusted series ends in Jan 2024 (source limitation, not scraper issue)

---

## Features

### ✅ All Requested Fields
- Value ✓
- Units ✓
- Frequency ✓
- Last Update ✓
- Series ID ✓

### ✅ Robust & Reliable
- Retry logic with exponential backoff
- Error handling and validation
- 100% coverage for composite series
- Clean CSV + metadata JSON output

---

## Advanced Usage

### Import as Module:
```python
from FRED.fred_oecd_scraper import FREDOECDScraper

scraper = FREDOECDScraper(output_dir='custom/path')
scraper.run()

# Access data
print(scraper.series_metadata)  # All metadata
print(scraper.series_data)      # Raw DataFrames
```

---

## Update Schedule

- **Frequency:** Monthly
- **Best time to run:** After month-end when OECD releases new data
- **Recommended:** Monthly automated runs

---

## Validation Results

From test run (December 10, 2025):

```
✓ OECD Composite Consumer Confidence: 100.0% coverage (790/790)
  Range: 53.8 to 120.5
  Latest: 57.67 (Oct 2025)

✓ OECD Consumer Confidence Amplitude Adjusted: 97.3% coverage (769/790)
  Range: 96.2 to 102.8
  Latest: 98.91 (Jan 2024)
```

---

## Performance

- **Runtime:** ~0.87 seconds
- **Data size:** ~40KB CSV file
- **API calls:** 4 total (2 metadata + 2 observations)
- **Memory:** Minimal (~5-10MB)

---

## Citation

When using this data:

```
Organisation for Economic Co-operation and Development (OECD)
Consumer Opinion Surveys: Composite Consumer Confidence for United States [USACSCICP02STSAM]
Composite Leading Indicators: Composite Consumer Confidence Amplitude Adjusted [CSCICP03USM665S]
Retrieved from FRED, Federal Reserve Bank of St. Louis
https://fred.stlouisfed.org/
```

---

## All Requested Fields ✅

### For USACSCICP02STSAM:
- [x] Value ← In CSV as `oecd_composite_confidence`
- [x] Units ← In metadata JSON
- [x] Frequency ← In metadata JSON
- [x] Last update ← In metadata JSON
- [x] Series ID ← In metadata JSON

### For CSCICP03USM665S:
- [x] Value ← In CSV as `oecd_amplitude_adjusted`
- [x] Units ← In metadata JSON
- [x] Frequency ← In metadata JSON
- [x] Last update ← In metadata JSON
- [x] Series ID ← In metadata JSON

---

## 🎉 Status: IN THE BAG! 🎉

Both OECD series are:
- ✅ Successfully scraped
- ✅ All fields captured
- ✅ Validated and tested
- ✅ Ready for production use
