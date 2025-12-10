# FRED Breakeven Inflation Scraper - Usage Guide

## ✅ Status: PRODUCTION READY

The FRED breakeven inflation scraper is **fully tested and working**. Both series are successfully scraped with all requested fields.

---

## Quick Start

### Run the scraper:
```bash
python FRED/fred_breakeven_scraper.py
```

Output files:
- `FRED/data/fred_breakeven_inflation.csv` - Combined data
- `FRED/data/fred_breakeven_metadata.json` - Metadata with all fields

---

## Data Retrieved

### 1. T10YIE - 10-Year Breakeven Inflation Rate
**Fields:**
- ✅ **10 year breakeven inflation** (value in CSV)
- ✅ **Units:** Percent (in metadata)
- ✅ **Frequency:** Daily (in metadata)
- ✅ **Last Update:** 2025-12-09 16:03:49-06 (in metadata)
- ✅ **Observation Date:** In CSV date column

**Latest Value:** 2.26% (Dec 9, 2025)

### 2. T5YIFR - 5-Year, 5-Year Forward Inflation Expectation Rate
**Fields:**
- ✅ **5y5y forward inflation** (value in CSV)
- ✅ **Units:** Percent (in metadata)
- ✅ **Frequency:** Daily (in metadata)
- ✅ **Last Update:** 2025-12-09 16:03:43-06 (in metadata)
- ✅ **Observation Date:** In CSV date column

**Latest Value:** 2.20% (Dec 9, 2025)

---

## Output Files

### CSV File (`fred_breakeven_inflation.csv`)
```csv
date,year,month,day,10_year_breakeven_inflation,5y5y_forward_inflation
2003-01-02,2003,1,2,1.64,1.98
2003-01-03,2003,1,3,1.62,1.96
...
2025-12-09,2025,12,9,2.26,2.2
```

**Columns:**
- `date` - Observation date (YYYY-MM-DD)
- `year` - Year
- `month` - Month number
- `day` - Day of month
- `10_year_breakeven_inflation` - T10YIE values (%)
- `5y5y_forward_inflation` - T5YIFR values (%)

### Metadata File (`fred_breakeven_metadata.json`)
```json
{
  "t10yie": {
    "series_id": "T10YIE",
    "title": "10-Year Breakeven Inflation Rate",
    "units": "Percent",
    "frequency": "Daily",
    "last_updated": "2025-12-09 16:03:49-06",
    "observation_start": "2003-01-02",
    "observation_end": "2025-12-09",
    ...
  },
  "t5yifr": {
    "series_id": "T5YIFR",
    "title": "5-Year, 5-Year Forward Inflation Expectation Rate",
    "units": "Percent",
    "frequency": "Daily",
    "last_updated": "2025-12-09 16:03:43-06",
    ...
  }
}
```

---

## Data Coverage

- **Total Observations:** 5,738 daily records
- **Date Range:** Jan 2, 2003 - Dec 9, 2025
- **Coverage:** 100% (no missing values)
- **Frequency:** Daily (business days)

### Coverage by Series:

| Series | Coverage | Range | Latest |
|--------|----------|-------|--------|
| T10YIE | 100% (5738/5738) | 0.04% - 3.02% | 2.26% |
| T5YIFR | 100% (5738/5738) | 0.43% - 3.05% | 2.20% |

---

## Features

### ✅ Robust & Reliable
- **Retry logic:** 3 attempts with exponential backoff
- **Error handling:** Graceful failures with detailed logging
- **Validation:** Automatic data quality checks
- **API rate limiting:** Respects FRED's 120 req/min limit

### ✅ Complete Data
- **All requested fields** included in metadata
- **Daily observations** since 2003
- **Both series** in single CSV file
- **Clean format:** Ready for analysis

### ✅ Transparent
- **Detailed logging:** See exactly what's happening
- **Validation report:** Coverage stats for each series
- **Metadata saved:** All series info preserved

---

## Advanced Usage

### Import as Module:
```python
from FRED.fred_breakeven_scraper import FREDBreakevenScraper

# Create scraper
scraper = FREDBreakevenScraper(output_dir='custom/path')

# Run full pipeline
scraper.run()

# Access the data
print(scraper.series_metadata)  # Metadata for both series
print(scraper.series_data)      # Raw DataFrames
```

### Custom Output Directory:
```python
scraper = FREDBreakevenScraper(output_dir='my_data_folder')
scraper.run()
```

---

## Understanding the Data

### T10YIE - 10-Year Breakeven Inflation
- **What it is:** Market-implied inflation expectations over next 10 years
- **Derived from:** Difference between 10-year Treasury and 10-year TIPS yields
- **Interpretation:** What markets expect average inflation to be over next 10 years

### T5YIFR - 5-Year, 5-Year Forward
- **What it is:** Expected inflation 5 years from now for the following 5 years
- **Formula:** Derived from 10-year and 5-year breakeven rates
- **Interpretation:** Long-term inflation expectations starting 5 years in the future

---

## Update Schedule

### When to Re-run:
- **Daily:** Data updates daily (business days)
- **Best time:** After 4:15 PM ET (when Treasury markets close)

### Recommended Frequency:
- Daily automated runs for up-to-date data
- Or on-demand when you need latest values

---

## Validation Results

From latest test run (December 10, 2025):

```
✓ 10-Year Breakeven Inflation Rate: 100.0% coverage (5738/5738)
  Range: 0.04% to 3.02%
  Latest: 2.26% (Dec 9, 2025)

✓ 5-Year, 5-Year Forward Inflation Expectation Rate: 100.0% coverage (5738/5738)
  Range: 0.43% to 3.05%
  Latest: 2.20% (Dec 9, 2025)
```

---

## Dependencies

```bash
pip install pandas requests python-dotenv
```

Requires FRED API key in `.env` file:
```
FRED_API_KEY=your_key_here
```

---

## Citation

When using this data:

```
Federal Reserve Bank of St. Louis
10-Year Breakeven Inflation Rate [T10YIE]
5-Year, 5-Year Forward Inflation Expectation Rate [T5YIFR]
Retrieved from FRED, Federal Reserve Bank of St. Louis
https://fred.stlouisfed.org/
```

---

## Troubleshooting

### Problem: API request fails
**Solution:** Check API key in `.env`. Scraper will retry 3 times automatically.

### Problem: Old data
**Solution:** Just re-run the scraper. It fetches fresh data from FRED each time.

### Problem: Missing dates
**Solution:** Normal - weekends/holidays don't have data (markets closed).

---

## Performance

- **Runtime:** ~0.75 seconds
- **Data size:** ~250KB CSV file
- **API calls:** 4 total (2 metadata + 2 observations)
- **Memory:** Minimal (~5-10MB)

---

## All Requested Fields ✅

### For T10YIE:
- [x] 10 year breakeven inflation ← In CSV as `10_year_breakeven_inflation`
- [x] Frequency ← In metadata JSON
- [x] Units ← In metadata JSON
- [x] Last update ← In metadata JSON
- [x] Observation date ← In CSV as `date` column

### For T5YIFR:
- [x] 5y5y forward inflation ← In CSV as `5y5y_forward_inflation`
- [x] Units ← In metadata JSON
- [x] Frequency ← In metadata JSON
- [x] Last update ← In metadata JSON
- [x] Observation date ← In CSV as `date` column

---

## 🎉 Status: IN THE BAG! 🎉

Both breakeven inflation series are:
- ✅ Successfully scraped
- ✅ All fields captured
- ✅ Validated and tested
- ✅ Ready for production use

**You can confidently rely on this scraper for your FRED breakeven inflation data needs.**
