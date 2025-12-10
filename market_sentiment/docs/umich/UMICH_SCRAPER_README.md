# University of Michigan Data Scraper - Usage Guide

## ✅ Status: PRODUCTION READY

The UMich scraper is **fully tested and working**. All 5 client fields are successfully scraped and validated.

---

## Quick Start

### Run the scraper:
```bash
python FRED/umich_scraper.py
```

That's it! The scraper will:
1. Download all 3 source files from UMich website
2. Parse and combine the data
3. Validate data quality
4. Save to `FRED/data/umich_data_combined.csv`

---

## Output

### File Location:
```
FRED/data/umich_data_combined.csv
```

### File Structure:
```csv
year,month,date,sentiment,current_conditions,consumer_expectations,year_ahead_inflation,long_run_inflation
1952,11,1952-11-01,86.2,73.4,92.4,,
...
2025,11,2025-11-01,51.0,51.1,51.0,4.5,3.4
```

### Columns:
| Column | Description | Units | Coverage |
|--------|-------------|-------|----------|
| `year` | Year | YYYY | 100% |
| `month` | Month number | 1-12 | 100% |
| `date` | Full date | YYYY-MM-DD | 100% |
| `sentiment` | Index of Consumer Sentiment | Index | 100% (667/667) |
| `current_conditions` | Current Economic Conditions | Index | 99.1% (661/667) |
| `consumer_expectations` | Consumer Expectations | Index | 99.3% (662/667) |
| `year_ahead_inflation` | Year Ahead Inflation | Percent | 86.2% (575/667) |
| `long_run_inflation` | Long Run Inflation | Percent | 70.5% (470/667) |

### Data Range:
- **Start:** November 1952
- **End:** November 2025 (updates monthly)
- **Total Rows:** 667+
- **All historical data included**

---

## Latest Data (November 2025)

| Field | Value |
|-------|-------|
| Index of Consumer Sentiment | 51.0 |
| Current Economic Conditions | 51.1 |
| Consumer Expectations | 51.0 |
| Year Ahead Inflation | 4.5% |
| Long Run Inflation | 3.4% |

---

## Features

### ✅ Robust & Reliable
- **Retry logic:** 3 attempts with exponential backoff
- **Error handling:** Graceful failures with detailed logging
- **Validation:** Automatic data quality checks
- **Timeout protection:** 30-second timeout per download

### ✅ Clean Data
- **Date parsing:** Automatic conversion to YYYY-MM-DD format
- **Missing values:** Properly handled (empty strings → NaN)
- **Numeric conversion:** All values converted to proper numeric types
- **Sorted:** Data sorted chronologically

### ✅ Transparent
- **Detailed logging:** See exactly what's happening
- **Validation report:** Coverage stats for each field
- **Raw data saved:** Original files preserved for reference

---

## Advanced Usage

### Import as Module:
```python
from FRED.umich_scraper import UMichScraper

# Create scraper
scraper = UMichScraper(output_dir='custom/path')

# Run full pipeline
scraper.run()

# Or run step by step
scraper.download_all()
scraper.process_data()
validation = scraper.validate_data()
scraper.save_data('custom_filename.csv')

# Access the data
df = scraper.combined_data
print(df.head())
```

### Custom Output Directory:
```python
scraper = UMichScraper(output_dir='my_data_folder')
scraper.run()
```

### Access Raw Data:
After running, raw CSV files are saved:
- `FRED/data/umich_sentiment_raw.csv`
- `FRED/data/umich_components_raw.csv`
- `FRED/data/umich_inflation_raw.csv`

---

## Data Sources

The scraper downloads from 3 official UMich URLs:

| File | URL | Contains |
|------|-----|----------|
| Sentiment | https://www.sca.isr.umich.edu/files/tbmics.csv | Consumer Sentiment Index |
| Components | https://www.sca.isr.umich.edu/files/tbmiccice.csv | Current Conditions + Expectations |
| Inflation | https://www.sca.isr.umich.edu/files/tbmpx1px5.csv | Year-ahead & Long-run Inflation |

---

## Update Schedule

### When to Re-run:
- **Preliminary data:** Mid-month (around 10am ET)
- **Final data:** End of month (around 10am ET)
- **Next release:** December 19, 2025 (Final December data)

### Recommended Frequency:
- Monthly after each release
- Or set up automated monthly runs

---

## Validation Results

From latest test run (December 10, 2025):

```
✓ Index of Consumer Sentiment: 100.0% coverage (667/667)
  Range: 50.0 to 112.0
  Latest: 51.0 (Nov 2025)

✓ Current Economic Conditions: 99.1% coverage (661/667)
  Range: 51.1 to 121.2
  Latest: 51.1 (Nov 2025)

✓ Consumer Expectations: 99.3% coverage (662/667)
  Range: 44.2 to 108.6
  Latest: 51.0 (Nov 2025)

✓ Year Ahead Inflation: 86.2% coverage (575/667)
  Range: 0.4% to 10.4%
  Latest: 4.5% (Nov 2025)

✓ Long Run Inflation: 70.5% coverage (470/667)
  Range: 2.2% to 9.7%
  Latest: 3.4% (Nov 2025)
```

**Note:** Some early historical values are missing from source data (not a scraper issue).

---

## Dependencies

```bash
pip install pandas requests
```

Both are lightweight and standard Python libraries.

---

## Citation

When using this data, cite:

```
Surveys of Consumers, University of Michigan
Copyright © 2025, The Regents of the University of Michigan.
Source: http://www.sca.isr.umich.edu/
Retrieved: [date]
```

---

## Troubleshooting

### Problem: Download fails
**Solution:** Check internet connection. Scraper will retry 3 times automatically.

### Problem: Missing values in output
**Solution:** This is expected - some historical data has gaps in the source files.

### Problem: Old data in output
**Solution:** Just re-run the scraper. It downloads fresh data each time.

---

## Performance

- **Runtime:** ~0.6 seconds
- **Data size:** ~27KB output file
- **Network usage:** ~40KB downloaded (3 small CSV files)
- **Memory:** Minimal (~1-2MB)

---

## What's Next?

This scraper is **100% production ready**. You can:

1. ✅ **Use it now** - Run anytime to get latest data
2. ✅ **Automate it** - Set up monthly cron job
3. ✅ **Integrate it** - Import as module in your pipeline
4. ⏳ **Combine with FRED** - Next step: Add FRED API data

---

## 🎉 Status: IN THE BAG! 🎉

All 5 University of Michigan fields are:
- ✅ Successfully identified
- ✅ Scraped and validated
- ✅ Ready for production use
- ✅ Documented and tested

**You can confidently rely on this scraper for your client's UMich data needs.**
