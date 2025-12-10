# University of Michigan Data Sources

## Overview
The 5 fields requested by the client are available directly from the University of Michigan Surveys of Consumers website, NOT from FRED.

## Client Fields Mapping

### ✅ All 5 Fields Available from UMich Website:

1. **Index of Consumer Sentiment**
2. **Current Economic Conditions**
3. **Consumer Expectations**
4. **Year Ahead Inflation**
5. **Long Run Inflation**

---

## Data Download URLs

### File Naming Convention:
- `tb` = table
- `c` = current (latest only)
- `m` = monthly historical data
- `q` = quarterly historical data
- `y` = yearly historical data
- `ics` = Index of Consumer Sentiment
- `iccice` = Index of Current Conditions & Consumer Expectations (components)
- `px1px5` = Expected inflation (1-year and 5-year+)

---

## 1. Index of Consumer Sentiment

**Monthly Historical Data:**
- **Excel:** https://www.sca.isr.umich.edu/files/tbmics.xls
- **CSV:** https://www.sca.isr.umich.edu/files/tbmics.csv

**Other Frequencies:**
- Current: `tbcics.xls` / `tbcics.csv`
- Quarterly: `tbqics.xls` / `tbqics.csv`
- Yearly: `tbyics.xls` / `tbyics.csv`

---

## 2 & 3. Current Economic Conditions + Consumer Expectations

These are in the "Components" files together:

**Monthly Historical Data:**
- **Excel:** https://www.sca.isr.umich.edu/files/tbmiccice.xls
- **CSV:** https://www.sca.isr.umich.edu/files/tbmiccice.csv

**Other Frequencies:**
- Current: `tbciccice.xls` / `tbciccice.csv`
- Quarterly: `tbqiccice.xls` / `tbqiccice.csv`
- Yearly: `tbyiccice.xls` / `tbyiccice.csv`

**Note:** This file contains BOTH:
- Index of Current Conditions (ICC)
- Index of Consumer Expectations (ICE)

---

## 4 & 5. Year Ahead Inflation + Long Run Inflation

These are in the "Expected Changes in Inflation Rates" files:

**Monthly Historical Data:**
- **Excel:** https://www.sca.isr.umich.edu/files/tbmpx1px5.xls
- **CSV:** https://www.sca.isr.umich.edu/files/tbmpx1px5.csv

**Other Frequencies:**
- Current: `tbcpx1px5.xls` / `tbcpx1px5.csv`
- Quarterly: `tbqpx1px5.xls` / `tbqpx1px5.csv`
- Yearly: `tbypx1px5.xls` / `tbypx1px5.csv`

**Note:** This file contains:
- PX1: Expected inflation over next 1 year (Year Ahead Inflation)
- PX5: Expected inflation over next 5-10 years (Long Run Inflation)

---

## Summary Table

| Client Field | Source File (Monthly) | Format | URL |
|--------------|----------------------|--------|-----|
| Index of Consumer Sentiment | `tbmics` | Excel/CSV | https://www.sca.isr.umich.edu/files/tbmics.xls |
| Current Economic Conditions | `tbmiccice` | Excel/CSV | https://www.sca.isr.umich.edu/files/tbmiccice.xls |
| Consumer Expectations | `tbmiccice` | Excel/CSV | https://www.sca.isr.umich.edu/files/tbmiccice.xls |
| Year Ahead Inflation | `tbmpx1px5` | Excel/CSV | https://www.sca.isr.umich.edu/files/tbmpx1px5.xls |
| Long Run Inflation | `tbmpx1px5` | Excel/CSV | https://www.sca.isr.umich.edu/files/tbmpx1px5.xls |

---

## Data Access Strategy

### Recommended Approach:
Download **3 files** to get all 5 fields:

1. **`tbmics.csv`** → Index of Consumer Sentiment
2. **`tbmiccice.csv`** → Current Economic Conditions + Consumer Expectations
3. **`tbmpx1px5.csv`** → Year Ahead Inflation + Long Run Inflation

### Why CSV instead of Excel?
- Easier to parse programmatically
- Smaller file size
- No Excel library dependencies needed

---

## Implementation Notes

### File Format:
The CSV files are likely formatted as:
```
Month,Year,ICS
11,2025,53.3
10,2025,51.0
...
```

Or similar structure with date columns and value columns.

### Download Method:
```python
import requests

# Download a file
url = "https://www.sca.isr.umich.edu/files/tbmics.csv"
response = requests.get(url)

# Save to file
with open('umich_sentiment.csv', 'wb') as f:
    f.write(response.content)
```

### Update Frequency:
- Data is updated monthly
- Preliminary results released mid-month
- Final results released end of month
- Next release: December 19, 2025 (Final December data)

---

## Citation Requirements

**Required Citation:**
```
Surveys of Consumers, University of Michigan
Copyright © 2025, The Regents of the University of Michigan. All Rights Reserved.
```

---

## Next Steps

1. ✅ Identified all 5 data sources
2. ✅ Found direct download URLs
3. ⏭️ Download sample files to understand structure
4. ⏭️ Build parsers for each file type
5. ⏭️ Implement automated data pipeline
