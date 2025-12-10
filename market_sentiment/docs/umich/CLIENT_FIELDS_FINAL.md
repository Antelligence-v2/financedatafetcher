# Client Fields - Final Mapping

## ✅ ALL 5 FIELDS FOUND!

All requested fields are available from the University of Michigan Surveys of Consumers website.

---

## Data Source Summary

### Download 3 CSV Files to Get All 5 Fields:

| File | URL | Contains Fields |
|------|-----|-----------------|
| **Sentiment** | https://www.sca.isr.umich.edu/files/tbmics.csv | Index of Consumer Sentiment |
| **Components** | https://www.sca.isr.umich.edu/files/tbmiccice.csv | Current Economic Conditions + Consumer Expectations |
| **Inflation** | https://www.sca.isr.umich.edu/files/tbmpx1px5.csv | Year Ahead Inflation + Long Run Inflation |

---

## Field Mappings

### 1. Index of Consumer Sentiment
**File:** `tbmics.csv`
**Column:** `ICS_ALL`
**Format:**
```csv
Month,YYYY,ICS_ALL
November,1952,86.2
February,1953,90.7
...
```
- **Data Start:** November 1952
- **Frequency:** Monthly
- **Units:** Index (1966:Q1=100)

---

### 2. Current Economic Conditions
**File:** `tbmiccice.csv`
**Column:** `ICC` (Index of Current Conditions)
**Format:**
```csv
Month,YYYY,ICC,ICE
February,1951,67.7,
May,1951,60.2,
November,1952,73.4,92.4
...
```
- **Data Start:** February 1951
- **Frequency:** Monthly
- **Units:** Index
- **Note:** Some early values may be missing

---

### 3. Consumer Expectations
**File:** `tbmiccice.csv`
**Column:** `ICE` (Index of Consumer Expectations)
**Format:**
```csv
Month,YYYY,ICC,ICE
February,1951,67.7,
May,1951,60.2,
November,1952,73.4,92.4
...
```
- **Data Start:** November 1952
- **Frequency:** Monthly
- **Units:** Index
- **Note:** Some early values may be missing

---

### 4. Year Ahead Inflation
**File:** `tbmpx1px5.csv`
**Column:** `PX_MD` (Median expected price change, next 12 months)
**Format:**
```csv
Month,YYYY,PX_MD,PX5_MD
January,1978,5.2,
February,1978,6.4,
...
```
- **Data Start:** January 1978
- **Frequency:** Monthly
- **Units:** Percent

---

### 5. Long Run Inflation
**File:** `tbmpx1px5.csv`
**Column:** `PX5_MD` (Median expected price change, next 5-10 years)
**Format:**
```csv
Month,YYYY,PX_MD,PX5_MD
January,1978,5.2,
...
```
- **Data Start:** Later than PX_MD (appears to have missing early values)
- **Frequency:** Monthly
- **Units:** Percent
- **Note:** Some early values may be missing

---

## File Structure Details

### Common Format:
All files use the same date format:
- **Month Column:** Full month name (e.g., "January", "February")
- **YYYY Column:** Four-digit year (e.g., "1978", "2025")

### Data Range:
- **Sentiment (ICS):** 1952-present
- **Current Conditions (ICC):** 1951-present
- **Expectations (ICE):** 1952-present
- **Year Ahead Inflation (PX_MD):** 1978-present
- **Long Run Inflation (PX5_MD):** ~1979/1980-present (exact start TBD)

### Missing Values:
- Represented as empty strings in CSV
- Need to handle when parsing

---

## Complete Field Summary

| # | Client Field | File Source | Column Name | Data Starts | Units |
|---|--------------|-------------|-------------|-------------|-------|
| 1 | Index of Consumer Sentiment | tbmics.csv | ICS_ALL | 1952-11 | Index |
| 2 | Current Economic Conditions | tbmiccice.csv | ICC | 1951-02 | Index |
| 3 | Consumer Expectations | tbmiccice.csv | ICE | 1952-11 | Index |
| 4 | Year Ahead Inflation | tbmpx1px5.csv | PX_MD | 1978-01 | Percent |
| 5 | Long Run Inflation | tbmpx1px5.csv | PX5_MD | ~1979+ | Percent |

---

## Implementation Plan

### Step 1: Download Files
```python
import requests

files = {
    'sentiment': 'https://www.sca.isr.umich.edu/files/tbmics.csv',
    'components': 'https://www.sca.isr.umich.edu/files/tbmiccice.csv',
    'inflation': 'https://www.sca.isr.umich.edu/files/tbmpx1px5.csv'
}

for name, url in files.items():
    response = requests.get(url)
    # Save or process data
```

### Step 2: Parse CSV
```python
import pandas as pd

# Read CSV
df = pd.read_csv('tbmics.csv')

# Convert Month/Year to proper date
# Handle missing values
# Extract relevant columns
```

### Step 3: Combine Data
- Join all dataframes on date
- Create single output with all 5 fields

### Step 4: Store
- Save to database or CSV
- Update regularly (monthly)

---

## Citation Requirements

**Required Citation:**
```
Surveys of Consumers, University of Michigan
Copyright © 2025, The Regents of the University of Michigan. All Rights Reserved.
Source: http://www.sca.isr.umich.edu/
```

---

## Update Schedule

- **Preliminary Results:** Mid-month (around 10am ET)
- **Final Results:** End of month (around 10am ET)
- **Next Release:** December 19, 2025 (Final December data)

---

## Status: ✅ COMPLETE

All 5 client-requested fields have been:
- ✅ Located on UMich website
- ✅ Download URLs identified
- ✅ File structure analyzed
- ✅ Column names mapped
- ✅ Sample data downloaded

**Ready to build data pipeline!**
