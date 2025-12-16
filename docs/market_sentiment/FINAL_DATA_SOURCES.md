# Client FRED Data Sources - Final Summary

## ✅ All 8 Data Sources Identified!

All requested data sources have been successfully mapped to FRED API endpoints.

---

## Quick Reference Table

| # | Client Request | Series/Release ID | Type | Frequency | Data Start | Latest Value |
|---|----------------|-------------------|------|-----------|------------|--------------|
| 1 | Surveys of Consumers | `91` | Release | Monthly | - | Multiple series |
| 2 | UMCSENT | `UMCSENT` | Series | Monthly | 1952-11-01 | 53.6 (Oct 2025) |
| 3 | EXPINF1YR | `EXPINF1YR` | Series | Monthly | 1982-01-01 | 2.74% (Oct 2025) |
| 4 | T10YIE | `T10YIE` | Series | **Daily** | 2003-01-02 | 2.26% (Dec 9, 2025) |
| 5 | US Consumer Confidence (OECD) | `USACSCICP02STSAM` | Series | Monthly | 1960-01-01 | 57.67 (Oct 2025) |
| 6 | US Consumer Confidence Amplitude | `CSCICP03USM665S` | Series | Monthly | - | - |
| 7 | EXPINF10YR | `EXPINF10YR` | Series | Monthly | 1982-01-01 | 2.29% (Oct 2025) |
| 8 | UMCSENT Table Data | `91` (release tables) | Release Tables | Monthly | - | Multiple series |

---

## Detailed Series Information

### 1. Surveys of Consumers (Release)
```
Release ID: 91
Type: Release containing multiple series
Source: University of Michigan
```

**Contains 3 series:**
- `UMCSENT` - University of Michigan: Consumer Sentiment
- `MICH` - University of Michigan: Inflation Expectation
- `UMCSENT1` - Historical Consumer Sentiment (discontinued)

**API Endpoints:**
```
Release info:        fred/release?release_id=91
All series:          fred/release/series?release_id=91
Release tables:      fred/release/tables?release_id=91
```

---

### 2. UMCSENT - University of Michigan Consumer Sentiment
```
Series ID: UMCSENT
Units: Index 1966:Q1=100
Frequency: Monthly
Seasonal Adjustment: Not Seasonally Adjusted
```

**Key Facts:**
- Historical data from 1952
- Delayed by 1 month at source's request
- Copyrighted - citation required
- Popularity: 83/100

**API Endpoint:**
```
Series info:         fred/series?series_id=UMCSENT
Observations:        fred/series/observations?series_id=UMCSENT
```

---

### 3. EXPINF1YR - 1-Year Expected Inflation
```
Series ID: EXPINF1YR
Units: Percent
Frequency: Monthly
Seasonal Adjustment: Not Seasonally Adjusted
```

**Key Facts:**
- Cleveland Fed inflation expectations model
- Based on Treasury yields, inflation data, and swaps
- Public domain (citation requested)
- Data since 1982

**API Endpoint:**
```
Series info:         fred/series?series_id=EXPINF1YR
Observations:        fred/series/observations?series_id=EXPINF1YR
```

---

### 4. T10YIE - 10-Year Breakeven Inflation Rate
```
Series ID: T10YIE
Units: Percent
Frequency: DAILY ⚠️
Seasonal Adjustment: Not Seasonally Adjusted
```

**Key Facts:**
- **DAILY updates** (most frequent of all series)
- Market-implied inflation expectations
- Derived from Treasury and TIPS spread
- Most current data available
- Popularity: 89/100

**API Endpoint:**
```
Series info:         fred/series?series_id=T10YIE
Observations:        fred/series/observations?series_id=T10YIE
```

**⚠️ Important:** This is daily data, so it will have significantly more observations than monthly series.

---

### 5. USACSCICP02STSAM - Composite Consumer Confidence (OECD)
```
Series ID: USACSCICP02STSAM
Units: Percentage balance
Frequency: Monthly
Seasonal Adjustment: Seasonally Adjusted
```

**Key Facts:**
- OECD Main Economic Indicators
- Longest historical data (since 1960)
- Seasonally adjusted
- OECD citation required

**API Endpoint:**
```
Series info:         fred/series?series_id=USACSCICP02STSAM
Observations:        fred/series/observations?series_id=USACSCICP02STSAM
```

---

### 6. CSCICP03USM665S - Composite Consumer Confidence Amplitude Adjusted
```
Series ID: CSCICP03USM665S
Units: Normalised (Normal=100)
Frequency: Monthly
Seasonal Adjustment: N/A
```

**Key Facts:**
- OECD amplitude-adjusted series
- Part of Composite Leading Indicators
- Normalized to 100

**API Endpoint:**
```
Series info:         fred/series?series_id=CSCICP03USM665S
Observations:        fred/series/observations?series_id=CSCICP03USM665S
```

---

### 7. EXPINF10YR - 10-Year Expected Inflation
```
Series ID: EXPINF10YR
Units: Percent
Frequency: Monthly
Seasonal Adjustment: Not Seasonally Adjusted
```

**Key Facts:**
- Cleveland Fed inflation expectations model
- 10-year horizon (longer than EXPINF1YR)
- Based on Treasury yields, inflation data, and swaps
- Public domain (citation requested)
- Data since 1982

**API Endpoint:**
```
Series info:         fred/series?series_id=EXPINF10YR
Observations:        fred/series/observations?series_id=EXPINF10YR
```

---

### 8. UMCSENT Table Data (Release Tables)
```
Release ID: 91
Type: Release Tables
```

**What it is:**
- Tabular view of Surveys of Consumers release
- Contains all series organized in table format
- Same as source #1 but in table structure

**API Endpoint:**
```
Release tables:      fred/release/tables?release_id=91
All series:          fred/release/series?release_id=91
```

---

## Data Structure

### For Individual Series (2-7):
Each observation contains:
```json
{
  "realtime_start": "2025-12-10",
  "realtime_end": "2025-12-10",
  "date": "2025-10-01",
  "value": "53.6"
}
```

### Series Metadata Includes:
- `id` - Series identifier
- `title` - Full descriptive name
- `units` - Unit of measurement
- `frequency` - Data frequency
- `seasonal_adjustment` - Adjustment type
- `observation_start` - First available date
- `observation_end` - Last available date
- `last_updated` - Last update timestamp
- `popularity` - Popularity score (0-100)
- `notes` - Detailed methodology

---

## API Usage Summary

### To Get All Historical Data:
```python
# For each series, single API call can get all data:
endpoint = f"fred/series/observations"
params = {
    'api_key': YOUR_KEY,
    'series_id': SERIES_ID,
    'file_type': 'json',
    'limit': 100000  # Max allowed
}
```

### Data Transformation Options:
The API can automatically transform data using the `units` parameter:
- `lin` - Linear (no transformation)
- `chg` - Change
- `ch1` - Change from year ago
- `pch` - Percent change
- `pc1` - Percent change from year ago
- `pca` - Compounded annual rate of change
- `cch` - Continuously compounded rate of change
- `cca` - Continuously compounded annual rate of change
- `log` - Natural log

---

## Important Considerations

### Rate Limits:
- 120 requests per minute
- 7 individual series + 1 release = 8-10 API calls total
- Can retrieve all historical data in a single batch
- Well within rate limits

### Data Frequency:
- **6 series** are Monthly
- **1 series** (T10YIE) is Daily
- Plan storage accordingly for daily vs monthly data

### Citation Requirements:
- **UMCSENT**: Copyrighted, citation required
- **T10YIE**: Copyrighted, citation required
- **USACSCICP02STSAM**: OECD citation required
- **EXPINF1YR/10YR**: Public domain, citation requested

### Data Delays:
- UMCSENT is delayed by 1 month at source's request
- Other series updated on their normal schedules

---

## Next Steps

1. **Define Specific Fields**: Which fields do you need from each observation?
   - Just `date` and `value`?
   - Include `realtime_start/end` for versioning?
   - Include metadata (units, last_updated, etc.)?

2. **Define Date Ranges**:
   - All historical data?
   - Only data from a specific date forward?
   - Different ranges for different series?

3. **Define Update Strategy**:
   - One-time historical pull?
   - Daily updates?
   - Weekly/monthly updates?

4. **Define Storage Format**:
   - CSV files?
   - Database?
   - JSON?
   - Separate files per series or combined?

5. **Build Pipeline**:
   - Automated data fetching
   - Data validation
   - Storage mechanism
   - Update scheduling
