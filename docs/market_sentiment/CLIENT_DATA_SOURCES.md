# Client FRED Data Sources - Summary

## Overview
This document maps the client's requested FRED pages to their API equivalents and shows what data is available from each source.

---

## 1. Surveys of Consumers
**Type:** Release (ID: 91)
**Source:** University of Michigan
**Link:** http://www.sca.isr.umich.edu/

**What it is:**
- A comprehensive release containing multiple series related to consumer sentiment and expectations
- Published monthly by the University of Michigan
- Contains data on consumer confidence, expectations, and economic outlook

**API Access:**
- Get release info: `fred/release?release_id=91`
- Get all series in release: `fred/release/series?release_id=91`
- Get release tables: `fred/release/tables?release_id=91`

**Key Series in This Release:**
- UMCSENT (Consumer Sentiment Index)
- Multiple other consumer survey metrics

---

## 2. UMCSENT - University of Michigan Consumer Sentiment
**Type:** Series
**Series ID:** `UMCSENT`
**Release:** Surveys of Consumers (ID: 91)

**Details:**
- **Title:** University of Michigan: Consumer Sentiment
- **Units:** Index 1966:Q1=100
- **Frequency:** Monthly
- **Seasonal Adjustment:** Not Seasonally Adjusted
- **Data Range:** 1952-11-01 to Present (2025-10-01)
- **Popularity:** 83/100
- **Category:** Financial Activity Measures

**Recent Values (as of late 2025):**
- October 2025: 53.6
- September 2025: 55.1
- August 2025: 58.2
- July 2025: 61.7

**Important Notes:**
- Data is delayed by 1 month at the request of the source
- Citation required (copyrighted data)
- For historical data prior to January 1978, see series UMCSENT1

**API Access:**
- Get series info: `fred/series?series_id=UMCSENT`
- Get observations: `fred/series/observations?series_id=UMCSENT`

**Available Fields per Observation:**
- `date` - Observation date
- `value` - Index value
- `realtime_start` - Real-time period start
- `realtime_end` - Real-time period end

---

## 3. EXPINF1YR - 1-Year Expected Inflation
**Type:** Series
**Series ID:** `EXPINF1YR`
**Release:** Inflation Expectations (ID: 500)

**Details:**
- **Title:** 1-Year Expected Inflation
- **Units:** Percent
- **Frequency:** Monthly
- **Seasonal Adjustment:** Not Seasonally Adjusted
- **Data Range:** 1982-01-01 to Present (2025-10-01)
- **Popularity:** 69/100
- **Category:** Prices
- **Source:** Federal Reserve Bank of Cleveland

**What it measures:**
- Expected rate of inflation over the next year
- Calculated using a model with Treasury yields, inflation data, and inflation swaps

**Recent Values:**
- October 2025: 2.74%
- September 2025: 2.80%
- August 2025: 2.69%

**API Access:**
- Get series info: `fred/series?series_id=EXPINF1YR`
- Get observations: `fred/series/observations?series_id=EXPINF1YR`

---

## 4. T10YIE - 10-Year Breakeven Inflation Rate
**Type:** Series
**Series ID:** `T10YIE`
**Release:** Interest Rate Spreads (ID: 304)

**Details:**
- **Title:** 10-Year Breakeven Inflation Rate
- **Units:** Percent
- **Frequency:** Daily (!)
- **Seasonal Adjustment:** Not Seasonally Adjusted
- **Data Range:** 2003-01-02 to Present (2025-12-09)
- **Popularity:** 89/100
- **Category:** Interest Rate Spreads
- **Source:** Federal Reserve Bank of St. Louis

**What it measures:**
- Market-implied inflation expectations over 10 years
- Derived from difference between 10-Year Treasury yields and 10-Year TIPS yields

**Recent Values:**
- December 9, 2025: 2.26%
- December 8, 2025: 2.26%
- December 5, 2025: 2.26%

**Important:**
- This is DAILY data (much higher frequency than monthly series)
- Most recent data available (updated daily)

**API Access:**
- Get series info: `fred/series?series_id=T10YIE`
- Get observations: `fred/series/observations?series_id=T10YIE`

---

## 5. US Consumer Confidence FRED/OECD (Composite Consumer Confidence)
**Type:** Series
**Series ID:** `USACSCICP02STSAM`
**Release:** Main Economic Indicators (ID: 205)

**Details:**
- **Title:** Consumer Opinion Surveys: Composite Consumer Confidence for United States
- **Units:** Percentage balance
- **Frequency:** Monthly
- **Seasonal Adjustment:** Seasonally Adjusted
- **Data Range:** 1960-01-01 to Present (2025-10-01)
- **Popularity:** 49/100
- **Source:** OECD (Organisation for Economic Co-operation and Development)

**What it measures:**
- OECD's composite consumer confidence indicator for the United States
- Measured as a percentage balance
- Seasonally adjusted for more accurate trend analysis
- Part of OECD's Main Economic Indicators release

**Recent Values:**
- October 2025: 57.67
- September 2025: 59.28
- August 2025: 62.62
- July 2025: 66.39

**Important Notes:**
- Citation required: OECD (year), (dataset name), (data source) DOI or https://data-explorer.oecd.org/
- Has data going back to 1960 (much longer history than some other series)

**API Access:**
- Get series info: `fred/series?series_id=USACSCICP02STSAM`
- Get observations: `fred/series/observations?series_id=USACSCICP02STSAM`

---

## 6. US Consumer Confidence Amplitude (FRED/OECD)
**Type:** Series
**Series ID:** `CSCICP03USM665S`

**Details:**
- **Title:** Composite Leading Indicators: Composite Consumer Confidence Amplitude Adjusted for United States
- **Units:** Normalised (Normal=100)
- **Frequency:** Monthly
- **Source:** OECD

**What it is:**
- OECD's amplitude-adjusted consumer confidence indicator for the US
- Part of the Composite Leading Indicators system
- Normalised to 100

**API Access:**
- Get series info: `fred/series?series_id=CSCICP03USM665S`
- Get observations: `fred/series/observations?series_id=CSCICP03USM665S`

---

## 7. EXPINF10YR - 10-Year Expected Inflation
**Type:** Series
**Series ID:** `EXPINF10YR`
**Release:** Inflation Expectations (ID: 500)

**Details:**
- **Title:** 10-Year Expected Inflation
- **Units:** Percent
- **Frequency:** Monthly
- **Seasonal Adjustment:** Not Seasonally Adjusted
- **Data Range:** 1982-01-01 to Present (2025-10-01)
- **Popularity:** 69/100
- **Category:** Prices
- **Source:** Federal Reserve Bank of Cleveland

**What it measures:**
- Expected rate of inflation over the next 10 years
- Calculated using a model with Treasury yields, inflation data, and inflation swaps

**Recent Values:**
- October 2025: 2.29%
- September 2025: 2.30%
- August 2025: 2.28%

**API Access:**
- Get series info: `fred/series?series_id=EXPINF10YR`
- Get observations: `fred/series/observations?series_id=EXPINF10YR`

---

## 8. UMCSENT - Table Data
**Type:** Release Tables
**Release ID:** 91 (Surveys of Consumers)

**What it is:**
- Tabular data from the Surveys of Consumers release
- Contains multiple related series organized in table format
- Provides a structured view of all consumer sentiment metrics

**API Access:**
- Get release tables: `fred/release/tables?release_id=91`
- Get all series in release: `fred/release/series?release_id=91`

**What you can get:**
- All series published as part of the Surveys of Consumers release
- Organized in table format showing relationships between series
- Includes UMCSENT and related metrics

---

## Summary Table

| Client Reference | Type | Series ID / Release ID | Frequency | Status |
|------------------|------|----------------------|-----------|---------|
| Surveys of Consumers | Release | 91 | Monthly | ✅ Found |
| UMCSENT | Series | UMCSENT | Monthly | ✅ Found |
| EXPINF1YR | Series | EXPINF1YR | Monthly | ✅ Found |
| T10YIE | Series | T10YIE | **Daily** | ✅ Found |
| US Consumer Confidence FRED/OECD | Series | USACSCICP02STSAM | Monthly | ✅ Found |
| US Consumer Confidence Amplitude | Series | CSCICP03USM665S | Monthly | ✅ Found |
| EXPINF10YR | Series | EXPINF10YR | Monthly | ✅ Found |
| UMCSENT - Table Data | Release Tables | 91 | Monthly | ✅ Found |

---

## Available Fields from Each Data Point

### For All Series Observations:
Every observation returned by the API includes:

```json
{
  "realtime_start": "2025-12-10",
  "realtime_end": "2025-12-10",
  "date": "2025-10-01",
  "value": "53.6"
}
```

**Fields:**
- `realtime_start` - Start date of the real-time period
- `realtime_end` - End date of the real-time period
- `date` - The observation date (the actual period the data represents)
- `value` - The data value

### For Series Metadata:
When you query series information, you get:

- `id` - Series ID
- `title` - Full descriptive title
- `observation_start` - First available observation date
- `observation_end` - Most recent observation date
- `frequency` - Data frequency (Daily, Monthly, Quarterly, etc.)
- `units` - Unit of measurement
- `seasonal_adjustment` - Seasonal adjustment status
- `last_updated` - When the series was last updated
- `popularity` - Popularity score (0-100)
- `notes` - Detailed description and methodology

### Additional Transformations Available:
The API can automatically transform data using the `units` parameter:
- **lin** - Linear (no transformation)
- **chg** - Change
- **ch1** - Change from year ago
- **pch** - Percent change
- **pc1** - Percent change from year ago
- **pca** - Compounded annual rate of change
- **cch** - Continuously compounded rate of change
- **cca** - Continuously compounded annual rate of change
- **log** - Natural log

---

## Next Steps

1. **Clarify Missing Data**: Ask client about "US Consumer Confidence FRED/OECD"
2. **Identify Specific Fields**: For each source, determine which fields to scrape
3. **Define Date Ranges**: What historical range is needed?
4. **Determine Update Frequency**: How often should data be refreshed?
5. **Plan Storage**: How should this data be stored (CSV, database, etc.)?

---

## API Rate Limits Reminder
- 120 requests per minute
- With 7-8 series to pull, plenty of headroom
- Can pull all historical data in single requests (up to 100,000 observations each)
