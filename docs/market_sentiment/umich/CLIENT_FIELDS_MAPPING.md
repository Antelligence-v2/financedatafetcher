# Client Requested Fields - Mapping Status

## Fields Requested:
1. Index of consumer sentiment
2. Current economic conditions
3. Consumer expectations
4. Year ahead inflation
5. Long run inflation

---

## Mapping Results:

### ✅ Available in FRED:

#### 1. Index of Consumer Sentiment
**Series ID:** `UMCSENT`
- **Title:** University of Michigan: Consumer Sentiment
- **Units:** Index 1966:Q1=100
- **Frequency:** Monthly
- **Range:** 1952-11-01 to 2025-10-01
- **Status:** ✅ FOUND

---

#### 4. Year Ahead Inflation
**Two possible series:**

**Option A - University of Michigan:**
- **Series ID:** `MICH`
- **Title:** University of Michigan: Inflation Expectation
- **Units:** Percent
- **Frequency:** Monthly
- **Range:** 1978-01-01 to 2025-10-01
- **Status:** ✅ FOUND

**Option B - Cleveland Fed:**
- **Series ID:** `EXPINF1YR`
- **Title:** 1-Year Expected Inflation
- **Units:** Percent
- **Frequency:** Monthly
- **Range:** 1982-01-01 to 2025-10-01
- **Source:** Federal Reserve Bank of Cleveland
- **Status:** ✅ FOUND

**❓ Need Clarification:** Which one does the client want?

---

#### 5. Long Run Inflation
**Series ID:** `EXPINF10YR`
- **Title:** 10-Year Expected Inflation
- **Units:** Percent
- **Frequency:** Monthly
- **Range:** 1982-01-01 to 2025-10-01
- **Source:** Federal Reserve Bank of Cleveland
- **Status:** ✅ FOUND

---

### ❌ NOT Available as Separate FRED Series:

#### 2. Current Economic Conditions
**Status:** ❌ NOT FOUND in FRED

**Possible explanations:**
1. **Not published by FRED** - Component may be available on University of Michigan website but not as a separate FRED series
2. **Part of UMCSENT** - May be a sub-component that's calculated into the overall sentiment index but not published separately
3. **Different name** - May exist under a different name/series ID

**What was checked:**
- ✗ No series found with "current economic conditions" + "Michigan"
- ✗ No series with IDs like ICC, ICSURRENT, UMCSCURR, etc.
- ✗ Release tables show no additional series

---

#### 3. Consumer Expectations
**Status:** ❌ NOT FOUND in FRED

**Possible explanations:**
1. **Not published by FRED** - Component may be available on University of Michigan website but not as a separate FRED series
2. **Part of UMCSENT** - May be a sub-component that's calculated into the overall sentiment index but not published separately
3. **Different name** - May exist under a different name/series ID

**What was checked:**
- ✗ No series found with "consumer expectations" + "Michigan"
- ✗ No series with IDs like ICE, ICEXPECT, UMCSEXP, etc.
- ✗ Release tables show no additional series

---

## What FRED Does Have from University of Michigan:

The **Surveys of Consumers (Release 91)** contains only **3 series**:

1. **UMCSENT** - University of Michigan: Consumer Sentiment
2. **MICH** - University of Michigan: Inflation Expectation
3. **UMCSENT1** - Historical Consumer Sentiment (discontinued, pre-1978)

---

## Next Steps / Questions for Client:

### 1. For "Current Economic Conditions" and "Consumer Expectations":

**Question:** Where do you expect to get these fields from?

**Options:**
- **A) University of Michigan website directly?**
  - These may be published on the UMich Surveys of Consumers website
  - Would require web scraping, not API access
  - Need to verify if publicly available

- **B) Are they sub-components you calculate from UMCSENT?**
  - If they're derived/calculated from the main sentiment index
  - Need formula/methodology

- **C) Different data source entirely?**
  - Another provider/database that has these components
  - Need source information

### 2. For "Year Ahead Inflation":

**Question:** Which series do you prefer?

**Option A:** `MICH` - University of Michigan: Inflation Expectation
- Same source as UMCSENT (University of Michigan)
- Part of the Surveys of Consumers
- Data since 1978

**Option B:** `EXPINF1YR` - 1-Year Expected Inflation
- Federal Reserve Bank of Cleveland model
- Different methodology (uses Treasury yields, swaps, etc.)
- Data since 1982

---

## Summary Table

| Field Requested | FRED Series ID | Status | Notes |
|-----------------|----------------|--------|-------|
| Index of Consumer Sentiment | UMCSENT | ✅ Found | Ready to use |
| Current Economic Conditions | ??? | ❌ Not Found | Need source clarification |
| Consumer Expectations | ??? | ❌ Not Found | Need source clarification |
| Year Ahead Inflation | MICH or EXPINF1YR | ⚠️ Multiple Options | Need client preference |
| Long Run Inflation | EXPINF10YR | ✅ Found | Ready to use |

---

## If Components ARE Available on UMich Website:

### University of Michigan Surveys of Consumers Website:
- **URL:** http://www.sca.isr.umich.edu/
- **Data Tables:** May have detailed breakdowns not available in FRED
- **Access:** Would require web scraping or checking if they offer downloads

### What We'd Need to Know:
1. Exact URL/page where the data is published
2. Format (PDF, HTML table, Excel download, etc.)
3. Update frequency and timing
4. Whether data requires login/subscription

---

## Recommended Clarification Questions:

1. **Are "Current Economic Conditions" and "Consumer Expectations" from the University of Michigan survey?**
   - If yes, where do you currently see/access this data?

2. **For inflation expectations:**
   - Do you want Michigan survey data (MICH)?
   - Or Cleveland Fed model (EXPINF1YR)?

3. **Do all these fields need to come from the same source/release?**
   - Or is it okay to combine data from multiple sources?

4. **For the components not in FRED:**
   - Are you open to web scraping the UMich website?
   - Or should we find alternative data sources?
