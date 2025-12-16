# Data Sources Reference

This document provides a comprehensive overview of all data sources scraped by the pipeline, including the specific fields extracted from each source.

---

## Overview

| Category | Source Count |
|----------|--------------|
| Cryptocurrency & Exchange | 4 |
| Blockchain Analytics | 1 |
| Macroeconomic Indicators | 3 |
| Equity Data | 1 |
| Dental ETF Sector | 4 |
| News Feeds | 6 |
| **Total** | **26** |

---

## Cryptocurrency & Exchange Data

### The Block

| Property | Value |
|----------|-------|
| **Source IDs** | `theblock_btc_eth_volume_7dma`, `theblock_exchange_volume` |
| **Website** | https://www.theblock.co |
| **Extraction Method** | API JSON |
| **Rate Limit** | 2 seconds |
| **Scraper Class** | `TheBlockScraper` |

**Fields Extracted:**
- BTC/ETH Total Exchange Volume (7-day moving average)
- Monthly spot volume
- Exchange-specific volumes (Binance, Bybit, Bitget)
- BTC, ETH, SOL volumes
- DEX monthly volume

---

### CoinGecko

| Property | Value |
|----------|-------|
| **Source IDs** | `coingecko_exchange_volume`, `coingecko_btc_market_chart` |
| **Website** | https://api.coingecko.com |
| **Extraction Method** | API JSON |
| **Rate Limit** | 1 second |
| **Scraper Class** | `CoinGeckoScraper` |

**Fields Extracted:**
- 24-hour trading volume for exchanges
- Historical price data (30 days)
- Market caps
- Total volumes

*Note: Supports both Free and Pro API tiers.*

---

### CoinGlass

| Property | Value |
|----------|-------|
| **Source IDs** | `coinglass_btc_overview`, `coinglass_spot_flows`, `coinglass_volatility`, `bitcoin_com_derivatives_snapshot`, `invezz_liquidations` |
| **Website** | https://www.coinglass.com |
| **Extraction Method** | Browser-based DOM extraction with JavaScript evaluation |
| **Rate Limit** | 5 seconds |
| **Scraper Class** | `CoinGlassScraper` |

**Fields Extracted:**
- BTC Price
- Futures Volume (24h)
- Spot Volume (24h)
- Open Interest
- Net Inflow (5min, 1h, 4h, 12h, 24h)
- Volatility metrics (BTC, ETH, SOL, XRP, DOGE - 1-day)
- Futures OI (All Exchanges, CME, Binance)
- BTC Options Calls/Puts OI
- Liquidations (Total 24h, Long, Short, BTC 24h, ETH 24h)

*Note: Uses Playwright for browser automation with stealth mode.*

---

### CryptoCompare (Deprecated)

| Property | Value |
|----------|-------|
| **Source ID** | `cryptocompare_exchange_volume` |
| **Website** | https://min-api.cryptocompare.com |
| **Extraction Method** | API JSON |
| **Rate Limit** | 0.5 seconds |
| **Scraper Class** | `CryptoCompareScraper` |
| **Status** | Deprecated - use CoinDesk alternative |

**Fields Extracted:**
- Daily exchange volume data (historical)
- Volume and price data

---

## Blockchain Analytics

### Dune Analytics

| Property | Value |
|----------|-------|
| **Source IDs** | `dune_eth_staking_total_deposited`, `dune_eth_staking_validators_depositors`, `dune_eth_staking` |
| **Website** | https://dune.com |
| **Extraction Method** | API with multi-step execution (execute -> poll -> fetch) |
| **Query IDs** | 2361452 (ETH Deposited), 2361448 (Validators/Depositors) |
| **Rate Limit** | 2 seconds |
| **Authentication** | `DUNE_API_KEY` (Free tier: 2500 credits) |
| **Scraper Class** | `DuneScraper` |

**Fields Extracted:**
- Total ETH Deposited
- Total Validators
- Distinct Depositor Addresses

*Note: Supports SDK and manual API fallback.*

---

## Macroeconomic Indicators

### FRED (Federal Reserve Economic Data)

| Property | Value |
|----------|-------|
| **Website** | https://api.stlouisfed.org |
| **Extraction Method** | API JSON |
| **Rate Limit** | 0.5 seconds (120 requests/minute) |
| **Authentication** | `FRED_API_KEY` |
| **Scraper Class** | `FredScraper` |

**Source IDs & Series:**

| Source ID | Series ID | Description |
|-----------|-----------|-------------|
| `fred_consumer_confidence` | USACSCICP02STSAM | Consumer Confidence |
| `fred_10y_breakeven_inflation` | T10YIE | 10-Year Breakeven Inflation |
| `fred_consumer_sentiment` | UMCSENT | Consumer Sentiment |
| `fred_5y5y_forward_inflation` | T5YIFR | 5-Year, 5-Year Forward Inflation |
| `fred_oecd_amplitude_adjusted` | CSCICP03USM665S | OECD Amplitude Adjusted |
| `fred_cleveland_1yr_inflation` | EXPINF1YR | Cleveland Fed 1-Year Inflation |
| `fred_cleveland_10yr_inflation` | EXPINF10YR | Cleveland Fed 10-Year Inflation |

**Fields Extracted:**
- Date
- Value
- Series ID
- Series Name
- Realtime Start/End dates

**Special Features:**
- Rate limiting with token bucket algorithm
- 429 error handling with exponential backoff
- Series info and observation fetching
- Historical data support

---

### University of Michigan - Surveys of Consumers

| Property | Value |
|----------|-------|
| **Source ID** | `umich_consumer_surveys` |
| **Website** | https://www.sca.isr.umich.edu |
| **Extraction Method** | CSV multi-file download |
| **Rate Limit** | 2 seconds |
| **Data History** | Monthly data from 1952 onwards |
| **Scraper Class** | `UMichScraper` |

**Files Downloaded:**
- `tbmics.csv` (Sentiment)
- `tbmiccice.csv` (Components)
- `tbmpx1px5.csv` (Inflation)

**Fields Extracted:**

| Field Code | Description |
|------------|-------------|
| ICS_ALL | Index of Consumer Sentiment |
| ICC | Current Economic Conditions |
| ICE | Consumer Expectations |
| PX_MD | Year Ahead Inflation |
| PX5_MD | Long Run Inflation |

---

### DG ECFIN - EU Business & Consumer Surveys

| Property | Value |
|----------|-------|
| **Source ID** | `dg_ecfin_surveys` |
| **Website** | https://ec.europa.eu/economy_finance/db_indicators/surveys |
| **Extraction Method** | ZIP -> Excel download |
| **Rate Limit** | 5 seconds |
| **Data History** | From 1985 onwards |
| **Scraper Class** | `DGECFINScraper` |

**Fields Extracted:**

| Field Code | Description |
|------------|-------------|
| EU.ESI | Economic Sentiment Indicator (EU) |
| EA.ESI | Economic Sentiment Indicator (Euro Area) |
| EU.EEI | Employment Expectations Indicator (EU) |
| EA.EEI | Employment Expectations Indicator (Euro Area) |
| EA.CONS | Flash Consumer Confidence (Euro Area) |

---

## Equity Data

### Alpha Vantage

| Property | Value |
|----------|-------|
| **Source IDs** | `alphavantage_company_overview`, `alphavantage_top_20_stocks` |
| **Website** | https://www.alphavantage.co |
| **Extraction Method** | API JSON |
| **Rate Limit** | 12 seconds (5 calls/minute - free tier) |
| **Authentication** | `ALPHA_VANTAGE_API_KEY` |
| **Scraper Class** | `AlphaVantageScraper` |

**Fields Extracted:**
- Symbol
- Name
- Market Capitalization
- Sector
- Industry
- 52-week High Price

---

## Dental ETF Sector Data

### SwingTradeBot - Dentistry ETF List

| Property | Value |
|----------|-------|
| **Source ID** | `dental_swingtradebot_etf_list` |
| **Website** | https://swingtradebot.com |
| **Extraction Method** | DOM table extraction |

**Fields Extracted:**
- Symbol
- Name
- Grade
- % Change
- Weighting
- Holdings

---

### Yahoo Finance - ETF Holdings

| Property | Value |
|----------|-------|
| **Source ID** | `dental_yahoo_etf_holdings` |
| **Website** | https://finance.yahoo.com |
| **Extraction Method** | DOM table extraction |

**Fields Extracted:**
- Symbol
- Name
- % Assets
- Shares

---

### Fintel - Dental Equipment Companies (SIC 3843)

| Property | Value |
|----------|-------|
| **Source ID** | `dental_fintel_sic_3843` |
| **Website** | https://fintel.io |
| **Extraction Method** | DOM table extraction |

**Fields Extracted:**
- Ticker
- Company
- Market Cap
- Country

---

### PortfolioPilot - Dental Stocks Risk/Return

| Property | Value |
|----------|-------|
| **Source ID** | `dental_portfoliopilot_risk_return` |
| **Website** | https://portfoliopilot.com |
| **Extraction Method** | DOM table extraction |

**Fields Extracted:**
- Ticker
- Expected Return
- Sharpe Ratio
- Beta
- Volatility
- P/E
- Dividend Yield

---

## News Feeds (RSS)

Configured in `/config/news_sources.yaml`:

| Source | Focus Area |
|--------|------------|
| CoinDesk | Crypto/blockchain news |
| The Block | Crypto markets and data |
| Decrypt | Crypto news and analysis |
| CoinTelegraph | Bitcoin and crypto news |
| Financial Times - Fintech | Financial technology news |
| TechCrunch - Fintech | Fintech startup news |

---

## Extraction Methods Summary

| Method | Description | Used By |
|--------|-------------|---------|
| **API JSON** | Direct REST API calls with rate limiting | The Block, CoinGecko, FRED, Alpha Vantage, Dune |
| **Browser DOM** | Playwright-based extraction with stealth mode | CoinGlass, Yahoo Finance, Fintel, SwingTradeBot, PortfolioPilot |
| **CSV Download** | Multi-file CSV downloads with parsing | UMich Surveys |
| **ZIP -> Excel** | ZIP archive extraction with Excel parsing | DG ECFIN |
| **JavaScript Evaluation** | Extract from React state/window objects | CoinGlass (supplementary) |

---

## Pipeline Architecture

```
Configuration (YAML)
       |
       v
PipelineRunner.run()
       |
       v
_run_scraper() - Select appropriate scraper
       |
       v
Site-Specific Scraper (TheBlockScraper, DuneScraper, etc.)
       |
       v
BaseScraper.scrape()
  |-- check_compliance() - Verify robots.txt
  |-- fetch_raw() - Get raw data
  |-- parse_raw() - Parse to DataFrame
  |-- validate() - Validate data
  +-- Return ScraperResult
       |
       v
DataValidator - Financial-specific validation
       |
       v
ExcelExporter - Export to Excel
```

### Scraper Routing

The pipeline automatically routes to the appropriate scraper based on `site_id` prefix:

| Prefix | Scraper Class |
|--------|---------------|
| `theblock_*` | TheBlockScraper |
| `coinglass_*` | CoinGlassScraper |
| `dune_*` | DuneScraper |
| `fred_*` | FredScraper |
| `umich_*` | UMichScraper |
| `dg_ecfin_*` | DGECFINScraper |
| `coingecko_*` | CoinGeckoScraper |
| `alphavantage_*` | AlphaVantageScraper |
| (default) | UniversalScraper |

---

## Authentication Requirements

| Source | Auth Type | Environment Variable |
|--------|-----------|---------------------|
| Dune Analytics | API Key (Header) | `DUNE_API_KEY` |
| FRED | API Key (Query Param) | `FRED_API_KEY` |
| Alpha Vantage | API Key (Query Param) | `ALPHA_VANTAGE_API_KEY` |
| CoinGecko Pro | API Key (Header) | `COINGECKO_API_KEY` |

See [API_KEYS.md](./API_KEYS.md) for detailed setup instructions.
