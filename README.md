# Data-Fetch: Financial Data Scraper Framework

An agentic AI framework for dynamically scraping financial data from websites and exporting to Excel.

## Features

- **Dynamic URL Support**: Add any website through interactive setup
- **AI-Powered Detection**: Uses OpenAI to intelligently detect data structures
- **robots.txt Compliance**: Automatically checks and respects website permissions
- **Multiple Data Sources**: Supports API endpoints, HTML tables, JavaScript data, CSV, and XML
- **Fallback Chain**: Automatically tries multiple extraction strategies (API → JS → Table → CSV → XML)
- **Anti-Bot Bypass**: Stealth mode with fingerprint randomization to bypass basic bot detection
- **Authentication Support**: API keys, cookies, sessions, and OAuth token management
- **Rate Limiting**: Per-domain rate limiting to prevent blocking
- **Financial Data Normalization**: Automatic currency, percentage, and number format standardization
- **Enhanced Validation**: Financial-specific validation (OHLC, price ranges, anomaly detection)
- **Excel Export**: Clean single-sheet Excel output with metadata

## Installation

### Prerequisites

- Python 3.9+
- OpenAI API key (for dynamic URL detection)

### Setup

```bash
# Navigate to the data-fetch directory
cd data-fetch

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers (for dynamic page loading)
playwright install chromium

# Copy environment template and add your API key
cp env.example .env
# Edit .env and add your OPENAI_API_KEY
```

## Deployment

### Railway Deployment (Recommended)

Railway provides excellent support for Docker-based deployments with Playwright.

**Quick Setup:**

1. **Push to GitHub**
   - Ensure your code is in a GitHub repository
   - Railway will auto-detect the Dockerfile

2. **Create Railway Project**
   - Sign up at [railway.app](https://railway.app)
   - Click "New Project" → "Deploy from GitHub repo"
   - Select your repository

3. **Configure Environment Variables**
   - In Railway dashboard, go to your service → Variables
   - Add required variables:
     - `OPENAI_API_KEY` (optional, for LLM features)
     - `ALPHA_VANTAGE_API_KEY` (optional, for Alpha Vantage data)
   - Railway automatically sets `PORT` variable

4. **Deploy**
   - Railway will automatically build and deploy using the Dockerfile
   - The app will be available at the provided Railway domain

**Key Files:**
- `Dockerfile` - Container configuration with Playwright
- `railway.json` - Railway deployment configuration
- `.dockerignore` - Files excluded from Docker build

**Troubleshooting:**

- **Browser automation fails**: Ensure you're using the Playwright Docker image (`mcr.microsoft.com/playwright/python`)
- **Memory issues**: Railway may need larger instance for multiple concurrent browser instances
- **Timeout errors**: Increase timeout values in scraper configurations for slow-loading sites

## Usage

### Quick Start

```bash
# List all configured sites
python main.py list-sites

# Scrape from a pre-configured site
python main.py scrape --site theblock_btc_eth_volume_7dma

# Scrape from any URL
python main.py scrape --url https://example.com/financial-data

# Test extraction without saving
python main.py test --url https://example.com/data

# Check robots.txt permissions
python main.py check-robots --url https://example.com
```

### Adding a New Website

```bash
# Interactive setup (recommended)
python main.py setup --url https://example.com/data

# This will:
# 1. Check robots.txt permissions
# 2. Discover data sources on the page
# 3. Show you what data was found
# 4. Ask for confirmation
# 5. Save configuration for future use
```

### CLI Commands

| Command | Description |
|---------|-------------|
| `scrape` | Extract data from a site or URL |
| `setup` | Add a new website interactively |
| `list-sites` | Show all configured sites |
| `test` | Test extraction without saving |
| `check-robots` | Check robots.txt permissions |

### Scrape Options

```bash
python main.py scrape --help

Options:
  -s, --site TEXT          Site ID from configuration
  -u, --url TEXT           URL to scrape (universal scraper)
  -o, --output TEXT        Output Excel file path
  --override-robots        Override robots.txt for UNKNOWN status
  --no-export              Skip Excel export
  --use-fallbacks/--no-fallbacks  Use fallback sources
```

## Configuration

Website configurations are stored in `config/websites.yaml`:

```yaml
sites:
  - id: my_site_id
    name: "My Site Name"
    base_url: "https://example.com"
    page_url: "https://example.com/data"
    extraction_strategy: "api_json"  # or "dom_table", "js_object"
    data_source:
      type: "api"
      endpoint: "https://example.com/api/data"
      method: "GET"
    field_mappings:
      date: "timestamp"
      value: "price"
    robots_policy:
      status: "ALLOWED"
```

## Project Structure

```
data-fetch/
├── main.py                 # CLI entry point
├── config/
│   └── websites.yaml       # Site configurations
├── src/
│   ├── utils/              # Utilities (logging, robots, browser)
│   ├── scraper/            # Scraper implementations
│   ├── detector/           # AI-powered data detection
│   ├── extractor/          # Data extraction (tables, JSON)
│   ├── setup/              # Interactive setup wizard
│   ├── pipeline/           # Data validation and orchestration
│   └── exporter/           # Excel export
├── outputs/
│   ├── raw/                # Raw response dumps
│   └── excel/              # Exported Excel files
└── tests/                  # Unit tests
```

## Environment Variables

Create a `.env` file with:

```bash
# Required for dynamic URL support
OPENAI_API_KEY=sk-your-key-here

# Optional - API Keys for fallback sources
COINGECKO_API_KEY=your-key
COINGECKO_USE_PRO=false  # Set to true/1/yes if using Pro tier
COINDESK_API_KEY=your-key
CRYPTOCOMPARE_API_KEY=your-key  # Legacy, use COINDESK_API_KEY

# Dune Analytics API key (required for Dune queries)
DUNE_API_KEY=your-dune-api-key-here

# Optional - CoinGlass API key (browser scraping used by default)
# COINGLASS_API_KEY=your-coinglass-key-here

# Optional - CME Group API key (requires paid subscription)
# CME_API_KEY=your-cme-api-key-here

# Optional - Authentication and Session Management
COOKIE_STORAGE_PATH=~/.data-fetch/cookies
SESSION_TIMEOUT=3600
AUTH_RETRY_ATTEMPTS=2

# Optional - Proxy server
PROXY_SERVER=http://proxy.example.com:8080

# Optional - Stealth mode (default: true)
USE_STEALTH_MODE=true

# Optional - Custom user agent
USER_AGENT=DataFetchBot/1.0 (Educational purposes)

# Optional - Logging
LOG_LEVEL=INFO
```

## Pre-configured Sites

The framework comes with several pre-configured data sources:

### Crypto Data Sources

1. **The Block** - BTC/ETH Exchange Volume 7DMA, BTC/ETH/SOL Combined Exchange Volume (TOTAL3)
2. **CoinGecko** - Market data and exchange volumes (supports free and Pro API)
3. **CoinDesk** - Historical price and volume data (formerly CryptoCompare)
4. **CoinGlass** - BTC Overview, Spot Inflow/Outflow, Volatility Metrics (browser-based)
5. **Dune Analytics** - ETH Staking and blockchain analytics (requires API key)
6. **CME Group** - BTC Futures data (requires paid subscription)
7. **Bitcoin.com** - Derivatives Snapshot (via CoinGlass data)
8. **Invezz** - Liquidations data
9. **24/7 Wall St** - Crypto Wipeout metrics

### Traditional Finance

10. **Alpha Vantage** - Company overview and stock data (free tier available)

See `config/websites.yaml` for all configured sites and examples. For API key setup, see `docs/API_KEYS.md`.

## Usage Examples

### Example 1: Scrape from URL

```bash
# Scrape any financial website
python main.py scrape --url https://www.example.com/market-data

# With stealth mode and override robots.txt
python main.py scrape --url https://www.example.com/data --override-robots
```

### Example 2: Programmatic Usage

```python
from src.scraper.universal_scraper import UniversalScraper
from src.exporter.excel_exporter import ExcelExporter

# Create scraper
scraper = UniversalScraper(use_stealth=True)

# Scrape URL
result = scraper.scrape(url="https://example.com/data")

# Export to Excel
if result.success:
    exporter = ExcelExporter()
    exporter.export(result.data, "output.xlsx")
```

### Example 3: Using Site Configuration

```python
from src.utils.config_manager import ConfigManager
from src.pipeline.pipeline_runner import PipelineRunner

# Load configuration
config_manager = ConfigManager()
site_config = config_manager.get("my_site_id")

# Run pipeline
runner = PipelineRunner(config_manager=config_manager)
result = runner.run(site_id="my_site_id")
```

### Example 4: Custom Extractor

```python
from src.extractor.csv_extractor import CsvExtractor

# Extract from CSV URL
extractor = CsvExtractor()
df = extractor.extract_from_url("https://example.com/data.csv")
```

### Example 5: Financial Normalization

```python
from src.extractor.financial_normalizer import FinancialNormalizer

normalizer = FinancialNormalizer()

# Normalize prices
normalized_price = normalizer.normalize_price("$1,234.56")  # Returns 1234.56

# Normalize percentages
normalized_pct = normalizer.normalize_percentage("15.5%")  # Returns 15.5

# Normalize entire DataFrame
normalized_df = normalizer.normalize_dataframe(df)
```

## robots.txt Compliance

The framework respects website permissions:

- **ALLOWED**: Proceed with scraping
- **DISALLOWED**: Cannot scrape (blocked)
- **UNKNOWN**: Requires `--override-robots` flag

```bash
# Check before scraping
python main.py check-robots --url https://example.com/data

# Override if needed (use responsibly)
python main.py scrape --url https://example.com/data --override-robots
```

## Development

### Running Tests

```bash
# Run unit tests
pytest tests/ -v

# Run with integration tests (requires network)
pytest tests/ -v -m "integration"
```

### Adding a Custom Scraper

1. Create a new file in `src/scraper/`
2. Inherit from `BaseScraper`
3. Implement `fetch_raw()` and `parse_raw()`
4. Register in `pipeline_runner.py`

```python
from src.scraper.base_scraper import BaseScraper

class MyCustomScraper(BaseScraper):
    def fetch_raw(self, url):
        # Fetch data
        pass
    
    def parse_raw(self, raw_data):
        # Parse to DataFrame
        pass
```

## Authentication Setup

### API Keys

For sites requiring API keys, add them to your `.env` file or configure in `websites.yaml`:

```yaml
sites:
  - id: my_site
    auth_config:
      auth_type: "api_key"
      api_key: "your-api-key"
      api_key_header: "Authorization"
      api_key_format: "Bearer {key}"
```

### Cookies

For sites requiring cookies:

1. Export cookies from your browser (Netscape format)
2. Save to a file (e.g., `cookies.txt`)
3. Configure in `websites.yaml`:

```yaml
sites:
  - id: my_site
    auth_config:
      auth_type: "cookies"
      cookie_file: "path/to/cookies.txt"
```

### Session Cookies

For programmatic session management:

```yaml
sites:
  - id: my_site
    auth_config:
      auth_type: "session"
      session_cookies:
        session_id: "abc123"
        csrf_token: "xyz789"
```

## Anti-Bot Bypass

The framework includes stealth mode to bypass basic bot detection:

- **Fingerprint Randomization**: Randomizes browser fingerprints (WebGL, platform, timezone)
- **Stealth Scripts**: Injects JavaScript to hide automation flags
- **Realistic Delays**: Adds randomized delays between actions
- **Browser-like Headers**: Uses realistic browser headers

Enable in `.env`:
```bash
USE_STEALTH_MODE=true
```

Or disable for specific scrapes:
```python
scraper = UniversalScraper(use_stealth=False)
```

## Rate Limiting

Configure per-domain rate limits in `websites.yaml`:

```yaml
sites:
  - id: my_site
    rate_limit: 1.0  # Requests per second
```

The framework automatically:
- Respects `Retry-After` headers
- Implements token bucket algorithm for smooth rate limiting
- Prevents getting blocked by aggressive rate limiting

## Data Extractors

The framework supports multiple data extraction formats:

### JSON Extractor
- Handles nested structures
- Supports JSON Lines (NDJSON)
- Automatic date parsing (ISO 8601, Unix timestamps)
- Embedded HTML/XML cleaning

### CSV Extractor
- Auto-detects delimiter
- Handles various encodings
- Supports headers and headerless CSV

### XML Extractor
- XPath query support
- RSS feed handling
- Automatic structure detection

### JavaScript Extractor
- Extracts data from `window` objects
- Parses JavaScript variables
- Handles embedded JSON in script tags

### Table Extractor
- Handles merged cells (colspan/rowspan)
- Detects nested tables
- Pagination detection
- Financial data pattern recognition

## Financial Data Normalization

The framework automatically normalizes financial data:

- **Currency Symbols**: Removes and standardizes ($, €, £, etc.)
- **Large Numbers**: Handles K/M/B suffixes (1.5M → 1,500,000)
- **Percentages**: Normalizes percentage values
- **Ticker Symbols**: Standardizes ticker formats

```python
from src.extractor.financial_normalizer import FinancialNormalizer

normalizer = FinancialNormalizer()
normalized_df = normalizer.normalize_dataframe(df)
```

## Financial Validation

Enhanced validation for financial data:

- **Price Ranges**: Detects suspicious price jumps and outliers
- **OHLC Validation**: Validates Open/High/Low/Close relationships
- **Currency Consistency**: Checks for mixed currency symbols
- **Volume Validation**: Ensures volumes are positive
- **Anomaly Detection**: Identifies unusual patterns in time series
- **Quality Score**: Calculates 0-100 data quality score

## Error Handling

The framework includes intelligent error handling:

- **Error Classification**: Automatically classifies errors (network, auth, rate limit, bot detection)
- **Recovery Strategies**: Suggests recovery actions based on error type
- **Adaptive Retries**: Different retry strategies for different error types
- **Error Context**: Captures page context (screenshots, console errors) on failure

## Troubleshooting

### Common Issues

1. **"OpenAI API key not found"**
   - Set `OPENAI_API_KEY` in your `.env` file

2. **"Playwright not installed"**
   - Run `playwright install chromium`

3. **"Scraping disallowed by robots.txt"**
   - The website doesn't allow scraping that path
   - Try using a public API instead
   - Use `--override-robots` flag (use responsibly)

4. **"No data sources found"**
   - The page may require authentication
   - Try enabling stealth mode
   - Check if the page loads data via JavaScript (may need to wait longer)
   - Try inspecting the page manually in browser dev tools

5. **"Bot detection / Cloudflare blocking"**
   - Enable stealth mode: `USE_STEALTH_MODE=true`
   - Use a proxy server: `PROXY_SERVER=http://proxy.example.com:8080`
   - Add delays between requests
   - Check if the site requires authentication

6. **"Rate limit exceeded"**
   - Reduce request frequency in `websites.yaml` (`rate_limit`)
   - The framework will automatically wait and retry
   - Consider using API keys if available (higher rate limits)

7. **"Authentication failed"**
   - Verify API keys are correct
   - Check cookie files are valid and not expired
   - Ensure session cookies are up to date
   - Try refreshing authentication

8. **"Parsing error: unhashable type"**
   - This usually means nested dictionaries/lists in data
   - The framework handles this automatically in most cases
   - Check the raw response to understand data structure

9. **"No data extracted"**
   - Try different extraction strategies (the framework tries multiple automatically)
   - Check if data is loaded via JavaScript (may need longer wait times)
   - Verify the URL is correct and accessible
   - Check browser console for JavaScript errors

10. **"Validation warnings"**
    - Review validation warnings in the output
    - Check data quality score
    - Some warnings are informational and don't prevent export

11. **"libgbm.so.1: cannot open shared object file" (Railway/Docker)**
    - This indicates system dependencies are missing
    - Ensure you're using the Playwright Docker image (`mcr.microsoft.com/playwright/python`)
    - Verify `playwright install-deps chromium` runs successfully in Dockerfile
    - Check Railway deployment logs for browser installation errors
    - If using a custom Dockerfile, ensure all system dependencies are installed

## License

MIT License

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests
5. Submit a pull request

