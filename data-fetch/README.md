# Data-Fetch: Financial Data Scraper Framework

An agentic AI framework for dynamically scraping financial data from websites and exporting to Excel.

## Features

- **Dynamic URL Support**: Add any website through interactive setup
- **AI-Powered Detection**: Uses OpenAI to intelligently detect data structures
- **robots.txt Compliance**: Automatically checks and respects website permissions
- **Multiple Data Sources**: Supports API endpoints, HTML tables, and JavaScript data
- **Fallback Sources**: Automatically tries alternative sources if primary fails
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

# Optional
COINGECKO_API_KEY=your-key
CRYPTOCOMPARE_API_KEY=your-key
LOG_LEVEL=INFO
```

## Pre-configured Sites

The framework comes with several pre-configured data sources:

1. **The Block** - BTC/ETH Exchange Volume 7DMA
2. **CoinGecko** - Market data and exchange volumes
3. **CryptoCompare** - Historical price and volume data

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

## Troubleshooting

### Common Issues

1. **"OpenAI API key not found"**
   - Set `OPENAI_API_KEY` in your `.env` file

2. **"Playwright not installed"**
   - Run `playwright install chromium`

3. **"Scraping disallowed by robots.txt"**
   - The website doesn't allow scraping that path
   - Try using a public API instead

4. **"No data sources found"**
   - The page may require authentication
   - Try inspecting the page manually in browser dev tools

## License

MIT License

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests
5. Submit a pull request

