# Site Configuration Guide

This guide explains how to add and configure new websites in the data-fetch framework.

## Overview

Website configurations are stored in `config/websites.yaml`. Each site has a unique ID and configuration that specifies how to extract data from it.

## Configuration Structure

A basic site configuration looks like this:

```yaml
sites:
  - id: my_site_id
    name: "My Site Name"
    base_url: "https://example.com"
    page_url: "https://example.com/data"
    extraction_strategy: "api_json"  # or "dom_table", "js_object", "dom_js_extraction"
    data_source:
      type: "api"  # or "browser", "dom_table", "js_object"
      endpoint: "https://example.com/api/data"
      method: "GET"
      requires_auth: false
      headers: {}
    field_mappings:
      date: "timestamp"
      value: "price"
    robots_policy:
      status: "ALLOWED"  # or "DISALLOWED", "UNKNOWN"
      last_checked: "2025-12-09T00:00:00Z"
      override_approved: false
    metadata:
      created: "2025-12-09T00:00:00Z"
      created_by: "manual"
      last_successful_extraction: null
      requires_subscription: false
    rate_limit: 2.0  # seconds between requests
```

## Extraction Strategies

### 1. API JSON (`api_json`)

For sites with REST API endpoints that return JSON:

```yaml
extraction_strategy: "api_json"
data_source:
  type: "api"
  endpoint: "https://api.example.com/data"
  method: "GET"
  requires_auth: false
  headers:
    Accept: "application/json"
```

**Use when:**
- Site has a public or documented API
- Data is returned as JSON
- No authentication required (or simple API key)

### 2. DOM Table (`dom_table`)

For sites with HTML tables:

```yaml
extraction_strategy: "dom_table"
data_source:
  type: "dom_table"
  selector: "table.data-table"  # Optional CSS selector
```

**Use when:**
- Data is in HTML tables
- Tables are visible in page source
- No JavaScript rendering required

### 3. JavaScript Object (`js_object`)

For sites that load data via JavaScript:

```yaml
extraction_strategy: "js_object"
data_source:
  type: "js_object"
```

**Use when:**
- Data is in JavaScript variables (e.g., `window.data`)
- React/Next.js sites with `__NEXT_DATA__`
- Data loaded dynamically after page load

### 4. Browser-Based Extraction (`dom_js_extraction`)

For sites requiring browser automation (Cloudflare protection, complex JS):

```yaml
extraction_strategy: "dom_js_extraction"
data_source:
  type: "browser"
  page_url: "https://example.com/data"
```

**Use when:**
- Site has Cloudflare or bot protection
- Complex JavaScript rendering
- Multiple data sources on page
- CoinGlass, CME Group, etc.

## Field Mappings

Field mappings translate API/HTML field names to standardized column names:

```yaml
field_mappings:
  date: "Date"  # Maps "Date" column to "date"
  btc_price: "BTC Price"
  volume_24h: "24h Volume"
```

The framework will automatically rename columns based on these mappings.

## Authentication

### API Key Authentication

```yaml
auth_config:
  auth_type: "api_key"
  api_key_env: "DUNE_API_KEY"  # Environment variable name
  api_key_header: "x-dune-api-key"  # Header name
  api_key_format: "{key}"  # Format string (e.g., "Bearer {key}")
```

### Cookie-Based Authentication

```yaml
auth_config:
  auth_type: "cookies"
  cookie_file: "path/to/cookies.txt"
```

## Rate Limiting

Set rate limits to avoid being blocked:

```yaml
rate_limit: 2.0  # seconds between requests
```

**Recommendations:**
- API endpoints: 1-2 seconds
- Browser scraping: 5-10 seconds
- Sites with strict limits: 10+ seconds

## Robots.txt Policy

```yaml
robots_policy:
  status: "ALLOWED"  # or "DISALLOWED", "UNKNOWN"
  last_checked: "2025-12-09T00:00:00Z"
  override_approved: false
```

**Status values:**
- `ALLOWED`: robots.txt permits scraping
- `DISALLOWED`: robots.txt blocks scraping
- `UNKNOWN`: robots.txt not checked or unclear

## Special Cases

### Dune Analytics Queries

Dune requires a 3-step process (execute → poll → fetch):

```yaml
data_source:
  type: "api"
  endpoint: "https://api.dune.com/api/v1/query"
  method: "POST"
  requires_auth: true
  query_id: "123456"  # Your Dune query ID
  max_poll_attempts: 30
  poll_interval: 2
  parameters: {}  # Query parameters if needed
auth_config:
  auth_type: "api_key"
  api_key_env: "DUNE_API_KEY"
  api_key_header: "x-dune-api-key"
```

### The Block API Pattern

The Block uses a predictable API pattern:

```yaml
page_url: "https://www.theblock.co/data/crypto-markets/spot/btc-and-eth-total-exchange-volume-7dma"
data_source:
  endpoint: "https://www.theblock.co/api/charts/chart/crypto-markets/spot/btc-and-eth-total-exchange-volume-7dma"
```

The endpoint pattern is: `/api/charts/chart/{path}` where `{path}` is from `/data/{path}`.

## Adding a New Site

### Step 1: Analyze the Site

1. Check robots.txt: `python main.py check-robots --url https://example.com`
2. Inspect the page source for data location
3. Check network requests for API endpoints
4. Identify extraction strategy

### Step 2: Create Configuration

Use the interactive setup:

```bash
python main.py setup --url https://example.com/data
```

Or manually add to `config/websites.yaml`.

### Step 3: Test Configuration

```bash
python main.py scrape --site my_site_id
```

### Step 4: Verify Output

Check the Excel output for:
- Correct field mappings
- Data quality
- Missing values
- Date formats

## Troubleshooting

### No Data Extracted

**Possible causes:**
1. Wrong extraction strategy
2. Selector not found
3. JavaScript not executed
4. Authentication required

**Solutions:**
- Try different extraction strategies
- Use browser-based extraction for complex sites
- Check network requests for API endpoints
- Verify authentication configuration

### Rate Limited

**Error:** `429 Too Many Requests`

**Solutions:**
- Increase `rate_limit` value
- Add delays between requests
- Use exponential backoff

### Cloudflare Blocking

**Error:** `403 Forbidden` or Cloudflare challenge

**Solutions:**
- Enable stealth mode
- Use browser-based extraction
- Add longer delays
- Check robots.txt compliance

## Best Practices

1. **Start with API endpoints** if available (faster, more reliable)
2. **Use browser extraction** only when necessary (slower, more resource-intensive)
3. **Set appropriate rate limits** to avoid blocking
4. **Test configurations** before adding to production
5. **Document special requirements** in metadata notes
6. **Update robots.txt status** after checking
7. **Use field mappings** for consistent column names

## Examples

See `config/websites.yaml` for complete examples of:
- API-based extraction (CoinGecko, The Block)
- Browser-based extraction (CoinGlass, CME)
- Dune Analytics queries
- Authentication configurations

## Next Steps

- See `API_KEYS.md` for authentication setup
- Check `README.md` for usage examples
- Review existing configurations in `config/websites.yaml`



