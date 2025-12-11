# API Key Setup Guide

This guide explains how to obtain and configure API keys for the data-fetch framework.

## Overview

Some data sources require API keys for access. The framework supports both free and paid API services. API keys are stored in environment variables for security.

## Required API Keys

### Dune Analytics (Required for Dune queries)

1. **Get your API key:**
   - Go to https://dune.com/settings/api
   - Sign in or create an account
   - Generate a new API key
   - Copy the API key

2. **Add to environment:**
   ```bash
   # Add to your .env file
   DUNE_API_KEY=your-dune-api-key-here
   ```

3. **Usage:**
   - Required for all Dune Analytics queries
   - Used in the `dune_eth_staking` and other Dune configurations
   - The API key is sent in the `x-dune-api-key` header

## Optional API Keys

### CoinGlass (Optional - Browser scraping is used instead)

CoinGlass offers a paid API, but the framework uses browser-based extraction to avoid API costs. If you prefer to use the API:

1. **Get your API key:**
   - Visit CoinGlass website
   - Sign up for API access
   - Obtain your API key

2. **Add to environment:**
   ```bash
   COINGLASS_API_KEY=your-coinglass-key-here
   ```

### CME Group (Optional - Requires paid subscription)

CME Group requires a paid subscription for API access:

1. **Get your API key:**
   - Visit CME Group website
   - Subscribe to their data service
   - Obtain your API key

2. **Add to environment:**
   ```bash
   CME_API_KEY=your-cme-api-key-here
   ```

## Environment Variable Setup

1. **Copy the example file:**
   ```bash
   cp env.example .env
   ```

2. **Edit `.env` file:**
   ```bash
   # Required for dynamic URL support
   OPENAI_API_KEY=sk-your-openai-key-here

   # Dune Analytics (required for Dune queries)
   DUNE_API_KEY=your-dune-api-key-here

   # Optional API keys
   COINGLASS_API_KEY=your-coinglass-key-here
   CME_API_KEY=your-cme-api-key-here
   ```

3. **Verify keys are loaded:**
   ```bash
   # Test that environment variables are set
   python -c "import os; print('DUNE_API_KEY:', 'SET' if os.getenv('DUNE_API_KEY') else 'NOT SET')"
   ```

## Testing API Keys

### Test Dune API Key

```python
from src.scraper.dune_scraper import DuneScraper
import os

api_key = os.getenv("DUNE_API_KEY")
if api_key:
    print("Dune API key is configured")
    # Test with a simple query
    scraper = DuneScraper(api_key=api_key)
    # Note: You'll need a valid query_id to test execution
else:
    print("Dune API key is not set")
```

## Troubleshooting

### API Key Not Found

**Error:** `DUNE_API_KEY is required`

**Solution:**
1. Check that the `.env` file exists in the `data-fetch` directory
2. Verify the variable name matches exactly (case-sensitive)
3. Restart your application/terminal after adding the key
4. For Streamlit, restart the Streamlit server

### Invalid API Key

**Error:** `401 Unauthorized` or `Invalid API key`

**Solution:**
1. Verify the API key is correct (no extra spaces)
2. Check if the API key has expired
3. Regenerate the API key from the provider's dashboard
4. Ensure you're using the correct API key for the correct service

### Rate Limit Exceeded

**Error:** `429 Too Many Requests`

**Solution:**
1. Check your API tier/plan limits
2. Reduce request frequency
3. Implement caching for repeated queries
4. Wait before retrying

## Security Best Practices

1. **Never commit API keys to version control:**
   - Add `.env` to `.gitignore`
   - Use environment variables, not hardcoded keys

2. **Use different keys for different environments:**
   - Development keys for testing
   - Production keys for live use

3. **Rotate keys regularly:**
   - Change API keys periodically
   - Revoke old keys when generating new ones

4. **Limit key permissions:**
   - Use read-only keys when possible
   - Restrict key access to specific IPs if supported

## Site-Specific Notes

### Dune Analytics
- Free tier has rate limits
- Queries may take time to execute (polling required)
- Query IDs must be set in configuration after creating queries in Dune

### CoinGlass
- Browser scraping is used by default (no API key needed)
- API key only needed if you want to use their paid API

### CME Group
- Requires paid subscription
- Browser scraping may have limited access
- API provides more reliable access

## Next Steps

After setting up API keys:
1. Test the configuration using the test scripts
2. Update site configurations in `config/websites.yaml` if needed
3. Check the Streamlit UI for API key status indicators
4. See `SITE_CONFIGURATION.md` for adding new sites



