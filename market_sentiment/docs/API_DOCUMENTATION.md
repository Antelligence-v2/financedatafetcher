# FRED API Documentation Summary

## Overview
The Federal Reserve Economic Data (FRED) API provides access to economic data from the Federal Reserve Bank of St. Louis. The API offers two versions for different use cases.

## API Versions
- **API Version 2**: Designed for bulk retrieval of observations across entire releases and complete historical data access
- **API Version 1**: Provides incremental, customizable data retrieval by source, release, category, and series

## Authentication

### API Key Requirements
- All requests require a 32-character lowercase alphanumeric API key
- API key is passed via the `api_key` parameter in requests
- Each application should have its own unique API key
- Each user should use their own registered API key (no sharing)

### Getting an API Key
1. Create an account at fredaccount.stlouisfed.org
2. Visit the API keys management page: https://fredaccount.stlouisfed.org/apikeys
3. Generate your API key

### Example Request
```
https://api.stlouisfed.org/fred/series/search?api_key=YOUR_KEY_HERE&search_text=canada
```

## Rate Limits
- **120 requests per minute**
- Contact FRED staff if you need to exceed this limit for legitimate use cases

## Base URL
```
https://api.stlouisfed.org/
```

## Available Endpoints

### Categories
Get information about FRED's hierarchical category structure.

| Endpoint | Description |
|----------|-------------|
| `fred/category` | Retrieve a specific category by ID |
| `fred/category/children` | Get child categories of a category |
| `fred/category/related` | Get related categories |
| `fred/category/series` | Get all series within a category |
| `fred/category/tags` | Get tags for a category |
| `fred/category/related_tags` | Get related tags for a category |

### Releases
Access economic data releases and their metadata.

| Endpoint | Description |
|----------|-------------|
| `fred/releases` | Get all economic data releases |
| `fred/releases/dates` | Get release dates for all releases |
| `fred/release` | Get a specific release |
| `fred/release/dates` | Get dates for a specific release |
| `fred/release/series` | Get all series in a release |
| `fred/release/sources` | Get sources for a release |
| `fred/release/tags` | Get tags for a release |
| `fred/release/related_tags` | Get related tags for a release |
| `fred/release/tables` | Get release tables |

### Series
The main endpoints for accessing economic data series and their observations.

| Endpoint | Description |
|----------|-------------|
| `fred/series` | Get metadata for an economic data series |
| `fred/series/categories` | Get categories for a series |
| `fred/series/observations` | **Get actual data values/observations** ⭐ |
| `fred/series/release` | Get release information for a series |
| `fred/series/search` | Search for series matching criteria |
| `fred/series/search/tags` | Get tags from search results |
| `fred/series/search/related_tags` | Get related tags from search results |
| `fred/series/tags` | Get tags for a series |
| `fred/series/updates` | Get recently updated series |
| `fred/series/vintagedates` | Get historical revision dates for a series |

### Sources
Information about data sources.

| Endpoint | Description |
|----------|-------------|
| `fred/sources` | Get all data sources |
| `fred/source` | Get a specific source |
| `fred/source/releases` | Get releases from a source |

### Tags
Search and filter data using tags.

| Endpoint | Description |
|----------|-------------|
| `fred/tags` | Get, search, or filter tags |
| `fred/related_tags` | Get related tags |
| `fred/tags/series` | Get series matching specific tags |

### Maps API
Geographic and regional data.

| Endpoint | Description |
|----------|-------------|
| Shape Files | Geographic shape files |
| Series Group Meta | Metadata for series groups |
| Series Regional Data | Regional economic data |
| Regional Data | General regional data |

## Series Observations Endpoint (Most Important)

This is the primary endpoint for retrieving actual economic data values.

### Endpoint
```
GET https://api.stlouisfed.org/fred/series/observations
```

### Key Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `api_key` | string | Yes | Your 32-character API key |
| `series_id` | string | Yes | The series identifier (e.g., "GNPCA") |
| `file_type` | string | No | Output format: xml, json, xlsx, or csv (default: xml) |
| `observation_start` | date | No | Start date for observations (YYYY-MM-DD) |
| `observation_end` | date | No | End date for observations (YYYY-MM-DD) |
| `realtime_start` | date | No | Start of real-time period (YYYY-MM-DD) |
| `realtime_end` | date | No | End of real-time period (YYYY-MM-DD) |
| `limit` | integer | No | Max results (default/max: 100,000) |
| `offset` | integer | No | Pagination offset |
| `units` | string | No | Data transformation (linear, percent change, etc.) |
| `frequency` | string | No | Data aggregation frequency |
| `sort_order` | string | No | 'asc' or 'desc' |

### Response Formats
- **XML** (default)
- **JSON**
- **Excel (.xlsx)**
- **CSV** (zipped)

### Data Transformations (units parameter)
The API can transform data automatically:
- Linear (default)
- Percent change
- Annual rates
- And more transformation options

### Limitations
- Maximum of 100,000 results per request
- Frequency aggregation only available for XML/JSON formats
- Vintage date queries limited by series type and output format
- Not all series have geographical data

## Response Structure

Each observation typically includes:
- Date of observation
- Value
- Realtime metadata attributes

## Important Notes

1. **Data Quality**: FRED contains high-quality, authoritative economic data from federal agencies and other sources
2. **Historical Revisions**: Many series include vintage dates showing how data was revised over time
3. **Geographic Data**: Not all series have associated geographic/regional data
4. **Bulk Access**: Use API Version 2 for bulk data retrieval
5. **Incremental Access**: Use API Version 1 for more granular, filtered data access

## Best Practices

1. Use your own unique API key per application
2. Stay within the 120 requests/minute rate limit
3. Use appropriate file_type for your use case (JSON for web apps, CSV for data analysis)
4. Leverage pagination (limit/offset) for large datasets
5. Cache data locally when possible to reduce API calls
6. Use data transformations (units parameter) to avoid manual calculations

## Next Steps

To start using the API:
1. Get your API key from fredaccount.stlouisfed.org
2. Identify series of interest using search or browsing categories
3. Use series/observations endpoint to retrieve actual data
4. Respect rate limits and implement appropriate error handling

## Useful Links

- API Documentation: https://fred.stlouisfed.org/docs/api/fred/
- API Key Management: https://fredaccount.stlouisfed.org/apikeys
- Error Codes: https://fred.stlouisfed.org/docs/api/fred/errors.html
