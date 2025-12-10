# FRED Exploration Scripts

This folder contains scripts used during the research and exploration phase to understand the FRED API and identify client data sources.

## Scripts

### [fred_explorer.py](fred_explorer.py)
**General FRED API Explorer**

A comprehensive example script showing how to interact with the FRED API.

Features:
- Search for series by text
- Get series information and metadata
- Retrieve observations (data points)
- Browse categories and releases
- Example usage patterns

Run it:
```bash
python exploration/fred_explorer.py
```

Example output:
- Searches for GDP series
- Shows recent GDP observations
- Browses top-level categories
- Lists economic data releases

---

### [explore_client_data.py](explore_client_data.py)
**Client-Specific Data Exploration**

Explores all 8 client-requested data sources in detail.

Features:
- Iterates through each client data source
- Retrieves detailed metadata
- Shows recent observations
- Identifies releases and categories
- Exports comprehensive information

Run it:
```bash
python exploration/explore_client_data.py
```

This script was used to:
- Identify the correct series IDs
- Understand data frequency and units
- Check data availability and ranges
- Verify all sources are accessible

---

### [check_release_series.py](check_release_series.py)
**Release Series Checker**

Checks what series are contained in a specific release.

Features:
- Lists all series in a release
- Shows metadata for each series
- Used to explore "Surveys of Consumers" release

Run it:
```bash
python exploration/check_release_series.py
```

Used to discover:
- Release 91 contains UMCSENT, MICH, and UMCSENT1
- Understanding release table structure

---

### [check_oecd_series.py](check_oecd_series.py)
**OECD Series Checker**

Explores the OECD Composite Consumer Confidence series in detail.

Features:
- Detailed metadata for USACSCICP02STSAM
- Recent observations
- Release information

Run it:
```bash
python exploration/check_oecd_series.py
```

Used to verify:
- Correct series ID for "US Consumer Confidence FRED/OECD"
- Data availability and format
- OECD citation requirements

---

## Usage Notes

These scripts are **exploration/research tools** and were used during the initial investigation phase. They are kept for reference and testing purposes.

For the actual data pipeline, see the main FRED directory for production scripts.

All scripts require:
- FRED API key in `.env`
- `requests` library
- `python-dotenv` library
