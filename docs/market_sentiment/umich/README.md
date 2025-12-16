# University of Michigan Documentation

This folder contains all documentation related to the University of Michigan Surveys of Consumers data scraper.

## Files

### [UMICH_SCRAPER_README.md](UMICH_SCRAPER_README.md)
**Quick Start Guide**

How to use the UMich scraper:
- Running the scraper
- Output format and location
- Latest data values
- Advanced usage examples

**Use this for:** Day-to-day usage and reference

---

### [UMICH_COMPLETE.md](UMICH_COMPLETE.md)
**Project Summary**

Complete overview of the UMich scraper project:
- What was accomplished
- Test results and validation
- Coverage statistics
- Files created

**Use this for:** Understanding what's been built

---

### [UMICH_DATA_SOURCES.md](UMICH_DATA_SOURCES.md)
**Technical Specifications**

Detailed technical information:
- Download URLs for all data files
- File naming conventions
- Data structure and format
- Implementation notes

**Use this for:** Understanding the data sources

---

### [CLIENT_FIELDS_FINAL.md](CLIENT_FIELDS_FINAL.md)
**Field Mappings**

Complete field mapping documentation:
- All 5 client fields mapped to source columns
- File structure details
- Data ranges for each field
- Implementation plan

**Use this for:** Understanding which data comes from where

---

### [CLIENT_FIELDS_MAPPING.md](CLIENT_FIELDS_MAPPING.md)
**Initial Field Discovery**

Historical document showing the initial research:
- How fields were discovered
- What was found in FRED vs UMich
- Questions asked during discovery

**Use this for:** Historical reference

---

## Quick Links

### I want to...

**...run the scraper**
→ See [UMICH_SCRAPER_README.md](UMICH_SCRAPER_README.md) - Quick Start section

**...understand the output**
→ See [UMICH_SCRAPER_README.md](UMICH_SCRAPER_README.md) - Output section

**...see what was built**
→ See [UMICH_COMPLETE.md](UMICH_COMPLETE.md)

**...understand the data sources**
→ See [UMICH_DATA_SOURCES.md](UMICH_DATA_SOURCES.md)

**...know which field is which**
→ See [CLIENT_FIELDS_FINAL.md](CLIENT_FIELDS_FINAL.md)

---

## Main Scraper Location

The actual scraper code is located at:
```
FRED/umich_scraper.py
```

Run it from the project root:
```bash
python FRED/umich_scraper.py
```
