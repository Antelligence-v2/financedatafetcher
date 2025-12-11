# FRED Folder Structure

## Clean & Organized Structure

```
FRED/
│
├── README.md                          # 📖 Main project overview & quick start
├── PROJECT_STATUS.md                  # 📊 Current project status & progress
├── FOLDER_STRUCTURE.md                # 📁 This file
│
├── umich_scraper.py                   # 🔧 Production UMich scraper (RUN THIS!)
│
├── docs/                              # 📚 All Documentation
│   ├── README.md                      # Documentation index
│   │
│   ├── API_DOCUMENTATION.md           # FRED API complete reference
│   ├── CLIENT_DATA_SOURCES.md         # FRED series details
│   ├── FINAL_DATA_SOURCES.md          # Quick FRED reference
│   │
│   └── umich/                         # UMich-specific docs
│       ├── README.md                  # UMich docs index
│       ├── UMICH_SCRAPER_README.md    # 👈 UMich scraper usage guide
│       ├── UMICH_COMPLETE.md          # Project completion summary
│       ├── UMICH_DATA_SOURCES.md      # Technical specs
│       ├── CLIENT_FIELDS_FINAL.md     # Field mappings
│       └── CLIENT_FIELDS_MAPPING.md   # Initial discovery notes
│
├── exploration/                       # 🔍 Research & Discovery Scripts
│   ├── README.md                      # Exploration scripts guide
│   │
│   ├── fred_explorer.py               # General FRED API examples
│   ├── explore_client_data.py         # Client data exploration
│   ├── check_release_series.py        # Release series checker
│   ├── check_oecd_series.py           # OECD series checker
│   ├── check_release_tables.py        # Release tables checker
│   ├── find_sentiment_components.py   # Component search
│   ├── download_umich_samples.py      # Sample data downloader
│   │
│   └── samples/                       # Sample data files
│       ├── sentiment.csv
│       ├── components.csv
│       └── inflation.csv
│
└── data/                              # 📊 Output Data (generated)
    ├── umich_data_combined.csv        # 👈 Main output file
    ├── umich_sentiment_raw.csv        # Raw sentiment data
    ├── umich_components_raw.csv       # Raw components data
    └── umich_inflation_raw.csv        # Raw inflation data
```

---

## Quick Navigation

### 🚀 I want to use the scraper
→ `python FRED/umich_scraper.py`
→ Read: `docs/umich/UMICH_SCRAPER_README.md`

### 📊 I want to see the data
→ Check: `data/umich_data_combined.csv`

### 📖 I want to understand the FRED API
→ Read: `docs/API_DOCUMENTATION.md`

### 🔍 I want to explore the codebase
→ Check: `exploration/` folder

### 📝 I want the project status
→ Read: `PROJECT_STATUS.md`

---

## File Categories

### Production Code (Ready to Use)
- `umich_scraper.py` ✅

### Main Documentation
- `README.md` - Project overview
- `PROJECT_STATUS.md` - Current status
- `FOLDER_STRUCTURE.md` - This file

### FRED API Documentation (`docs/`)
- API reference
- Series details
- Quick references

### UMich Documentation (`docs/umich/`)
- Scraper usage guide
- Technical specs
- Field mappings

### Research Code (`exploration/`)
- Discovery scripts
- Sample data
- Exploratory analysis

### Output Data (`data/`)
- Combined CSV output
- Raw data files

---

## Key Files

| File | Purpose | Status |
|------|---------|--------|
| `umich_scraper.py` | Main production scraper | ✅ Ready |
| `docs/umich/UMICH_SCRAPER_README.md` | How to use the scraper | ✅ Complete |
| `data/umich_data_combined.csv` | Scraped output data | ✅ Generated |
| `PROJECT_STATUS.md` | Project progress tracker | ✅ Updated |
| `docs/API_DOCUMENTATION.md` | FRED API reference | ✅ Complete |

---

## Clean & Professional

### What's in Production:
- ✅ UMich scraper
- ✅ Comprehensive documentation
- ✅ Sample data
- ✅ Validation scripts

### What's Organized:
- 📚 All docs in `docs/`
- 🔍 All research in `exploration/`
- 📊 All output in `data/`
- 🔧 Main code at root level

### What's Clear:
- Every file has a purpose
- Every folder has a README
- Every script is documented
- Every output is labeled
