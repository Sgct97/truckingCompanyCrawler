# Full Carrier Crawl - Server Instructions

## System Requirements
- **OS**: Windows (tested) / Linux / macOS
- **RAM**: 32GB (recommended for 8 parallel workers)
- **Python**: 3.10+
- **Network**: Stable internet connection

## Quick Start (Windows)

### 1. Clone the Repository
```powershell
git clone https://github.com/Sgct97/truckingCompanyCrawler.git
cd truckingCompanyCrawler
```

### 2. Create Virtual Environment
```powershell
python -m venv venv
.\venv\Scripts\activate
```

### 3. Install Dependencies
```powershell
pip install -r requirements.txt
playwright install chromium
```

### 4. Copy the Excel File
Copy `2025-11-25 Top Fleets for Spenser.xlsx` to the parent directory:
```
truckingCompanyCrawler/
├── crawler/
│   ├── run_full_crawl.py
│   ├── ...
└── 2025-11-25 Top Fleets for Spenser.xlsx  <-- HERE
```

### 5. Run the Crawl
```powershell
cd crawler
python run_full_crawl.py --workers 8
```

## Command Options

| Option | Description | Default |
|--------|-------------|---------|
| `--workers N` | Number of parallel browsers | 8 |
| `--start N` | Start from carrier index (0-based) | 0 |
| `--resume` | Resume from last checkpoint | - |

### Examples
```powershell
# Run with 10 parallel workers (for 32GB RAM)
python run_full_crawl.py --workers 10

# Start from carrier #100
python run_full_crawl.py --start 100

# Resume after interruption
python run_full_crawl.py --resume
```

## Expected Runtime
- **500 carriers @ 8 workers**: ~6-10 hours
- **500 carriers @ 10 workers**: ~5-8 hours

## Output Files

After completion, find results in `data/`:

```
data/
├── crawled_pages/           # Raw HTML from each carrier
│   ├── averitt_com/
│   ├── jbhunt_com/
│   └── ...
├── reports/
│   ├── crawl_results_YYYYMMDD_HHMMSS.json   # Full results
│   └── modality_report_YYYYMMDD_HHMMSS.txt  # Deliverable report
└── crawl_checkpoint.json    # Resume checkpoint
```

## Modality Report Format

The final `modality_report_*.txt` contains:

```
### Averitt (averitt_com)
Location pages: 116/490
Top URL: https://www.averitt.com/locations
Modalities: INDEX_PAGE, ADDRESS_LIST, GOOGLE_MAPS_API
Extraction: HTML: Scrape address list | GMAPS_EMBED: Extract markers
```

## Troubleshooting

### "Playwright browsers not installed"
```powershell
playwright install chromium
```

### "Out of memory"
Reduce workers:
```powershell
python run_full_crawl.py --workers 4
```

### "Crawl interrupted"
Resume from checkpoint:
```powershell
python run_full_crawl.py --resume
```

### "Excel file not found"
Ensure `2025-11-25 Top Fleets for Spenser.xlsx` is in the parent directory.

## What the Agent on This Computer Should Know

**Goal**: Generate a modality report showing, for each of 500+ trucking carriers:
1. Which page(s) contain location data (terminals, service centers, drop yards)
2. What type of data it is (address list, embedded map, PDF, iframe, etc.)
3. How to extract the data later (specific extraction approach)

**This is NOT about extracting actual addresses** - it's about auditing HOW each carrier surfaces their location data.

**Key Files**:
- `page_crawler.py` - Crawls websites using Playwright
- `classifier.py` - Classifies pages and recommends extraction methods
- `run_full_crawl.py` - Orchestrates the full 500+ carrier crawl

**The classifier detects**:
- Address lists (10+ addresses = high confidence)
- Google Maps embeds/API (including Journey Sharing for loadboards)
- Coordinates in page source
- Location search forms
- PDFs with service maps
- Interactive maps (Mapbox, Leaflet, ArcGIS)
- Iframes with location content

**Known limitations**:
- Some carriers have wrong URLs in the Excel (Knight-Swift, CRST, etc.)
- ~5-10% of carriers may show NO_LOC and need manual review
- Non-US URLs are penalized but not completely filtered

