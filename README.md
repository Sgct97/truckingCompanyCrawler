# Trucking Company Crawler

A robust web crawler designed to crawl 500+ trucking company websites and classify how they surface location data (terminals, service centers, drop yards).

## Purpose

**Phase 1: Classification/Audit**
- Crawl every page on ~500 carrier websites
- Classify each page to identify HOW location data is served
- Generate a modality report showing extraction approaches

## Location Data Modalities Detected

| Signal Type | Description |
|-------------|-------------|
| Text list | Addresses in plain HTML |
| Google Maps embed | `maps.google.com` iframes |
| Static image map | Map images (may need OCR) |
| Interactive maps | Mapbox, Leaflet, ArcGIS, custom |
| Clickable lists | Elements with onclick handlers |
| PDF links | Links to `.pdf` files |
| Search forms | Forms with zip/city/state inputs |

## Features

- **Parallel crawling** with configurable concurrency
- **Bot detection bypass** using playwright-stealth
- **JavaScript rendering** via Playwright (handles SPAs)
- **Multiple link extraction methods** (JS + regex fallback)
- **Sitemap discovery** + recursive link following
- **Dedicated browser per carrier** (avoids cross-site fingerprinting)

## Installation

```bash
cd crawler
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
```

## Usage

### Test with a few carriers
```bash
python test_parallel.py
```

### Run full crawl
```bash
python -c "
import asyncio
from page_crawler import crawl_all_carriers
from load_carriers import load_carriers

async def main():
    carriers = load_carriers()
    results = await crawl_all_carriers(carriers, max_concurrent=5)
    print(f'Crawled {len(results)} carriers')

asyncio.run(main())
"
```

## Project Structure

```
crawler/
├── page_crawler.py    # Main crawler with Playwright
├── url_discovery.py   # Sitemap and robots.txt parsing
├── load_carriers.py   # Load carriers from Excel
├── config.py          # Configuration settings
├── utils.py           # URL normalization utilities
├── requirements.txt   # Python dependencies
└── data/
    └── crawled_pages/ # Saved HTML files (gitignored)
```

## Configuration

Edit `config.py` to adjust:
- `MAX_PAGES_PER_SITE` - Max pages to crawl per carrier (default: 500)
- `PAGE_TIMEOUT_MS` - Page load timeout (default: 30000ms)
- `REQUEST_DELAY_MS` - Delay between requests (default: 50ms)

## Output

Crawled pages are saved to `data/crawled_pages/{domain}/`:
- `*.html` - Raw HTML content
- `crawl_summary.json` - Crawl statistics and metadata

## License

Proprietary - Internal use only

