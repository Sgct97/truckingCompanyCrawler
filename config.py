"""
Crawler configuration settings.
"""
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
CRAWL_OUTPUT_DIR = DATA_DIR / "crawled_pages"
REPORTS_DIR = DATA_DIR / "reports"

# Input file
CARRIERS_FILE = PROJECT_ROOT.parent / "2025-11-25 Top Fleets for Spenser.xlsx"

# Crawl settings
MAX_PAGES_PER_SITE = 200  # Crawl up to 200 pages per domain (enough to find location pages)
MAX_CONCURRENT_BROWSERS = 10  # Parallel browser instances
PAGE_TIMEOUT_MS = 30000  # 30 seconds per page - shared browser needs more time
REQUEST_DELAY_MS = 0  # No delay - page load time is enough

# URL discovery settings
LOCATION_URL_KEYWORDS = [
    'location', 'terminal', 'facilit', 'service-center', 'service_center',
    'coverage', 'network', 'find-us', 'find_us', 'where-we', 'branch',
    'office', 'warehouse', 'yard', 'depot', 'hub', 'contact', 'about',
    'center', 'site', 'operation'
]

LOCATION_LINK_TEXT_KEYWORDS = [
    'location', 'terminal', 'find', 'near you', 'service center',
    'coverage', 'network', 'where we', 'our facilities', 'branches',
    'contact us', 'offices'
]

# Excluded URL patterns (don't crawl these)
EXCLUDED_URL_PATTERNS = [
    '/blog', '/news', '/press', '/career', '/job', '/apply',
    '/login', '/signin', '/register', '/cart', '/checkout',
    '/privacy', '/terms', '/legal', '/cookie',
    '.pdf', '.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp',
    '.mp4', '.mp3', '.avi', '.mov',
    '.zip', '.rar', '.exe', '.dmg',
    'facebook.com', 'twitter.com', 'linkedin.com', 'instagram.com',
    'youtube.com', 'mailto:', 'tel:', 'javascript:'
]

# Location signal detection patterns
ADDRESS_PATTERNS = [
    # Full US address: 123 Main St, City, ST 12345
    r'\d+\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Way|Lane|Ln|Court|Ct|Circle|Cir|Highway|Hwy|Parkway|Pkwy)[,.\s]+[A-Za-z\s]+,?\s*[A-Z]{2}\s*\d{5}',
    # City, State ZIP
    r'[A-Z][a-z]+(?:\s[A-Z][a-z]+)*,?\s*[A-Z]{2}\s+\d{5}',
    # City, State (without ZIP)
    r'[A-Z][a-z]+(?:\s[A-Z][a-z]+)*,\s*[A-Z]{2}(?:\s|$|<)',
]

# Map embed detection patterns
GOOGLE_MAPS_PATTERNS = [
    'google.com/maps',
    'maps.google.com',
    'maps.googleapis.com',
]

GOOGLE_MY_MAPS_PATTERN = '/maps/d/embed'

OTHER_MAP_PATTERNS = [
    'mapbox',
    'leaflet',
    'openstreetmap',
    'arcgis',
    'here.com/maps',
]

# User agent for requests
USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

