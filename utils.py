"""
Utility functions for the crawler.
"""
import re
from urllib.parse import urljoin, urlparse
from typing import Optional

from config import EXCLUDED_URL_PATTERNS


def normalize_url(url: str, base_url: str) -> Optional[str]:
    """
    Normalize a URL: make absolute, remove fragments, lowercase domain.
    Returns None if URL should be excluded.
    """
    if not url or not url.strip():
        return None
    
    url = url.strip()
    
    # Skip excluded patterns
    url_lower = url.lower()
    for pattern in EXCLUDED_URL_PATTERNS:
        if pattern in url_lower:
            return None
    
    # Make absolute
    if url.startswith('//'):
        url = 'https:' + url
    elif url.startswith('/'):
        url = urljoin(base_url, url)
    elif not url.startswith('http'):
        url = urljoin(base_url, url)
    
    # Parse and rebuild without fragment
    parsed = urlparse(url)
    
    # Only http/https
    if parsed.scheme not in ('http', 'https'):
        return None
    
    # Rebuild without fragment
    normalized = f"{parsed.scheme}://{parsed.netloc.lower()}{parsed.path}"
    if parsed.query:
        normalized += f"?{parsed.query}"
    
    # Remove trailing slash for consistency (except root)
    if normalized.endswith('/') and parsed.path != '/':
        normalized = normalized[:-1]
    
    return normalized


def is_same_domain(url: str, base_domain: str) -> bool:
    """Check if URL belongs to the same domain (including subdomains)."""
    try:
        parsed = urlparse(url)
        url_domain = parsed.netloc.lower()
        base_domain = base_domain.lower()
        
        # Remove www. prefix for comparison
        url_domain = url_domain.replace('www.', '')
        base_domain = base_domain.replace('www.', '')
        
        # Exact match or subdomain
        return url_domain == base_domain or url_domain.endswith('.' + base_domain)
    except:
        return False


def extract_domain(url: str) -> str:
    """Extract domain from URL."""
    parsed = urlparse(url)
    return parsed.netloc.lower().replace('www.', '')


def clean_text(text: str) -> str:
    """Clean text by removing extra whitespace."""
    if not text:
        return ''
    return ' '.join(text.split())


def looks_like_address(text: str) -> bool:
    """Quick heuristic to check if text might contain an address."""
    if not text:
        return False
    
    # Must have a number (street number or ZIP)
    if not re.search(r'\d', text):
        return False
    
    # Should have state abbreviation or common street suffix
    state_pattern = r'\b[A-Z]{2}\b'
    street_pattern = r'\b(Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Way|Lane|Ln|Highway|Hwy)\b'
    
    return bool(re.search(state_pattern, text) or re.search(street_pattern, text, re.IGNORECASE))


def format_duration(seconds: float) -> str:
    """Format seconds into human-readable duration."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"

