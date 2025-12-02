"""
URL Discovery Module

Finds all URLs on a website using multiple methods:
1. sitemap.xml - Lists all pages the site wants indexed
2. robots.txt - May reference additional sitemaps
3. Navigation menus - Important pages are usually in the nav
4. Recursive link extraction - Follow all internal links
"""
import asyncio
import re
from typing import Set, List, Optional
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET

import aiohttp
from bs4 import BeautifulSoup

from config import (
    USER_AGENT, 
    LOCATION_URL_KEYWORDS,
    LOCATION_LINK_TEXT_KEYWORDS,
    EXCLUDED_URL_PATTERNS,
    PAGE_TIMEOUT_MS
)
from utils import normalize_url, is_same_domain, extract_domain


class URLDiscovery:
    """Discovers URLs on a website using multiple methods."""
    
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
        self.domain = extract_domain(base_url)
        self.discovered_urls: Set[str] = set()
        self.sitemap_urls: Set[str] = set()
        self.nav_urls: Set[str] = set()
        self.priority_urls: Set[str] = set()  # URLs likely to have location data
        
    async def discover_all(self, session: aiohttp.ClientSession) -> Set[str]:
        """
        Run all discovery methods and return combined URLs.
        Only uses real URLs - no guessing.
        """
        # Run sitemap and robots.txt discovery
        await self._discover_from_sitemap(session)
        await self._discover_from_robots(session)
        
        # Always include the homepage - crawler will follow links from there
        self.discovered_urls.add(self.base_url)
        
        # Don't guess at URLs - let the crawler discover them by following links
        return self.discovered_urls
    
    async def _discover_from_sitemap(self, session: aiohttp.ClientSession) -> None:
        """Fetch and parse sitemap.xml."""
        sitemap_urls_to_try = [
            f"{self.base_url}/sitemap.xml",
            f"{self.base_url}/sitemap_index.xml",
            f"{self.base_url}/sitemap/sitemap.xml",
        ]
        
        for sitemap_url in sitemap_urls_to_try:
            try:
                urls = await self._fetch_sitemap(session, sitemap_url)
                if urls:
                    self.sitemap_urls.update(urls)
                    self.discovered_urls.update(urls)
                    self._identify_priority_urls(urls)
                    break  # Found a working sitemap
            except Exception:
                continue
    
    async def _fetch_sitemap(self, session: aiohttp.ClientSession, url: str) -> Set[str]:
        """Fetch and parse a single sitemap, handling sitemap indexes."""
        urls = set()
        
        try:
            timeout = aiohttp.ClientTimeout(total=PAGE_TIMEOUT_MS / 1000)
            async with session.get(url, timeout=timeout, headers={'User-Agent': USER_AGENT}) as response:
                if response.status != 200:
                    return urls
                
                content = await response.text()
                
                # Check if it's XML
                if not content.strip().startswith('<?xml') and '<urlset' not in content and '<sitemapindex' not in content:
                    return urls
                
                # Parse XML
                try:
                    root = ET.fromstring(content)
                except ET.ParseError:
                    return urls
                
                # Handle namespace
                ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                
                # Check if it's a sitemap index (contains other sitemaps)
                sitemap_refs = root.findall('.//sm:sitemap/sm:loc', ns)
                if not sitemap_refs:
                    sitemap_refs = root.findall('.//sitemap/loc')
                
                if sitemap_refs:
                    # It's a sitemap index, fetch each referenced sitemap
                    for sitemap_ref in sitemap_refs[:10]:  # Limit to 10 sub-sitemaps
                        sub_url = sitemap_ref.text
                        if sub_url:
                            sub_urls = await self._fetch_sitemap(session, sub_url.strip())
                            urls.update(sub_urls)
                else:
                    # It's a regular sitemap, extract URLs
                    url_elements = root.findall('.//sm:url/sm:loc', ns)
                    if not url_elements:
                        url_elements = root.findall('.//url/loc')
                    
                    for url_elem in url_elements:
                        if url_elem.text:
                            page_url = url_elem.text.strip()
                            if is_same_domain(page_url, self.domain):
                                urls.add(page_url)
                
        except asyncio.TimeoutError:
            pass
        except Exception:
            pass
        
        return urls
    
    async def _discover_from_robots(self, session: aiohttp.ClientSession) -> None:
        """Check robots.txt for sitemap references."""
        robots_url = f"{self.base_url}/robots.txt"
        
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with session.get(robots_url, timeout=timeout, headers={'User-Agent': USER_AGENT}) as response:
                if response.status != 200:
                    return
                
                content = await response.text()
                
                # Find Sitemap directives
                for line in content.split('\n'):
                    line = line.strip()
                    if line.lower().startswith('sitemap:'):
                        sitemap_url = line.split(':', 1)[1].strip()
                        if sitemap_url and sitemap_url not in self.sitemap_urls:
                            urls = await self._fetch_sitemap(session, sitemap_url)
                            self.sitemap_urls.update(urls)
                            self.discovered_urls.update(urls)
                            self._identify_priority_urls(urls)
                            
        except Exception:
            pass
    
    def _add_common_location_urls(self) -> None:
        """Add common URL patterns that might contain location data."""
        common_paths = [
            '/locations', '/terminals', '/facilities', '/service-centers',
            '/contact', '/about', '/about-us', '/contact-us',
            '/find-us', '/our-locations', '/terminal-locations',
            '/service-center-locations', '/network', '/coverage',
            '/where-we-service', '/shipping-locations'
        ]
        
        for path in common_paths:
            url = self.base_url + path
            self.discovered_urls.add(url)
            self.priority_urls.add(url)
    
    def _identify_priority_urls(self, urls: Set[str]) -> None:
        """Identify URLs that are likely to contain location data."""
        for url in urls:
            url_lower = url.lower()
            if any(keyword in url_lower for keyword in LOCATION_URL_KEYWORDS):
                self.priority_urls.add(url)
    
    def extract_links_from_html(self, html: str, page_url: str) -> Set[str]:
        """
        Extract all internal links from HTML content.
        Returns set of normalized URLs.
        """
        links = set()
        
        try:
            soup = BeautifulSoup(html, 'lxml')
            
            # Find all anchor tags
            for a_tag in soup.find_all('a', href=True):
                href = a_tag.get('href', '')
                if not href:
                    continue
                
                # Normalize the URL
                normalized = normalize_url(href, page_url)
                if normalized and is_same_domain(normalized, self.domain):
                    links.add(normalized)
                    
                    # Check if this looks like a location link
                    link_text = a_tag.get_text().lower()
                    href_lower = href.lower()
                    
                    if any(kw in href_lower for kw in LOCATION_URL_KEYWORDS):
                        self.priority_urls.add(normalized)
                    elif any(kw in link_text for kw in LOCATION_LINK_TEXT_KEYWORDS):
                        self.priority_urls.add(normalized)
            
        except Exception:
            pass
        
        return links
    
    def extract_nav_links(self, html: str, page_url: str) -> Set[str]:
        """
        Extract links specifically from navigation elements.
        These are high-value links.
        """
        nav_links = set()
        
        try:
            soup = BeautifulSoup(html, 'lxml')
            
            # Common navigation selectors
            nav_selectors = [
                'nav',
                'header',
                '[role="navigation"]',
                '.nav', '.navbar', '.navigation',
                '.menu', '.main-menu', '.primary-menu',
                '#nav', '#navigation', '#menu',
                'footer',  # Footer often has location links
            ]
            
            for selector in nav_selectors:
                try:
                    elements = soup.select(selector)
                    for element in elements:
                        for a_tag in element.find_all('a', href=True):
                            href = a_tag.get('href', '')
                            normalized = normalize_url(href, page_url)
                            if normalized and is_same_domain(normalized, self.domain):
                                nav_links.add(normalized)
                                self.nav_urls.add(normalized)
                except Exception:
                    continue
            
        except Exception:
            pass
        
        return nav_links


async def test_discovery(url: str):
    """Test URL discovery on a single site."""
    print(f"\nTesting URL discovery for: {url}")
    print("=" * 60)
    
    discovery = URLDiscovery(url)
    
    async with aiohttp.ClientSession() as session:
        urls = await discovery.discover_all(session)
        
        print(f"\nTotal URLs discovered: {len(urls)}")
        print(f"From sitemap: {len(discovery.sitemap_urls)}")
        print(f"Priority URLs (likely location pages): {len(discovery.priority_urls)}")
        
        if discovery.priority_urls:
            print("\nPriority URLs:")
            for purl in sorted(discovery.priority_urls)[:20]:
                print(f"  {purl}")
        
        print("\nSample of all URLs:")
        for sample_url in sorted(urls)[:10]:
            print(f"  {sample_url}")


if __name__ == '__main__':
    # Test with a few sample sites
    test_urls = [
        'https://www.odfl.com',
        'https://www.estes-express.com', 
        'https://www.saia.com',
    ]
    
    for url in test_urls:
        asyncio.run(test_discovery(url))
        print("\n")

