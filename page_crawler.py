"""
Page Crawler Module

Uses Playwright to:
1. Visit pages with full JavaScript rendering
2. Extract all links from the rendered page
3. Save HTML content for later analysis
4. Detect location signals while crawling
"""
import asyncio
import json
import hashlib
import random
from pathlib import Path
from typing import Dict, Set, List, Optional, Any
from datetime import datetime
from urllib.parse import urlparse

import aiohttp
from playwright.async_api import async_playwright, Page, Browser, BrowserContext
from playwright_stealth import Stealth
from bs4 import BeautifulSoup

# Rotate user agents to avoid detection
USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0',
]

from config import (
    PAGE_TIMEOUT_MS,
    REQUEST_DELAY_MS,
    MAX_PAGES_PER_SITE,
    CRAWL_OUTPUT_DIR,
    USER_AGENT,
)
from utils import normalize_url, is_same_domain, extract_domain
from url_discovery import URLDiscovery


class PageCrawler:
    """Crawls a single website using Playwright."""
    
    def __init__(self, base_url: str, carrier_name: str):
        self.base_url = base_url.rstrip('/')
        self.carrier_name = carrier_name
        self.domain = extract_domain(base_url)
        
        # URL tracking
        self.urls_to_visit: List[str] = []
        self.visited_urls: Set[str] = set()
        self.failed_urls: Set[str] = set()
        
        # Results
        self.pages_data: List[Dict[str, Any]] = []
        
        # Stats
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        
        # Output directory for this carrier
        safe_domain = self.domain.replace('.', '_').replace('/', '_')
        self.output_dir = CRAWL_OUTPUT_DIR / safe_domain
        
    async def crawl(self, browser: Browser, initial_urls: Set[str] = None) -> Dict[str, Any]:
        """
        Crawl the website starting from initial_urls.
        Returns crawl results and statistics.
        
        NEW APPROACH: Crawl ALL homepage links + ALL PDFs + tool subdomains.
        Let classifier score pages instead of pre-filtering.
        """
        self.start_time = datetime.now()
        
        # Set up initial URLs - ALWAYS start with homepage
        self.urls_to_visit = [self.base_url]  # Homepage first!
        
        if initial_urls:
            # Add sitemap URLs - prioritize index pages but include all
            index_urls = []
            other_urls = []
            
            for url in initial_urls:
                if url != self.base_url and url != self.base_url + '/':
                    if self._is_index_page(url) or self._is_pdf_or_map(url):
                        index_urls.append(url)
                    else:
                        other_urls.append(url)
            
            # Add index/PDF URLs first, then others (up to limit)
            self.urls_to_visit.extend(index_urls)
            # Add some other URLs too (classifier will filter)
            self.urls_to_visit.extend(other_urls[:50])
            
            print(f"  {self.carrier_name}: {len(index_urls)} index/PDF, {len(other_urls)} other from sitemap")
        
        print(f"  {self.carrier_name}: Starting with {len(self.urls_to_visit)} seed URLs")
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create browser context with anti-detection
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={'width': 1920, 'height': 1080},
            ignore_https_errors=True,
            locale='en-US',
            timezone_id='America/New_York',
        )
        
        try:
            page = await context.new_page()
            # Apply stealth to avoid bot detection
            stealth = Stealth()
            await stealth.apply_stealth_async(page)
            
            # Crawl pages
            page_num = 0
            while self.urls_to_visit and len(self.visited_urls) < MAX_PAGES_PER_SITE:
                url = self.urls_to_visit.pop(0)
                
                # Skip if already visited
                if url in self.visited_urls or url in self.failed_urls:
                    continue
                
                page_num += 1
                
                # First page gets extra time to render JS navigation
                is_first = (page_num == 1)
                
                # Crawl the page
                page_data = await self._crawl_page(page, url, is_homepage=is_first)
                
                if page_data:
                    self.pages_data.append(page_data)
                    
                    # Extract and queue new URLs found on this page
                    # NEW: Add ALL links from homepage, prioritize index/PDFs from other pages
                    new_urls = page_data.get('extracted_links', [])
                    added_count = 0
                    for new_url in new_urls:
                        if new_url not in self.visited_urls and new_url not in self.failed_urls:
                            if new_url not in self.urls_to_visit:
                                # Always add index pages and PDFs/maps at front
                                if self._is_index_page(new_url) or self._is_pdf_or_map(new_url):
                                    self.urls_to_visit.insert(0, new_url)
                                    added_count += 1
                                # Add tool subdomain links (often have location finders)
                                elif self._is_tool_subdomain(new_url):
                                    self.urls_to_visit.insert(1, new_url)
                                    added_count += 1
                                # From ANY page: add ALL same-site links (classifier will filter)
                                elif self._is_same_site(new_url):
                                    self.urls_to_visit.append(new_url)
                                    added_count += 1
                    
                    # Show progress every 10 pages or if we found many new links
                    if page_num % 10 == 0 or added_count > 20:
                        print(f"  {self.carrier_name}: {page_num} pages, queue={len(self.urls_to_visit)}, +{added_count} links")
                
                # Small delay between requests
                await asyncio.sleep(REQUEST_DELAY_MS / 1000)
            
            print(f"  {self.carrier_name}: Done - {page_num} pages crawled, {len(self.failed_urls)} failed")
                
        finally:
            await context.close()
        
        self.end_time = datetime.now()
        
        # Save crawl summary
        summary = self._generate_summary()
        await self._save_summary(summary)
        
        return summary
    
    async def _crawl_page(self, page: Page, url: str, is_homepage: bool = False) -> Optional[Dict[str, Any]]:
        """Crawl a single page and extract data."""
        self.visited_urls.add(url)
        
        try:
            # Navigate to page
            response = await page.goto(
                url,
                timeout=PAGE_TIMEOUT_MS,
                wait_until='domcontentloaded'
            )
            
            if not response:
                self.failed_urls.add(url)
                return None
            
            status_code = response.status
            
            # Skip non-success responses
            if status_code >= 400:
                self.failed_urls.add(url)
                return None
            
            # Check if this is an INDEX page (highest priority - needs full JS render)
            url_lower = url.lower()
            is_index = self._is_index_page(url)
            
            # Only INDEX pages and homepage get extended wait time
            if is_homepage or is_index:
                # Give JS time to render important pages
                await page.wait_for_timeout(2000)
                # Try scrolling to trigger lazy-loaded content
                try:
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(500)
                except:
                    pass
            # No wait for other pages - just get the content
            
            # Get the rendered HTML
            html = await page.content()
            
            # Get the final URL (in case of redirects)
            final_url = page.url
            
            # Extract ALL links using Playwright JS (sees JS-rendered content)
            links = await self._extract_links_js(page, final_url)
            
            # Extract page metadata
            title = await page.title()
            
            # Check for location signals
            signals = self._detect_location_signals(html, page)
            
            # Create page data
            page_data = {
                'url': url,
                'final_url': final_url,
                'status_code': status_code,
                'title': title,
                'html_length': len(html),
                'extracted_links': list(links),
                'link_count': len(links),
                'location_signals': signals,
                'has_location_data': bool(signals),
                'crawled_at': datetime.now().isoformat(),
            }
            
            # Save HTML to file
            await self._save_page_html(url, html)
            
            return page_data
            
        except asyncio.TimeoutError:
            self.failed_urls.add(url)
            return None
        except Exception as e:
            self.failed_urls.add(url)
            return None
    
    async def _extract_links_js(self, page: Page, page_url: str) -> Set[str]:
        """Extract all internal links using Playwright JS (sees JS-rendered content)."""
        links = set()
        
        try:
            # Use locator which is more stable than eval_on_selector_all
            all_links = await page.locator('a[href]').all()
            for link in all_links:
                try:
                    href = await link.get_attribute('href')
                    if href:
                        normalized = normalize_url(href, page_url)
                        if normalized and is_same_domain(normalized, self.domain):
                            links.add(normalized)
                except:
                    continue
        except Exception:
            # Fallback: try regex on HTML content
            try:
                html = await page.content()
                import re
                hrefs = re.findall(r'href="([^"]+)"', html)
                for href in hrefs:
                    normalized = normalize_url(href, page_url)
                    if normalized and is_same_domain(normalized, self.domain):
                        links.add(normalized)
            except:
                pass
        
        return links
    
    def _extract_links(self, html: str, page_url: str) -> Set[str]:
        """Extract all internal links from HTML (fallback/classification use)."""
        links = set()
        
        try:
            soup = BeautifulSoup(html, 'lxml')
            
            for a_tag in soup.find_all('a', href=True):
                href = a_tag.get('href', '')
                if not href:
                    continue
                
                normalized = normalize_url(href, page_url)
                if normalized and is_same_domain(normalized, self.domain):
                    links.add(normalized)
                    
        except Exception:
            pass
        
        return links
    
    def _detect_location_signals(self, html: str, page: Page) -> Dict[str, Any]:
        """
        Basic signal detection - full classification happens later.
        Just flag pages that might have location data.
        """
        # Keep it simple - classifier will do detailed analysis
        return {}
    
    def _is_priority_url(self, url: str) -> bool:
        """Check if URL is likely to contain location data."""
        url_lower = url.lower()
        priority_keywords = [
            'location', 'terminal', 'facilit', 'service-center', 'service-location',
            'find-us', 'find-location', 'branch', 'yard', 'depot',
            'loadboard', 'load-board', 'map', 'locator', 'finder',
            'servicemap', 'branch-locator', 'store-locator', 'dealer-locator',
            'centers', 'coverage', 'network'  # Added back - classifier will filter
        ]
        return any(kw in url_lower for kw in priority_keywords)
    
    def _is_pdf_or_map(self, url: str) -> bool:
        """Check if URL is a PDF or map file - ALWAYS crawl these."""
        url_lower = url.lower()
        # PDFs with location-related names
        if '.pdf' in url_lower:
            pdf_keywords = ['map', 'service', 'terminal', 'location', 'network', 
                           'coverage', 'facility', 'directory']
            return any(kw in url_lower for kw in pdf_keywords)
        return False
    
    def _is_tool_subdomain(self, url: str) -> bool:
        """Check if URL is on a tool/app subdomain - often have location finders."""
        url_lower = url.lower()
        tool_patterns = [
            'ext-web.', 'tools.', 'apps.', 'app.', 'my.', 'portal.',
            'locator.', 'finder.', 'search.'
        ]
        return any(p in url_lower for p in tool_patterns)
    
    def _is_same_site(self, url: str) -> bool:
        """Check if URL is on the same site (including subdomains)."""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            base_parsed = urlparse(self.base_url)
            # Extract base domain (e.g., xpo.com from ext-web.ltl-xpo.com)
            url_domain = parsed.netloc.lower()
            base_domain = base_parsed.netloc.lower().replace('www.', '')
            # Check if it's same domain or subdomain
            return base_domain in url_domain or url_domain.endswith('.' + base_domain)
        except:
            return False
    
    def _is_index_page(self, url: str) -> bool:
        """Check if URL is likely an INDEX page listing all locations (highest priority)."""
        url_lower = url.lower().rstrip('/')
        # Index pages end with the keyword, not have more path segments after
        index_patterns = [
            '/locations', '/locations.html', '/our-locations',
            '/terminals', '/terminal-locations', '/all-locations',
            '/service-centers', '/service-center', '/service-center-locator',
            '/service-locations', '/facilities', '/branches',
            '/find-us', '/find-location', '/branch-locator',
            '/load-board/map', '/loadboard/map', '/map', '/map.html',
            '/locator', '/store-locator', '/dealer-locator'
        ]
        # Also check for pattern matches (case-insensitive)
        if any(url_lower.endswith(pattern) for pattern in index_patterns):
            return True
        # Check for servicemap.pdf
        if 'servicemap' in url_lower and '.pdf' in url_lower:
            return True
        return False
    
    async def _save_page_html(self, url: str, html: str) -> None:
        """Save page HTML to file with original URL preserved."""
        # Create filename from URL hash
        url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
        filename = f"{url_hash}.html"
        filepath = self.output_dir / filename
        
        try:
            # Inject original URL as meta tag if not already present
            # This ensures classifier can identify the page even without canonical
            if '<meta name="crawler-original-url"' not in html:
                inject_tag = f'<meta name="crawler-original-url" content="{url}">\n'
                # Insert after <head> tag
                if '<head>' in html:
                    html = html.replace('<head>', f'<head>\n{inject_tag}', 1)
                elif '<head ' in html:
                    # Handle <head with attributes
                    head_end = html.find('>', html.find('<head ')) + 1
                    html = html[:head_end] + f'\n{inject_tag}' + html[head_end:]
            
            filepath.write_text(html, encoding='utf-8')
        except Exception:
            pass
    
    async def _save_summary(self, summary: Dict[str, Any]) -> None:
        """Save crawl summary to JSON file."""
        filepath = self.output_dir / "crawl_summary.json"
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, default=str)
        except Exception:
            pass
    
    def _generate_summary(self) -> Dict[str, Any]:
        """Generate crawl summary statistics."""
        duration = (self.end_time - self.start_time).total_seconds() if self.end_time and self.start_time else 0
        
        # Count pages with location signals
        pages_with_signals = [p for p in self.pages_data if p.get('has_location_data')]
        
        # Aggregate signal types
        signal_types = {}
        for page in pages_with_signals:
            for signal in page.get('location_signals', {}).keys():
                signal_types[signal] = signal_types.get(signal, 0) + 1
        
        return {
            'carrier_name': self.carrier_name,
            'base_url': self.base_url,
            'domain': self.domain,
            'crawl_stats': {
                'pages_crawled': len(self.visited_urls),
                'pages_failed': len(self.failed_urls),
                'pages_with_location_signals': len(pages_with_signals),
                'duration_seconds': duration,
                'pages_per_second': len(self.visited_urls) / duration if duration > 0 else 0,
            },
            'signal_summary': signal_types,
            'pages_with_signals': [
                {
                    'url': p['url'],
                    'title': p.get('title', ''),
                    'signals': p.get('location_signals', {}),
                }
                for p in pages_with_signals
            ],
            'crawled_at': self.start_time.isoformat() if self.start_time else None,
        }


async def crawl_single_carrier(
    carrier: Dict[str, str],
    semaphore: asyncio.Semaphore,
    playwright,  # Playwright instance to create browsers
    http_session: 'aiohttp.ClientSession',
    progress_callback=None
) -> Dict[str, Any]:
    """
    Crawl a single carrier with semaphore for concurrency control.
    Each carrier gets its own browser instance (more isolated, better for bot detection).
    """
    name = carrier['name']
    url = carrier['website']
    
    async with semaphore:
        if progress_callback:
            progress_callback(f"Starting: {name}")
        
        # Create a dedicated browser for this carrier
        browser = await playwright.chromium.launch(
            headless=True,
            args=['--disable-blink-features=AutomationControlled']
        )
        
        try:
            # Discover URLs from sitemap/robots.txt
            discovery = URLDiscovery(url)
            initial_urls = await discovery.discover_all(http_session)
            print(f"  {name}: Discovered {len(initial_urls)} URLs from sitemap")
            
            # Crawl the site
            crawler = PageCrawler(url, name)
            summary = await crawler.crawl(browser, initial_urls)
            
            if progress_callback:
                pages = summary['crawl_stats']['pages_crawled']
                progress_callback(f"Done: {name} ({pages} pages)")
            
            return {'carrier': name, 'status': 'success', 'summary': summary}
                
        except Exception as e:
            print(f"  {name}: ERROR - {str(e)}")
            if progress_callback:
                progress_callback(f"Failed: {name} - {str(e)[:50]}")
            return {'carrier': name, 'status': 'error', 'error': str(e)}
        finally:
            await browser.close()


async def crawl_all_carriers(
    carriers: List[Dict[str, str]],
    max_concurrent: int = 5,
    progress_callback=None
) -> List[Dict[str, Any]]:
    """
    Crawl all carriers in parallel with concurrency limit.
    
    Args:
        carriers: List of {'name': ..., 'website': ...} dicts
        max_concurrent: Max number of browsers running at once
        progress_callback: Optional function to call with progress updates
    
    Returns:
        List of results for each carrier
    """
    print(f"\nCrawling {len(carriers)} carriers ({max_concurrent} concurrent)...")
    print("=" * 60)
    
    semaphore = asyncio.Semaphore(max_concurrent)
    results = []
    
    async with async_playwright() as p:
        # Each carrier creates its own browser (more isolated, better for bot detection)
        async with aiohttp.ClientSession() as session:
            tasks = [
                crawl_single_carrier(carrier, semaphore, p, session, progress_callback)
                for carrier in carriers
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
    # Handle any exceptions that weren't caught
    final_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            final_results.append({
                'carrier': carriers[i]['name'],
                'status': 'error',
                'error': str(result)
            })
        else:
            final_results.append(result)
    
    return final_results


async def test_parallel_crawl(carriers: List[Dict[str, str]], max_concurrent: int = 3):
    """Test parallel crawling on a few carriers."""
    print(f"\n{'='*60}")
    print(f"PARALLEL CRAWL TEST - {len(carriers)} carriers, {max_concurrent} concurrent")
    print(f"{'='*60}\n")
    
    start_time = datetime.now()
    completed = [0]
    
    def progress(msg):
        completed[0] += 1 if msg.startswith("Done") or msg.startswith("Failed") else 0
        print(f"[{completed[0]}/{len(carriers)}] {msg}")
    
    results = await crawl_all_carriers(carriers, max_concurrent, progress)
    
    duration = (datetime.now() - start_time).total_seconds()
    
    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Total time: {duration:.1f}s")
    print(f"Carriers processed: {len(results)}")
    
    successful = [r for r in results if r['status'] == 'success']
    failed = [r for r in results if r['status'] == 'error']
    
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")
    
    if successful:
        total_pages = sum(r['summary']['crawl_stats']['pages_crawled'] for r in successful)
        total_signals = sum(r['summary']['crawl_stats']['pages_with_location_signals'] for r in successful)
        print(f"Total pages crawled: {total_pages}")
        print(f"Pages with location signals: {total_signals}")
    
    if failed:
        print(f"\nFailed carriers:")
        for r in failed:
            print(f"  {r['carrier']}: {r.get('error', 'Unknown error')[:60]}")
    
    return results


if __name__ == '__main__':
    import sys
    
    # Force unbuffered output
    sys.stdout.reconfigure(line_buffering=True)
    
    print("=" * 60, flush=True)
    print("PAGE CRAWLER TEST", flush=True)
    print("=" * 60, flush=True)
    
    # Test with a few carriers
    from load_carriers import load_carriers
    
    carriers = load_carriers()[:2]  # Test with first 2 only
    print(f"\nTesting with {len(carriers)} carriers:", flush=True)
    for c in carriers:
        print(f"  - {c['name']}: {c['website']}", flush=True)
    
    print("\nStarting crawl...", flush=True)
    asyncio.run(test_parallel_crawl(carriers, max_concurrent=1))  # Just 1 at a time for testing

