"""
Quick test of the crawler with verbose output.
Only crawls 10 pages to verify it works.
"""
import asyncio
import aiohttp
from playwright.async_api import async_playwright

from url_discovery import URLDiscovery
from config import CRAWL_OUTPUT_DIR

# Override max pages for test
MAX_TEST_PAGES = 10


async def test_crawl():
    print("=" * 60)
    print("QUICK CRAWLER TEST - Saia.com (10 pages max)")
    print("=" * 60)
    
    url = "https://www.saia.com"
    
    # Step 1: Discover URLs
    print("\n[1/3] Discovering URLs from sitemap...")
    discovery = URLDiscovery(url)
    async with aiohttp.ClientSession() as session:
        initial_urls = await discovery.discover_all(session)
    
    print(f"      Found {len(initial_urls)} URLs in sitemap")
    print(f"      Priority URLs: {len(discovery.priority_urls)}")
    
    # Take only priority URLs for test, limit to 10
    test_urls = list(discovery.priority_urls)[:MAX_TEST_PAGES]
    print(f"      Testing with {len(test_urls)} priority URLs")
    
    # Step 2: Launch browser
    print("\n[2/3] Launching browser...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            viewport={'width': 1920, 'height': 1080},
        )
        page = await context.new_page()
        
        # Step 3: Crawl each page
        print("\n[3/3] Crawling pages...")
        results = []
        
        for i, test_url in enumerate(test_urls, 1):
            print(f"\n      [{i}/{len(test_urls)}] {test_url}")
            
            try:
                response = await page.goto(
                    test_url,
                    timeout=15000,  # 15 second timeout
                    wait_until='domcontentloaded'  # Faster than networkidle
                )
                
                if response:
                    status = response.status
                    title = await page.title()
                    html = await page.content()
                    
                    # Quick signal detection
                    signals = []
                    html_lower = html.lower()
                    if 'google.com/maps' in html_lower:
                        signals.append('google_maps')
                    if 'mapbox' in html_lower:
                        signals.append('mapbox')
                    if 'application/ld+json' in html_lower:
                        signals.append('json_ld')
                    if '<iframe' in html_lower:
                        signals.append('iframe')
                    
                    print(f"           Status: {status}, Title: {title[:50]}...")
                    print(f"           HTML size: {len(html):,} bytes")
                    if signals:
                        print(f"           Signals: {signals}")
                    
                    results.append({
                        'url': test_url,
                        'status': status,
                        'signals': signals
                    })
                else:
                    print(f"           FAILED: No response")
                    
            except Exception as e:
                print(f"           ERROR: {str(e)[:50]}")
        
        await browser.close()
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Pages crawled: {len(results)}")
    
    pages_with_signals = [r for r in results if r['signals']]
    print(f"Pages with signals: {len(pages_with_signals)}")
    
    if pages_with_signals:
        print("\nPages with location signals:")
        for r in pages_with_signals:
            print(f"  {r['url']}")
            print(f"    -> {r['signals']}")
    
    print("\nDone!")


if __name__ == '__main__':
    asyncio.run(test_crawl())

