#!/usr/bin/env python3
"""
Full 500+ Carrier Crawl Script
Run this on a server with 32GB+ RAM for optimal performance.

Usage:
    python run_full_crawl.py                    # Run all carriers
    python run_full_crawl.py --workers 8        # Custom worker count
    python run_full_crawl.py --start 100        # Start from carrier #100
    python run_full_crawl.py --resume           # Resume from last checkpoint
"""

import asyncio
import argparse
import json
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

import aiohttp
from playwright.async_api import async_playwright

from page_crawler import PageCrawler
from url_discovery import URLDiscovery
from classifier import LocationClassifier, generate_modality_report
from load_carriers import load_carriers
from config import MAX_CONCURRENT_BROWSERS


# Output paths
DATA_DIR = Path('data')
CRAWLED_DIR = DATA_DIR / 'crawled_pages'
REPORTS_DIR = DATA_DIR / 'reports'
CHECKPOINT_FILE = DATA_DIR / 'crawl_checkpoint.json'


async def crawl_single_carrier(carrier: Dict, playwright, semaphore: asyncio.Semaphore) -> Dict[str, Any]:
    """Crawl a single carrier with semaphore-controlled concurrency."""
    async with semaphore:
        name = carrier.get('name', carrier.get('Company', 'Unknown'))
        url = carrier.get('website', carrier.get('Website', ''))
        
        if not url or not url.startswith('http'):
            return {'name': name, 'status': 'SKIP', 'reason': 'No valid URL'}
        
        domain = url.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0].replace('.', '_')
        carrier_dir = CRAWLED_DIR / domain
        
        # Create fresh directory
        if carrier_dir.exists():
            import shutil
            shutil.rmtree(carrier_dir)
        carrier_dir.mkdir(parents=True, exist_ok=True)
        
        start_time = time.time()
        browser = None
        
        try:
            # Each carrier gets its own browser instance (isolation)
            browser = await playwright.chromium.launch(headless=True)
            
            # Discover URLs
            async with aiohttp.ClientSession() as session:
                discovery = URLDiscovery(url)
                initial_urls = await discovery.discover_all(session)
            
            # Crawl
            crawler = PageCrawler(url, name)
            summary = await crawler.crawl(browser, initial_urls)
            pages_crawled = summary['crawl_stats']['pages_crawled']
            pages_failed = summary['crawl_stats']['pages_failed']
            
            # Classify
            classifier = LocationClassifier()
            report = classifier.classify_carrier(name, carrier_dir)
            
            elapsed = time.time() - start_time
            
            result = {
                'name': name,
                'url': url,
                'domain': domain,
                'status': 'OK' if report.location_pages > 0 else 'NO_LOC',
                'pages_crawled': pages_crawled,
                'pages_failed': pages_failed,
                'location_pages': report.location_pages,
                'total_pages': report.total_pages,
                'top_url': report.top_pages[0].url if report.top_pages else None,
                'top_score': report.top_pages[0].total_score if report.top_pages else 0,
                'modalities': list(report.modalities_found.keys()) if report.modalities_found else [],
                'extraction_approach': report.recommended_approach,
                'time_seconds': round(elapsed, 1)
            }
            
            return result
            
        except Exception as e:
            elapsed = time.time() - start_time
            return {
                'name': name,
                'url': url,
                'domain': domain,
                'status': 'ERROR',
                'error': str(e)[:200],
                'time_seconds': round(elapsed, 1)
            }
        finally:
            if browser:
                await browser.close()


def save_checkpoint(results: List[Dict], start_idx: int):
    """Save progress checkpoint."""
    checkpoint = {
        'timestamp': datetime.now().isoformat(),
        'completed_count': len(results),
        'start_idx': start_idx,
        'last_idx': start_idx + len(results) - 1,
        'results': results
    }
    CHECKPOINT_FILE.write_text(json.dumps(checkpoint, indent=2))


def load_checkpoint() -> tuple:
    """Load checkpoint if exists."""
    if CHECKPOINT_FILE.exists():
        data = json.loads(CHECKPOINT_FILE.read_text())
        return data.get('last_idx', -1) + 1, data.get('results', [])
    return 0, []


async def run_full_crawl(workers: int = 8, start_idx: int = 0, resume: bool = False):
    """Run the full crawl with parallel workers."""
    
    # Setup directories
    CRAWLED_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Load carriers
    all_carriers = load_carriers()
    total_carriers = len(all_carriers)
    
    # Handle resume
    if resume:
        start_idx, previous_results = load_checkpoint()
        print(f"Resuming from carrier #{start_idx + 1}")
    else:
        previous_results = []
    
    carriers = all_carriers[start_idx:]
    
    print("=" * 80)
    print("FULL CARRIER CRAWL")
    print("=" * 80)
    print(f"Total carriers: {total_carriers}")
    print(f"Starting from: #{start_idx + 1}")
    print(f"To crawl: {len(carriers)}")
    print(f"Parallel workers: {workers}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    results = previous_results.copy()
    semaphore = asyncio.Semaphore(workers)
    crawl_start = time.time()
    
    async with async_playwright() as p:
        # Process in batches for checkpointing
        batch_size = 20
        
        for batch_start in range(0, len(carriers), batch_size):
            batch = carriers[batch_start:batch_start + batch_size]
            batch_num = (start_idx + batch_start) // batch_size + 1
            
            print(f"\n--- Batch {batch_num} (carriers {start_idx + batch_start + 1}-{start_idx + batch_start + len(batch)}) ---")
            
            # Create tasks for this batch
            tasks = [
                crawl_single_carrier(carrier, p, semaphore) 
                for carrier in batch
            ]
            
            # Run batch with progress
            batch_results = []
            for i, coro in enumerate(asyncio.as_completed(tasks)):
                result = await coro
                batch_results.append(result)
                status_icon = "✓" if result['status'] == 'OK' else "✗" if result['status'] == 'ERROR' else "○"
                print(f"  [{status_icon}] {result['name'][:40]}: {result['status']} ({result.get('time_seconds', 0):.0f}s)")
            
            results.extend(batch_results)
            
            # Save checkpoint after each batch
            save_checkpoint(results, start_idx)
            
            # Progress summary
            ok = sum(1 for r in results if r.get('status') == 'OK')
            no_loc = sum(1 for r in results if r.get('status') == 'NO_LOC')
            errors = sum(1 for r in results if r.get('status') == 'ERROR')
            elapsed = time.time() - crawl_start
            
            print(f"  Progress: {len(results)}/{total_carriers} | OK: {ok} | NO_LOC: {no_loc} | ERR: {errors} | Time: {elapsed/60:.1f}m")
    
    # Final summary
    total_time = time.time() - crawl_start
    
    print("\n" + "=" * 80)
    print("CRAWL COMPLETE")
    print("=" * 80)
    
    ok_results = [r for r in results if r.get('status') == 'OK']
    no_loc_results = [r for r in results if r.get('status') == 'NO_LOC']
    error_results = [r for r in results if r.get('status') == 'ERROR']
    skip_results = [r for r in results if r.get('status') == 'SKIP']
    
    print(f"Total time: {total_time/60:.1f} minutes ({total_time/3600:.1f} hours)")
    print(f"Average per carrier: {total_time/len(results):.1f} seconds")
    print(f"\nResults:")
    print(f"  OK (location found): {len(ok_results)} ({100*len(ok_results)/len(results):.1f}%)")
    print(f"  NO_LOC: {len(no_loc_results)}")
    print(f"  ERROR: {len(error_results)}")
    print(f"  SKIPPED: {len(skip_results)}")
    
    # Save final results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    results_file = REPORTS_DIR / f'crawl_results_{timestamp}.json'
    results_file.write_text(json.dumps(results, indent=2))
    print(f"\nResults saved to: {results_file}")
    
    # Generate modality report
    print("\nGenerating modality report...")
    classifier = LocationClassifier()
    reports = {}
    for r in ok_results:
        carrier_dir = CRAWLED_DIR / r['domain']
        if carrier_dir.exists():
            reports[r['name']] = classifier.classify_carrier(r['name'], carrier_dir)
    
    report_file = REPORTS_DIR / f'modality_report_{timestamp}.txt'
    generate_modality_report(reports, report_file)
    print(f"Modality report saved to: {report_file}")
    
    # Print top results
    print("\n" + "=" * 80)
    print("TOP 20 RESULTS (by score)")
    print("=" * 80)
    for r in sorted(ok_results, key=lambda x: x.get('top_score', 0), reverse=True)[:20]:
        print(f"  [{r['top_score']:2d}pts] {r['name'][:35]:35} | {r['top_url'][:50] if r['top_url'] else 'N/A'}")
    
    # Print NO_LOC carriers (need manual review)
    if no_loc_results:
        print("\n" + "=" * 80)
        print(f"NO LOCATION PAGES FOUND ({len(no_loc_results)} carriers) - Manual review needed:")
        print("=" * 80)
        for r in no_loc_results:
            print(f"  {r['name']}: {r['url']}")
    
    return results


def main():
    parser = argparse.ArgumentParser(description='Run full carrier crawl')
    parser.add_argument('--workers', type=int, default=8, help='Number of parallel workers (default: 8)')
    parser.add_argument('--start', type=int, default=0, help='Start from carrier index (0-based)')
    parser.add_argument('--resume', action='store_true', help='Resume from last checkpoint')
    
    args = parser.parse_args()
    
    # Run the crawl
    asyncio.run(run_full_crawl(
        workers=args.workers,
        start_idx=args.start,
        resume=args.resume
    ))


if __name__ == '__main__':
    main()

