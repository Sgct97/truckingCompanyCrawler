#!/usr/bin/env python3
"""
Test parallel crawler with 3 carriers to verify it works.
Verbose output to track progress.
"""
import asyncio
import sys
from datetime import datetime

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)

from page_crawler import crawl_all_carriers
from load_carriers import load_carriers


async def main():
    print("=" * 70, flush=True)
    print("PARALLEL CRAWLER TEST - 5 carriers, 2 concurrent", flush=True)
    print("=" * 70, flush=True)
    
    # Load first 5 carriers for testing
    all_carriers = load_carriers()
    test_carriers = all_carriers[:5]
    
    print(f"\nTesting with {len(test_carriers)} carriers:", flush=True)
    for c in test_carriers:
        print(f"  - {c['name']}: {c['website']}", flush=True)
    
    print("\n" + "-" * 70, flush=True)
    print("Starting parallel crawl...", flush=True)
    print("-" * 70 + "\n", flush=True)
    
    start_time = datetime.now()
    
    def progress(msg):
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"[{elapsed:6.1f}s] {msg}", flush=True)
    
    results = await crawl_all_carriers(
        test_carriers,
        max_concurrent=2,  # 2 concurrent - less contention
        progress_callback=progress
    )
    
    duration = (datetime.now() - start_time).total_seconds()
    
    # Summary
    print("\n" + "=" * 70, flush=True)
    print("RESULTS", flush=True)
    print("=" * 70, flush=True)
    print(f"Total time: {duration:.1f}s", flush=True)
    
    for r in results:
        print(f"\n{r['carrier']}:", flush=True)
        if r['status'] == 'success':
            stats = r['summary']['crawl_stats']
            print(f"  ✓ Pages crawled: {stats['pages_crawled']}", flush=True)
            print(f"  ✓ Pages with signals: {stats['pages_with_location_signals']}", flush=True)
            print(f"  ✓ Time: {stats['duration_seconds']:.1f}s", flush=True)
            
            if r['summary'].get('signal_summary'):
                print(f"  ✓ Signal types: {r['summary']['signal_summary']}", flush=True)
        else:
            print(f"  ✗ ERROR: {r.get('error', 'Unknown')}", flush=True)
    
    print("\nDone!", flush=True)


if __name__ == '__main__':
    asyncio.run(main())

