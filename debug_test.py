#!/usr/bin/env python3
"""Minimal debug test to find what's hanging."""

print("Step 1: Basic imports...")
import sys
print(f"  Python: {sys.version}")

print("Step 2: Import asyncio...")
import asyncio
print("  OK")

print("Step 3: Import aiohttp...")
import aiohttp
print("  OK")

print("Step 4: Import playwright...")
from playwright.async_api import async_playwright
print("  OK")

print("Step 5: Test basic async...")
async def test_async():
    print("  Inside async function")
    return "OK"

result = asyncio.run(test_async())
print(f"  Result: {result}")

print("Step 6: Test playwright launch...")
async def test_playwright():
    print("  Launching playwright...")
    async with async_playwright() as p:
        print("  Playwright context created")
        browser = await p.chromium.launch(headless=True)
        print("  Browser launched")
        await browser.close()
        print("  Browser closed")
    return "OK"

result = asyncio.run(test_playwright())
print(f"  Result: {result}")

print("\nAll tests passed!")

