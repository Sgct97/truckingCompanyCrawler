"""
Load carrier data from the Excel file.
"""
import pandas as pd
from pathlib import Path
from typing import List, Dict

from config import CARRIERS_FILE


def load_carriers() -> List[Dict[str, str]]:
    """
    Load carriers from Excel file.
    Returns list of dicts with 'name' and 'website' keys.
    """
    df = pd.read_excel(CARRIERS_FILE)
    
    carriers = []
    for _, row in df.iterrows():
        name = str(row['Top Fleet Company Name']).strip()
        website = str(row['Top Fleet Website']).strip()
        
        # Clean up website URL
        if not website or website == 'nan':
            continue
            
        # Handle multiple URLs (comma-separated)
        if ',' in website:
            # Take the first one
            website = website.split(',')[0].strip()
        
        # Ensure https://
        if not website.startswith('http'):
            website = 'https://' + website
        
        # Remove trailing slash
        website = website.rstrip('/')
        
        carriers.append({
            'name': name,
            'website': website
        })
    
    return carriers


def get_carrier_domains() -> List[str]:
    """Get just the list of website URLs."""
    carriers = load_carriers()
    return [c['website'] for c in carriers]


if __name__ == '__main__':
    # Test loading
    carriers = load_carriers()
    print(f"Loaded {len(carriers)} carriers")
    print("\nFirst 10:")
    for c in carriers[:10]:
        print(f"  {c['name']}: {c['website']}")
    print("\nLast 10:")
    for c in carriers[-10:]:
        print(f"  {c['name']}: {c['website']}")

