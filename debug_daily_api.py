#!/usr/bin/env python3
"""Debug script to inspect daily API response structure."""

import os
import json
from src.crawler import fetch_daily_aqi

api_key = os.getenv("API_KEY")
if not api_key:
    print("ERROR: API_KEY not set")
    exit(1)

print("Fetching daily AQI data...")
data = fetch_daily_aqi(api_key)

if data:
    print(f"\nTotal records: {len(data)}")
    print(f"\nFirst record structure:")
    print(json.dumps(data[0], indent=2, ensure_ascii=False))
    
    print(f"\nAll keys in first 5 records:")
    for i in range(min(5, len(data))):
        print(f"Record {i}: {list(data[i].keys())}")
else:
    print("No data retrieved")
