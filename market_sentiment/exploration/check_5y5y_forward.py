"""
Check for 5-year, 5-year forward inflation expectation series
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv('FRED_API_KEY')

# Try different possible series IDs for 5y5y forward
possible_ids = [
    'T5YIFR',      # Most likely
    'T5YFF',
    'T5Y5YIFR',
    'THREEFYTP5',  # This is the actual 5y5y forward rate
]

print("Searching for 5-Year, 5-Year Forward Inflation Expectation Rate...\n")

for series_id in possible_ids:
    response = requests.get(
        'https://api.stlouisfed.org/fred/series',
        params={
            'api_key': api_key,
            'series_id': series_id,
            'file_type': 'json'
        }
    )

    if response.status_code == 200:
        data = response.json()
        if 'seriess' in data:
            series = data['seriess'][0]
            print(f"✓ FOUND: {series_id}")
            print(f"  Title: {series['title']}")
            print(f"  Units: {series.get('units', 'N/A')}")
            print(f"  Frequency: {series.get('frequency', 'N/A')}")
            print(f"  Last Updated: {series.get('last_updated', 'N/A')}")
            print()

# Also search by text
print("\nSearching by keywords...")
response = requests.get(
    'https://api.stlouisfed.org/fred/series/search',
    params={
        'api_key': api_key,
        'search_text': '5-year 5-year forward inflation',
        'file_type': 'json',
        'limit': 10
    }
)

if response.status_code == 200:
    data = response.json()
    if 'seriess' in data:
        print("\nSearch results:")
        for series in data['seriess']:
            print(f"  {series['id']}: {series['title']}")
            print(f"    Units: {series.get('units', 'N/A')} | Freq: {series.get('frequency', 'N/A')}")
            print()
