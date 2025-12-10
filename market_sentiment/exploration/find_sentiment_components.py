"""
Find the specific components of consumer sentiment that the client wants:
1. Index of consumer sentiment
2. Current economic conditions
3. Consumer expectations
4. Year ahead inflation
5. Long run inflation
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

def search_series(search_terms):
    """Search for series matching specific terms"""
    api_key = os.getenv('FRED_API_KEY')

    for term in search_terms:
        print(f"\n{'='*80}")
        print(f"SEARCHING FOR: '{term}'")
        print(f"{'='*80}\n")

        url = "https://api.stlouisfed.org/fred/series/search"
        params = {
            'api_key': api_key,
            'search_text': term,
            'file_type': 'json',
            'limit': 20
        }

        response = requests.get(url, params=params)
        data = response.json()

        if 'seriess' in data:
            # Filter for University of Michigan series
            umich_series = [s for s in data['seriess'] if 'michigan' in s['title'].lower() or 'umich' in s['title'].lower()]

            if umich_series:
                print("University of Michigan related series:\n")
                for series in umich_series[:10]:
                    print(f"  {series['id']}: {series['title']}")
                    print(f"    Units: {series.get('units', 'N/A')} | Frequency: {series.get('frequency', 'N/A')}")
                    print()
            else:
                print("Top results:\n")
                for series in data['seriess'][:10]:
                    print(f"  {series['id']}: {series['title']}")
                    print(f"    Units: {series.get('units', 'N/A')} | Frequency: {series.get('frequency', 'N/A')}")
                    print()

def check_specific_series():
    """Check specific series IDs that might match"""
    api_key = os.getenv('FRED_API_KEY')

    # Known related series
    series_to_check = [
        'UMCSENT',   # Consumer Sentiment
        'MICH',      # Inflation Expectation
        'UMCSENT1',  # Historical Consumer Sentiment
        'UMCSICOR',  # Possible current conditions
        'UMCSEXP',   # Possible expectations
    ]

    print(f"\n{'='*80}")
    print(f"CHECKING SPECIFIC SERIES IDs")
    print(f"{'='*80}\n")

    for series_id in series_to_check:
        url = "https://api.stlouisfed.org/fred/series"
        params = {
            'api_key': api_key,
            'series_id': series_id,
            'file_type': 'json'
        }

        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()
            if 'seriess' in data:
                series = data['seriess'][0]
                print(f"✓ {series['id']}: {series['title']}")
                print(f"  Units: {series.get('units', 'N/A')}")
                print(f"  Frequency: {series.get('frequency', 'N/A')}")
                print(f"  Range: {series.get('observation_start')} to {series.get('observation_end')}")
                print()
        else:
            print(f"✗ {series_id}: Not found")

def get_all_umich_series():
    """Get all series with 'University of Michigan' in the title"""
    api_key = os.getenv('FRED_API_KEY')

    print(f"\n{'='*80}")
    print(f"ALL UNIVERSITY OF MICHIGAN SERIES")
    print(f"{'='*80}\n")

    url = "https://api.stlouisfed.org/fred/series/search"
    params = {
        'api_key': api_key,
        'search_text': 'University of Michigan',
        'file_type': 'json',
        'limit': 100
    }

    response = requests.get(url, params=params)
    data = response.json()

    if 'seriess' in data:
        # Filter and sort by relevance/popularity
        umich_series = [s for s in data['seriess']
                       if 'university of michigan' in s['title'].lower()]

        # Sort by popularity
        umich_series.sort(key=lambda x: x.get('popularity', 0), reverse=True)

        print(f"Found {len(umich_series)} University of Michigan series\n")
        print("Top series by popularity:\n")

        for i, series in enumerate(umich_series[:30], 1):
            print(f"{i}. {series['id']}: {series['title']}")
            print(f"   Units: {series.get('units', 'N/A')} | Frequency: {series.get('frequency', 'N/A')}")
            print(f"   Popularity: {series.get('popularity', 'N/A')}")
            print()

if __name__ == "__main__":
    # Search for specific terms
    search_terms = [
        "current economic conditions michigan",
        "consumer expectations michigan",
        "index consumer sentiment",
    ]

    search_series(search_terms)

    # Check specific series
    check_specific_series()

    # Get all UMich series
    get_all_umich_series()
