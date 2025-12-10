"""
Check if the Surveys of Consumers release has tables with more detailed components
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

def get_release_tables(release_id=91):
    """Get tables from the Surveys of Consumers release"""
    api_key = os.getenv('FRED_API_KEY')

    url = "https://api.stlouisfed.org/fred/release/tables"
    params = {
        'api_key': api_key,
        'release_id': release_id,
        'file_type': 'json'
    }

    print(f"\n{'='*80}")
    print(f"RELEASE TABLES: Surveys of Consumers (Release {release_id})")
    print(f"{'='*80}\n")

    response = requests.get(url, params=params)

    if response.status_code == 200:
        print(f"Response received (status 200)")
        print(f"Content type: {response.headers.get('content-type')}")
        print(f"\nRaw response:\n")
        print(response.text[:1000])  # First 1000 chars
    else:
        print(f"Error: Status code {response.status_code}")

def search_broader_terms():
    """Search for current conditions and expectations more broadly"""
    api_key = os.getenv('FRED_API_KEY')

    search_terms = [
        "current conditions index",
        "expectations index",
        "current economic conditions index",
        "consumer expectations index",
        "ICS current",  # Index of Consumer Sentiment Current
        "ICE",  # Index of Consumer Expectations
    ]

    print(f"\n{'='*80}")
    print(f"BROADER SEARCH FOR COMPONENTS")
    print(f"{'='*80}\n")

    for term in search_terms:
        url = "https://api.stlouisfed.org/fred/series/search"
        params = {
            'api_key': api_key,
            'search_text': term,
            'file_type': 'json',
            'limit': 10
        }

        response = requests.get(url, params=params)
        data = response.json()

        if 'seriess' in data and len(data['seriess']) > 0:
            print(f"\n'{term}':")
            for series in data['seriess'][:5]:
                if 'michigan' in series['title'].lower() or 'consumer' in series['title'].lower():
                    print(f"  {series['id']}: {series['title']}")

def check_umich_website_series():
    """
    Check if there are standard component series IDs from UMich
    Common abbreviations might be:
    - ICC: Index of Current Conditions
    - ICE: Index of Consumer Expectations
    """
    api_key = os.getenv('FRED_API_KEY')

    possible_ids = [
        'ICC',
        'ICE',
        'ICSURRENT',
        'ICEXPECT',
        'UMCSCURR',
        'UMCSEXP',
        'UMCSICC',
        'UMCSICE',
    ]

    print(f"\n{'='*80}")
    print(f"CHECKING POSSIBLE COMPONENT SERIES IDs")
    print(f"{'='*80}\n")

    for series_id in possible_ids:
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
                print(f"✓ FOUND: {series['id']}: {series['title']}")
                print(f"  Units: {series.get('units', 'N/A')}")
                print(f"  Frequency: {series.get('frequency', 'N/A')}")
                print()

if __name__ == "__main__":
    get_release_tables()
    search_broader_terms()
    check_umich_website_series()

    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}\n")
    print("If component indices (Current Conditions, Expectations) are not in FRED,")
    print("they may need to be:")
    print("  1. Scraped directly from University of Michigan website")
    print("  2. Calculated from other data")
    print("  3. Available in a different format/source")
