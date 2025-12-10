"""
Check what series are in the Surveys of Consumers release
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

def get_release_series(release_id=91):
    """Get all series in a release"""
    api_key = os.getenv('FRED_API_KEY')

    url = f"https://api.stlouisfed.org/fred/release/series"
    params = {
        'api_key': api_key,
        'release_id': release_id,
        'file_type': 'json',
        'limit': 1000
    }

    response = requests.get(url, params=params)
    data = response.json()

    print(f"\n{'='*80}")
    print(f"SERIES IN RELEASE: Surveys of Consumers (ID: {release_id})")
    print(f"{'='*80}\n")

    if 'seriess' in data:
        print(f"Total series found: {len(data['seriess'])}\n")

        for i, series in enumerate(data['seriess'], 1):
            print(f"{i}. {series['id']}")
            print(f"   Title: {series['title']}")
            print(f"   Units: {series.get('units', 'N/A')}")
            print(f"   Frequency: {series.get('frequency', 'N/A')}")
            print(f"   Obs Start: {series.get('observation_start', 'N/A')} to {series.get('observation_end', 'N/A')}")
            print()

    return data

if __name__ == "__main__":
    get_release_series()
