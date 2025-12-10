"""
Check the OECD Composite Consumer Confidence series
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

def explore_series(series_id):
    """Explore a series in detail"""
    api_key = os.getenv('FRED_API_KEY')
    base_url = "https://api.stlouisfed.org/fred"

    print(f"\n{'='*80}")
    print(f"EXPLORING SERIES: {series_id}")
    print(f"{'='*80}")

    # Get series info
    print(f"\n📊 Series Information:")
    response = requests.get(f"{base_url}/series", params={
        'api_key': api_key,
        'series_id': series_id,
        'file_type': 'json'
    })
    series_info = response.json()

    if 'seriess' in series_info:
        series = series_info['seriess'][0]
        print(f"\n  ✓ Series ID: {series['id']}")
        print(f"  ✓ Title: {series['title']}")
        print(f"  ✓ Units: {series.get('units', 'N/A')}")
        print(f"  ✓ Frequency: {series.get('frequency', 'N/A')}")
        print(f"  ✓ Seasonal Adjustment: {series.get('seasonal_adjustment', 'N/A')}")
        print(f"  ✓ Observation Start: {series.get('observation_start', 'N/A')}")
        print(f"  ✓ Observation End: {series.get('observation_end', 'N/A')}")
        print(f"  ✓ Last Updated: {series.get('last_updated', 'N/A')}")
        print(f"  ✓ Popularity: {series.get('popularity', 'N/A')}")
        if 'notes' in series:
            print(f"\n  📝 Notes:\n  {series['notes'][:500]}...")

    # Get recent observations
    print(f"\n📈 Recent Observations (last 15):")
    response = requests.get(f"{base_url}/series/observations", params={
        'api_key': api_key,
        'series_id': series_id,
        'file_type': 'json',
        'limit': 15,
        'sort_order': 'desc'
    })
    obs_data = response.json()

    if 'observations' in obs_data:
        for observation in obs_data['observations']:
            print(f"  {observation['date']}: {observation['value']}")

    # Get release info
    print(f"\n📋 Release Information:")
    response = requests.get(f"{base_url}/series/release", params={
        'api_key': api_key,
        'series_id': series_id,
        'file_type': 'json'
    })
    release_info = response.json()
    if 'releases' in release_info:
        release = release_info['releases'][0]
        print(f"  ✓ Release ID: {release['id']}")
        print(f"  ✓ Release Name: {release['name']}")

    return series_info

if __name__ == "__main__":
    # The OECD Composite Consumer Confidence series
    explore_series("USACSCICP02STSAM")
