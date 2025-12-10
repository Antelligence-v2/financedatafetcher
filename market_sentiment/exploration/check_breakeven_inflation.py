"""
Check T10YIE and T5YIE breakeven inflation series
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

def explore_series(series_id):
    """Explore a FRED series in detail"""
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

    if response.status_code == 200:
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
    else:
        print(f"  ✗ Error: Status code {response.status_code}")
        return

    # Get recent observations
    print(f"\n📈 Recent Observations (last 15):")
    response = requests.get(f"{base_url}/series/observations", params={
        'api_key': api_key,
        'series_id': series_id,
        'file_type': 'json',
        'limit': 15,
        'sort_order': 'desc'
    })

    if response.status_code == 200:
        obs_data = response.json()
        if 'observations' in obs_data:
            for obs in obs_data['observations']:
                print(f"  {obs['date']}: {obs['value']}")

    # Get metadata fields
    print(f"\n📋 Available Metadata Fields:")
    print(f"  • id: {series['id']}")
    print(f"  • title: {series['title']}")
    print(f"  • units: {series.get('units', 'N/A')}")
    print(f"  • frequency: {series.get('frequency', 'N/A')}")
    print(f"  • last_updated: {series.get('last_updated', 'N/A')}")
    print(f"  • observation_start: {series.get('observation_start', 'N/A')}")
    print(f"  • observation_end: {series.get('observation_end', 'N/A')}")

if __name__ == "__main__":
    # Check T10YIE
    explore_series('T10YIE')

    # Check T5YIE
    explore_series('T5YIE')

    # Check T5YIFR (5y5y forward)
    explore_series('T5YIFR')
