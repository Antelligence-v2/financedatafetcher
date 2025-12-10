"""
FRED API Explorer
Simple examples for interacting with the Federal Reserve Economic Data (FRED) API
"""

import os
import requests
import json
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class FREDExplorer:
    """Helper class for exploring the FRED API"""

    BASE_URL = "https://api.stlouisfed.org/fred"

    def __init__(self, api_key=None):
        """Initialize with API key from environment or parameter"""
        self.api_key = api_key or os.getenv('FRED_API_KEY')
        if not self.api_key:
            raise ValueError("FRED_API_KEY not found in environment variables")

        self.session = requests.Session()

    def _make_request(self, endpoint, params=None):
        """Make a request to the FRED API"""
        if params is None:
            params = {}

        params['api_key'] = self.api_key
        params['file_type'] = 'json'  # Always use JSON for easier parsing

        url = f"{self.BASE_URL}/{endpoint}"

        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error making request to {endpoint}: {e}")
            return None

    def search_series(self, search_text, limit=10):
        """Search for series by text"""
        print(f"\n🔍 Searching for series: '{search_text}'")
        params = {
            'search_text': search_text,
            'limit': limit
        }
        result = self._make_request('series/search', params)

        if result and 'seriess' in result:
            print(f"\nFound {result.get('count', 0)} series (showing {limit}):")
            for series in result['seriess']:
                print(f"\n  ID: {series['id']}")
                print(f"  Title: {series['title']}")
                print(f"  Units: {series.get('units', 'N/A')}")
                print(f"  Frequency: {series.get('frequency', 'N/A')}")
                print(f"  Last Updated: {series.get('last_updated', 'N/A')}")

        return result

    def get_series_info(self, series_id):
        """Get information about a specific series"""
        print(f"\n📊 Getting info for series: {series_id}")
        result = self._make_request('series', {'series_id': series_id})

        if result and 'seriess' in result:
            series = result['seriess'][0]
            print(f"\nSeries Information:")
            print(f"  ID: {series['id']}")
            print(f"  Title: {series['title']}")
            print(f"  Units: {series.get('units', 'N/A')}")
            print(f"  Frequency: {series.get('frequency', 'N/A')}")
            print(f"  Seasonal Adjustment: {series.get('seasonal_adjustment', 'N/A')}")
            print(f"  Observation Start: {series.get('observation_start', 'N/A')}")
            print(f"  Observation End: {series.get('observation_end', 'N/A')}")
            print(f"  Last Updated: {series.get('last_updated', 'N/A')}")
            print(f"  Popularity: {series.get('popularity', 'N/A')}")
            print(f"  Notes: {series.get('notes', 'N/A')[:200]}...")

        return result

    def get_series_observations(self, series_id, limit=10, sort_order='desc',
                               observation_start=None, observation_end=None):
        """Get observations (data points) for a series"""
        print(f"\n📈 Getting observations for series: {series_id}")

        params = {
            'series_id': series_id,
            'limit': limit,
            'sort_order': sort_order
        }

        if observation_start:
            params['observation_start'] = observation_start
        if observation_end:
            params['observation_end'] = observation_end

        result = self._make_request('series/observations', params)

        if result and 'observations' in result:
            print(f"\nShowing {len(result['observations'])} observations:")
            for obs in result['observations']:
                print(f"  {obs['date']}: {obs['value']}")

        return result

    def get_categories(self, category_id=0):
        """Get category information"""
        print(f"\n📁 Getting category: {category_id}")
        result = self._make_request('category', {'category_id': category_id})

        if result and 'categories' in result:
            cat = result['categories'][0]
            print(f"\nCategory: {cat['name']} (ID: {cat['id']})")
            print(f"Parent ID: {cat.get('parent_id', 'N/A')}")

        return result

    def get_category_children(self, category_id=0):
        """Get child categories"""
        print(f"\n📂 Getting child categories of: {category_id}")
        result = self._make_request('category/children', {'category_id': category_id})

        if result and 'categories' in result:
            print(f"\nChild categories:")
            for cat in result['categories']:
                print(f"  {cat['id']}: {cat['name']}")

        return result

    def get_all_releases(self, limit=10):
        """Get all economic data releases"""
        print(f"\n📋 Getting all releases")
        result = self._make_request('releases', {'limit': limit})

        if result and 'releases' in result:
            print(f"\nShowing {len(result['releases'])} releases:")
            for release in result['releases']:
                print(f"\n  ID: {release['id']}")
                print(f"  Name: {release['name']}")
                print(f"  Link: {release.get('link', 'N/A')}")

        return result


def main():
    """Example usage of the FRED API"""

    print("=" * 80)
    print("FRED API Explorer")
    print("=" * 80)

    try:
        fred = FREDExplorer()

        # Example 1: Search for GDP series
        print("\n" + "=" * 80)
        print("EXAMPLE 1: Search for GDP series")
        print("=" * 80)
        fred.search_series("GDP", limit=5)

        # Example 2: Get info about a specific series (GDP)
        print("\n" + "=" * 80)
        print("EXAMPLE 2: Get information about GDP series")
        print("=" * 80)
        fred.get_series_info("GDP")

        # Example 3: Get recent observations for GDP
        print("\n" + "=" * 80)
        print("EXAMPLE 3: Get recent GDP observations")
        print("=" * 80)
        fred.get_series_observations("GDP", limit=10, sort_order='desc')

        # Example 4: Browse categories
        print("\n" + "=" * 80)
        print("EXAMPLE 4: Browse top-level categories")
        print("=" * 80)
        fred.get_category_children(0)

        # Example 5: Get all releases
        print("\n" + "=" * 80)
        print("EXAMPLE 5: Get economic data releases")
        print("=" * 80)
        fred.get_all_releases(limit=5)

        print("\n" + "=" * 80)
        print("Examples complete!")
        print("=" * 80)

    except Exception as e:
        print(f"\n❌ Error: {e}")


if __name__ == "__main__":
    main()
