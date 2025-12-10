"""
Explore the specific FRED data sources requested by the client
"""

import os
import requests
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class FREDClientExplorer:
    """Explore specific client-requested FRED data"""

    BASE_URL = "https://api.stlouisfed.org/fred"

    def __init__(self, api_key=None):
        self.api_key = api_key or os.getenv('FRED_API_KEY')
        if not self.api_key:
            raise ValueError("FRED_API_KEY not found")
        self.session = requests.Session()

    def _make_request(self, endpoint, params=None):
        """Make API request"""
        if params is None:
            params = {}
        params['api_key'] = self.api_key
        params['file_type'] = 'json'

        url = f"{self.BASE_URL}/{endpoint}"
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ Error: {e}")
            return None

    def explore_series(self, series_id):
        """Explore a series in detail"""
        print(f"\n{'='*80}")
        print(f"EXPLORING SERIES: {series_id}")
        print(f"{'='*80}")

        # Get series info
        print(f"\n📊 Series Information:")
        series_info = self._make_request('series', {'series_id': series_id})

        if series_info and 'seriess' in series_info:
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
                print(f"  ✓ Notes: {series['notes'][:300]}...")

        # Get release info
        print(f"\n📋 Release Information:")
        release_info = self._make_request('series/release', {'series_id': series_id})
        if release_info and 'releases' in release_info:
            release = release_info['releases'][0]
            print(f"  ✓ Release ID: {release['id']}")
            print(f"  ✓ Release Name: {release['name']}")
            print(f"  ✓ Link: {release.get('link', 'N/A')}")

        # Get categories
        print(f"\n📁 Categories:")
        category_info = self._make_request('series/categories', {'series_id': series_id})
        if category_info and 'categories' in category_info:
            for cat in category_info['categories']:
                print(f"  ✓ {cat['name']} (ID: {cat['id']})")

        # Get recent observations
        print(f"\n📈 Recent Observations (last 10):")
        obs = self._make_request('series/observations', {
            'series_id': series_id,
            'limit': 10,
            'sort_order': 'desc'
        })
        if obs and 'observations' in obs:
            for observation in obs['observations']:
                print(f"  {observation['date']}: {observation['value']}")

        # Get tags
        print(f"\n🏷️  Tags:")
        tags = self._make_request('series/tags', {'series_id': series_id})
        if tags and 'tags' in tags:
            tag_names = [tag['name'] for tag in tags['tags'][:10]]
            print(f"  {', '.join(tag_names)}")

        return {
            'series_info': series_info,
            'release_info': release_info,
            'category_info': category_info,
            'observations': obs,
            'tags': tags
        }

    def search_by_text(self, search_text):
        """Search for series by text"""
        print(f"\n{'='*80}")
        print(f"SEARCHING FOR: '{search_text}'")
        print(f"{'='*80}")

        result = self._make_request('series/search', {
            'search_text': search_text,
            'limit': 20
        })

        if result and 'seriess' in result:
            print(f"\n✓ Found {result.get('count', 0)} total matches (showing top 20):\n")
            for i, series in enumerate(result['seriess'], 1):
                print(f"{i}. {series['id']}: {series['title']}")
                print(f"   Units: {series.get('units', 'N/A')} | Frequency: {series.get('frequency', 'N/A')}")
                print()

        return result

    def explore_release(self, release_name_or_id):
        """Explore a release by name or ID"""
        print(f"\n{'='*80}")
        print(f"EXPLORING RELEASE: {release_name_or_id}")
        print(f"{'='*80}")

        # Try as release ID first
        if isinstance(release_name_or_id, int) or release_name_or_id.isdigit():
            result = self._make_request('release', {'release_id': release_name_or_id})
            if result and 'releases' in result:
                release = result['releases'][0]
                print(f"\n✓ Release Name: {release['name']}")
                print(f"✓ Release ID: {release['id']}")
                print(f"✓ Link: {release.get('link', 'N/A')}")

                # Get series in this release
                print(f"\n📊 Series in this release:")
                series_result = self._make_request('release/series', {
                    'release_id': release['id'],
                    'limit': 20
                })
                if series_result and 'seriess' in series_result:
                    for series in series_result['seriess']:
                        print(f"  • {series['id']}: {series['title']}")
        else:
            # Search for release by name
            all_releases = self._make_request('releases', {'limit': 1000})
            if all_releases and 'releases' in all_releases:
                matches = [r for r in all_releases['releases']
                          if release_name_or_id.lower() in r['name'].lower()]

                if matches:
                    print(f"\n✓ Found {len(matches)} matching release(s):\n")
                    for release in matches[:5]:
                        print(f"  • {release['name']} (ID: {release['id']})")
                        print(f"    Link: {release.get('link', 'N/A')}\n")
                else:
                    print(f"\n❌ No releases found matching '{release_name_or_id}'")


def main():
    """Explore all client-requested data sources"""

    explorer = FREDClientExplorer()

    # Client's requested data sources
    requests_list = [
        ("series", "UMCSENT"),
        ("series", "EXPINF1YR"),
        ("series", "T10YIE"),
        ("series", "EXPINF10YR"),
        ("search", "Surveys of Consumers"),
        ("search", "US Consumer Confidence FRED/OECD"),
        ("search", "US Consumer Confidence Amplitude"),
    ]

    print("\n" + "="*80)
    print("FRED CLIENT DATA EXPLORATION")
    print("="*80)

    results = {}

    for request_type, identifier in requests_list:
        try:
            if request_type == "series":
                results[identifier] = explorer.explore_series(identifier)
            elif request_type == "search":
                results[identifier] = explorer.search_by_text(identifier)

            # Small delay to respect rate limits
            import time
            time.sleep(0.6)  # ~100 requests/minute to be safe

        except Exception as e:
            print(f"\n❌ Error exploring {identifier}: {e}")

    # Special exploration for "Surveys of Consumers" release
    print(f"\n{'='*80}")
    print("EXPLORING 'Surveys of Consumers' as a RELEASE")
    print(f"{'='*80}")
    explorer.explore_release("Surveys of Consumers")

    print(f"\n{'='*80}")
    print("EXPLORATION COMPLETE!")
    print(f"{'='*80}")
    print("\n✓ All requested data sources have been explored")
    print("✓ Check the output above for details on each source")
    print("✓ Next step: Identify specific fields needed from each source")


if __name__ == "__main__":
    main()
