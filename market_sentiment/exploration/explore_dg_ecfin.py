"""
Explore DG ECFIN data sources and download options
"""

import requests
from bs4 import BeautifulSoup
import re

# DG ECFIN time series download page
url = "https://economy-finance.ec.europa.eu/economic-forecast-and-surveys/business-and-consumer-surveys/download-business-and-consumer-survey-data/time-series_en"

print("Exploring DG ECFIN Time Series Download Page")
print("=" * 80)
print(f"URL: {url}\n")

try:
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, 'html.parser')

    # Find all download links
    print("Looking for download links...\n")

    # Find all links that contain .zip
    zip_links = soup.find_all('a', href=re.compile(r'\.zip'))

    if zip_links:
        print(f"Found {len(zip_links)} ZIP download links:\n")
        for i, link in enumerate(zip_links, 1):
            href = link.get('href')
            text = link.get_text(strip=True)

            # Make absolute URL if relative
            if not href.startswith('http'):
                if href.startswith('/'):
                    href = f"https://economy-finance.ec.europa.eu{href}"
                else:
                    href = f"https://economy-finance.ec.europa.eu/{href}"

            print(f"{i}. {text}")
            print(f"   URL: {href}")
            print()
    else:
        print("No ZIP links found directly. Looking for other download patterns...")

        # Try finding links with "download" in them
        download_links = soup.find_all('a', href=re.compile(r'download', re.I))
        print(f"\nFound {len(download_links)} links with 'download':")
        for i, link in enumerate(download_links[:10], 1):  # Show first 10
            print(f"{i}. {link.get_text(strip=True)}: {link.get('href')}")

    # Look for specific indicators mentioned
    print("\n" + "=" * 80)
    print("Looking for specific indicators in page text:")
    print("=" * 80)

    page_text = soup.get_text()

    indicators = [
        "Economic Sentiment Indicator",
        "ESI",
        "Consumer Confidence",
        "Employment Expectations Indicator",
        "EEI"
    ]

    for indicator in indicators:
        if indicator in page_text:
            print(f"✓ Found mention of: {indicator}")
        else:
            print(f"✗ No mention of: {indicator}")

except Exception as e:
    print(f"Error: {e}")
