"""
Download sample files from UMich to understand their structure
"""

import requests
import os

# Create samples directory
os.makedirs('/Users/bhavyajain/Code/stock_scraper/FRED/exploration/samples', exist_ok=True)

files_to_download = {
    'sentiment': 'https://www.sca.isr.umich.edu/files/tbmics.csv',
    'components': 'https://www.sca.isr.umich.edu/files/tbmiccice.csv',
    'inflation': 'https://www.sca.isr.umich.edu/files/tbmpx1px5.csv'
}

print("Downloading sample files from University of Michigan...\n")

for name, url in files_to_download.items():
    print(f"Downloading {name}...")
    print(f"  URL: {url}")

    response = requests.get(url)

    if response.status_code == 200:
        filename = f'/Users/bhavyajain/Code/stock_scraper/FRED/exploration/samples/{name}.csv'
        with open(filename, 'wb') as f:
            f.write(response.content)

        print(f"  ✓ Saved to: {filename}")
        print(f"  Size: {len(response.content)} bytes")

        # Show first few lines
        lines = response.content.decode('utf-8', errors='ignore').split('\n')[:10]
        print(f"\n  First 10 lines:")
        for i, line in enumerate(lines, 1):
            print(f"    {i}: {line}")
        print()
    else:
        print(f"  ✗ Error: Status code {response.status_code}\n")

print("Download complete!")
