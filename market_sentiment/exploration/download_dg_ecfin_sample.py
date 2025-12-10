"""
Download and explore DG ECFIN main indicators data
"""

import requests
import zipfile
import os
import pandas as pd

# Main indicators file (seasonally adjusted)
url = "https://ec.europa.eu/economy_finance/db_indicators/surveys/documents/series/nace2_ecfin_2511/main_indicators_sa_nace2.zip"

print("Downloading DG ECFIN Main Indicators (Seasonally Adjusted)")
print("=" * 80)
print(f"URL: {url}\n")

# Create directory for downloads
os.makedirs("FRED/exploration/dg_ecfin_samples", exist_ok=True)

try:
    # Download ZIP file
    print("Downloading ZIP file...")
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    zip_path = "FRED/exploration/dg_ecfin_samples/main_indicators_sa.zip"
    with open(zip_path, 'wb') as f:
        f.write(response.content)

    print(f"✓ Downloaded {len(response.content)} bytes")
    print(f"✓ Saved to: {zip_path}\n")

    # Extract ZIP file
    print("Extracting ZIP file...")
    extract_dir = "FRED/exploration/dg_ecfin_samples/extracted"
    os.makedirs(extract_dir, exist_ok=True)

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
        files = zip_ref.namelist()

    print(f"✓ Extracted {len(files)} files\n")

    # List extracted files
    print("Extracted files:")
    print("-" * 80)
    for i, file in enumerate(files, 1):
        file_path = os.path.join(extract_dir, file)
        if os.path.isfile(file_path):
            size = os.path.getsize(file_path)
            print(f"{i}. {file} ({size:,} bytes)")

    # Try to read one of the files to understand format
    print("\n" + "=" * 80)
    print("Exploring file contents...")
    print("=" * 80)

    for file in files:
        file_path = os.path.join(extract_dir, file)

        if file.endswith('.csv'):
            print(f"\n📄 Reading CSV: {file}")
            try:
                df = pd.read_csv(file_path, nrows=10)
                print(f"  Shape: {df.shape}")
                print(f"  Columns: {list(df.columns)}")
                print(f"\n  First few rows:")
                print(df.head())
            except Exception as e:
                print(f"  Error reading CSV: {e}")

        elif file.endswith('.txt'):
            print(f"\n📄 Reading TXT: {file}")
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read(1000)
                    print(f"  First 1000 characters:")
                    print(content)
            except Exception as e:
                print(f"  Error reading TXT: {e}")

        elif file.endswith('.xlsx') or file.endswith('.xls'):
            print(f"\n📄 Excel file found: {file}")
            try:
                # Try to read with pandas
                df = pd.read_excel(file_path, nrows=10)
                print(f"  Shape: {df.shape}")
                print(f"  Columns: {list(df.columns)}")
                print(f"\n  First few rows:")
                print(df.head())
            except Exception as e:
                print(f"  Note: Excel reading requires openpyxl: {e}")

except Exception as e:
    print(f"✗ Error: {e}")
