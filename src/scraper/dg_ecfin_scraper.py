"""
DG ECFIN Business and Consumer Surveys Scraper

Integrates with data-fetch framework via BaseScraper.

Scrapes Economic Sentiment and Employment Expectations indicators from
European Commission DG ECFIN surveys.

Fields:
1. Economic Sentiment Indicator - EU (EU.ESI)
2. Economic Sentiment Indicator - Euro Area (EA.ESI)
3. Employment Expectations Indicator - EU (EU.EEI)
4. Employment Expectations Indicator - Euro Area (EA.EEI)
5. Flash Consumer Confidence - Euro Area (EA.CONS)

Author: Bhavya Jain
Date: December 10, 2025
"""

import requests
import zipfile
import os
import pandas as pd
from typing import Dict, Any, List, Optional
import tempfile
import time

from .base_scraper import BaseScraper
from ..utils.config_manager import SiteConfig


class DGECFINScraper(BaseScraper):
    """Scraper for DG ECFIN Business and Consumer Surveys"""

    # Data source URL
    ZIP_URL = "https://ec.europa.eu/economy_finance/db_indicators/surveys/documents/series/nace2_ecfin_2511/main_indicators_sa_nace2.zip"

    # Fields to extract
    FIELDS = {
        'esi_eu': {
            'column': 'EU.ESI',
            'name': 'Economic Sentiment Indicator (EU)',
            'field_name': 'esi_eu'
        },
        'esi_ea': {
            'column': 'EA.ESI',
            'name': 'Economic Sentiment Indicator (Euro Area)',
            'field_name': 'esi_ea'
        },
        'eei_eu': {
            'column': 'EU.EEI',
            'name': 'Employment Expectations Indicator (EU)',
            'field_name': 'eei_eu'
        },
        'eei_ea': {
            'column': 'EA.EEI',
            'name': 'Employment Expectations Indicator (Euro Area)',
            'field_name': 'eei_ea'
        },
        'flash_consumer_confidence_ea': {
            'column': 'EA.CONS',
            'name': 'Flash Consumer Confidence (Euro Area)',
            'field_name': 'flash_consumer_confidence_ea'
        }
    }

    def __init__(self, config: Optional[SiteConfig] = None, **kwargs):
        """
        Initialize the DG ECFIN scraper.

        Args:
            config: Site configuration (optional)
            **kwargs: Additional arguments for BaseScraper
        """
        super().__init__(config=config, **kwargs)

    def _download_and_extract_zip(self) -> Optional[str]:
        """
        Download ZIP file and extract Excel file.

        Returns:
            Path to extracted Excel file or None if failed
        """
        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Downloading DG ECFIN data (attempt {attempt + 1}/{self.max_retries})...")
                self.logger.info(f"URL: {self.ZIP_URL}")

                response = requests.get(
                    self.ZIP_URL,
                    timeout=60,  # Larger file, needs more time
                    headers={'User-Agent': self.user_agent}
                )
                response.raise_for_status()

                self.logger.info(f"✓ Downloaded {len(response.content):,} bytes")

                # Create temp directory
                temp_dir = tempfile.mkdtemp()

                # Save ZIP file
                zip_path = os.path.join(temp_dir, 'dg_ecfin.zip')
                with open(zip_path, 'wb') as f:
                    f.write(response.content)

                # Extract ZIP
                self.logger.info("Extracting ZIP file...")
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                    files = zip_ref.namelist()

                self.logger.info(f"✓ Extracted {len(files)} files")

                # Find Excel file
                excel_file = None
                for file in files:
                    if file.endswith('.xlsx') or file.endswith('.xls'):
                        excel_file = os.path.join(temp_dir, file)
                        self.logger.info(f"✓ Found Excel file: {file}")
                        break

                if excel_file:
                    return excel_file
                else:
                    raise ValueError("No Excel file found in ZIP")

            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Download attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (2 ** attempt))
                else:
                    self.logger.error(f"Failed to download after {self.max_retries} attempts")
                    return None

            except Exception as e:
                self.logger.error(f"Error downloading/extracting ZIP: {e}")
                return None

        return None

    def fetch_raw(self, url: str) -> Dict[str, Any]:
        """
        Download ZIP file, extract Excel, and read data.

        Args:
            url: URL to fetch (uses self.ZIP_URL instead)

        Returns:
            Dictionary containing raw Excel data
        """
        self.logger.info("=" * 80)
        self.logger.info("Starting DG ECFIN data download")
        self.logger.info("=" * 80)

        # Download and extract
        excel_path = self._download_and_extract_zip()

        if not excel_path:
            raise ValueError("Failed to download and extract ZIP file")

        # Read Excel file
        try:
            self.logger.info("Reading Excel MONTHLY sheet...")
            df = pd.read_excel(excel_path, sheet_name='MONTHLY', header=None)
            self.logger.info(f"✓ Loaded sheet with shape: {df.shape}")

            # Convert DataFrame to JSON-serializable format for raw data saving
            # We'll recreate the DataFrame in parse_raw from this dict representation
            return {
                "type": "excel_from_zip",
                "content": df.to_dict('split'),  # Convert to dict for JSON serialization
                "shape": df.shape,
                "excel_path": excel_path,
                "zip_url": self.ZIP_URL
            }

        except Exception as e:
            self.logger.error(f"Error reading Excel file: {e}")
            raise

    def parse_raw(self, raw_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Parse Excel data and extract 5 EU indicators.

        Args:
            raw_data: Raw data from fetch_raw

        Returns:
            DataFrame with extracted indicators
        """
        self.logger.info("=" * 80)
        self.logger.info("Parsing Excel file...")
        self.logger.info("=" * 80)

        content = raw_data.get("content")

        if content is None:
            raise ValueError("No Excel data to parse")

        # Recreate DataFrame from dict representation (if coming from saved JSON)
        if isinstance(content, dict):
            df = pd.DataFrame(**content)
        else:
            df = content

        # First row contains column names
        # Column 0 is date, then each country's indicators
        headers = df.iloc[0].tolist()

        # Find indices of our required columns
        column_indices = {}
        for key, field_info in self.FIELDS.items():
            col_name = field_info['column']
            try:
                idx = headers.index(col_name)
                column_indices[key] = idx
                self.logger.info(f"  ✓ Found {col_name} at column {idx}")
            except ValueError:
                self.logger.warning(f"  ✗ Could not find column: {col_name}")

        if not column_indices:
            raise ValueError("No required columns found in Excel!")

        # Extract data starting from row 1 (row 0 is headers)
        data_df = df.iloc[1:].copy()

        # Build result dataframe
        result = pd.DataFrame()

        # Get date column (column 0)
        result['date'] = pd.to_datetime(data_df.iloc[:, 0], errors='coerce')

        # Extract each field
        for key, col_idx in column_indices.items():
            field_name = self.FIELDS[key]['field_name']
            result[field_name] = pd.to_numeric(data_df.iloc[:, col_idx], errors='coerce')

        # Remove rows with invalid dates
        result = result.dropna(subset=['date'])

        # Sort by date
        result = result.sort_values('date').reset_index(drop=True)

        # Add year, month columns at the beginning
        result.insert(0, 'year', result['date'].dt.year)
        result.insert(1, 'month', result['date'].dt.month)

        self.logger.info(f"\n✓ Extracted {len(result)} observations")
        self.logger.info(f"  Date range: {result['date'].min().date()} to {result['date'].max().date()}")

        return result

    def validate(self, df: pd.DataFrame) -> List[str]:
        """
        Validate the DG ECFIN dataset.

        Args:
            df: DataFrame to validate

        Returns:
            List of validation warnings
        """
        warnings = super().validate(df)

        # Check for required columns
        required_cols = ['date', 'year', 'month'] + [
            self.FIELDS[key]['field_name'] for key in self.FIELDS
        ]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            warnings.append(f"Missing required columns: {missing_cols}")

        # Check data coverage for each field
        for key, field_info in self.FIELDS.items():
            field_name = field_info['field_name']
            if field_name in df.columns:
                series = df[field_name]
                coverage = (series.notna().sum() / len(series)) * 100

                if coverage < 80:
                    warnings.append(
                        f"{field_info['name']} has low coverage: {coverage:.1f}%"
                    )

                # Log field statistics
                if series.notna().any():
                    latest = series.dropna().iloc[-1]
                    latest_date = df.loc[series.notna(), 'date'].iloc[-1]
                    self.logger.info(
                        f"{field_info['name']}: Latest value = {latest} ({latest_date.date()}), "
                        f"Coverage = {coverage:.1f}%"
                    )

        return warnings


# For standalone testing
def main():
    """Main entry point for standalone testing"""
    import logging
    logging.basicConfig(level=logging.INFO)

    scraper = DGECFINScraper()
    # Pass a dummy URL - the scraper uses its own ZIP_URL
    result = scraper.scrape(url="https://ec.europa.eu/economy_finance/db_indicators/surveys/index_en.htm")

    if result.success:
        print(f"\n✅ SUCCESS! Scraped {result.rows_extracted} rows")
        print(f"Date range: {result.date_range}")
        print(f"\nData preview:")
        print(result.data.head())
    else:
        print(f"\n❌ FAILED: {result.error}")

    return result.success


if __name__ == "__main__":
    main()
