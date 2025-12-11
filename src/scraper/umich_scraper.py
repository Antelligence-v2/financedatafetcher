"""
University of Michigan Surveys of Consumers Data Scraper

Integrates with data-fetch framework via BaseScraper.

Scrapes 5 key fields from UMich website:
1. Index of Consumer Sentiment (ICS_ALL)
2. Current Economic Conditions (ICC)
3. Consumer Expectations (ICE)
4. Year Ahead Inflation (PX_MD)
5. Long Run Inflation (PX5_MD)

Author: Bhavya Jain
Date: December 10, 2025
"""

import requests
import pandas as pd
from typing import Dict, Any, List, Optional
import time
import io

from .base_scraper import BaseScraper
from ..utils.config_manager import SiteConfig


class UMichScraper(BaseScraper):
    """Scraper for University of Michigan Surveys of Consumers data"""

    # Data source URLs
    URLS = {
        'sentiment': 'https://www.sca.isr.umich.edu/files/tbmics.csv',
        'components': 'https://www.sca.isr.umich.edu/files/tbmiccice.csv',
        'inflation': 'https://www.sca.isr.umich.edu/files/tbmpx1px5.csv'
    }

    # Field mappings
    FIELD_MAPPINGS = {
        'sentiment': {
            'file': 'sentiment',
            'column': 'ICS_ALL',
            'name': 'Index of Consumer Sentiment'
        },
        'current_conditions': {
            'file': 'components',
            'column': 'ICC',
            'name': 'Current Economic Conditions'
        },
        'consumer_expectations': {
            'file': 'components',
            'column': 'ICE',
            'name': 'Consumer Expectations'
        },
        'year_ahead_inflation': {
            'file': 'inflation',
            'column': 'PX_MD',
            'name': 'Year Ahead Inflation'
        },
        'long_run_inflation': {
            'file': 'inflation',
            'column': 'PX5_MD',
            'name': 'Long Run Inflation'
        }
    }

    def __init__(self, config: Optional[SiteConfig] = None, **kwargs):
        """
        Initialize the UMich scraper.

        Args:
            config: Site configuration (optional)
            **kwargs: Additional arguments for BaseScraper
        """
        super().__init__(config=config, **kwargs)

    def _download_csv(self, name: str, url: str) -> Optional[pd.DataFrame]:
        """
        Download and parse a single CSV file with retry logic.

        Args:
            name: Name identifier for the file
            url: URL to download from

        Returns:
            DataFrame or None if failed
        """
        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Downloading {name} (attempt {attempt + 1}/{self.max_retries})...")

                response = requests.get(
                    url,
                    timeout=self.timeout,
                    headers={'User-Agent': self.user_agent}
                )
                response.raise_for_status()

                # Parse CSV
                df = pd.read_csv(io.StringIO(response.text))

                # Validate basic structure
                if 'Month' not in df.columns or 'YYYY' not in df.columns:
                    raise ValueError(f"Invalid CSV structure: missing Month or YYYY columns")

                self.logger.info(f"✓ Successfully downloaded {name} ({len(df)} rows)")
                return df

            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Download attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (2 ** attempt))  # Exponential backoff
                else:
                    self.logger.error(f"Failed to download {name} after {self.max_retries} attempts")
                    return None

            except Exception as e:
                self.logger.error(f"Error parsing {name}: {e}")
                return None

        return None

    def fetch_raw(self, url: str) -> Dict[str, Any]:
        """
        Download all 3 CSV files from UMich website.

        Args:
            url: Base URL (not used, URLs are hardcoded)

        Returns:
            Dictionary containing raw CSV data
        """
        self.logger.info("=" * 80)
        self.logger.info("Starting University of Michigan data download")
        self.logger.info("=" * 80)

        raw_data = {}
        success = True

        for name, csv_url in self.URLS.items():
            df = self._download_csv(name, csv_url)

            if df is not None:
                raw_data[name] = df
            else:
                success = False
                self.logger.error(f"Failed to download {name}")

        if not success:
            raise ValueError("Failed to download one or more CSV files")

        return {
            "type": "csv_files",
            "content": raw_data,
            "urls": self.URLS
        }

    def _parse_date(self, row) -> Optional[pd.Timestamp]:
        """
        Parse Month and YYYY columns into a datetime.

        Args:
            row: DataFrame row with Month and YYYY columns

        Returns:
            Timestamp or None if invalid
        """
        try:
            month = row['Month']
            year = row['YYYY']

            # Handle month names
            date_str = f"{month} {year}"
            return pd.to_datetime(date_str, format='%B %Y')

        except Exception:
            self.logger.warning(f"Could not parse date: {row.get('Month')} {row.get('YYYY')}")
            return None

    def parse_raw(self, raw_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Parse and combine 3 CSV files into single DataFrame.

        Args:
            raw_data: Raw data from fetch_raw

        Returns:
            Combined DataFrame with 5 fields
        """
        self.logger.info("=" * 80)
        self.logger.info("Processing and combining data")
        self.logger.info("=" * 80)

        csv_data = raw_data.get("content", {})

        if not csv_data:
            raise ValueError("No CSV data to process")

        # Process each file
        processed = {}

        for file_key, df in csv_data.items():
            self.logger.info(f"Processing {file_key}...")

            # Add date column
            df['date'] = df.apply(self._parse_date, axis=1)

            # Remove rows with invalid dates
            df = df.dropna(subset=['date'])

            # Set date as index
            df = df.set_index('date')

            # Keep only data columns (drop Month, YYYY)
            cols_to_keep = [col for col in df.columns
                           if col not in ['Month', 'YYYY']]
            df = df[cols_to_keep]

            # Convert to numeric, coercing errors to NaN
            for col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            processed[file_key] = df
            self.logger.info(f"  ✓ Processed {len(df)} rows")

        # Combine all data on date index
        self.logger.info("Combining all datasets...")

        combined = pd.DataFrame()

        for field_key, mapping in self.FIELD_MAPPINGS.items():
            file_key = mapping['file']
            column = mapping['column']
            name = mapping['name']

            if file_key in processed and column in processed[file_key].columns:
                combined[field_key] = processed[file_key][column]
                self.logger.info(f"  ✓ Added {name} ({field_key})")
            else:
                self.logger.warning(f"  ✗ Could not find {name} ({column} in {file_key})")

        # Sort by date
        combined = combined.sort_index()

        # Reset index to make date a column
        combined = combined.reset_index()

        # Add year and month columns
        combined.insert(0, 'year', combined['date'].dt.year)
        combined.insert(1, 'month', combined['date'].dt.month)

        self.logger.info(f"✓ Combined dataset created: {len(combined)} rows, {len(combined.columns)} columns")
        self.logger.info(f"  Date range: {combined['date'].min()} to {combined['date'].max()}")

        return combined

    def validate(self, df: pd.DataFrame) -> List[str]:
        """
        Validate the UMich dataset.

        Args:
            df: DataFrame to validate

        Returns:
            List of validation warnings
        """
        warnings = super().validate(df)

        # Check for required columns
        required_cols = ['date', 'year', 'month'] + list(self.FIELD_MAPPINGS.keys())
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            warnings.append(f"Missing required columns: {missing_cols}")

        # Check data coverage for each field
        for field_key, mapping in self.FIELD_MAPPINGS.items():
            if field_key in df.columns:
                series = df[field_key]
                coverage = (series.notna().sum() / len(series)) * 100

                if coverage < 90:
                    warnings.append(
                        f"{mapping['name']} has low coverage: {coverage:.1f}%"
                    )

                # Log field statistics
                if series.notna().any():
                    latest = series.dropna().iloc[-1]
                    latest_date = df.loc[series.notna(), 'date'].iloc[-1]
                    self.logger.info(
                        f"{mapping['name']}: Latest value = {latest} ({latest_date.date()}), "
                        f"Coverage = {coverage:.1f}%"
                    )

        return warnings


# For standalone testing
def main():
    """Main entry point for standalone testing"""
    import logging
    logging.basicConfig(level=logging.INFO)

    scraper = UMichScraper()
    # Pass a dummy URL - the scraper uses its own URLs
    result = scraper.scrape(url="https://www.sca.isr.umich.edu/tables.html")

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
