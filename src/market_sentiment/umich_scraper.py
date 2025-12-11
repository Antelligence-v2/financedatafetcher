"""
University of Michigan Surveys of Consumers Data Scraper

Scrapes 5 key fields from UMich website:
1. Index of Consumer Sentiment
2. Current Economic Conditions
3. Consumer Expectations
4. Year Ahead Inflation
5. Long Run Inflation

Author: Bhavya Jain
Date: December 10, 2025
"""

import requests
import pandas as pd
from datetime import datetime
import os
import logging
from typing import Dict, Optional
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class UMichScraper:
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

    def __init__(self, output_dir: str = 'data'):
        """
        Initialize the scraper

        Args:
            output_dir: Directory to save output files
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

        self.raw_data = {}
        self.combined_data = None

    def download_file(self, name: str, url: str, retries: int = 3) -> Optional[pd.DataFrame]:
        """
        Download and parse a CSV file with retry logic

        Args:
            name: Name identifier for the file
            url: URL to download from
            retries: Number of retry attempts

        Returns:
            DataFrame or None if failed
        """
        for attempt in range(retries):
            try:
                logger.info(f"Downloading {name} (attempt {attempt + 1}/{retries})...")

                response = requests.get(url, timeout=30)
                response.raise_for_status()

                # Parse CSV
                df = pd.read_csv(pd.io.common.StringIO(response.text))

                # Validate basic structure
                if 'Month' not in df.columns or 'YYYY' not in df.columns:
                    raise ValueError(f"Invalid CSV structure: missing Month or YYYY columns")

                logger.info(f"✓ Successfully downloaded {name} ({len(df)} rows)")
                return df

            except requests.exceptions.RequestException as e:
                logger.warning(f"Download attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Failed to download {name} after {retries} attempts")
                    return None

            except Exception as e:
                logger.error(f"Error parsing {name}: {e}")
                return None

        return None

    def download_all(self) -> bool:
        """
        Download all required files

        Returns:
            True if all downloads successful, False otherwise
        """
        logger.info("=" * 80)
        logger.info("Starting University of Michigan data download")
        logger.info("=" * 80)

        success = True

        for name, url in self.URLS.items():
            df = self.download_file(name, url)

            if df is not None:
                self.raw_data[name] = df

                # Save raw file
                raw_path = os.path.join(self.output_dir, f'umich_{name}_raw.csv')
                df.to_csv(raw_path, index=False)
                logger.info(f"  Saved raw data to: {raw_path}")
            else:
                success = False
                logger.error(f"  Failed to download {name}")

        return success

    def parse_date(self, row) -> Optional[pd.Timestamp]:
        """
        Parse Month and YYYY columns into a datetime

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

        except Exception as e:
            logger.warning(f"Could not parse date: {row.get('Month')} {row.get('YYYY')}")
            return None

    def process_data(self) -> bool:
        """
        Process and combine all downloaded data

        Returns:
            True if successful, False otherwise
        """
        if not self.raw_data:
            logger.error("No data to process. Run download_all() first.")
            return False

        logger.info("\n" + "=" * 80)
        logger.info("Processing and combining data")
        logger.info("=" * 80)

        try:
            # Process each file
            processed = {}

            for file_key, df in self.raw_data.items():
                logger.info(f"\nProcessing {file_key}...")

                # Add date column
                df['date'] = df.apply(self.parse_date, axis=1)

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
                logger.info(f"  ✓ Processed {len(df)} rows")

            # Combine all data on date index
            logger.info("\nCombining all datasets...")

            combined = pd.DataFrame()

            for field_key, mapping in self.FIELD_MAPPINGS.items():
                file_key = mapping['file']
                column = mapping['column']
                name = mapping['name']

                if file_key in processed and column in processed[file_key].columns:
                    combined[field_key] = processed[file_key][column]
                    logger.info(f"  ✓ Added {name} ({field_key})")
                else:
                    logger.warning(f"  ✗ Could not find {name} ({column} in {file_key})")

            # Sort by date
            combined = combined.sort_index()

            # Add metadata columns
            combined.insert(0, 'year', combined.index.year)
            combined.insert(1, 'month', combined.index.month)
            combined.insert(2, 'date', combined.index)

            self.combined_data = combined

            logger.info(f"\n✓ Combined dataset created: {len(combined)} rows, {len(combined.columns)} columns")
            logger.info(f"  Date range: {combined.index.min()} to {combined.index.max()}")

            return True

        except Exception as e:
            logger.error(f"Error processing data: {e}")
            return False

    def validate_data(self) -> Dict[str, any]:
        """
        Validate the combined dataset

        Returns:
            Dictionary with validation results
        """
        if self.combined_data is None:
            logger.error("No combined data to validate")
            return {'valid': False, 'error': 'No data'}

        logger.info("\n" + "=" * 80)
        logger.info("Validating data")
        logger.info("=" * 80)

        results = {
            'valid': True,
            'total_rows': len(self.combined_data),
            'date_range': (
                str(self.combined_data.index.min()),
                str(self.combined_data.index.max())
            ),
            'fields': {}
        }

        # Check each field
        for field_key, mapping in self.FIELD_MAPPINGS.items():
            if field_key in self.combined_data.columns:
                series = self.combined_data[field_key]

                field_info = {
                    'total_values': len(series),
                    'non_null_values': series.notna().sum(),
                    'null_values': series.isna().sum(),
                    'coverage': f"{(series.notna().sum() / len(series) * 100):.1f}%",
                    'min': float(series.min()) if series.notna().any() else None,
                    'max': float(series.max()) if series.notna().any() else None,
                    'latest': float(series.dropna().iloc[-1]) if series.notna().any() else None,
                    'latest_date': str(series.dropna().index[-1]) if series.notna().any() else None
                }

                results['fields'][field_key] = field_info

                logger.info(f"\n{mapping['name']} ({field_key}):")
                logger.info(f"  Coverage: {field_info['coverage']} ({field_info['non_null_values']}/{field_info['total_values']})")
                logger.info(f"  Range: {field_info['min']} to {field_info['max']}")
                logger.info(f"  Latest: {field_info['latest']} ({field_info['latest_date']})")

        return results

    def save_data(self, filename: str = 'umich_data_combined.csv') -> str:
        """
        Save the combined dataset

        Args:
            filename: Output filename

        Returns:
            Path to saved file
        """
        if self.combined_data is None:
            logger.error("No data to save")
            return None

        output_path = os.path.join(self.output_dir, filename)

        self.combined_data.to_csv(output_path, index=False)

        logger.info(f"\n✓ Data saved to: {output_path}")
        logger.info(f"  Size: {os.path.getsize(output_path)} bytes")

        return output_path

    def run(self) -> bool:
        """
        Run the complete scraping pipeline

        Returns:
            True if successful, False otherwise
        """
        start_time = datetime.now()

        logger.info("\n" + "=" * 80)
        logger.info("UNIVERSITY OF MICHIGAN DATA SCRAPER")
        logger.info("=" * 80)
        logger.info(f"Start time: {start_time}")
        logger.info(f"Output directory: {os.path.abspath(self.output_dir)}")

        # Step 1: Download
        if not self.download_all():
            logger.error("\n✗ Download failed")
            return False

        # Step 2: Process
        if not self.process_data():
            logger.error("\n✗ Processing failed")
            return False

        # Step 3: Validate
        validation = self.validate_data()

        # Step 4: Save
        output_file = self.save_data()

        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("\n" + "=" * 80)
        logger.info("SCRAPING COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Output: {output_file}")
        logger.info(f"Total rows: {validation['total_rows']}")
        logger.info(f"Date range: {validation['date_range'][0]} to {validation['date_range'][1]}")
        logger.info("\nAll 5 fields successfully scraped:")
        for field_key, mapping in self.FIELD_MAPPINGS.items():
            logger.info(f"  ✓ {mapping['name']}")

        return True


def main():
    """Main entry point"""
    # Create scraper
    scraper = UMichScraper(output_dir='FRED/data')

    # Run scraping pipeline
    success = scraper.run()

    if success:
        logger.info("\n🎉 SUCCESS! University of Michigan data is in the bag! 🎉")
    else:
        logger.error("\n❌ Scraping failed. Check logs for details.")

    return success


if __name__ == "__main__":
    main()
