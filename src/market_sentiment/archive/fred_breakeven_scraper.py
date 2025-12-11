"""
FRED Breakeven Inflation Scraper

Scrapes breakeven inflation rates from FRED API:
1. T10YIE - 10-Year Breakeven Inflation Rate
2. T5YIFR - 5-Year, 5-Year Forward Inflation Expectation Rate

Author: Bhavya Jain
Date: December 10, 2025
"""

import requests
import pandas as pd
from datetime import datetime
import os
import logging
from typing import Dict, Optional, List
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FREDBreakevenScraper:
    """Scraper for FRED breakeven inflation rates"""

    # API Configuration
    BASE_URL = "https://api.stlouisfed.org/fred"

    # Series to scrape
    SERIES = {
        't10yie': {
            'id': 'T10YIE',
            'name': '10-Year Breakeven Inflation Rate',
            'field_name': '10_year_breakeven_inflation'
        },
        't5yifr': {
            'id': 'T5YIFR',
            'name': '5-Year, 5-Year Forward Inflation Expectation Rate',
            'field_name': '5y5y_forward_inflation'
        }
    }

    def __init__(self, api_key: str = None, output_dir: str = 'data'):
        """
        Initialize the scraper

        Args:
            api_key: FRED API key (reads from .env if not provided)
            output_dir: Directory to save output files
        """
        self.api_key = api_key or os.getenv('FRED_API_KEY')
        if not self.api_key:
            raise ValueError("FRED_API_KEY not found in environment variables")

        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

        self.series_metadata = {}
        self.series_data = {}

    def get_series_metadata(self, series_id: str, retries: int = 3) -> Optional[Dict]:
        """
        Get metadata for a series

        Args:
            series_id: FRED series ID
            retries: Number of retry attempts

        Returns:
            Dictionary with metadata or None if failed
        """
        for attempt in range(retries):
            try:
                logger.info(f"Fetching metadata for {series_id} (attempt {attempt + 1}/{retries})...")

                response = requests.get(
                    f"{self.BASE_URL}/series",
                    params={
                        'api_key': self.api_key,
                        'series_id': series_id,
                        'file_type': 'json'
                    },
                    timeout=30
                )
                response.raise_for_status()

                data = response.json()

                if 'seriess' in data and len(data['seriess']) > 0:
                    series = data['seriess'][0]

                    metadata = {
                        'series_id': series['id'],
                        'title': series['title'],
                        'units': series.get('units', ''),
                        'frequency': series.get('frequency', ''),
                        'seasonal_adjustment': series.get('seasonal_adjustment', ''),
                        'last_updated': series.get('last_updated', ''),
                        'observation_start': series.get('observation_start', ''),
                        'observation_end': series.get('observation_end', ''),
                        'popularity': series.get('popularity', ''),
                        'notes': series.get('notes', '')
                    }

                    logger.info(f"✓ Got metadata for {series_id}")
                    return metadata

                else:
                    logger.error(f"No data found for {series_id}")
                    return None

            except requests.exceptions.RequestException as e:
                logger.warning(f"Metadata fetch attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Failed to get metadata for {series_id} after {retries} attempts")
                    return None

        return None

    def get_series_observations(self, series_id: str, retries: int = 3) -> Optional[pd.DataFrame]:
        """
        Get all observations for a series

        Args:
            series_id: FRED series ID
            retries: Number of retry attempts

        Returns:
            DataFrame with observations or None if failed
        """
        for attempt in range(retries):
            try:
                logger.info(f"Fetching observations for {series_id} (attempt {attempt + 1}/{retries})...")

                response = requests.get(
                    f"{self.BASE_URL}/series/observations",
                    params={
                        'api_key': self.api_key,
                        'series_id': series_id,
                        'file_type': 'json',
                        'limit': 100000  # Max allowed
                    },
                    timeout=30
                )
                response.raise_for_status()

                data = response.json()

                if 'observations' in data:
                    observations = data['observations']

                    # Convert to DataFrame
                    df = pd.DataFrame(observations)

                    # Parse date
                    df['date'] = pd.to_datetime(df['date'])

                    # Convert value to numeric (. becomes NaN)
                    df['value'] = pd.to_numeric(df['value'], errors='coerce')

                    # Remove missing values
                    df = df.dropna(subset=['value'])

                    # Keep only date and value
                    df = df[['date', 'value']]

                    # Sort by date
                    df = df.sort_values('date').reset_index(drop=True)

                    logger.info(f"✓ Got {len(df)} observations for {series_id}")
                    logger.info(f"  Date range: {df['date'].min()} to {df['date'].max()}")
                    logger.info(f"  Latest value: {df.iloc[-1]['value']} ({df.iloc[-1]['date'].date()})")

                    return df

                else:
                    logger.error(f"No observations found for {series_id}")
                    return None

            except requests.exceptions.RequestException as e:
                logger.warning(f"Observations fetch attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Failed to get observations for {series_id} after {retries} attempts")
                    return None

        return None

    def fetch_all_series(self) -> bool:
        """
        Fetch metadata and observations for all series

        Returns:
            True if successful, False otherwise
        """
        logger.info("=" * 80)
        logger.info("Fetching breakeven inflation data from FRED")
        logger.info("=" * 80)

        success = True

        for key, series_info in self.SERIES.items():
            series_id = series_info['id']
            logger.info(f"\n📊 Processing {series_info['name']} ({series_id})")

            # Get metadata
            metadata = self.get_series_metadata(series_id)
            if metadata:
                self.series_metadata[key] = metadata
            else:
                success = False
                continue

            # Get observations
            observations = self.get_series_observations(series_id)
            if observations is not None:
                self.series_data[key] = observations
            else:
                success = False

        return success

    def create_combined_dataset(self) -> Optional[pd.DataFrame]:
        """
        Combine all series into a single dataset

        Returns:
            Combined DataFrame or None if failed
        """
        if not self.series_data:
            logger.error("No data to combine")
            return None

        logger.info("\n" + "=" * 80)
        logger.info("Creating combined dataset")
        logger.info("=" * 80)

        # Start with date range from first series
        first_key = list(self.series_data.keys())[0]
        combined = self.series_data[first_key][['date']].copy()

        # Add each series as a column
        for key, df in self.series_data.items():
            field_name = self.SERIES[key]['field_name']

            # Merge on date
            combined = combined.merge(
                df.rename(columns={'value': field_name}),
                on='date',
                how='outer'
            )

            logger.info(f"  ✓ Added {self.SERIES[key]['name']}")

        # Sort by date
        combined = combined.sort_values('date').reset_index(drop=True)

        # Add year, month, day columns
        combined.insert(1, 'year', combined['date'].dt.year)
        combined.insert(2, 'month', combined['date'].dt.month)
        combined.insert(3, 'day', combined['date'].dt.day)

        logger.info(f"\n✓ Combined dataset created: {len(combined)} rows, {len(combined.columns)} columns")
        logger.info(f"  Date range: {combined['date'].min()} to {combined['date'].max()}")

        return combined

    def save_data(self, combined_df: pd.DataFrame, filename: str = 'fred_breakeven_inflation.csv') -> str:
        """
        Save combined data and metadata

        Args:
            combined_df: Combined DataFrame
            filename: Output filename

        Returns:
            Path to saved file
        """
        # Save combined data
        output_path = os.path.join(self.output_dir, filename)
        combined_df.to_csv(output_path, index=False)
        logger.info(f"\n✓ Data saved to: {output_path}")

        # Save metadata
        metadata_path = os.path.join(self.output_dir, 'fred_breakeven_metadata.json')
        import json
        with open(metadata_path, 'w') as f:
            json.dump(self.series_metadata, f, indent=2)
        logger.info(f"✓ Metadata saved to: {metadata_path}")

        return output_path

    def validate_data(self, combined_df: pd.DataFrame) -> Dict:
        """
        Validate the combined dataset

        Returns:
            Dictionary with validation results
        """
        logger.info("\n" + "=" * 80)
        logger.info("Validating data")
        logger.info("=" * 80)

        results = {
            'valid': True,
            'total_rows': len(combined_df),
            'date_range': (
                str(combined_df['date'].min()),
                str(combined_df['date'].max())
            ),
            'series': {}
        }

        for key, series_info in self.SERIES.items():
            field_name = series_info['field_name']

            if field_name in combined_df.columns:
                series_data = combined_df[field_name]

                series_results = {
                    'total_values': len(series_data),
                    'non_null_values': series_data.notna().sum(),
                    'coverage': f"{(series_data.notna().sum() / len(series_data) * 100):.1f}%",
                    'min': float(series_data.min()) if series_data.notna().any() else None,
                    'max': float(series_data.max()) if series_data.notna().any() else None,
                    'latest': float(series_data.dropna().iloc[-1]) if series_data.notna().any() else None,
                    'latest_date': str(combined_df[series_data.notna()]['date'].iloc[-1]) if series_data.notna().any() else None
                }

                results['series'][key] = series_results

                logger.info(f"\n{series_info['name']}:")
                logger.info(f"  Coverage: {series_results['coverage']} ({series_results['non_null_values']}/{series_results['total_values']})")
                logger.info(f"  Range: {series_results['min']}% to {series_results['max']}%")
                logger.info(f"  Latest: {series_results['latest']}% ({series_results['latest_date'][:10]})")

        return results

    def run(self) -> bool:
        """
        Run the complete scraping pipeline

        Returns:
            True if successful, False otherwise
        """
        start_time = datetime.now()

        logger.info("\n" + "=" * 80)
        logger.info("FRED BREAKEVEN INFLATION SCRAPER")
        logger.info("=" * 80)
        logger.info(f"Start time: {start_time}")
        logger.info(f"Output directory: {os.path.abspath(self.output_dir)}")

        # Step 1: Fetch data
        if not self.fetch_all_series():
            logger.error("\n✗ Data fetch failed")
            return False

        # Step 2: Combine data
        combined_df = self.create_combined_dataset()
        if combined_df is None:
            logger.error("\n✗ Data combination failed")
            return False

        # Step 3: Validate
        validation = self.validate_data(combined_df)

        # Step 4: Save
        output_file = self.save_data(combined_df)

        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("\n" + "=" * 80)
        logger.info("SCRAPING COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Output: {output_file}")
        logger.info(f"Total rows: {validation['total_rows']}")
        logger.info(f"Date range: {validation['date_range'][0][:10]} to {validation['date_range'][1][:10]}")
        logger.info("\nAll series successfully scraped:")
        for key, series_info in self.SERIES.items():
            logger.info(f"  ✓ {series_info['name']}")

        return True


def main():
    """Main entry point"""
    # Create scraper
    scraper = FREDBreakevenScraper(output_dir='FRED/data')

    # Run scraping pipeline
    success = scraper.run()

    if success:
        logger.info("\n🎉 SUCCESS! FRED breakeven inflation data scraped! 🎉")
    else:
        logger.error("\n❌ Scraping failed. Check logs for details.")

    return success


if __name__ == "__main__":
    main()
