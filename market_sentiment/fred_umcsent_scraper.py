"""
FRED UMCSENT (University of Michigan Consumer Sentiment) Scraper

Scrapes UMCSENT series metadata and observations from FRED API.

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
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FREDUMCSENTScraper:
    """Scraper for FRED UMCSENT series"""

    # API Configuration
    BASE_URL = "https://api.stlouisfed.org/fred"
    SERIES_ID = "UMCSENT"

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

        self.metadata = None
        self.observations = None

    def get_series_metadata(self, retries: int = 3) -> Optional[Dict]:
        """
        Get metadata for UMCSENT series

        Args:
            retries: Number of retry attempts

        Returns:
            Dictionary with metadata or None if failed
        """
        for attempt in range(retries):
            try:
                logger.info(f"Fetching metadata for {self.SERIES_ID} (attempt {attempt + 1}/{retries})...")

                response = requests.get(
                    f"{self.BASE_URL}/series",
                    params={
                        'api_key': self.api_key,
                        'series_id': self.SERIES_ID,
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

                    logger.info(f"✓ Got metadata for {self.SERIES_ID}")
                    return metadata

                else:
                    logger.error(f"No data found for {self.SERIES_ID}")
                    return None

            except requests.exceptions.RequestException as e:
                logger.warning(f"Metadata fetch attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Failed to get metadata for {self.SERIES_ID} after {retries} attempts")
                    return None

        return None

    def get_series_observations(self, retries: int = 3) -> Optional[pd.DataFrame]:
        """
        Get all observations for UMCSENT series

        Args:
            retries: Number of retry attempts

        Returns:
            DataFrame with observations or None if failed
        """
        for attempt in range(retries):
            try:
                logger.info(f"Fetching observations for {self.SERIES_ID} (attempt {attempt + 1}/{retries})...")

                response = requests.get(
                    f"{self.BASE_URL}/series/observations",
                    params={
                        'api_key': self.api_key,
                        'series_id': self.SERIES_ID,
                        'file_type': 'json',
                        'limit': 100000
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

                    logger.info(f"✓ Got {len(df)} observations for {self.SERIES_ID}")
                    logger.info(f"  Date range: {df['date'].min().date()} to {df['date'].max().date()}")
                    logger.info(f"  First value: {df.iloc[0]['value']} ({df.iloc[0]['date'].date()})")
                    logger.info(f"  Latest value: {df.iloc[-1]['value']} ({df.iloc[-1]['date'].date()})")

                    return df

                else:
                    logger.error(f"No observations found for {self.SERIES_ID}")
                    return None

            except requests.exceptions.RequestException as e:
                logger.warning(f"Observations fetch attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Failed to get observations for {self.SERIES_ID} after {retries} attempts")
                    return None

        return None

    def create_table_data(self) -> Dict:
        """
        Create table data with requested fields

        Returns:
            Dictionary with table data
        """
        if not self.metadata or self.observations is None:
            logger.error("Cannot create table data - missing metadata or observations")
            return None

        logger.info("\n" + "=" * 80)
        logger.info("Creating table data")
        logger.info("=" * 80)

        table_data = {
            'series_id': self.SERIES_ID,
            'title': self.metadata['title'],
            'latest_value': float(self.observations.iloc[-1]['value']),
            'latest_date': str(self.observations.iloc[-1]['date'].date()),
            'first_value': float(self.observations.iloc[0]['value']),
            'first_date': str(self.observations.iloc[0]['date'].date()),
            'date_range': f"{self.observations.iloc[0]['date'].date()} to {self.observations.iloc[-1]['date'].date()}",
            'seasonal_adjustment': self.metadata['seasonal_adjustment'],
            'last_updated': self.metadata['last_updated'],
            'frequency': self.metadata['frequency'],
            'units': self.metadata['units'],
            'total_observations': len(self.observations)
        }

        logger.info("\nTable Data:")
        logger.info(f"  Series ID: {table_data['series_id']}")
        logger.info(f"  Latest Value: {table_data['latest_value']} ({table_data['latest_date']})")
        logger.info(f"  First Value: {table_data['first_value']} ({table_data['first_date']})")
        logger.info(f"  Date Range: {table_data['date_range']}")
        logger.info(f"  Seasonal Adjustment: {table_data['seasonal_adjustment']}")
        logger.info(f"  Last Updated: {table_data['last_updated']}")
        logger.info(f"  Frequency: {table_data['frequency']}")
        logger.info(f"  Units: {table_data['units']}")
        logger.info(f"  Total Observations: {table_data['total_observations']}")

        return table_data

    def save_data(self, table_data: Dict) -> str:
        """
        Save data and table metadata

        Args:
            table_data: Table data dictionary

        Returns:
            Path to saved file
        """
        # Save observations CSV
        output_path = os.path.join(self.output_dir, 'fred_umcsent.csv')
        df = self.observations.copy()
        df.insert(1, 'year', df['date'].dt.year)
        df.insert(2, 'month', df['date'].dt.month)
        df.to_csv(output_path, index=False)
        logger.info(f"\n✓ Data saved to: {output_path}")

        # Save table data
        table_path = os.path.join(self.output_dir, 'fred_umcsent_table.json')
        import json
        with open(table_path, 'w') as f:
            json.dump(table_data, f, indent=2)
        logger.info(f"✓ Table data saved to: {table_path}")

        return output_path

    def run(self) -> bool:
        """
        Run the complete scraping pipeline

        Returns:
            True if successful, False otherwise
        """
        start_time = datetime.now()

        logger.info("\n" + "=" * 80)
        logger.info("FRED UMCSENT SCRAPER")
        logger.info("=" * 80)
        logger.info(f"Start time: {start_time}")
        logger.info(f"Output directory: {os.path.abspath(self.output_dir)}")

        # Step 1: Fetch metadata
        self.metadata = self.get_series_metadata()
        if not self.metadata:
            logger.error("\n✗ Metadata fetch failed")
            return False

        # Step 2: Fetch observations
        self.observations = self.get_series_observations()
        if self.observations is None:
            logger.error("\n✗ Observations fetch failed")
            return False

        # Step 3: Create table data
        table_data = self.create_table_data()
        if not table_data:
            logger.error("\n✗ Table data creation failed")
            return False

        # Step 4: Save
        output_file = self.save_data(table_data)

        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("\n" + "=" * 80)
        logger.info("SCRAPING COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Output: {output_file}")
        logger.info(f"Total observations: {len(self.observations)}")
        logger.info(f"Date range: {table_data['date_range']}")
        logger.info("\n✅ All requested fields captured:")
        logger.info(f"  ✓ Latest value: {table_data['latest_value']}")
        logger.info(f"  ✓ First value: {table_data['first_value']}")
        logger.info(f"  ✓ Date range: {table_data['date_range']}")
        logger.info(f"  ✓ Seasonal adjustment: {table_data['seasonal_adjustment']}")
        logger.info(f"  ✓ Last updated: {table_data['last_updated']}")

        return True


def main():
    """Main entry point"""
    # Create scraper
    scraper = FREDUMCSENTScraper(output_dir='FRED/data')

    # Run scraping pipeline
    success = scraper.run()

    if success:
        logger.info("\n🎉 SUCCESS! UMCSENT data scraped! 🎉")
    else:
        logger.error("\n❌ Scraping failed. Check logs for details.")

    return success


if __name__ == "__main__":
    main()
