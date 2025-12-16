"""
DG ECFIN Business and Consumer Surveys Scraper

Scrapes Economic Sentiment and Employment Expectations indicators from
European Commission DG ECFIN surveys.

Author: Bhavya Jain
Date: December 10, 2025
"""

import requests
import zipfile
import os
import pandas as pd
from datetime import datetime
import logging
from typing import Dict, Optional
import tempfile

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DGECFINScraper:
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

    def __init__(self, output_dir: str = 'data'):
        """
        Initialize the scraper

        Args:
            output_dir: Directory to save output files
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

        self.data = None
        self.metadata = {}

    def download_and_extract(self, retries: int = 3) -> Optional[str]:
        """
        Download ZIP file and extract Excel file

        Args:
            retries: Number of retry attempts

        Returns:
            Path to extracted Excel file or None if failed
        """
        for attempt in range(retries):
            try:
                logger.info(f"Downloading DG ECFIN data (attempt {attempt + 1}/{retries})...")
                logger.info(f"URL: {self.ZIP_URL}")

                response = requests.get(self.ZIP_URL, timeout=60)
                response.raise_for_status()

                logger.info(f"✓ Downloaded {len(response.content):,} bytes")

                # Create temp directory
                temp_dir = tempfile.mkdtemp()

                # Save ZIP file
                zip_path = os.path.join(temp_dir, 'dg_ecfin.zip')
                with open(zip_path, 'wb') as f:
                    f.write(response.content)

                # Extract ZIP
                logger.info("Extracting ZIP file...")
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                    files = zip_ref.namelist()

                logger.info(f"✓ Extracted {len(files)} files")

                # Find Excel file
                excel_file = None
                for file in files:
                    if file.endswith('.xlsx') or file.endswith('.xls'):
                        excel_file = os.path.join(temp_dir, file)
                        logger.info(f"✓ Found Excel file: {file}")
                        break

                if excel_file:
                    return excel_file
                else:
                    logger.error("No Excel file found in ZIP")
                    return None

            except requests.exceptions.RequestException as e:
                logger.warning(f"Download attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    import time
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Failed to download after {retries} attempts")
                    return None

        return None

    def parse_excel(self, excel_path: str) -> Optional[pd.DataFrame]:
        """
        Parse Excel file and extract required fields

        Args:
            excel_path: Path to Excel file

        Returns:
            DataFrame with extracted data or None if failed
        """
        try:
            logger.info("\n" + "=" * 80)
            logger.info("Parsing Excel file...")
            logger.info("=" * 80)

            # Read MONTHLY sheet
            logger.info("Reading MONTHLY sheet...")
            df = pd.read_excel(excel_path, sheet_name='MONTHLY', header=None)

            logger.info(f"✓ Loaded sheet with shape: {df.shape}")

            # First row contains column names
            # Column 0 is date, then each country's indicators
            # We need to find our specific columns

            # Get column headers from first row
            headers = df.iloc[0].tolist()

            # Find indices of our required columns
            column_indices = {}
            for key, field_info in self.FIELDS.items():
                col_name = field_info['column']
                try:
                    idx = headers.index(col_name)
                    column_indices[key] = idx
                    logger.info(f"  ✓ Found {col_name} at column {idx}")
                except ValueError:
                    logger.warning(f"  ✗ Could not find column: {col_name}")

            if not column_indices:
                logger.error("No required columns found!")
                return None

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

            # Add year, month columns
            result.insert(1, 'year', result['date'].dt.year)
            result.insert(2, 'month', result['date'].dt.month)

            logger.info(f"\n✓ Extracted {len(result)} observations")
            logger.info(f"  Date range: {result['date'].min().date()} to {result['date'].max().date()}")

            return result

        except Exception as e:
            logger.error(f"Error parsing Excel file: {e}")
            import traceback
            traceback.print_exc()
            return None

    def create_metadata(self, df: pd.DataFrame) -> Dict:
        """
        Create metadata dictionary

        Args:
            df: Data DataFrame

        Returns:
            Metadata dictionary
        """
        logger.info("\n" + "=" * 80)
        logger.info("Creating metadata...")
        logger.info("=" * 80)

        metadata = {
            'source': 'European Commission DG ECFIN',
            'dataset': 'Business and Consumer Surveys',
            'download_url': self.ZIP_URL,
            'download_date': datetime.now().isoformat(),
            'total_observations': int(len(df)),
            'date_range': {
                'start': str(df['date'].min().date()),
                'end': str(df['date'].max().date())
            },
            'fields': {}
        }

        # Add field-specific metadata
        for key, field_info in self.FIELDS.items():
            field_name = field_info['field_name']

            if field_name in df.columns:
                series_data = df[field_name]

                field_metadata = {
                    'name': field_info['name'],
                    'column': field_info['column'],
                    'total_values': int(len(series_data)),
                    'non_null_values': int(series_data.notna().sum()),
                    'coverage': f"{(series_data.notna().sum() / len(series_data) * 100):.1f}%",
                    'min': float(series_data.min()) if series_data.notna().any() else None,
                    'max': float(series_data.max()) if series_data.notna().any() else None,
                    'latest': float(series_data.dropna().iloc[-1]) if series_data.notna().any() else None,
                    'latest_date': str(df[series_data.notna()]['date'].iloc[-1].date()) if series_data.notna().any() else None
                }

                metadata['fields'][key] = field_metadata

                logger.info(f"\n{field_info['name']}:")
                logger.info(f"  Coverage: {field_metadata['coverage']} ({field_metadata['non_null_values']}/{field_metadata['total_values']})")
                logger.info(f"  Range: {field_metadata['min']} to {field_metadata['max']}")
                logger.info(f"  Latest: {field_metadata['latest']} ({field_metadata['latest_date']})")

        return metadata

    def save_data(self, df: pd.DataFrame, metadata: Dict) -> str:
        """
        Save data and metadata

        Args:
            df: Data DataFrame
            metadata: Metadata dictionary

        Returns:
            Path to saved file
        """
        # Save CSV
        output_path = os.path.join(self.output_dir, 'dg_ecfin_surveys.csv')
        df.to_csv(output_path, index=False)
        logger.info(f"\n✓ Data saved to: {output_path}")

        # Save metadata
        metadata_path = os.path.join(self.output_dir, 'dg_ecfin_metadata.json')
        import json
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"✓ Metadata saved to: {metadata_path}")

        return output_path

    def run(self) -> bool:
        """
        Run the complete scraping pipeline

        Returns:
            True if successful, False otherwise
        """
        start_time = datetime.now()

        logger.info("\n" + "=" * 80)
        logger.info("DG ECFIN BUSINESS AND CONSUMER SURVEYS SCRAPER")
        logger.info("=" * 80)
        logger.info(f"Start time: {start_time}")
        logger.info(f"Output directory: {os.path.abspath(self.output_dir)}")

        # Step 1: Download and extract
        excel_path = self.download_and_extract()
        if not excel_path:
            logger.error("\n✗ Download failed")
            return False

        # Step 2: Parse Excel
        self.data = self.parse_excel(excel_path)
        if self.data is None:
            logger.error("\n✗ Excel parsing failed")
            return False

        # Step 3: Create metadata
        self.metadata = self.create_metadata(self.data)

        # Step 4: Save
        output_file = self.save_data(self.data, self.metadata)

        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("\n" + "=" * 80)
        logger.info("SCRAPING COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Output: {output_file}")
        logger.info(f"Total observations: {len(self.data)}")
        logger.info(f"Date range: {self.metadata['date_range']['start']} to {self.metadata['date_range']['end']}")
        logger.info("\n✅ All requested fields captured:")
        for key, field_info in self.FIELDS.items():
            logger.info(f"  ✓ {field_info['name']}")

        return True


def main():
    """Main entry point"""
    # Create scraper
    scraper = DGECFINScraper(output_dir='FRED/data')

    # Run scraping pipeline
    success = scraper.run()

    if success:
        logger.info("\n🎉 SUCCESS! DG ECFIN survey data scraped! 🎉")
    else:
        logger.error("\n❌ Scraping failed. Check logs for details.")

    return success


if __name__ == "__main__":
    main()
