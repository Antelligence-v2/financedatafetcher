"""
Real-time normalization orchestrator.
Coordinates the full flow: scrape → normalize → validate → store.
"""

from dataclasses import dataclass
from typing import List, Dict, Optional, Any
import pandas as pd

from .realtime_scraper import RealtimeScraper
from .asset_mapper import AssetMapper
from .metric_category_mapper import MetricCategoryMapper
from ..normalizer import (
    NormalizedDataPoint,
    CoinglassTransformer,
    InvezzTransformer,
    CoinGeckoTransformer,
    DuneTransformer,
    TheBlockTransformer,
)
from ..normalizer.ai_validator import AIValidator, ValidationReport
from ..warehouse import DataWarehouse
from ..utils.logger import get_logger


@dataclass
class NormalizationResult:
    """Result of real-time normalization process."""
    success: bool
    asset: str
    categories: List[str]
    raw_data: Dict[str, pd.DataFrame]  # site_id -> DataFrame
    normalized_points: List[NormalizedDataPoint]
    validation_reports: Dict[str, ValidationReport]  # source -> report
    points_added: int
    errors: List[str]
    warnings: List[str]
    sources_scraped: List[str]
    sources_failed: List[str]


class RealtimeNormalizer:
    """
    Orchestrator for real-time data normalization.
    Coordinates scraping, transformation, validation, and storage.
    """
    
    # Source to transformer mapping
    TRANSFORMERS = {
        'coinglass': CoinglassTransformer,
        'invezz': InvezzTransformer,
        'coingecko': CoinGeckoTransformer,
        'dune': DuneTransformer,
        'theblock': TheBlockTransformer,
    }
    
    def __init__(
        self,
        warehouse: Optional[DataWarehouse] = None,
        scraper: Optional[RealtimeScraper] = None,
        asset_mapper: Optional[AssetMapper] = None,
        metric_mapper: Optional[MetricCategoryMapper] = None,
        validator: Optional[AIValidator] = None,
    ):
        """
        Initialize real-time normalizer.
        
        Args:
            warehouse: DataWarehouse instance
            scraper: RealtimeScraper instance
            asset_mapper: AssetMapper instance
            metric_mapper: MetricCategoryMapper instance
            validator: AIValidator instance
        """
        self.warehouse = warehouse or DataWarehouse()
        self.scraper = scraper or RealtimeScraper()
        self.asset_mapper = asset_mapper or AssetMapper()
        self.metric_mapper = metric_mapper or MetricCategoryMapper()
        self.validator = validator or AIValidator()
        self.logger = get_logger()
    
    def fetch_and_normalize(
        self,
        asset: str,
        categories: List[str]
    ) -> NormalizationResult:
        """
        Main orchestrator: fetch data, normalize, validate, and store.
        
        Args:
            asset: Asset name (e.g., 'BTC', 'ETH')
            categories: List of metric categories (e.g., ['volume', 'liquidations'])
            
        Returns:
            NormalizationResult with all results
        """
        self.logger.info(f"Starting normalization for asset: {asset}, categories: {categories}")
        
        errors = []
        warnings = []
        raw_data = {}
        normalized_points = []
        validation_reports = {}
        sources_scraped = []
        sources_failed = []
        
        try:
            # Step 1: Scrape sources
            self.logger.info("Step 1: Scraping sources...")
            raw_data = self._scrape_sources(asset)
            
            if not raw_data:
                errors.append(f"No data scraped for asset: {asset}")
                return NormalizationResult(
                    success=False,
                    asset=asset,
                    categories=categories,
                    raw_data={},
                    normalized_points=[],
                    validation_reports={},
                    points_added=0,
                    errors=errors,
                    warnings=warnings,
                    sources_scraped=[],
                    sources_failed=[],
                )
            
            sources_scraped = list(raw_data.keys())
            self.logger.info(f"Scraped {len(sources_scraped)} sources: {sources_scraped}")
            
            # Step 2: Normalize data
            self.logger.info("Step 2: Normalizing data...")
            normalized_points = self._normalize_data(raw_data)
            
            if not normalized_points:
                errors.append("No normalized data points created")
                return NormalizationResult(
                    success=False,
                    asset=asset,
                    categories=categories,
                    raw_data=raw_data,
                    normalized_points=[],
                    validation_reports={},
                    points_added=0,
                    errors=errors,
                    warnings=warnings,
                    sources_scraped=sources_scraped,
                    sources_failed=sources_failed,
                )
            
            self.logger.info(f"Created {len(normalized_points)} normalized data points")
            
            # Step 3: Validate data
            self.logger.info("Step 3: Validating normalization...")
            validation_reports = self._validate_data(raw_data, normalized_points)
            
            # Collect validation warnings/errors
            for source, report in validation_reports.items():
                if not report.passed:
                    errors.extend([f"{source}: {e}" for e in report.errors])
                warnings.extend([f"{source}: {w}" for w in report.warnings])
            
            # Step 4: Filter by categories (if specified)
            #
            # IMPORTANT: category filtering is a *view* concern and must NOT cause data loss.
            # We always store all produced points, but may return a filtered view for the UI.
            all_normalized_points = list(normalized_points)
            if categories:
                target_metrics = self.metric_mapper.get_metrics_for_categories(categories, asset)
                filtered_points = [
                    p for p in normalized_points
                    if p.metric in target_metrics
                ]
                if len(filtered_points) < len(normalized_points):
                    self.logger.info(
                        f"Filtered {len(normalized_points)} points to {len(filtered_points)} "
                        f"based on categories: {categories}"
                    )
                    normalized_points = filtered_points

                # If the filter removes everything, warn loudly (common when categories don't map
                # to emitted transformer metrics).
                if not normalized_points and all_normalized_points:
                    warnings.append(
                        "No normalized metrics matched the selected categories; stored unfiltered points "
                        "but returned an empty filtered view. Consider selecting 'volume' or 'trust', or "
                        "update metric-category mappings."
                    )
            
            # Step 5: Store in warehouse
            self.logger.info("Step 4: Storing in warehouse...")
            points_added = self._store_data(all_normalized_points)
            
            self.logger.info(f"Successfully normalized and stored {points_added} data points")
            
            return NormalizationResult(
                success=True,
                asset=asset,
                categories=categories,
                raw_data=raw_data,
                # Return the (possibly filtered) view for the UI, but store all points.
                normalized_points=normalized_points,
                validation_reports=validation_reports,
                points_added=points_added,
                errors=errors,
                warnings=warnings,
                sources_scraped=sources_scraped,
                sources_failed=sources_failed,
            )
        
        except Exception as e:
            self.logger.error(f"Error in normalization flow: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            errors.append(f"Normalization failed: {str(e)}")
            
            return NormalizationResult(
                success=False,
                asset=asset,
                categories=categories,
                raw_data=raw_data,
                normalized_points=normalized_points,
                validation_reports=validation_reports,
                points_added=0,
                errors=errors,
                warnings=warnings,
                sources_scraped=sources_scraped,
                sources_failed=sources_failed,
            )
    
    def _scrape_sources(self, asset: str) -> Dict[str, pd.DataFrame]:
        """Scrape all sources for the asset."""
        return self.scraper.scrape_asset_data(asset)
    
    def _normalize_data(
        self,
        raw_data: Dict[str, pd.DataFrame]
    ) -> List[NormalizedDataPoint]:
        """Normalize all raw data using appropriate transformers."""
        all_points = []
        
        for site_id, df in raw_data.items():
            if df.empty:
                continue
            
            # Determine source name from site_id
            source_name = self._get_source_name(site_id)
            
            # Get transformer
            transformer_class = self.TRANSFORMERS.get(source_name)
            if not transformer_class:
                self.logger.warning(f"No transformer found for source: {source_name} (site_id: {site_id})")
                continue
            
            try:
                transformer = transformer_class()
                points = transformer.transform(df)
                all_points.extend(points)
                self.logger.info(f"Normalized {len(points)} points from {source_name}")
            except Exception as e:
                self.logger.error(f"Error normalizing {source_name}: {str(e)}")
                import traceback
                self.logger.error(traceback.format_exc())
                continue
        
        return all_points
    
    def _validate_data(
        self,
        raw_data: Dict[str, pd.DataFrame],
        normalized_points: List[NormalizedDataPoint]
    ) -> Dict[str, ValidationReport]:
        """Validate normalization for each source."""
        reports = {}
        
        # Group normalized points by source
        points_by_source: Dict[str, List[NormalizedDataPoint]] = {}
        for point in normalized_points:
            points_by_source.setdefault(point.source, []).append(point)
        
        # Validate each source
        for site_id, raw_df in raw_data.items():
            source_name = self._get_source_name(site_id)
            source_points = points_by_source.get(source_name, [])
            
            if source_points:
                try:
                    report = self.validator.validate_normalization(
                        raw_df,
                        source_points,
                        source_name
                    )
                    reports[source_name] = report
                except Exception as e:
                    self.logger.error(f"Validation error for {source_name}: {str(e)}")
                    # Create a failed report
                    reports[source_name] = ValidationReport(
                        passed=False,
                        warnings=[],
                        errors=[f"Validation failed: {str(e)}"],
                        raw_field_count=len(raw_df.columns),
                        normalized_metric_count=len(set(p.metric for p in source_points)),
                        missing_fields=[],
                        suggestions=[],
                    )
        
        return reports
    
    def _store_data(self, points: List[NormalizedDataPoint]) -> int:
        """Store normalized points in warehouse."""
        if not points:
            return 0
        
        self.warehouse.add_data_points(points)
        return len(points)
    
    def _get_source_name(self, site_id: str) -> str:
        """Extract source name from site_id."""
        site_id_lower = site_id.lower()
        
        if 'coinglass' in site_id_lower:
            return 'coinglass'
        elif 'invezz' in site_id_lower:
            return 'invezz'
        elif 'coingecko' in site_id_lower:
            return 'coingecko'
        elif 'dune' in site_id_lower:
            return 'dune'
        elif 'theblock' in site_id_lower:
            return 'theblock'
        else:
            # Try to infer from first part of site_id
            parts = site_id.split('_')
            return parts[0] if parts else 'unknown'

