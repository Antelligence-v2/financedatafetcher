"""
Pipeline runner for orchestrating the data extraction workflow.
Handles scraper selection, compliance checks, validation, and export.
"""

from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd

from ..utils.logger import get_logger
from ..utils.config_manager import ConfigManager, SiteConfig
from ..scraper.base_scraper import BaseScraper, ScraperResult
from ..pipeline.validators import (
    DataValidator, 
    ValidationResult,
    get_validation_profile,
)
from ..exporter.excel_exporter import ExcelExporter


@dataclass
class PipelineResult:
    """Result of running the pipeline."""
    success: bool
    data: Optional[pd.DataFrame] = None
    source_used: str = ""
    sources_tried: List[str] = field(default_factory=list)
    sources_failed: Dict[str, str] = field(default_factory=dict)
    validation_result: Optional[ValidationResult] = None
    output_path: Optional[str] = None
    scraper_result: Optional[ScraperResult] = None
    run_time_seconds: float = 0.0
    error: Optional[str] = None


class PipelineRunner:
    """
    Orchestrator for the data extraction pipeline.
    Manages scraper selection, execution, validation, and export.
    """
    
    def __init__(
        self,
        config_manager: Optional[ConfigManager] = None,
        scrapers: Optional[Dict[str, BaseScraper]] = None,
        validator: Optional[DataValidator] = None,
        exporter: Optional[ExcelExporter] = None,
    ):
        """
        Initialize the pipeline runner.
        
        Args:
            config_manager: Config manager for site configurations
            scrapers: Dictionary of scrapers keyed by site_id
            validator: Data validator instance
            exporter: Excel exporter instance
        """
        self.config_manager = config_manager or ConfigManager()
        self.scrapers = scrapers or {}
        # Create validator with default profile (will be updated per-site)
        self.validator = validator or DataValidator(require_date_column=False)
        self.exporter = exporter or ExcelExporter()
        self.logger = get_logger()
    
    def register_scraper(self, site_id: str, scraper: BaseScraper):
        """Register a scraper for a site."""
        self.scrapers[site_id] = scraper
        self.logger.info(f"Registered scraper for {site_id}")
    
    def get_scraper(self, site_id: str) -> Optional[BaseScraper]:
        """Get a scraper for a site."""
        return self.scrapers.get(site_id)
    
    def run(
        self,
        site_id: Optional[str] = None,
        url: Optional[str] = None,
        override_robots: bool = False,
        export: bool = True,
        validate: bool = True,
        fallback_sites: Optional[List[str]] = None,
    ) -> PipelineResult:
        """
        Run the data extraction pipeline.
        
        Args:
            site_id: Site ID to scrape (uses config)
            url: URL to scrape (overrides site_id)
            override_robots: Override robots.txt for UNKNOWN status
            export: Whether to export to Excel
            validate: Whether to validate data
            fallback_sites: List of fallback site IDs to try if primary fails
        
        Returns:
            PipelineResult with extracted data and metadata
        """
        start_time = datetime.now()
        result = PipelineResult(success=False)
        
        # Determine which sites to try
        sites_to_try = []
        
        if url:
            # Use universal scraper for arbitrary URLs
            sites_to_try.append(("_universal", url))
        elif site_id:
            # Use configured site
            config = self.config_manager.get(site_id)
            if config:
                sites_to_try.append((site_id, config.page_url))
            else:
                result.error = f"Site '{site_id}' not found in configuration"
                return result
        else:
            result.error = "Either site_id or url must be provided"
            return result
        
        # Add fallback sites
        if fallback_sites:
            for fb_site in fallback_sites:
                config = self.config_manager.get(fb_site)
                if config:
                    sites_to_try.append((fb_site, config.page_url))
        
        # Try each site
        for try_site_id, try_url in sites_to_try:
            result.sources_tried.append(try_site_id)
            
            self.logger.info(f"Trying source: {try_site_id}")
            
            try:
                scraper_result = self._run_scraper(
                    try_site_id, try_url, override_robots
                )
                
                if scraper_result.success and scraper_result.data is not None:
                    result.success = True
                    result.data = scraper_result.data
                    result.source_used = try_site_id
                    result.scraper_result = scraper_result
                    break
                else:
                    result.sources_failed[try_site_id] = scraper_result.error or "Unknown error"
            
            except Exception as e:
                self.logger.error(f"Scraper {try_site_id} failed: {e}")
                result.sources_failed[try_site_id] = str(e)
        
        # Validate if successful
        if result.success and validate and result.data is not None:
            self.logger.info("Validating extracted data...")
            
            # Get site-specific validation profile
            validation_profile = get_validation_profile(result.source_used)
            if validation_profile:
                # Create a new validator with the profile
                site_validator = DataValidator(
                    strict_mode=self.validator.strict_mode,
                    date_column=self.validator.date_column,
                    numeric_columns=self.validator.numeric_columns,
                    validation_profile=validation_profile,
                )
                result.validation_result = site_validator.validate(result.data)
            else:
                result.validation_result = self.validator.validate(result.data)
            
            if not result.validation_result.is_valid:
                self.logger.warning(
                    f"Validation issues: {result.validation_result.errors}"
                )
        
        # Export if successful
        if result.success and export and result.data is not None:
            self.logger.info("Exporting to Excel...")
            try:
                metadata = {
                    "source": result.source_used,
                    "rows_extracted": len(result.data),
                    "validation_warnings": len(
                        result.validation_result.warnings
                    ) if result.validation_result else 0,
                }
                
                output_path = self.exporter.export(
                    result.data,
                    site_id=result.source_used,
                    metadata=metadata,
                )
                result.output_path = str(output_path)
            except Exception as e:
                self.logger.error(f"Export failed: {e}")
        
        # Calculate run time
        result.run_time_seconds = (datetime.now() - start_time).total_seconds()
        
        self.logger.info(
            f"Pipeline complete: success={result.success}, "
            f"source={result.source_used}, "
            f"time={result.run_time_seconds:.2f}s"
        )
        
        return result
    
    def _run_scraper(
        self,
        site_id: str,
        url: str,
        override_robots: bool,
    ) -> ScraperResult:
        """Run a scraper for a site."""
        # Check if we have a registered scraper
        if site_id in self.scrapers:
            scraper = self.scrapers[site_id]
        elif site_id == "_universal":
            # Use universal scraper
            from ..scraper.universal_scraper import UniversalScraper
            import os
            use_stealth = os.getenv("USE_STEALTH_MODE", "true").lower() in ("true", "1", "yes")
            scraper = UniversalScraper(use_stealth=use_stealth)
        else:
            # Try to create from config
            config = self.config_manager.get(site_id)
            if config:
                # Use site-specific scrapers when available
                if site_id.startswith("theblock"):
                    from ..scraper.theblock_scraper import TheBlockScraper
                    scraper = TheBlockScraper(config=config)
                elif site_id.startswith("coinglass"):
                    from ..scraper.coinglass_scraper import CoinGlassScraper
                    import os
                    use_stealth = os.getenv("USE_STEALTH_MODE", "true").lower() in ("true", "1", "yes")
                    scraper = CoinGlassScraper(config=config, use_stealth=use_stealth)
                elif site_id.startswith("dune"):
                    from ..scraper.dune_scraper import DuneScraper
                    scraper = DuneScraper(config=config)
                elif site_id.startswith("fred"):
                    from ..scraper.fred_scraper import FredScraper
                    scraper = FredScraper(config=config)
                elif site_id.startswith("umich"):
                    from ..scraper.umich_scraper import UMichScraper
                    scraper = UMichScraper(config=config)
                elif site_id.startswith("dg_ecfin"):
                    from ..scraper.dg_ecfin_scraper import DGECFINScraper
                    scraper = DGECFINScraper(config=config)
                elif site_id.startswith("coingecko"):
                    from ..scraper.fallback_scrapers import CoinGeckoScraper
                    scraper = CoinGeckoScraper(config=config)
                elif site_id.startswith("coindesk") or site_id.startswith("cryptocompare"):
                    from ..scraper.fallback_scrapers import CryptoCompareScraper
                    scraper = CryptoCompareScraper(config=config)
                elif site_id.startswith("alphavantage"):
                    from ..scraper.fallback_scrapers import AlphaVantageScraper
                    scraper = AlphaVantageScraper(config=config)
                else:
                    # Fallback to universal scraper
                    from ..scraper.universal_scraper import UniversalScraper
                    # Check if stealth mode should be enabled (from env or default True)
                    import os
                    use_stealth = os.getenv("USE_STEALTH_MODE", "true").lower() in ("true", "1", "yes")
                    scraper = UniversalScraper(config=config, use_stealth=use_stealth)
            else:
                return ScraperResult(
                    success=False,
                    error=f"No scraper available for {site_id}",
                )
        
        # Run the scraper
        return scraper.scrape(url, override_robots=override_robots)
    
    def run_batch(
        self,
        site_ids: List[str],
        override_robots: bool = False,
        export: bool = True,
        stop_on_error: bool = False,
    ) -> Dict[str, PipelineResult]:
        """
        Run the pipeline for multiple sites.
        
        Args:
            site_ids: List of site IDs to scrape
            override_robots: Override robots.txt
            export: Whether to export
            stop_on_error: Stop if any site fails
        
        Returns:
            Dictionary mapping site_id to PipelineResult
        """
        results = {}
        
        for site_id in site_ids:
            self.logger.info(f"Processing site: {site_id}")
            
            result = self.run(
                site_id=site_id,
                override_robots=override_robots,
                export=export,
            )
            
            results[site_id] = result
            
            if not result.success and stop_on_error:
                self.logger.warning(f"Stopping batch due to error: {result.error}")
                break
        
        return results

