"""
Configuration generator for the data-fetch framework.
Auto-generates website configurations from successful extractions.
"""

import hashlib
from datetime import datetime
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse

import pandas as pd

from ..utils.logger import get_logger
from ..utils.config_manager import ConfigManager, SiteConfig, DataSource, RobotsPolicy, SiteMetadata
from ..utils.io_utils import timestamp_now
from ..detector.data_detector import ExtractionStrategy


class ConfigGenerator:
    """
    Generator for website configurations.
    Creates config entries from successful extractions.
    """
    
    def __init__(self, config_manager: Optional[ConfigManager] = None):
        """
        Initialize the config generator.
        
        Args:
            config_manager: Config manager for saving configs
        """
        self.config_manager = config_manager or ConfigManager()
        self.logger = get_logger()
    
    def generate_site_id(self, url: str) -> str:
        """
        Generate a unique site ID from a URL.
        
        Args:
            url: The website URL
        
        Returns:
            Site ID string
        """
        parsed = urlparse(url)
        
        # Clean domain
        domain = parsed.netloc.replace("www.", "").replace(".", "_")
        
        # Clean path for slug
        path = parsed.path.strip("/")
        slug = "_".join(path.split("/")[:3])  # Max 3 path segments
        slug = "".join(c if c.isalnum() or c == "_" else "_" for c in slug)
        slug = slug[:30]  # Limit length
        
        # Hash for uniqueness
        url_hash = hashlib.md5(url.encode()).hexdigest()[:6]
        
        parts = [domain]
        if slug:
            parts.append(slug)
        parts.append(url_hash)
        
        return "_".join(parts).lower()
    
    def generate_config(
        self,
        url: str,
        extraction_strategy: str,
        data_source: Dict[str, Any],
        field_mappings: Dict[str, str],
        sample_data: Optional[pd.DataFrame] = None,
        robots_status: str = "UNKNOWN",
        name: Optional[str] = None,
    ) -> SiteConfig:
        """
        Generate a site configuration.
        
        Args:
            url: The page URL
            extraction_strategy: Strategy type (api_json, dom_table, etc.)
            data_source: Data source configuration
            field_mappings: Field mapping dictionary
            sample_data: Optional sample data for validation
            robots_status: Status from robots.txt check
            name: Optional display name for the site
        
        Returns:
            Generated SiteConfig
        """
        parsed = urlparse(url)
        site_id = self.generate_site_id(url)
        
        # Generate name if not provided
        if name is None:
            name = f"{parsed.netloc} - {parsed.path[:30]}"
        
        # Create data source
        ds = DataSource(
            type=data_source.get("type", "api"),
            endpoint=data_source.get("endpoint"),
            selector=data_source.get("selector"),
            method=data_source.get("method", "GET"),
            requires_auth=data_source.get("requires_auth", False),
            headers=data_source.get("headers", {}),
        )
        
        # Create robots policy
        robots = RobotsPolicy(
            status=robots_status,
            last_checked=timestamp_now(),
            override_approved=False,
        )
        
        # Create metadata
        metadata = SiteMetadata(
            created=timestamp_now(),
            created_by="config_generator",
            last_successful_extraction=timestamp_now() if sample_data is not None else None,
        )
        
        config = SiteConfig(
            id=site_id,
            name=name,
            base_url=f"{parsed.scheme}://{parsed.netloc}",
            page_url=url,
            extraction_strategy=extraction_strategy,
            data_source=ds,
            field_mappings=field_mappings,
            robots_policy=robots,
            metadata=metadata,
        )
        
        self.logger.info(f"Generated config for site: {site_id}")
        return config
    
    def generate_from_strategy(
        self,
        url: str,
        strategy: ExtractionStrategy,
        robots_status: str = "UNKNOWN",
    ) -> SiteConfig:
        """
        Generate config from an extraction strategy.
        
        Args:
            url: The page URL
            strategy: Extraction strategy from data detector
            robots_status: Status from robots.txt check
        
        Returns:
            Generated SiteConfig
        """
        return self.generate_config(
            url=url,
            extraction_strategy=strategy.strategy_type,
            data_source=strategy.data_source,
            field_mappings=strategy.field_mappings,
            robots_status=robots_status,
        )
    
    def save_config(self, config: SiteConfig) -> bool:
        """
        Save a configuration to the config file.
        
        Args:
            config: Config to save
        
        Returns:
            True if saved successfully
        """
        # Validate config
        errors = self.config_manager.validate_config(config)
        if errors:
            self.logger.error(f"Config validation failed: {errors}")
            return False
        
        # Save
        self.config_manager.add(config)
        self.logger.info(f"Saved config: {config.id}")
        return True
    
    def generate_and_save(
        self,
        url: str,
        extraction_strategy: str,
        data_source: Dict[str, Any],
        field_mappings: Dict[str, str],
        robots_status: str = "UNKNOWN",
    ) -> Optional[SiteConfig]:
        """
        Generate and save a configuration.
        
        Args:
            url: The page URL
            extraction_strategy: Strategy type
            data_source: Data source configuration
            field_mappings: Field mapping dictionary
            robots_status: Status from robots.txt check
        
        Returns:
            Generated config if saved, None otherwise
        """
        config = self.generate_config(
            url=url,
            extraction_strategy=extraction_strategy,
            data_source=data_source,
            field_mappings=field_mappings,
            robots_status=robots_status,
        )
        
        if self.save_config(config):
            return config
        return None
    
    def suggest_field_mappings(
        self,
        field_names: List[str],
    ) -> Dict[str, str]:
        """
        Suggest field mappings based on common patterns.
        
        Args:
            field_names: List of field names from the data source
        
        Returns:
            Suggested field mappings
        """
        mappings = {}
        
        # Common patterns for financial data
        patterns = {
            "date": ["date", "time", "timestamp", "datetime", "created_at", "period"],
            "volume": ["volume", "vol", "trading_volume", "total_volume"],
            "price": ["price", "close", "closing_price", "value", "rate"],
            "open": ["open", "opening", "open_price"],
            "high": ["high", "highest", "high_price", "max"],
            "low": ["low", "lowest", "low_price", "min"],
            "market_cap": ["market_cap", "marketcap", "mcap", "market_capitalization"],
        }
        
        field_names_lower = {f.lower(): f for f in field_names}
        
        for target, keywords in patterns.items():
            for kw in keywords:
                # Exact match
                if kw in field_names_lower:
                    mappings[target] = field_names_lower[kw]
                    break
                # Partial match
                for fn_lower, fn_orig in field_names_lower.items():
                    if kw in fn_lower:
                        mappings[target] = fn_orig
                        break
                if target in mappings:
                    break
        
        return mappings

