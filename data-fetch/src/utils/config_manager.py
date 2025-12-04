"""
Configuration management for the data-fetch framework.
Handles loading, saving, and validating website configurations.
"""

import yaml
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any

from .logger import get_logger
from .io_utils import get_config_path, ensure_dir, timestamp_now


@dataclass
class DataSource:
    """Configuration for a data source."""
    type: str  # "api", "dom_table", "js_object"
    endpoint: Optional[str] = None
    selector: Optional[str] = None
    method: str = "GET"
    requires_auth: bool = False
    headers: Dict[str, str] = field(default_factory=dict)


@dataclass
class RobotsPolicy:
    """Robots.txt policy configuration."""
    status: str  # "ALLOWED", "DISALLOWED", "UNKNOWN"
    last_checked: Optional[str] = None
    override_approved: bool = False


@dataclass
class SiteMetadata:
    """Metadata about a site configuration."""
    created: str = ""
    created_by: str = "manual"
    last_successful_extraction: Optional[str] = None
    last_modified: Optional[str] = None


@dataclass
class SiteConfig:
    """Configuration for a website."""
    id: str
    name: str
    base_url: str
    page_url: str
    extraction_strategy: str  # "api_json", "dom_table", "js_object", "hybrid"
    data_source: DataSource
    field_mappings: Dict[str, str]
    robots_policy: RobotsPolicy = field(default_factory=lambda: RobotsPolicy(status="UNKNOWN"))
    metadata: SiteMetadata = field(default_factory=SiteMetadata)
    rate_limit: Optional[float] = None  # Seconds between requests
    
    def to_dict(self) -> dict:
        """Convert to dictionary for YAML serialization."""
        return {
            "id": self.id,
            "name": self.name,
            "base_url": self.base_url,
            "page_url": self.page_url,
            "extraction_strategy": self.extraction_strategy,
            "data_source": asdict(self.data_source),
            "field_mappings": self.field_mappings,
            "robots_policy": asdict(self.robots_policy),
            "metadata": asdict(self.metadata),
            "rate_limit": self.rate_limit,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "SiteConfig":
        """Create from dictionary."""
        # Handle nested dataclasses
        data_source = DataSource(**data.get("data_source", {}))
        robots_policy = RobotsPolicy(**data.get("robots_policy", {"status": "UNKNOWN"}))
        metadata = SiteMetadata(**data.get("metadata", {}))
        
        return cls(
            id=data["id"],
            name=data.get("name", data["id"]),
            base_url=data["base_url"],
            page_url=data["page_url"],
            extraction_strategy=data.get("extraction_strategy", "api_json"),
            data_source=data_source,
            field_mappings=data.get("field_mappings", {}),
            robots_policy=robots_policy,
            metadata=metadata,
            rate_limit=data.get("rate_limit"),
        )


class ConfigManager:
    """
    Manager for website configurations.
    Handles loading, saving, and validating site configs.
    """
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize the config manager.
        
        Args:
            config_path: Path to the websites.yaml file
        """
        self.config_path = config_path or get_config_path()
        self.logger = get_logger()
        self._sites: Dict[str, SiteConfig] = {}
        self._loaded = False
    
    def _ensure_config_file(self):
        """Ensure the config file exists."""
        ensure_dir(self.config_path.parent)
        if not self.config_path.exists():
            with open(self.config_path, "w") as f:
                yaml.dump({"sites": []}, f)
    
    def load(self, force: bool = False) -> Dict[str, SiteConfig]:
        """
        Load configurations from the YAML file.
        
        Args:
            force: Force reload even if already loaded
        
        Returns:
            Dictionary of site configs keyed by site_id
        """
        if self._loaded and not force:
            return self._sites
        
        self._ensure_config_file()
        
        try:
            with open(self.config_path, "r") as f:
                data = yaml.safe_load(f) or {}
        except Exception as e:
            self.logger.error(f"Error loading config: {e}")
            data = {}
        
        self._sites = {}
        for site_data in data.get("sites", []):
            try:
                site = SiteConfig.from_dict(site_data)
                self._sites[site.id] = site
            except Exception as e:
                self.logger.warning(f"Error parsing site config: {e}")
        
        self._loaded = True
        self.logger.info(f"Loaded {len(self._sites)} site configurations")
        return self._sites
    
    def save(self):
        """Save all configurations to the YAML file."""
        self._ensure_config_file()
        
        data = {
            "sites": [site.to_dict() for site in self._sites.values()]
        }
        
        with open(self.config_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        
        self.logger.info(f"Saved {len(self._sites)} site configurations")
    
    def get(self, site_id: str) -> Optional[SiteConfig]:
        """
        Get a site configuration by ID.
        
        Args:
            site_id: The site identifier
        
        Returns:
            SiteConfig or None if not found
        """
        self.load()
        return self._sites.get(site_id)
    
    def add(self, site: SiteConfig, save: bool = True) -> bool:
        """
        Add or update a site configuration.
        
        Args:
            site: The site configuration to add
            save: Whether to save immediately
        
        Returns:
            True if added/updated successfully
        """
        self.load()
        
        # Update metadata
        if site.id in self._sites:
            site.metadata.last_modified = timestamp_now()
        else:
            if not site.metadata.created:
                site.metadata.created = timestamp_now()
        
        self._sites[site.id] = site
        
        if save:
            self.save()
        
        self.logger.info(f"Added/updated site config: {site.id}")
        return True
    
    def remove(self, site_id: str, save: bool = True) -> bool:
        """
        Remove a site configuration.
        
        Args:
            site_id: The site identifier to remove
            save: Whether to save immediately
        
        Returns:
            True if removed, False if not found
        """
        self.load()
        
        if site_id not in self._sites:
            return False
        
        del self._sites[site_id]
        
        if save:
            self.save()
        
        self.logger.info(f"Removed site config: {site_id}")
        return True
    
    def list_sites(self) -> List[Dict[str, str]]:
        """
        List all configured sites.
        
        Returns:
            List of dicts with id, name, and page_url
        """
        self.load()
        return [
            {"id": site.id, "name": site.name, "page_url": site.page_url}
            for site in self._sites.values()
        ]
    
    def validate_config(self, site: SiteConfig) -> List[str]:
        """
        Validate a site configuration.
        
        Args:
            site: The site configuration to validate
        
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        if not site.id:
            errors.append("Site ID is required")
        if not site.base_url:
            errors.append("Base URL is required")
        if not site.page_url:
            errors.append("Page URL is required")
        if not site.extraction_strategy:
            errors.append("Extraction strategy is required")
        if not site.data_source:
            errors.append("Data source is required")
        
        # Validate URLs
        if site.base_url and not site.base_url.startswith(("http://", "https://")):
            errors.append("Base URL must start with http:// or https://")
        if site.page_url and not site.page_url.startswith(("http://", "https://")):
            errors.append("Page URL must start with http:// or https://")
        
        # Validate extraction strategy
        valid_strategies = ["api_json", "dom_table", "js_object", "hybrid"]
        if site.extraction_strategy and site.extraction_strategy not in valid_strategies:
            errors.append(f"Invalid extraction strategy. Must be one of: {valid_strategies}")
        
        return errors
    
    def update_last_extraction(self, site_id: str):
        """Update the last successful extraction timestamp for a site."""
        site = self.get(site_id)
        if site:
            site.metadata.last_successful_extraction = timestamp_now()
            self.add(site)
    
    def update_robots_policy(self, site_id: str, status: str, override_approved: bool = False):
        """Update the robots policy for a site."""
        site = self.get(site_id)
        if site:
            site.robots_policy.status = status
            site.robots_policy.last_checked = timestamp_now()
            site.robots_policy.override_approved = override_approved
            self.add(site)

