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
    type: str  # "api", "dom_table", "js_object", "browser"
    endpoint: Optional[str] = None
    page_url: Optional[str] = None  # For browser-based extraction
    selector: Optional[str] = None
    method: str = "GET"
    requires_auth: bool = False
    headers: Dict[str, str] = field(default_factory=dict)
    query_id: Optional[str] = None  # For Dune queries
    max_poll_attempts: int = 30  # For Dune queries
    poll_interval: int = 2  # For Dune queries
    parameters: Dict[str, Any] = field(default_factory=dict)  # For Dune queries, FRED series_id, etc.
    series_id: Optional[str] = None  # For FRED series


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
    requires_subscription: bool = False
    notes: Optional[str] = None


@dataclass
class AuthConfig:
    """Authentication configuration for a site."""
    auth_type: str = "none"  # "none", "api_key", "cookies", "session", "oauth"
    api_key: Optional[str] = None
    api_key_env: Optional[str] = None  # Environment variable name for API key
    api_key_header: str = "Authorization"
    api_key_param: Optional[str] = None  # Query parameter name for API key (e.g., "api_key" for FRED)
    api_key_format: str = "Bearer {key}"  # Format string
    cookie_file: Optional[str] = None
    session_cookies: Dict[str, str] = field(default_factory=dict)
    session_timeout: int = 3600


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
    auth_config: Optional[AuthConfig] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for YAML serialization."""
        result = {
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
        if self.auth_config:
            result["auth_config"] = asdict(self.auth_config)
        return result
    
    @classmethod
    def from_dict(cls, data: dict) -> "SiteConfig":
        """Create from dictionary."""
        # Handle nested dataclasses
        data_source_dict = data.get("data_source", {})
        # Ensure all DataSource fields are present
        data_source = DataSource(
            type=data_source_dict.get("type", "api"),
            endpoint=data_source_dict.get("endpoint"),
            page_url=data_source_dict.get("page_url"),
            selector=data_source_dict.get("selector"),
            method=data_source_dict.get("method", "GET"),
            requires_auth=data_source_dict.get("requires_auth", False),
            headers=data_source_dict.get("headers", {}),
            query_id=data_source_dict.get("query_id"),
            max_poll_attempts=data_source_dict.get("max_poll_attempts", 30),
            poll_interval=data_source_dict.get("poll_interval", 2),
            parameters=data_source_dict.get("parameters", {}),
            series_id=data_source_dict.get("series_id"),
        )
        robots_policy = RobotsPolicy(**data.get("robots_policy", {"status": "UNKNOWN"}))
        # Handle metadata with optional fields
        metadata_dict = data.get("metadata", {})
        metadata = SiteMetadata(
            created=metadata_dict.get("created", ""),
            created_by=metadata_dict.get("created_by", "manual"),
            last_successful_extraction=metadata_dict.get("last_successful_extraction"),
            last_modified=metadata_dict.get("last_modified"),
            requires_subscription=metadata_dict.get("requires_subscription", False),
            notes=metadata_dict.get("notes"),
        )
        auth_config = None
        if "auth_config" in data:
            auth_config = AuthConfig(**data.get("auth_config", {}))
        
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
            auth_config=auth_config,
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
    
    def list_sites(self) -> List[Dict[str, Any]]:
        """
        List all configured sites with metadata.
        
        Returns:
            List of dictionaries with id, name, page_url, and metadata
        """
        import os
        self.load()
        sites_list = []
        
        for site in self._sites.values():
            site_dict = {
                "id": site.id,
                "name": site.name,
                "page_url": site.page_url,
                "extraction_strategy": site.extraction_strategy,
                "requires_auth": site.data_source.requires_auth if site.data_source else False,
            }
            
            # Check API key status
            api_key_status = "Not Required"
            if site.auth_config:
                if site.auth_config.api_key_env:
                    api_key = os.getenv(site.auth_config.api_key_env)
                    api_key_status = "Configured" if api_key else "Missing"
                elif site.auth_config.api_key:
                    api_key_status = "Configured"
            
            site_dict["api_key_status"] = api_key_status
            
            # Check subscription requirement
            requires_subscription = False
            if hasattr(site.metadata, "requires_subscription"):
                requires_subscription = getattr(site.metadata, "requires_subscription", False)
            elif "requires_subscription" in site.metadata.__dict__:
                requires_subscription = site.metadata.__dict__.get("requires_subscription", False)
            
            site_dict["requires_subscription"] = requires_subscription
            
            # Add robots.txt status
            site_dict["robots_status"] = site.robots_policy.status if site.robots_policy else "UNKNOWN"
            
            sites_list.append(site_dict)
        
        return sites_list
    
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

