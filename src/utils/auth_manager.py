"""
Authentication manager for handling cookies, sessions, and API keys.
Supports cookie files, session persistence, and API key rotation.
"""

import os
import json
import time
from pathlib import Path
from typing import Optional, Dict, List, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from .logger import get_logger
from .io_utils import ensure_dir


@dataclass
class AuthConfig:
    """Authentication configuration for a site."""
    auth_type: str  # "none", "api_key", "cookies", "session", "oauth"
    api_key: Optional[str] = None
    api_key_header: str = "Authorization"  # Header name for API key
    api_key_param: Optional[str] = None  # Query parameter name for API key (e.g., "api_key" for FRED)
    api_key_format: str = "Bearer {key}"  # Format string for API key
    cookie_file: Optional[str] = None  # Path to cookie file (Netscape format)
    session_cookies: Dict[str, str] = field(default_factory=dict)  # Dict of cookie name:value
    oauth_token: Optional[str] = None
    oauth_token_expires: Optional[datetime] = None
    session_timeout: int = 3600  # Session timeout in seconds
    last_refresh: Optional[datetime] = None


class AuthManager:
    """
    Manager for authentication across different sites.
    Handles cookies, sessions, API keys, and OAuth tokens.
    """
    
    def __init__(self, cookie_storage_path: Optional[Path] = None):
        """
        Initialize the authentication manager.
        
        Args:
            cookie_storage_path: Path to directory for storing cookie files
        """
        self.logger = get_logger()
        self.cookie_storage_path = cookie_storage_path or Path.home() / ".data-fetch" / "cookies"
        ensure_dir(self.cookie_storage_path)
        
        self._auth_configs: Dict[str, AuthConfig] = {}
        self._session_cookies: Dict[str, List[Dict[str, Any]]] = {}
    
    def load_auth_config(self, site_id: str, config: Dict[str, Any]) -> AuthConfig:
        """
        Load authentication configuration from site config.
        
        Args:
            site_id: Site identifier
            config: Authentication configuration dict
        
        Returns:
            AuthConfig object
        """
        auth_type = config.get("auth_type", "none")
        
        auth_config = AuthConfig(
            auth_type=auth_type,
            api_key=config.get("api_key") or os.getenv(config.get("api_key_env", "")),
            api_key_header=config.get("api_key_header", "Authorization"),
            api_key_param=config.get("api_key_param"),  # For query parameter auth (e.g., FRED)
            api_key_format=config.get("api_key_format", "Bearer {key}"),
            cookie_file=config.get("cookie_file"),
            session_cookies=config.get("session_cookies", {}),
            session_timeout=config.get("session_timeout", 3600),
        )
        
        self._auth_configs[site_id] = auth_config
        return auth_config
    
    def get_auth_headers(self, site_id: str) -> Dict[str, str]:
        """
        Get authentication headers for a site.
        
        Args:
            site_id: Site identifier
        
        Returns:
            Dictionary of headers to include in requests
        """
        auth_config = self._auth_configs.get(site_id)
        if not auth_config or auth_config.auth_type == "none":
            return {}
        
        headers = {}
        
        if auth_config.auth_type == "api_key" and auth_config.api_key:
            header_value = auth_config.api_key_format.format(key=auth_config.api_key)
            headers[auth_config.api_key_header] = header_value
        
        elif auth_config.auth_type == "oauth":
            token = self._get_valid_oauth_token(site_id, auth_config)
            if token:
                headers["Authorization"] = f"Bearer {token}"
        
        return headers
    
    def get_cookies(self, site_id: str) -> List[Dict[str, Any]]:
        """
        Get cookies for a site in Playwright format.
        
        Args:
            site_id: Site identifier
        
        Returns:
            List of cookie dicts in Playwright format
        """
        auth_config = self._auth_configs.get(site_id)
        if not auth_config:
            return []
        
        cookies = []
        
        # Load from cookie file if specified
        if auth_config.cookie_file:
            file_cookies = self._load_cookie_file(auth_config.cookie_file)
            cookies.extend(file_cookies)
        
        # Add session cookies
        if auth_config.session_cookies:
            # Convert to Playwright format (requires domain/url)
            # This is a simplified version - in practice, you'd need the domain
            for name, value in auth_config.session_cookies.items():
                cookies.append({
                    "name": name,
                    "value": value,
                    "domain": "",  # Should be set based on site URL
                    "path": "/",
                })
        
        # Get stored session cookies
        if site_id in self._session_cookies:
            cookies.extend(self._session_cookies[site_id])
        
        return cookies
    
    def _load_cookie_file(self, cookie_file: str) -> List[Dict[str, Any]]:
        """
        Load cookies from a Netscape-format cookie file.
        
        Args:
            cookie_file: Path to cookie file
        
        Returns:
            List of cookie dicts
        """
        cookies = []
        cookie_path = Path(cookie_file)
        
        if not cookie_path.exists():
            self.logger.warning(f"Cookie file not found: {cookie_file}")
            return cookies
        
        try:
            with open(cookie_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("#") or not line:
                        continue
                    
                    # Netscape cookie format:
                    # domain, flag, path, secure, expiration, name, value
                    parts = line.split("\t")
                    if len(parts) >= 7:
                        domain = parts[0]
                        path = parts[2]
                        secure = parts[3] == "TRUE"
                        expiration = int(parts[4]) if parts[4].isdigit() else None
                        name = parts[5]
                        value = parts[6]
                        
                        # Check if cookie is expired
                        if expiration and expiration < time.time():
                            continue
                        
                        cookies.append({
                            "name": name,
                            "value": value,
                            "domain": domain,
                            "path": path,
                            "secure": secure,
                            "expires": expiration,
                        })
            
            self.logger.info(f"Loaded {len(cookies)} cookies from {cookie_file}")
        except Exception as e:
            self.logger.error(f"Error loading cookie file {cookie_file}: {e}")
        
        return cookies
    
    def save_cookies(self, site_id: str, cookies: List[Dict[str, Any]], domain: str):
        """
        Save cookies for a site.
        
        Args:
            site_id: Site identifier
            cookies: List of cookie dicts (Playwright format)
            domain: Domain for the cookies
        """
        # Update session cookies
        self._session_cookies[site_id] = cookies
        
        # Update auth config if it exists
        if site_id in self._auth_configs:
            auth_config = self._auth_configs[site_id]
            auth_config.last_refresh = datetime.now()
            
            # Convert to session_cookies dict format
            for cookie in cookies:
                if "name" in cookie and "value" in cookie:
                    auth_config.session_cookies[cookie["name"]] = cookie["value"]
        
        self.logger.info(f"Saved {len(cookies)} cookies for {site_id}")
    
    def _get_valid_oauth_token(self, site_id: str, auth_config: AuthConfig) -> Optional[str]:
        """
        Get a valid OAuth token, refreshing if necessary.
        
        Args:
            site_id: Site identifier
            auth_config: Authentication configuration
        
        Returns:
            OAuth token or None if unavailable
        """
        if not auth_config.oauth_token:
            return None
        
        # Check if token is expired
        if auth_config.oauth_token_expires:
            if datetime.now() >= auth_config.oauth_token_expires:
                self.logger.info(f"OAuth token expired for {site_id}, refresh needed")
                # In a real implementation, you'd refresh the token here
                return None
        
        return auth_config.oauth_token
    
    def refresh_session(self, site_id: str) -> bool:
        """
        Refresh session for a site (e.g., re-authenticate).
        
        Args:
            site_id: Site identifier
        
        Returns:
            True if refresh was successful
        """
        auth_config = self._auth_configs.get(site_id)
        if not auth_config:
            return False
        
        # In a real implementation, you'd perform re-authentication here
        auth_config.last_refresh = datetime.now()
        self.logger.info(f"Refreshed session for {site_id}")
        return True
    
    def is_session_valid(self, site_id: str) -> bool:
        """
        Check if session is still valid.
        
        Args:
            site_id: Site identifier
        
        Returns:
            True if session is valid
        """
        auth_config = self._auth_configs.get(site_id)
        if not auth_config:
            return False
        
        if auth_config.last_refresh:
            elapsed = (datetime.now() - auth_config.last_refresh).total_seconds()
            return elapsed < auth_config.session_timeout
        
        return True
    
    def rotate_api_key(self, site_id: str, new_key: str):
        """
        Rotate API key for a site.
        
        Args:
            site_id: Site identifier
            new_key: New API key
        """
        if site_id in self._auth_configs:
            self._auth_configs[site_id].api_key = new_key
            self.logger.info(f"Rotated API key for {site_id}")




