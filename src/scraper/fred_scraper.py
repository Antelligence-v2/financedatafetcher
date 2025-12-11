"""
FRED (Federal Reserve Economic Data) API scraper.
Fetches economic indicators and market sentiment data.
Implements rate limiting (120 requests/minute) with exponential backoff.
"""

import os
import time
import requests
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from collections import deque

import pandas as pd

from .base_scraper import BaseScraper, ScraperResult
from ..utils.logger import get_logger
from ..utils.config_manager import SiteConfig


class FredScraper(BaseScraper):
    """
    Scraper for FRED (Federal Reserve Economic Data) API.
    Handles rate limiting and fetches both latest and historical observations.
    """
    
    API_BASE = "https://api.stlouisfed.org/fred"
    RATE_LIMIT_REQUESTS_PER_MINUTE = 120
    RATE_LIMIT_SECONDS_BETWEEN_REQUESTS = 0.5  # 120 req/min = 0.5 sec/req
    
    def __init__(
        self,
        config: Optional[SiteConfig] = None,
        api_key: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize FRED scraper.
        
        Args:
            config: Site configuration
            api_key: FRED API key (uses FRED_API_KEY env var if not provided)
        """
        super().__init__(config=config, **kwargs)
        self.api_key = api_key or os.getenv("FRED_API_KEY")
        if not self.api_key:
            self.logger.warning("FRED_API_KEY not found in environment variables")
        
        # Rate limiting: track request timestamps
        self._request_times: deque = deque(maxlen=120)  # Track last 120 requests
        self._last_request_time: float = 0.0
        
        # Series ID from config
        self.series_id = None
        if config and config.data_source:
            # Check if series_id is in parameters or data_source directly
            if hasattr(config.data_source, 'parameters') and config.data_source.parameters:
                self.series_id = config.data_source.parameters.get('series_id')
            # Also check if it's a direct attribute
            if not self.series_id and hasattr(config.data_source, 'series_id'):
                self.series_id = config.data_source.series_id
            # Fallback: try to extract from endpoint URL if it contains series_id
            if not self.series_id and config.data_source.endpoint:
                # FRED endpoint might have series_id in it
                import re
                match = re.search(r'series_id=([^&]+)', config.data_source.endpoint)
                if match:
                    self.series_id = match.group(1)
    
    def _rate_limit(self):
        """
        Implement rate limiting: 120 requests per minute.
        Uses token bucket approach with request timestamp tracking.
        """
        now = time.time()
        
        # Remove requests older than 1 minute
        cutoff_time = now - 60.0
        while self._request_times and self._request_times[0] < cutoff_time:
            self._request_times.popleft()
        
        # If we have 120 requests in the last minute, wait
        if len(self._request_times) >= self.RATE_LIMIT_REQUESTS_PER_MINUTE:
            # Wait until the oldest request is more than 1 minute old
            oldest_request = self._request_times[0]
            wait_time = 60.0 - (now - oldest_request) + 0.1  # Add small buffer
            if wait_time > 0:
                self.logger.info(f"Rate limit: waiting {wait_time:.1f} seconds...")
                time.sleep(wait_time)
                # Clean up again after waiting
                now = time.time()
                cutoff_time = now - 60.0
                while self._request_times and self._request_times[0] < cutoff_time:
                    self._request_times.popleft()
        
        # Ensure minimum time between requests (0.5 seconds)
        time_since_last = now - self._last_request_time
        if time_since_last < self.RATE_LIMIT_SECONDS_BETWEEN_REQUESTS:
            wait_time = self.RATE_LIMIT_SECONDS_BETWEEN_REQUESTS - time_since_last
            time.sleep(wait_time)
            now = time.time()
        
        # Record this request
        self._request_times.append(now)
        self._last_request_time = now
    
    def _handle_429_error(self, attempt: int, max_retries: int = 3) -> float:
        """
        Handle 429 (Too Many Requests) error with exponential backoff.
        
        Args:
            attempt: Current retry attempt number (1-indexed)
            max_retries: Maximum number of retries
        
        Returns:
            Wait time in seconds
        """
        # Exponential backoff: 60 seconds base, doubled each attempt
        wait_time = 60.0 * (2 ** (attempt - 1))
        self.logger.warning(
            f"Rate limit exceeded (429). Waiting {wait_time:.1f} seconds before retry {attempt}/{max_retries}..."
        )
        return wait_time
    
    def _get_api_params(self, **kwargs) -> Dict[str, Any]:
        """
        Get base API parameters including API key.
        
        Args:
            **kwargs: Additional parameters to include
        
        Returns:
            Dictionary of API parameters
        """
        if not self.api_key:
            raise ValueError("FRED_API_KEY is required for FRED API access")
        
        params = {
            "api_key": self.api_key,
            "file_type": "json",
        }
        params.update(kwargs)
        return params
    
    def get_series_info(self, series_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get series information/metadata.
        
        Args:
            series_id: Series ID (uses config if not provided)
        
        Returns:
            Series information dictionary
        """
        series_id = series_id or self.series_id
        if not series_id:
            raise ValueError("Series ID is required")
        
        self._rate_limit()
        
        url = f"{self.API_BASE}/series"
        params = self._get_api_params(series_id=series_id)
        
        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            
            # Handle rate limiting
            if response.status_code == 429:
                wait_time = self._handle_429_error(1)
                time.sleep(wait_time)
                response = requests.get(url, params=params, timeout=self.timeout)
            
            response.raise_for_status()
            data = response.json()
            
            # FRED returns series in a list
            if "seriess" in data and len(data["seriess"]) > 0:
                return data["seriess"][0]
            elif "series" in data:
                return data["series"]
            else:
                raise ValueError(f"No series found for ID: {series_id}")
                
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch series info for {series_id}: {e}")
            raise
    
    def get_observations(
        self,
        series_id: Optional[str] = None,
        limit: Optional[int] = None,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        sort_order: str = "desc",
    ) -> List[Dict[str, Any]]:
        """
        Get series observations (time series data).
        
        Args:
            series_id: Series ID (uses config if not provided)
            limit: Maximum number of observations to return
            observation_start: Start date (YYYY-MM-DD format)
            observation_end: End date (YYYY-MM-DD format)
            sort_order: "asc" or "desc" (default: "desc" for latest first)
        
        Returns:
            List of observation dictionaries
        """
        series_id = series_id or self.series_id
        if not series_id:
            raise ValueError("Series ID is required")
        
        self._rate_limit()
        
        url = f"{self.API_BASE}/series/observations"
        params = self._get_api_params(
            series_id=series_id,
            sort_order=sort_order,
        )
        
        if limit:
            params["limit"] = limit
        if observation_start:
            params["observation_start"] = observation_start
        if observation_end:
            params["observation_end"] = observation_end
        
        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            
            # Handle rate limiting with retries
            attempt = 1
            max_retries = 3
            while response.status_code == 429 and attempt <= max_retries:
                wait_time = self._handle_429_error(attempt, max_retries)
                time.sleep(wait_time)
                response = requests.get(url, params=params, timeout=self.timeout)
                attempt += 1
            
            if response.status_code == 429:
                raise requests.HTTPError("Rate limit exceeded after retries")
            
            response.raise_for_status()
            data = response.json()
            
            if "observations" in data:
                return data["observations"]
            else:
                self.logger.warning(f"No observations found in response for {series_id}")
                return []
                
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch observations for {series_id}: {e}")
            raise
    
    def get_latest_observation(self, series_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get the most recent observation for a series.
        
        Args:
            series_id: Series ID (uses config if not provided)
        
        Returns:
            Latest observation dictionary or None
        """
        observations = self.get_observations(series_id=series_id, limit=1, sort_order="desc")
        if observations:
            return observations[0]
        return None
    
    def fetch_raw(self, url: str) -> Dict[str, Any]:
        """
        Fetch raw data from FRED API.
        
        Args:
            url: Not used directly, series_id comes from config
        
        Returns:
            Dict with observations and metadata
        """
        if not self.config:
            raise ValueError("Site configuration required for FRED scraper")
        
        if not self.api_key:
            raise ValueError("FRED_API_KEY is required for FRED API access")
        
        series_id = self.series_id
        if not series_id:
            raise ValueError("Series ID not found in configuration")
        
        # Get series info for metadata
        try:
            series_info = self.get_series_info(series_id)
        except Exception as e:
            self.logger.warning(f"Could not fetch series info: {e}")
            series_info = {}
        
        # Get observations
        # Check config for limit and date range
        limit = None
        observation_start = None
        observation_end = None
        
        if self.config.data_source and self.config.data_source.parameters:
            limit = self.config.data_source.parameters.get("limit")
            observation_start = self.config.data_source.parameters.get("observation_start")
            observation_end = self.config.data_source.parameters.get("observation_end")
        
        observations = self.get_observations(
            series_id=series_id,
            limit=limit,
            observation_start=observation_start,
            observation_end=observation_end,
        )
        
        return {
            "type": "api_json",
            "content": {
                "observations": observations,
                "series_info": series_info,
            },
            "series_id": series_id,
        }
    
    def parse_raw(self, raw_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Parse FRED API results into DataFrame.
        
        Args:
            raw_data: Raw results from fetch_raw
        
        Returns:
            Parsed DataFrame with date, value, and series information
        """
        content = raw_data.get("content", {})
        observations = content.get("observations", [])
        series_info = content.get("series_info", {})
        
        if not observations:
            self.logger.warning("No observations found in FRED response")
            return pd.DataFrame()
        
        # Extract series name from config or series info
        series_name = series_info.get("title", "")
        if self.config and self.config.field_mappings:
            # Check if series_name is mapped
            for key, value in self.config.field_mappings.items():
                if "series_name" in key.lower() or "name" in key.lower():
                    series_name = value
                    break
        
        # Convert observations to DataFrame
        rows = []
        for obs in observations:
            # FRED uses "." to indicate missing data
            value = obs.get("value")
            if value == ".":
                value = None
            else:
                try:
                    value = float(value) if value else None
                except (ValueError, TypeError):
                    value = None
            
            rows.append({
                "date": obs.get("date"),
                "value": value,
                "series_id": raw_data.get("series_id", ""),
                "series_name": series_name,
                "realtime_start": obs.get("realtime_start"),
                "realtime_end": obs.get("realtime_end"),
            })
        
        df = pd.DataFrame(rows)
        
        # Convert date column to datetime
        if "date" in df.columns and not df.empty:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            # Sort by date descending (latest first)
            df = df.sort_values("date", ascending=False).reset_index(drop=True)
        
        # Apply field mappings if available
        if self.config and self.config.field_mappings:
            rename_map = {}
            for target_col, source_col in self.config.field_mappings.items():
                # Map common field names
                if source_col.lower() in ["date", "observation_date"]:
                    if "date" in df.columns:
                        rename_map["date"] = target_col
                elif source_col.lower() in ["value", "observation_value"]:
                    if "value" in df.columns:
                        rename_map["value"] = target_col
                elif source_col in df.columns:
                    rename_map[source_col] = target_col
            
            if rename_map:
                df = df.rename(columns=rename_map)
        
        self.logger.info(f"Parsed {len(df)} observations from FRED series")
        return df

