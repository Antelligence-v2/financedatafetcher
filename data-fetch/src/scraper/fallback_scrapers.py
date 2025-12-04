"""
Fallback scrapers for alternative data sources.
CoinGecko, CoinDesk (formerly CryptoCompare), and other public APIs.
"""

import os
import json
import requests
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta

import pandas as pd

from .base_scraper import BaseScraper, ScraperResult
from ..utils.logger import get_logger
from ..utils.config_manager import ConfigManager, SiteConfig


class CoinGeckoScraper(BaseScraper):
    """
    Scraper for CoinGecko's public API.
    Provides exchange volume and market data.
    Supports both Demo and Pro API tiers.
    """
    
    # CoinGecko API endpoints
    API_BASE_FREE = "https://api.coingecko.com/api/v3"
    API_BASE_PRO = "https://pro-api.coingecko.com/api/v3"
    
    def __init__(
        self,
        config: Optional[SiteConfig] = None,
        api_key: Optional[str] = None,
        use_pro_api: Optional[bool] = None,
        **kwargs
    ):
        """
        Initialize CoinGecko scraper.
        
        Args:
            config: Site configuration
            api_key: Optional API key (auto-loaded from env if not provided)
            use_pro_api: Force Pro API usage (auto-detected if None)
        """
        super().__init__(config=config, **kwargs)
        self.api_key = api_key or os.getenv("COINGECKO_API_KEY")
        
        # Auto-detect Pro API usage
        if use_pro_api is None:
            # Check for explicit env var or detect from key/endpoint
            use_pro_api = os.getenv("COINGECKO_USE_PRO", "").lower() in ("true", "1", "yes")
            if not use_pro_api and self.api_key:
                # Demo keys start with "CG-", Pro keys are longer or contain "pro"
                # Demo format: CG-7LNeSZUuK1MsPJ21DwZ6kug9
                if self.api_key.startswith("CG-"):
                    use_pro_api = False  # Explicitly demo key
                else:
                    use_pro_api = len(self.api_key) > 50 or "pro" in self.api_key.lower()
        
        self.use_pro_api = use_pro_api
        self.api_base = self.API_BASE_PRO if self.use_pro_api else self.API_BASE_FREE
        
        # Rate limiting (Pro API has higher limits)
        self._last_request_time: Optional[datetime] = None
        self._min_request_interval = 0.5 if self.use_pro_api else 1.0  # seconds
    
    def _get_headers(self) -> dict:
        """Get request headers."""
        headers = {
            "User-Agent": self.user_agent,
            "Accept": "application/json",
        }
        if self.api_key:
            # CoinGecko uses different header names based on plan type
            if self.use_pro_api:
                headers["x-cg-pro-api-key"] = self.api_key
            else:
                headers["x-cg-demo-api-key"] = self.api_key
        return headers
    
    def _rate_limit(self):
        """Apply rate limiting."""
        import time
        if self._last_request_time:
            elapsed = (datetime.now() - self._last_request_time).total_seconds()
            if elapsed < self._min_request_interval:
                time.sleep(self._min_request_interval - elapsed)
        self._last_request_time = datetime.now()
    
    def fetch_raw(self, url: str) -> Dict[str, Any]:
        """Fetch data from CoinGecko API."""
        self._rate_limit()
        
        # Determine endpoint
        if "market_chart" in url or "simple/price" in url:
            endpoint = url
            # Ensure required parameters are present for market_chart
            if "market_chart" in endpoint and "vs_currency" not in endpoint:
                separator = "&" if "?" in endpoint else "?"
                endpoint = f"{endpoint}{separator}vs_currency=usd&days=30"
        elif self.config and self.config.data_source.endpoint:
            endpoint = self.config.data_source.endpoint
            # Ensure required parameters for market_chart
            if "market_chart" in endpoint and "vs_currency" not in endpoint:
                separator = "&" if "?" in endpoint else "?"
                endpoint = f"{endpoint}{separator}vs_currency=usd&days=30"
        else:
            # Default to BTC market chart
            endpoint = f"{self.api_base}/coins/bitcoin/market_chart?vs_currency=usd&days=30"
        
        # Ensure we're using the correct API base URL
        if endpoint.startswith("https://api.coingecko.com") and self.use_pro_api:
            endpoint = endpoint.replace("https://api.coingecko.com", self.API_BASE_PRO)
        elif endpoint.startswith("https://pro-api.coingecko.com") and not self.use_pro_api:
            endpoint = endpoint.replace("https://pro-api.coingecko.com", self.API_BASE_FREE)
        
        # Add API key as query parameter (CoinGecko supports both header and query param)
        # Query param uses underscores: x_cg_demo_api_key or x_cg_pro_api_key
        if self.api_key:
            from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
            parsed = urlparse(endpoint)
            query_params = parse_qs(parsed.query)
            
            # Add API key as query parameter (CoinGecko's preferred method for demo keys)
            if self.use_pro_api:
                query_params["x_cg_pro_api_key"] = [self.api_key]
            else:
                query_params["x_cg_demo_api_key"] = [self.api_key]
            
            # Rebuild URL with query params
            new_query = urlencode(query_params, doseq=True)
            endpoint = urlunparse((
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                new_query,
                parsed.fragment
            ))
        
        api_tier = "Pro" if self.use_pro_api else "Free/Demo"
        self.logger.info(f"Fetching from CoinGecko ({api_tier}): {endpoint[:100]}...")
        
        # Use headers as well (CoinGecko accepts both methods)
        response = requests.get(
            endpoint,
            headers=self._get_headers(),
            timeout=self.timeout,
        )
        
        response.raise_for_status()
        
        return {
            "type": "api_json",
            "content": response.text,
            "endpoint_url": endpoint.split("?")[0] if "?" in endpoint else endpoint,  # Don't log full URL with key
            "status_code": response.status_code,
        }
    
    def parse_raw(self, raw_data: Dict[str, Any]) -> pd.DataFrame:
        """Parse CoinGecko API response."""
        content = raw_data.get("content")
        
        try:
            json_data = json.loads(content) if isinstance(content, str) else content
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON: {e}")
            return pd.DataFrame()
        
        # CoinGecko simple/price format:
        # { "bitcoin": { "usd": 92412 }, "ethereum": { "usd": 2500 } }
        if isinstance(json_data, dict) and not any(k in json_data for k in ["prices", "total_volumes", "market_caps"]):
            # Simple price format - convert to DataFrame with current timestamp
            data = []
            current_time = datetime.now()
            for coin_id, prices in json_data.items():
                row = {
                    "date": current_time,
                    "coin_id": coin_id,
                }
                row.update(prices)  # Add all currency prices (usd, eur, etc.)
                data.append(row)
            df = pd.DataFrame(data)
        
        # CoinGecko market_chart format:
        # { "prices": [[timestamp, price], ...], "total_volumes": [[timestamp, volume], ...] }
        elif "prices" in json_data:
            prices = json_data.get("prices", [])
            volumes = json_data.get("total_volumes", [])
            market_caps = json_data.get("market_caps", [])
            
            data = []
            for i, (ts, price) in enumerate(prices):
                row = {
                    "date": pd.to_datetime(ts, unit="ms"),
                    "price": price,
                }
                if i < len(volumes):
                    row["volume"] = volumes[i][1]
                if i < len(market_caps):
                    row["market_cap"] = market_caps[i][1]
                data.append(row)
            
            df = pd.DataFrame(data)
        
        elif isinstance(json_data, list):
            # Exchange list format
            df = pd.DataFrame(json_data)
        
        else:
            df = pd.DataFrame([json_data])
        
        # Sort by date if present
        if "date" in df.columns:
            df = df.sort_values("date").reset_index(drop=True)
        
        self.logger.info(f"Parsed {len(df)} rows from CoinGecko")
        return df


class CryptoCompareScraper(BaseScraper):
    """
    Scraper for CoinDesk Data API (formerly CryptoCompare).
    Provides historical price and volume data.
    Note: CryptoCompare API is now maintained by CoinDesk.
    """
    
    # CoinDesk Data API base URL
    API_BASE = "https://api.coindesk.com/v1"
    # Legacy CryptoCompare endpoint (still works but deprecated)
    LEGACY_API_BASE = "https://min-api.cryptocompare.com/data"
    
    def __init__(
        self,
        config: Optional[SiteConfig] = None,
        api_key: Optional[str] = None,
        use_coindesk: bool = True,
        **kwargs
    ):
        """
        Initialize CoinDesk/CryptoCompare scraper.
        
        Args:
            config: Site configuration
            api_key: Optional API key (auto-loaded from env if not provided)
            use_coindesk: Use CoinDesk API (True) or legacy CryptoCompare (False)
        """
        super().__init__(config=config, **kwargs)
        # Auto-detect API key from environment
        if not api_key:
            api_key = os.getenv("COINDESK_API_KEY") or os.getenv("CRYPTOCOMPARE_API_KEY")
        self.api_key = api_key
        self.use_coindesk = use_coindesk
    
    def _get_headers(self) -> dict:
        """Get request headers."""
        headers = {
            "User-Agent": self.user_agent,
            "Accept": "application/json",
        }
        if self.api_key:
            if self.use_coindesk:
                # CoinDesk uses Bearer token or API key in header
                headers["Authorization"] = f"Bearer {self.api_key}"
            else:
                # Legacy CryptoCompare format
                headers["authorization"] = f"Apikey {self.api_key}"
        return headers
    
    def fetch_raw(self, url: str) -> Dict[str, Any]:
        """Fetch data from CoinDesk or CryptoCompare API."""
        # Determine endpoint
        if self.config and self.config.data_source.endpoint:
            endpoint = self.config.data_source.endpoint
        else:
            if self.use_coindesk:
                # CoinDesk API endpoint for BTC price history
                endpoint = f"{self.API_BASE}/bpi/historical/close.json?currency=USD&start=2024-01-01&end=2024-01-31"
            else:
                # Legacy CryptoCompare endpoint
                endpoint = f"{self.LEGACY_API_BASE}/v2/histoday?fsym=BTC&tsym=USD&limit=30"
        
        api_name = "CoinDesk" if self.use_coindesk else "CryptoCompare"
        self.logger.info(f"Fetching from {api_name}: {endpoint}")
        
        response = requests.get(
            endpoint,
            headers=self._get_headers(),
            timeout=self.timeout,
        )
        
        response.raise_for_status()
        
        return {
            "type": "api_json",
            "content": response.text,
            "endpoint_url": endpoint,
            "status_code": response.status_code,
        }
    
    def parse_raw(self, raw_data: Dict[str, Any]) -> pd.DataFrame:
        """Parse CoinDesk or CryptoCompare API response."""
        content = raw_data.get("content")
        
        try:
            json_data = json.loads(content) if isinstance(content, str) else content
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON: {e}")
            return pd.DataFrame()
        
        if self.use_coindesk:
            # CoinDesk API format:
            # { "bpi": { "2024-01-01": 42000.5, "2024-01-02": 43000.2, ... }, "disclaimer": "...", "time": {...} }
            if "bpi" in json_data:
                bpi_data = json_data["bpi"]
                data = [{"date": date, "price": price} for date, price in bpi_data.items()]
                df = pd.DataFrame(data)
                df["date"] = pd.to_datetime(df["date"])
            else:
                # Try other CoinDesk formats
                df = pd.DataFrame([json_data])
        else:
            # Legacy CryptoCompare format:
            # { "Data": { "Data": [...] } } or { "Data": [...] }
            data = json_data
            if "Data" in data:
                data = data["Data"]
                if isinstance(data, dict) and "Data" in data:
                    data = data["Data"]
            
            if not isinstance(data, list):
                data = [data]
            
            df = pd.DataFrame(data)
            
            # Convert timestamp to datetime
            if "time" in df.columns:
                df["date"] = pd.to_datetime(df["time"], unit="s")
            
            # Rename columns to standard names
            column_map = {
                "volumeto": "volume",
                "volumefrom": "volume_from",
                "close": "price",
                "high": "high",
                "low": "low",
                "open": "open",
            }
            df = df.rename(columns=column_map)
        
        # Sort by date
        if "date" in df.columns:
            df = df.sort_values("date").reset_index(drop=True)
        
        api_name = "CoinDesk" if self.use_coindesk else "CryptoCompare"
        self.logger.info(f"Parsed {len(df)} rows from {api_name}")
        return df


class FallbackManager:
    """
    Manager for fallback data sources.
    Tries multiple sources in order until one succeeds.
    """
    
    def __init__(
        self,
        config_manager: Optional[ConfigManager] = None,
    ):
        """
        Initialize the fallback manager.
        
        Args:
            config_manager: Config manager for loading configurations
        """
        self.config_manager = config_manager or ConfigManager()
        self.logger = get_logger()
        
        # Default fallback order
        self.fallback_order = [
            ("coingecko_btc_market_chart", CoinGeckoScraper),
            ("coindesk_btc_price_history", CryptoCompareScraper),  # CoinDesk (preferred)
            ("cryptocompare_exchange_volume", CryptoCompareScraper),  # Legacy CryptoCompare
            ("coingecko_exchange_volume", CoinGeckoScraper),
        ]
    
    def scrape_with_fallbacks(
        self,
        primary_scraper: BaseScraper,
        override_robots: bool = False,
    ) -> ScraperResult:
        """
        Try primary scraper, then fallbacks if it fails.
        
        Args:
            primary_scraper: Primary scraper to try first
            override_robots: Override robots.txt
        
        Returns:
            ScraperResult from first successful source
        """
        sources_tried = []
        
        # Try primary first
        try:
            result = primary_scraper.scrape(override_robots=override_robots)
            if result.success:
                return result
            sources_tried.append((primary_scraper.site_id, result.error))
        except Exception as e:
            self.logger.warning(f"Primary scraper failed: {e}")
            sources_tried.append((primary_scraper.site_id, str(e)))
        
        # Try fallbacks
        for site_id, scraper_class in self.fallback_order:
            self.logger.info(f"Trying fallback: {site_id}")
            
            try:
                config = self.config_manager.get(site_id)
                scraper = scraper_class(config=config)
                
                result = scraper.scrape(override_robots=override_robots)
                
                if result.success:
                    result.metadata["fallback_sources_tried"] = sources_tried
                    return result
                
                sources_tried.append((site_id, result.error))
                
            except Exception as e:
                self.logger.warning(f"Fallback {site_id} failed: {e}")
                sources_tried.append((site_id, str(e)))
        
        # All failed
        return ScraperResult(
            success=False,
            error=f"All sources failed: {sources_tried}",
            metadata={"sources_tried": sources_tried},
        )


def get_fallback_scraper(site_id: str) -> Optional[BaseScraper]:
    """
    Get a fallback scraper by site ID.
    
    Args:
        site_id: Site identifier
    
    Returns:
        Scraper instance or None
    """
    import os
    config_manager = ConfigManager()
    config = config_manager.get(site_id)
    
    if site_id.startswith("coingecko"):
        api_key = os.getenv("COINGECKO_API_KEY")
        return CoinGeckoScraper(config=config, api_key=api_key)
    elif site_id.startswith("coindesk"):
        # Use CoinDesk API
        api_key = os.getenv("COINDESK_API_KEY") or os.getenv("CRYPTOCOMPARE_API_KEY")
        return CryptoCompareScraper(config=config, api_key=api_key, use_coindesk=True)
    elif site_id.startswith("cryptocompare"):
        # Legacy CryptoCompare
        api_key = os.getenv("CRYPTOCOMPARE_API_KEY") or os.getenv("COINDESK_API_KEY")
        return CryptoCompareScraper(config=config, api_key=api_key, use_coindesk=False)
    
    return None

