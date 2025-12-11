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
from ..utils.io_utils import generate_run_id


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
        # Check for error responses first
        if isinstance(json_data, dict) and "error" in json_data:
            self.logger.error(f"CoinGecko API error: {json_data.get('error', 'Unknown error')}")
            return pd.DataFrame()
        
        if isinstance(json_data, dict) and not any(k in json_data for k in ["prices", "total_volumes", "market_caps"]):
            # Simple price format - convert to DataFrame with current timestamp
            data = []
            current_time = datetime.now()
            for coin_id, prices in json_data.items():
                if isinstance(prices, dict):
                    row = {
                        "date": current_time,
                        "coin_id": coin_id,
                    }
                    row.update(prices)  # Add all currency prices (usd, eur, etc.)
                    data.append(row)
                else:
                    # Handle unexpected format
                    self.logger.warning(f"Unexpected price format for {coin_id}: {type(prices)}")
            df = pd.DataFrame(data) if data else pd.DataFrame()
        
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
            # Update date parameters if they're in the endpoint and outdated
            if self.use_coindesk and "start=" in endpoint and "end=" in endpoint:
                from datetime import datetime, timedelta
                today = datetime.now()
                # Update to last 30 days if dates are old
                if "2024-01" in endpoint or "2023" in endpoint:
                    end_date = today.strftime("%Y-%m-%d")
                    start_date = (today - timedelta(days=30)).strftime("%Y-%m-%d")
                    # Replace date parameters
                    import re
                    endpoint = re.sub(r"start=[^&]+", f"start={start_date}", endpoint)
                    endpoint = re.sub(r"end=[^&]+", f"end={end_date}", endpoint)
        else:
            if self.use_coindesk:
                # CoinDesk API endpoint for BTC price history - use current dates
                from datetime import datetime, timedelta
                today = datetime.now()
                end_date = today.strftime("%Y-%m-%d")
                start_date = (today - timedelta(days=30)).strftime("%Y-%m-%d")
                endpoint = f"{self.API_BASE}/bpi/historical/close.json?currency=USD&start={start_date}&end={end_date}"
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
            # Legacy CryptoCompare format - handle multiple response structures
            # Exchange volume format: { "Response": "Success", "Data": [...] }
            # Historical format: { "Data": { "Data": [...] } } or { "Data": [...] }
            
            # Check for Response field (exchange volume API)
            if "Response" in json_data and "Data" in json_data:
                # Exchange volume format - Data is a list of objects
                data = json_data["Data"]
                if isinstance(data, list) and len(data) > 0:
                    # Flatten nested objects in the data
                    flattened_data = []
                    for item in data:
                        if isinstance(item, dict):
                            flat_item = {}
                            for key, value in item.items():
                                # Handle nested objects/dicts
                                if isinstance(value, dict):
                                    # Flatten nested dict by prefixing keys
                                    for nested_key, nested_value in value.items():
                                        flat_item[f"{key}_{nested_key}"] = nested_value
                                elif isinstance(value, list):
                                    # Convert lists to string representation or first element
                                    if len(value) > 0 and isinstance(value[0], (int, float)):
                                        flat_item[key] = value[0] if len(value) == 1 else str(value)
                                    else:
                                        flat_item[key] = str(value)
                                else:
                                    flat_item[key] = value
                            flattened_data.append(flat_item)
                        else:
                            flattened_data.append(item)
                    df = pd.DataFrame(flattened_data)
                else:
                    df = pd.DataFrame([data] if not isinstance(data, list) else data)
            else:
                # Historical data format
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
            elif "TimeFrom" in df.columns:
                df["date"] = pd.to_datetime(df["TimeFrom"], unit="s")
            
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
            
            # Clean up object columns that might contain [object Object]
            for col in df.columns:
                if df[col].dtype == object:
                    # Check if column contains string representations of objects
                    sample = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else None
                    if sample and isinstance(sample, str) and "[object" in sample.lower():
                        # Try to extract meaningful data or remove the column
                        self.logger.warning(f"Column '{col}' contains object representations, removing")
                        df = df.drop(columns=[col])
        
        # Sort by date
        if "date" in df.columns:
            df = df.sort_values("date").reset_index(drop=True)
        
        api_name = "CoinDesk" if self.use_coindesk else "CryptoCompare"
        self.logger.info(f"Parsed {len(df)} rows from {api_name}")
        return df


class AlphaVantageScraper(BaseScraper):
    """
    Scraper for Alpha Vantage API.
    Provides company overview and stock market data.
    Supports free tier (5 calls/minute, 500/day).
    """
    
    # Alpha Vantage API base URL
    API_BASE = "https://www.alphavantage.co/query"
    
    # Top 20 stocks by market cap (predefined list)
    TOP_20_STOCKS = [
        "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK.B", 
        "V", "UNH", "JNJ", "WMT", "XOM", "JPM", "MA", "PG", "HD", "CVX", 
        "ABBV", "AVGO"
    ]
    
    def __init__(
        self,
        config: Optional[SiteConfig] = None,
        api_key: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize Alpha Vantage scraper.
        
        Args:
            config: Site configuration
            api_key: Optional API key (auto-loaded from env if not provided)
        """
        super().__init__(config=config, **kwargs)
        
        # Try to get API key from parameter, env var, or Streamlit secrets
        if api_key:
            self.api_key = api_key
        else:
            self.api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
            
            # Try Streamlit secrets if available (when running in Streamlit)
            if not self.api_key:
                try:
                    import streamlit as st
                    self.api_key = st.secrets.get("ALPHA_VANTAGE_API_KEY", None)
                except (ImportError, AttributeError, FileNotFoundError, TypeError):
                    # Streamlit not available or secrets not configured
                    pass
        
        if not self.api_key:
            self.logger.warning("ALPHA_VANTAGE_API_KEY not found in environment or Streamlit secrets")
        
        # Rate limiting (free tier: 5 calls/minute)
        self._last_request_time: Optional[datetime] = None
        self._min_request_interval = 12.0  # seconds (60/5 = 12 seconds between calls)
        self._request_times: List[datetime] = []  # Track last 5 requests
    
    def _rate_limit(self):
        """Apply rate limiting (5 calls per minute for free tier)."""
        import time
        now = datetime.now()
        
        # Remove requests older than 1 minute
        self._request_times = [
            req_time for req_time in self._request_times
            if (now - req_time).total_seconds() < 60
        ]
        
        # If we have 5 requests in the last minute, wait
        if len(self._request_times) >= 5:
            # Wait until the oldest request is more than 1 minute old
            oldest_request = min(self._request_times)
            wait_time = 60 - (now - oldest_request).total_seconds() + 1
            if wait_time > 0:
                self.logger.info(f"Rate limit: waiting {wait_time:.1f} seconds...")
                time.sleep(wait_time)
                # Clean up again after waiting
                now = datetime.now()
                self._request_times = [
                    req_time for req_time in self._request_times
                    if (now - req_time).total_seconds() < 60
                ]
        
        # Record this request
        self._request_times.append(datetime.now())
        self._last_request_time = now
    
    def _get_headers(self) -> dict:
        """Get request headers."""
        return {
            "User-Agent": self.user_agent,
            "Accept": "application/json",
        }
    
    def fetch_raw(self, url: str) -> Dict[str, Any]:
        """Fetch data from Alpha Vantage API."""
        if not self.api_key:
            # Try one more time to get from environment (in case .env was loaded after scraper init)
            self.api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
            if not self.api_key:
                # Try Streamlit secrets one more time
                try:
                    import streamlit as st
                    self.api_key = st.secrets.get("ALPHA_VANTAGE_API_KEY", None)
                except (ImportError, AttributeError, FileNotFoundError, TypeError):
                    pass
            
            if not self.api_key:
                raise ValueError(
                    "ALPHA_VANTAGE_API_KEY is required. "
                    "Please set it in your .env file or Streamlit secrets. "
                    "Get a free API key from https://www.alphavantage.co/support/#api-key"
                )
        
        self._rate_limit()
        
        # Determine endpoint
        if self.config and self.config.data_source.endpoint:
            endpoint = self.config.data_source.endpoint
        else:
            # Default to company overview for a single symbol
            # Extract symbol from URL if present, otherwise use AAPL as default
            symbol = "AAPL"
            if "symbol=" in url:
                from urllib.parse import urlparse, parse_qs
                parsed = urlparse(url)
                params = parse_qs(parsed.query)
                if "symbol" in params:
                    symbol = params["symbol"][0]
            
            endpoint = f"{self.API_BASE}?function=OVERVIEW&symbol={symbol}&apikey={self.api_key}"
        
        # Ensure API key is in the endpoint
        if "apikey=" not in endpoint:
            separator = "&" if "?" in endpoint else "?"
            endpoint = f"{endpoint}{separator}apikey={self.api_key}"
        
        self.logger.info(f"Fetching from Alpha Vantage: {endpoint.split('apikey=')[0]}...")
        
        response = requests.get(
            endpoint,
            headers=self._get_headers(),
            timeout=self.timeout,
        )
        
        response.raise_for_status()
        json_data = response.json()
        
        # Check for API errors
        if "Error Message" in json_data:
            error_msg = json_data["Error Message"]
            self.logger.error(f"Alpha Vantage API error: {error_msg}")
            raise ValueError(f"Alpha Vantage API error: {error_msg}")
        
        if "Note" in json_data:
            # Rate limit message
            note = json_data["Note"]
            self.logger.warning(f"Alpha Vantage rate limit: {note}")
            raise ValueError(f"Rate limit exceeded: {note}")
        
        return {
            "type": "api_json",
            "content": response.text,
            "endpoint_url": endpoint.split("apikey=")[0] if "apikey=" in endpoint else endpoint,
            "status_code": response.status_code,
            "json_data": json_data,  # Include parsed JSON for convenience
        }
    
    def parse_raw(self, raw_data: Dict[str, Any]) -> pd.DataFrame:
        """Parse Alpha Vantage API response."""
        # Use pre-parsed JSON if available, otherwise parse from content
        if "json_data" in raw_data:
            json_data = raw_data["json_data"]
        else:
            content = raw_data.get("content")
            try:
                json_data = json.loads(content) if isinstance(content, str) else content
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to parse JSON: {e}")
                return pd.DataFrame()
        
        # Check for errors
        if "Error Message" in json_data:
            self.logger.error(f"API error: {json_data['Error Message']}")
            return pd.DataFrame()
        
        if "Note" in json_data:
            self.logger.warning(f"Rate limit: {json_data['Note']}")
            return pd.DataFrame()
        
        # Company Overview format: single object with all company fields
        # Convert to single-row DataFrame
        if isinstance(json_data, dict) and "Symbol" in json_data:
            # Single company overview
            df = pd.DataFrame([json_data])
        elif isinstance(json_data, list):
            # List of companies (for top stocks)
            df = pd.DataFrame(json_data)
        else:
            # Try to convert to DataFrame
            df = pd.DataFrame([json_data])
        
        # Convert MarketCapitalization to numeric if present
        if "MarketCapitalization" in df.columns:
            # Market cap is usually a string like "1234567890"
            df["MarketCapitalization"] = pd.to_numeric(
                df["MarketCapitalization"], 
                errors="coerce"
            )
        
        # Sort by MarketCapitalization if present
        if "MarketCapitalization" in df.columns:
            df = df.sort_values("MarketCapitalization", ascending=False, na_position="last")
        
        self.logger.info(f"Parsed {len(df)} rows from Alpha Vantage")
        return df
    
    def fetch_top_stocks_by_market_cap(self) -> pd.DataFrame:
        """
        Fetch company overview for top 20 stocks by market cap.
        
        Returns:
            DataFrame with all stocks, sorted by market cap descending
        """
        if not self.api_key:
            raise ValueError("ALPHA_VANTAGE_API_KEY is required")
        
        all_data = []
        failed_symbols = []
        
        self.logger.info(f"Fetching company overview for {len(self.TOP_20_STOCKS)} stocks...")
        
        for i, symbol in enumerate(self.TOP_20_STOCKS, 1):
            try:
                self.logger.info(f"Fetching {i}/{len(self.TOP_20_STOCKS)}: {symbol}")
                
                # Build endpoint URL
                endpoint = f"{self.API_BASE}?function=OVERVIEW&symbol={symbol}&apikey={self.api_key}"
                
                # Apply rate limiting
                self._rate_limit()
                
                # Make request
                response = requests.get(
                    endpoint,
                    headers=self._get_headers(),
                    timeout=self.timeout,
                )
                
                response.raise_for_status()
                json_data = response.json()
                
                # Check for errors
                if "Error Message" in json_data:
                    error_msg = json_data["Error Message"]
                    self.logger.warning(f"Error fetching {symbol}: {error_msg}")
                    failed_symbols.append(symbol)
                    continue
                
                if "Note" in json_data:
                    note = json_data["Note"]
                    self.logger.warning(f"Rate limit for {symbol}: {note}")
                    # Wait longer and retry
                    import time
                    time.sleep(60)  # Wait 1 minute
                    # Retry once
                    response = requests.get(
                        endpoint,
                        headers=self._get_headers(),
                        timeout=self.timeout,
                    )
                    response.raise_for_status()
                    json_data = response.json()
                    if "Error Message" in json_data or "Note" in json_data:
                        failed_symbols.append(symbol)
                        continue
                
                # Add to results
                if "Symbol" in json_data:
                    all_data.append(json_data)
                else:
                    self.logger.warning(f"No data returned for {symbol}")
                    failed_symbols.append(symbol)
                
            except Exception as e:
                self.logger.error(f"Error fetching {symbol}: {e}")
                failed_symbols.append(symbol)
                continue
        
        if not all_data:
            self.logger.error("No data fetched for any stocks")
            return pd.DataFrame()
        
        # Create DataFrame
        df = pd.DataFrame(all_data)
        
        # Convert MarketCapitalization to numeric
        if "MarketCapitalization" in df.columns:
            df["MarketCapitalization"] = pd.to_numeric(
                df["MarketCapitalization"],
                errors="coerce"
            )
            # Sort by market cap descending
            df = df.sort_values("MarketCapitalization", ascending=False, na_position="last")
        
        if failed_symbols:
            self.logger.warning(f"Failed to fetch data for: {', '.join(failed_symbols)}")
        
        self.logger.info(f"Successfully fetched {len(df)} stocks")
        return df
    
    def scrape(
        self,
        url: Optional[str] = None,
        override_robots: bool = False,
        save_raw: bool = True,
    ) -> ScraperResult:
        """
        Override scrape method to handle top_20_stocks case.
        """
        # Check if this is the top_20_stocks site
        if self.site_id == "alphavantage_top_20_stocks":
            self._run_id = generate_run_id(self.site_id)
            self.logger.info(f"Fetching top 20 stocks by market cap (run_id: {self.run_id})")
            
            result = ScraperResult(
                success=False,
                source=self.site_id,
                url=url or "top_20_stocks",
                run_id=self.run_id,
            )
            
            try:
                # Skip robots.txt check for API calls
                result.robots_decision = None
                
                # Fetch top stocks
                df = self.fetch_top_stocks_by_market_cap()
                
                if df is None or df.empty:
                    result.error = "No data extracted"
                    return result
                
                # Validate
                warnings = self.validate(df)
                result.validation_warnings = warnings
                
                if warnings:
                    self.logger.warning(f"Validation warnings: {warnings}")
                
                # Set result
                result.success = True
                result.data = df
                result.rows_extracted = len(df)
                
                self.logger.info(
                    f"Scrape successful: {result.rows_extracted} rows extracted"
                )
                
            except Exception as e:
                self.logger.error(f"Scrape failed: {e}")
                result.error = str(e)
            
            return result
        else:
            # Use parent scrape method for single company overview
            return super().scrape(url, override_robots, save_raw)


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
    elif site_id.startswith("alphavantage"):
        api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        return AlphaVantageScraper(config=config, api_key=api_key)
    
    return None

