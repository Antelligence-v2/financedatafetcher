"""
The Block scraper - site-specific implementation.
Optimized for extracting data from The Block's chart pages.
"""

import json
import requests
from typing import Dict, Any, Optional

import pandas as pd

from .base_scraper import BaseScraper, ScraperResult
from .universal_scraper import UniversalScraper
from ..utils.logger import get_logger
from ..utils.config_manager import ConfigManager, SiteConfig


class TheBlockScraper(BaseScraper):
    """
    Specialized scraper for The Block website.
    Directly calls the known API endpoint for faster extraction.
    Falls back to universal scraper if needed.
    """
    
    # Known API endpoint pattern
    API_BASE = "https://www.theblock.co/api/charts/chart"
    
    def __init__(
        self,
        config: Optional[SiteConfig] = None,
        config_manager: Optional[ConfigManager] = None,
        **kwargs
    ):
        """
        Initialize The Block scraper.
        
        Args:
            config: Site configuration (auto-loads if not provided)
            config_manager: Config manager for loading configurations
        """
        self.config_manager = config_manager or ConfigManager()
        
        # Load config if not provided
        if config is None:
            config = self.config_manager.get("theblock_btc_eth_volume_7dma")
        
        super().__init__(config=config, **kwargs)
        
        # Universal scraper as fallback
        self._universal_scraper: Optional[UniversalScraper] = None
    
    @property
    def universal_scraper(self) -> UniversalScraper:
        """Lazy-loaded universal scraper for fallback."""
        if self._universal_scraper is None:
            self._universal_scraper = UniversalScraper(
                config=self.config,
                user_agent=self.user_agent,
            )
        return self._universal_scraper
    
    def _get_api_url(self, page_url: str) -> str:
        """
        Convert a page URL to the API endpoint URL.
        
        The Block uses a pattern like:
        Page: /data/crypto-markets/spot/btc-and-eth-total-exchange-volume-7dma
        API:  /api/charts/chart/crypto-markets/spot/btc-and-eth-total-exchange-volume-7dma
        """
        # If config has explicit endpoint, use it
        if self.config and self.config.data_source.endpoint:
            return self.config.data_source.endpoint
        
        # Otherwise, try to construct from page URL
        if "/data/" in page_url:
            path = page_url.split("/data/")[1]
            return f"{self.API_BASE}/{path}"
        
        return page_url
    
    def fetch_raw(self, url: str) -> Dict[str, Any]:
        """
        Fetch raw data from The Block API.
        
        Args:
            url: Page URL or API URL
        
        Returns:
            Dict with content and metadata
        """
        # Try direct API first (bypasses Cloudflare better than browser)
        api_url = self._get_api_url(url)
        self.logger.info(f"Fetching from API: {api_url}")
        
        # Try multiple API endpoint patterns
        api_endpoints_to_try = [api_url]
        
        # Also try the discovered endpoint pattern if available
        if "/indicesHistory/" in api_url or "/api/charts/chart/" in api_url:
            # Keep the discovered endpoint
            pass
        else:
            # Try the chart API endpoint pattern
            if "/data/" in url:
                path = url.split("/data/")[1]
                chart_api = f"https://www.theblock.co/api/charts/chart/{path}"
                api_endpoints_to_try.insert(0, chart_api)
        
        for endpoint in api_endpoints_to_try:
            try:
                # Use more browser-like headers to bypass Cloudflare
                headers = {
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": url,
                    "Origin": "https://www.theblock.co",
                }
                
                response = requests.get(
                    endpoint,
                    headers=headers,
                    timeout=self.timeout,
                )
                
                if response.status_code == 200:
                    self.logger.info(f"Successfully fetched from {endpoint}")
                    return {
                        "type": "api_json",
                        "content": response.text,
                        "endpoint_url": endpoint,
                        "status_code": response.status_code,
                    }
                else:
                    self.logger.warning(
                        f"API {endpoint} returned status {response.status_code}"
                    )
            except requests.RequestException as e:
                self.logger.warning(f"API request to {endpoint} failed: {e}")
        
        # Fall back to universal scraper (browser-based) as last resort
        self.logger.info("All direct API attempts failed, using universal scraper fallback")
        try:
            return self.universal_scraper.fetch_raw(url)
        except Exception as e:
            self.logger.error(f"Universal scraper fallback also failed: {e}")
            raise ValueError(f"Could not fetch data from The Block: {e}")
    
    def parse_raw(self, raw_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Parse raw data from The Block API.
        
        Args:
            raw_data: Raw response from fetch_raw
        
        Returns:
            Parsed DataFrame
        """
        content = raw_data.get("content")
        data_type = raw_data.get("type", "api_json")
        
        if data_type != "api_json":
            # Use universal scraper for non-API data
            return self.universal_scraper.parse_raw(raw_data)
        
        try:
            if isinstance(content, str):
                json_data = json.loads(content)
            else:
                json_data = content
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON: {e}")
            return pd.DataFrame()
        
        # The Block API can return different structures:
        # Format 1: Chart API (most common)
        # {
        #   "chart": {
        #     "jsonFile": {
        #       "Series": { "Date": [...], "BTC": [...], "ETH": [...] }
        #     }
        #   }
        # }
        # Format 2: Indices History API
        # {
        #   "data": [
        #     {"date": "...", "value": ...},
        #     ...
        #   ]
        # }
        
        try:
            # Navigate to data
            if "chart" in json_data:
                # Chart API format
                chart_data = json_data["chart"]
                if "jsonFile" in chart_data:
                    series_data = chart_data["jsonFile"].get("Series", {})
                elif "Series" in chart_data:
                    series_data = chart_data["Series"]
                else:
                    series_data = chart_data
                
                # Handle Series format: 
                # Option 1: { "Date": [...], "BTC": [...], "ETH": [...] } - parallel arrays
                # Option 2: { "BTC": {"Data": [...], "YAxis": "USD", ...}, "ETH": {"Data": [...], ...} } - nested with Data arrays
                if isinstance(series_data, dict):
                    # Check if BTC/ETH are dicts with "Data" arrays containing {Timestamp, Result} objects
                    data_arrays = {}
                    timestamps = None
                    
                    for key, value in series_data.items():
                        if isinstance(value, dict) and "Data" in value:
                            # Extract the Data array: [{"Timestamp": ..., "Result": ...}, ...]
                            data_list = value["Data"]
                            if isinstance(data_list, list) and len(data_list) > 0:
                                # Extract timestamps from first entry (all should have same timestamps)
                                if timestamps is None and isinstance(data_list[0], dict) and "Timestamp" in data_list[0]:
                                    timestamps = [item.get("Timestamp", 0) for item in data_list if isinstance(item, dict)]
                                
                                # Extract Result values for this series
                                results = [item.get("Result", 0) for item in data_list if isinstance(item, dict)]
                                data_arrays[key] = results
                        elif isinstance(value, list):
                            # Direct array
                            data_arrays[key] = value
                    
                    if data_arrays:
                        # Create DataFrame with timestamps and data arrays
                        df_data = {}
                        
                        # Add date column from timestamps
                        if timestamps:
                            df_data["date"] = pd.to_datetime(timestamps, unit="s", errors="coerce")
                        
                        # Add data columns
                        for key, values in data_arrays.items():
                            # Map BTC/ETH to expected column names
                            if key == "BTC":
                                df_data["btc_volume_7dma"] = values
                            elif key == "ETH":
                                df_data["eth_volume_7dma"] = values
                            else:
                                df_data[key.lower()] = values
                        
                        # Ensure all arrays are same length
                        lengths = [len(v) for v in df_data.values() if isinstance(v, (list, pd.Series))]
                        if lengths and len(set(lengths)) == 1:
                            df = pd.DataFrame(df_data)
                        else:
                            # Different lengths - use the longest
                            max_len = max(lengths) if lengths else 0
                            df = pd.DataFrame({k: (v[:max_len] if isinstance(v, (list, pd.Series)) else v) for k, v in df_data.items()})
                        
                        # Ensure all numeric columns are actually numeric
                        for col in df.columns:
                            if col != "date" and df[col].dtype == object:
                                try:
                                    df[col] = pd.to_numeric(df[col], errors="coerce")
                                except Exception:
                                    pass
                        
                        # Sort by date if available
                        if "date" in df.columns and pd.api.types.is_datetime64_any_dtype(df["date"]):
                            try:
                                df = df.sort_values("date").reset_index(drop=True)
                            except Exception:
                                pass
                        
                        # Apply field mappings
                        if self.config and self.config.field_mappings:
                            rename_map = {v: k for k, v in self.config.field_mappings.items()}
                            df = df.rename(columns=rename_map)
                        
                        self.logger.info(f"Parsed {len(df)} rows from The Block Chart API (Data format)")
                        return df
                    else:
                        # Check if it's parallel arrays (Date, BTC, ETH, etc.)
                        list_cols = {k: v for k, v in series_data.items() if isinstance(v, list)}
                        if list_cols and len(list_cols) > 1:
                            # All arrays should be same length
                            lengths = [len(v) for v in list_cols.values()]
                            if len(set(lengths)) == 1:
                                # Create DataFrame from parallel arrays
                                df = pd.DataFrame(list_cols)
                            
                                # Rename Date column if present
                                date_col_name = None
                                for col in ["Date", "date", "Date", "timestamp"]:
                                    if col in df.columns:
                                        date_col_name = col
                                        break
                                
                                if date_col_name:
                                    df = df.rename(columns={date_col_name: "date"})
                                    try:
                                        # Try to parse dates (could be timestamps or date strings)
                                        if df["date"].dtype in ["int64", "float64"]:
                                            df["date"] = pd.to_datetime(df["date"], unit="s", errors="coerce")
                                        else:
                                            df["date"] = pd.to_datetime(df["date"], errors="coerce")
                                    except Exception:
                                        pass
                                
                                # Apply field mappings
                                if self.config and self.config.field_mappings:
                                    rename_map = {v: k for k, v in self.config.field_mappings.items()}
                                    df = df.rename(columns=rename_map)
                                
                                # Sort by date if available
                                if "date" in df.columns and pd.api.types.is_datetime64_any_dtype(df["date"]):
                                    try:
                                        df = df.sort_values("date").reset_index(drop=True)
                                    except Exception:
                                        pass
                                
                                # Ensure all numeric columns are actually numeric
                                for col in df.columns:
                                    if col != "date" and df[col].dtype == object:
                                        try:
                                            df[col] = pd.to_numeric(df[col], errors="coerce")
                                        except Exception:
                                            pass
                                
                                self.logger.info(f"Parsed {len(df)} rows from The Block Chart API")
                                return df
            elif "data" in json_data and isinstance(json_data["data"], list):
                # Indices History API format - array of objects
                df = pd.DataFrame(json_data["data"])
                # Apply field mappings if available
                if self.config and self.config.field_mappings:
                    rename_map = {v: k for k, v in self.config.field_mappings.items()}
                    df = df.rename(columns=rename_map)
                # Convert date column
                if "date" in df.columns:
                    df["date"] = pd.to_datetime(df["date"])
                elif "Date" in df.columns:
                    df["date"] = pd.to_datetime(df["Date"])
                    df = df.drop(columns=["Date"])
                # Sort by date
                if "date" in df.columns:
                    df = df.sort_values("date").reset_index(drop=True)
                self.logger.info(f"Parsed {len(df)} rows from The Block Indices API")
                return df
            elif isinstance(json_data, dict) and all(isinstance(v, list) for v in json_data.values() if isinstance(v, list)):
                # Format where columns are index IDs and values are lists of JSON strings
                # Example: {"277090": [{"timestamp": "...", "price": "..."}, ...], ...}
                # Parse each column's JSON strings
                parsed_data = {}
                date_column = None
                
                for col_name, values in json_data.items():
                    if isinstance(values, list) and len(values) > 0:
                        # Check if first value is a JSON string
                        if isinstance(values[0], str) and values[0].strip().startswith("{"):
                            # Parse JSON strings
                            parsed_values = []
                            for val_str in values:
                                try:
                                    val_dict = json.loads(val_str)
                                    parsed_values.append(val_dict)
                                except:
                                    parsed_values.append(val_str)
                            
                            # Extract timestamp and price from parsed values
                            if parsed_values and isinstance(parsed_values[0], dict):
                                # Create date column from first timestamp (only once)
                                if date_column is None and "timestamp" in parsed_values[0]:
                                    timestamps = []
                                    for v in parsed_values:
                                        if isinstance(v, dict) and "timestamp" in v:
                                            try:
                                                ts = int(v["timestamp"])
                                                timestamps.append(ts)
                                            except (ValueError, TypeError):
                                                timestamps.append(0)
                                        else:
                                            timestamps.append(0)
                                    if timestamps:
                                        date_column = pd.to_datetime(timestamps, unit="s", errors="coerce")
                                
                                # Extract prices for this column (ensure they're simple floats, not dicts)
                                prices = []
                                for v in parsed_values:
                                    if isinstance(v, dict):
                                        try:
                                            price = float(v.get("price", 0))
                                            prices.append(price)
                                        except (ValueError, TypeError):
                                            prices.append(0.0)
                                    elif isinstance(v, (int, float)):
                                        prices.append(float(v))
                                    else:
                                        prices.append(0.0)
                                parsed_data[col_name] = prices
                        else:
                            # Already parsed or not JSON - ensure it's a simple list
                            # Convert any nested structures to strings
                            simple_values = []
                            for v in values:
                                if isinstance(v, (dict, list)):
                                    simple_values.append(json.dumps(v))
                                else:
                                    simple_values.append(v)
                            parsed_data[col_name] = simple_values
                
                if parsed_data:
                    df = pd.DataFrame(parsed_data)
                    if date_column is not None and len(date_column) == len(df):
                        df.insert(0, "date", date_column)
                    self.logger.info(f"Parsed {len(df)} rows from The Block Indices API (parsed JSON strings)")
                    return df
            else:
                series_data = json_data
            
            # Check for parallel arrays structure
            if isinstance(series_data, dict):
                # Check if all values are lists of same length
                list_values = {k: v for k, v in series_data.items() if isinstance(v, list)}
                
                if list_values:
                    lengths = [len(v) for v in list_values.values()]
                    if len(set(lengths)) == 1:
                        df = pd.DataFrame(list_values)
                    else:
                        # Different lengths, try first list that looks like data
                        for key, value in list_values.items():
                            if len(value) > 10 and isinstance(value[0], dict):
                                df = pd.DataFrame(value)
                                break
                        else:
                            df = pd.DataFrame(list_values)
                else:
                    df = pd.DataFrame([series_data])
            elif isinstance(series_data, list):
                df = pd.DataFrame(series_data)
            else:
                self.logger.error(f"Unexpected data structure: {type(series_data)}")
                return pd.DataFrame()
            
            # Apply field mappings from config
            if self.config and self.config.field_mappings:
                rename_map = {v: k for k, v in self.config.field_mappings.items()}
                df = df.rename(columns=rename_map)
            
            # Convert date column
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"])
            elif "Date" in df.columns:
                df["date"] = pd.to_datetime(df["Date"])
                df = df.drop(columns=["Date"])
            
            # Sort by date
            if "date" in df.columns:
                df = df.sort_values("date").reset_index(drop=True)
            
            self.logger.info(f"Parsed {len(df)} rows from The Block API")
            return df
            
        except Exception as e:
            self.logger.error(f"Error parsing The Block data: {e}")
            return pd.DataFrame()
    
    def validate(self, df: pd.DataFrame) -> list:
        """
        Validate The Block data with site-specific checks.
        """
        warnings = super().validate(df)
        
        # Check for expected columns
        expected_cols = ["date", "btc_volume_7dma", "eth_volume_7dma"]
        for col in expected_cols:
            alt_names = [col, col.upper(), col.replace("_", " ")]
            if not any(name in df.columns for name in alt_names):
                # Check for similar columns
                similar = [c for c in df.columns if col.split("_")[0] in c.lower()]
                if similar:
                    warnings.append(f"Expected column '{col}' not found, but found similar: {similar}")
        
        # Check that volume values are reasonable (in billions)
        volume_cols = [c for c in df.columns if "volume" in c.lower() or "btc" in c.lower() or "eth" in c.lower()]
        for col in volume_cols:
            if col in df.columns:
                max_val = df[col].max()
                if max_val > 1e15:  # > 1 quadrillion seems wrong
                    warnings.append(f"Column '{col}' has suspiciously large values (max: {max_val})")
        
        return warnings


def scrape_theblock_volume(
    override_robots: bool = False,
) -> ScraperResult:
    """
    Convenience function to scrape The Block BTC/ETH volume data.
    
    Args:
        override_robots: Override robots.txt for UNKNOWN status
    
    Returns:
        ScraperResult with extracted data
    """
    scraper = TheBlockScraper()
    return scraper.scrape(override_robots=override_robots)

