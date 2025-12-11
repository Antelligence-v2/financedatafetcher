"""
Dune Analytics scraper - multi-step API execution.
Implements execute → poll → fetch pattern for Dune queries.
Uses Dune Python SDK (dune-client) when available, falls back to manual API calls.
"""

import os
import time
import io
import requests
from typing import Dict, Any, Optional, List

import pandas as pd

# Try to import Dune SDK, fallback to manual API if not available
try:
    from dune_client.client import DuneClient
    from dune_client.query import QueryBase
    DUNE_SDK_AVAILABLE = True
except ImportError:
    DUNE_SDK_AVAILABLE = False
    DuneClient = None
    QueryBase = None

from .base_scraper import BaseScraper, ScraperResult
from ..utils.logger import get_logger
from ..utils.config_manager import SiteConfig
from ..extractor.json_extractor import JsonExtractor


class DuneScraper(BaseScraper):
    """
    Scraper for Dune Analytics API.
    Implements the 3-step process: execute query, poll status, fetch results.
    """
    
    API_BASE = "https://api.dune.com/api/v1"
    
    def __init__(
        self,
        config: Optional[SiteConfig] = None,
        api_key: Optional[str] = None,
        use_sdk: bool = True,
        **kwargs
    ):
        """
        Initialize Dune scraper.
        
        Args:
            config: Site configuration
            api_key: Dune API key (uses DUNE_API_KEY env var if not provided)
            use_sdk: Whether to use Dune SDK if available (default: True)
        """
        super().__init__(config=config, **kwargs)
        self.api_key = api_key or os.getenv("DUNE_API_KEY")
        if not self.api_key:
            self.logger.warning("DUNE_API_KEY not found in environment variables")
        self.json_extractor = JsonExtractor()
        self._execution_cache: Dict[str, str] = {}  # Cache execution IDs
        self.use_sdk = use_sdk and DUNE_SDK_AVAILABLE
        if use_sdk and not DUNE_SDK_AVAILABLE:
            self.logger.warning(
                "Dune SDK (dune-client) not available. Install with: pip install dune-client. "
                "Falling back to manual API calls."
            )
        elif self.use_sdk:
            self.logger.info("Using Dune Python SDK for query execution")
    
    def get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers for Dune API."""
        if not self.api_key:
            raise ValueError("DUNE_API_KEY is required for Dune API access")
        return {
            "x-dune-api-key": self.api_key,
            "Content-Type": "application/json",
        }
    
    def execute_query(
        self,
        query_id: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Execute a Dune query and return execution ID.
        
        Args:
            query_id: Dune query ID
            parameters: Optional query parameters
        
        Returns:
            Execution ID
        """
        if not self.api_key:
            raise ValueError("DUNE_API_KEY is required")
        
        url = f"{self.API_BASE}/query/{query_id}/execute"
        
        payload = {}
        if parameters:
            payload["query_parameters"] = parameters
        
        try:
            response = requests.post(
                url,
                headers=self.get_auth_headers(),
                json=payload,
                timeout=self.timeout,
            )
            response.raise_for_status()
            
            data = response.json()
            execution_id = data.get("execution_id")
            
            if not execution_id:
                raise ValueError(f"No execution_id in response: {data}")
            
            self.logger.info(f"Query {query_id} executed, execution_id: {execution_id}")
            return execution_id
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to execute query {query_id}: {e}")
            raise
    
    def poll_query_status(
        self,
        query_id: str,
        execution_id: str,
        max_attempts: int = 30,
        poll_interval: int = 2,
    ) -> Dict[str, Any]:
        """
        Poll query execution status until complete.
        
        Args:
            query_id: Dune query ID
            execution_id: Execution ID from execute_query
            max_attempts: Maximum number of polling attempts
            poll_interval: Seconds between polls
        
        Returns:
            Status response dict
        """
        # Use execution endpoint for status
        url = f"{self.API_BASE}/execution/{execution_id}/status"
        
        for attempt in range(max_attempts):
            try:
                response = requests.get(
                    url,
                    headers=self.get_auth_headers(),
                    params={"execution_id": execution_id},
                    timeout=self.timeout,
                )
                response.raise_for_status()
                
                data = response.json()
                state = data.get("state", "").lower()
                
                self.logger.debug(f"Query status (attempt {attempt + 1}): {state}")
                
                if state == "completed":
                    self.logger.info(f"Query {query_id} completed successfully")
                    return data
                elif state in ["failed", "cancelled"]:
                    error = data.get("error", "Unknown error")
                    raise ValueError(f"Query execution {state}: {error}")
                elif state == "pending" or state == "executing":
                    # Continue polling
                    time.sleep(poll_interval)
                else:
                    # Unknown state, continue polling
                    self.logger.warning(f"Unknown query state: {state}")
                    time.sleep(poll_interval)
                    
            except requests.RequestException as e:
                if attempt < max_attempts - 1:
                    self.logger.warning(f"Poll attempt {attempt + 1} failed: {e}, retrying...")
                    time.sleep(poll_interval)
                else:
                    raise
        
        raise TimeoutError(
            f"Query {query_id} did not complete within {max_attempts * poll_interval} seconds"
        )
    
    def fetch_query_results(
        self,
        query_id: str,
        execution_id: str,
    ) -> Dict[str, Any]:
        """
        Fetch results from a completed query execution.
        
        Args:
            query_id: Dune query ID
            execution_id: Execution ID from execute_query
        
        Returns:
            Results response dict
        """
        # Use execution endpoint for results
        url = f"{self.API_BASE}/execution/{execution_id}/results"
        
        try:
            response = requests.get(
                url,
                headers=self.get_auth_headers(),
                timeout=self.timeout,
            )
            response.raise_for_status()
            
            data = response.json()
            self.logger.info(f"Fetched results for query {query_id}")
            return data
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch results for query {query_id}: {e}")
            raise
    
    def _extract_query_id_from_url(self, url: str) -> Optional[str]:
        """
        Extract query ID from a Dune URL.
        Supports formats:
        - https://dune.com/queries/{query_id}
        - https://dune.com/{username}/{query_name} (requires page inspection)
        """
        import re
        # Try standard query URL format
        query_match = re.search(r'/queries/(\d+)', url)
        if query_match:
            return query_match.group(1)
        
        # For named queries like /underfire/eth-staking-statistics, try to fetch the page
        # and extract query ID from the HTML/JavaScript
        try:
            response = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            if response.status_code == 200:
                html = response.text
                # Look for query ID in various formats in the page
                patterns = [
                    r'"query_id":\s*(\d+)',
                    r'/queries/(\d+)',
                    r'queryId["\']?\s*[:=]\s*["\']?(\d+)',
                    r'query["\']?\s*[:=]\s*["\']?(\d+)',
                    r'query_id["\']?\s*[:=]\s*["\']?(\d+)',
                    r'"id":\s*(\d+).*"type":\s*"query"',
                ]
                for pattern in patterns:
                    matches = re.findall(pattern, html)
                    if matches:
                        query_id = matches[0]  # Take first match
                        self.logger.info(f"Extracted query ID {query_id} from page HTML using pattern: {pattern}")
                        return query_id
        except Exception as e:
            self.logger.debug(f"Could not extract query ID from page: {e}")
        
        return None
    
    def fetch_raw(self, url: str) -> Dict[str, Any]:
        """
        Fetch raw data from Dune API using SDK or 3-step process.
        
        Args:
            url: Not used directly, query_id comes from config (or can extract from URL)
        
        Returns:
            Dict with query results
        """
        if not self.config:
            raise ValueError("Site configuration required for Dune scraper")
        
        if not self.api_key:
            raise ValueError("DUNE_API_KEY is required for Dune API access")
        
        # Get query ID from config
        query_id = self.config.data_source.query_id
        
        if not query_id or query_id == "null":
            # Try to extract from endpoint URL
            endpoint = self.config.data_source.endpoint
            if endpoint and "/query/" in endpoint:
                parts = endpoint.split("/query/")
                if len(parts) > 1:
                    query_id = parts[1].split("/")[0]
        
        # Try to extract from page_url if still not found
        if (not query_id or query_id == "null") and self.config.page_url:
            extracted_id = self._extract_query_id_from_url(self.config.page_url)
            if extracted_id:
                query_id = extracted_id
                self.logger.info(f"Extracted query ID {query_id} from URL: {self.config.page_url}")
        
        if not query_id or query_id == "null":
            # Try to extract from page_url one more time
            if self.config.page_url:
                extracted_id = self._extract_query_id_from_url(self.config.page_url)
                if extracted_id:
                    query_id = extracted_id
                    self.logger.info(f"Extracted query ID {query_id} from page URL")
        
        # Check if this is the combined ETH staking config that needs multiple queries
        if self.config and self.config.id == "dune_eth_staking" and self.config.name == "Dune - ETH Staking Statistics (Combined)":
            # Fetch from both queries and combine
            return self._fetch_combined_eth_staking()
        
        if not query_id or query_id == "null":
            raise ValueError(
                "Query ID not found in configuration or is null. "
                "Please set a valid query_id in data_source.query_id. "
                "For URLs like https://dune.com/underfire/eth-staking-statistics, "
                "you can find the query ID by:\n"
                "1. Opening the page in a browser\n"
                "2. Looking for a 'View Query' or 'Edit Query' button/link\n"
                "3. The URL will contain /queries/{query_id}\n"
                "4. Or inspect the page source and search for 'query_id'"
            )
        
        # Try to fetch latest results first (fastest, no execution needed)
        try:
            return self._fetch_latest_results_csv(query_id)
        except Exception as e:
            self.logger.info(f"Latest results not available, trying SDK: {e}")
        
        # Try SDK if available
        if self.use_sdk:
            try:
                return self._fetch_with_sdk(query_id)
            except Exception as e:
                self.logger.warning(
                    f"Dune SDK failed: {e}. Falling back to manual API calls."
                )
        
        # Fallback to manual API calls (execute → poll → fetch)
        return self._fetch_with_manual_api(query_id)
    
    def _fetch_combined_eth_staking(self) -> Dict[str, Any]:
        """
        Fetch ETH staking data from multiple queries and combine results.
        Query 2361452: Total ETH Deposited
        Query 2361448: Total Validators and Distinct Depositor Addresses
        """
        combined_data = {}
        
        # Fetch from query 2361452 (Total ETH Deposited)
        try:
            result_2361452 = self._fetch_single_query("2361452")
            if result_2361452 and result_2361452.get("content"):
                rows = result_2361452["content"].get("rows", [])
                if rows:
                    # Extract total ETH deposited value
                    # The query might return a single row with the total, or multiple rows
                    for row in rows:
                        # Look for columns that might contain the total ETH value
                        for key, value in row.items():
                            key_lower = key.lower()
                            # Check for ETH-related columns
                            if any(term in key_lower for term in ["eth", "deposited", "staked", "total"]) and isinstance(value, (int, float)):
                                if value > 0:  # Only accept positive values
                                    combined_data["total_eth_deposited"] = value
                                    self.logger.info(f"Found total_eth_deposited: {value} from column '{key}'")
                                    break
                        if "total_eth_deposited" in combined_data:
                            break
                    # If no specific column found, try to sum numeric values or take largest numeric value
                    if "total_eth_deposited" not in combined_data and rows:
                        first_row = rows[0]
                        numeric_values = []
                        for key, value in first_row.items():
                            if isinstance(value, (int, float)) and value > 0:
                                numeric_values.append((key, value))
                        if numeric_values:
                            # Take the largest value (likely the total)
                            largest = max(numeric_values, key=lambda x: x[1])
                            combined_data["total_eth_deposited"] = largest[1]
                            self.logger.info(f"Using largest numeric value for total_eth_deposited: {largest[1]} from column '{largest[0]}'")
        except Exception as e:
            self.logger.warning(f"Failed to fetch from query 2361452: {e}")
        
        # Fetch from query 2361448 (Validators and Depositors)
        try:
            result_2361448 = self._fetch_single_query("2361448")
            if result_2361448 and result_2361448.get("content"):
                rows = result_2361448["content"].get("rows", [])
                if rows:
                    # Extract validators and depositors
                    for row in rows:
                        for key, value in row.items():
                            key_lower = key.lower()
                            # Check for validator-related columns
                            if any(term in key_lower for term in ["validator", "validators", "active_validators"]) and isinstance(value, (int, float)):
                                if value > 0 and "total_validators" not in combined_data:
                                    combined_data["total_validators"] = value
                                    self.logger.info(f"Found total_validators: {value} from column '{key}'")
                            # Check for depositor-related columns
                            elif any(term in key_lower for term in ["depositor", "address", "unique", "distinct", "depositors"]) and isinstance(value, (int, float)):
                                if value > 0 and "distinct_depositor_addresses" not in combined_data:
                                    combined_data["distinct_depositor_addresses"] = value
                                    self.logger.info(f"Found distinct_depositor_addresses: {value} from column '{key}'")
                        if "total_validators" in combined_data and "distinct_depositor_addresses" in combined_data:
                            break
        except Exception as e:
            self.logger.warning(f"Failed to fetch from query 2361448: {e}")
        
        # Convert combined data to the format expected by parse_raw
        # Create a single row with all the combined metrics
        combined_row = {
            "timestamp": pd.Timestamp.now().isoformat(),
            **combined_data
        }
        
        return {
            "type": "api_json",
            "content": {
                "rows": [combined_row],
                "metadata": {
                    "row_count": 1,
                    "column_count": len(combined_row),
                }
            },
            "query_id": "2361448,2361452",
            "execution_id": "combined",
        }
    
    def _fetch_single_query(self, query_id: str) -> Optional[Dict[str, Any]]:
        """Helper method to fetch from a single query ID."""
        # Try to fetch latest results first (fastest, no execution needed)
        try:
            return self._fetch_latest_results_csv(query_id)
        except Exception as e:
            self.logger.debug(f"Latest CSV results not available for query {query_id}: {e}")
        
        # Try SDK if available
        if self.use_sdk:
            try:
                return self._fetch_with_sdk(query_id)
            except Exception as e:
                self.logger.debug(f"Dune SDK failed for query {query_id}: {e}")
        
        # Fallback to manual API calls
        try:
            return self._fetch_with_manual_api(query_id)
        except Exception as e:
            self.logger.error(f"All methods failed for query {query_id}: {e}")
            return None
    
    def _fetch_with_sdk(self, query_id: str) -> Dict[str, Any]:
        """Fetch data using Dune Python SDK."""
        try:
            dune = DuneClient(api_key=self.api_key)
            query = QueryBase(query_id=int(query_id))
            
            # Get parameters from config
            parameters = self.config.data_source.parameters or {}
            if parameters:
                # SDK expects parameters in a specific format
                query_params = {}
                for key, value in parameters.items():
                    query_params[key] = value
                # Note: SDK may handle parameters differently, adjust as needed
            
            # Try to get latest results first (faster, no execution needed)
            try:
                self.logger.info(f"Fetching latest results for Dune query {query_id} using SDK...")
                results_df = dune.run_query_dataframe(query)
                
                # Convert DataFrame to dict format expected by parse_raw
                results_data = {
                    "rows": results_df.to_dict("records"),
                    "metadata": {
                        "row_count": len(results_df),
                        "column_count": len(results_df.columns),
                    }
                }
                
                self.logger.info(f"Successfully fetched {len(results_df)} rows using SDK (latest results)")
                
                return {
                    "type": "api_json",
                    "content": results_data,
                    "query_id": query_id,
                    "execution_id": "sdk_latest",
                }
            except Exception as e:
                self.logger.warning(f"Failed to get latest results, executing query: {e}")
                # Fallback to executing query
                results_df = dune.run_query_dataframe(query)
                
                results_data = {
                    "rows": results_df.to_dict("records"),
                    "metadata": {
                        "row_count": len(results_df),
                        "column_count": len(results_df.columns),
                    }
                }
                
                self.logger.info(f"Successfully fetched {len(results_df)} rows using SDK (executed)")
                
                return {
                    "type": "api_json",
                    "content": results_data,
                    "query_id": query_id,
                    "execution_id": "sdk_execution",
                }
            
        except Exception as e:
            self.logger.error(f"Dune SDK execution failed: {e}")
            raise
    
    def _fetch_latest_results_csv(self, query_id: str) -> Dict[str, Any]:
        """
        Fetch latest query results in CSV format using the direct API endpoint.
        This is faster than executing a new query.
        Reference: https://docs.dune.com/api-reference/executions/endpoint/get-query-result-csv
        """
        url = f"{self.API_BASE}/query/{query_id}/results/csv"
        
        try:
            response = requests.get(
                url,
                headers=self.get_auth_headers(),
                timeout=self.timeout,
            )
            response.raise_for_status()
            
            # Parse CSV response
            csv_data = response.text
            df = pd.read_csv(io.StringIO(csv_data))
            
            self.logger.info(f"Fetched {len(df)} rows from latest query results (CSV)")
            
            results_data = {
                "rows": df.to_dict("records"),
                "metadata": {
                    "row_count": len(df),
                    "column_count": len(df.columns),
                }
            }
            
            return {
                "type": "api_json",
                "content": results_data,
                "query_id": query_id,
                "execution_id": "latest_csv",
            }
            
        except requests.RequestException as e:
            self.logger.warning(f"Failed to fetch latest CSV results: {e}")
            raise
    
    def _fetch_with_manual_api(self, query_id: str) -> Dict[str, Any]:
        """Fetch data using manual API calls (3-step process)."""
        # Check cache for recent execution
        parameters = self.config.data_source.parameters or {}
        cache_key = f"{query_id}_{parameters}"
        if cache_key in self._execution_cache:
            execution_id = self._execution_cache[cache_key]
            self.logger.info(f"Using cached execution_id: {execution_id}")
        else:
            # Step 1: Execute query
            execution_id = self.execute_query(query_id, parameters)
            self._execution_cache[cache_key] = execution_id
        
        # Step 2: Poll status
        max_attempts = self.config.data_source.max_poll_attempts
        poll_interval = self.config.data_source.poll_interval
        status_data = self.poll_query_status(query_id, execution_id, max_attempts, poll_interval)
        
        # Step 3: Fetch results
        results_data = self.fetch_query_results(query_id, execution_id)
        
        return {
            "type": "api_json",
            "content": results_data,
            "query_id": query_id,
            "execution_id": execution_id,
        }
    
    def parse_raw(self, raw_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Parse Dune query results into DataFrame.
        
        Args:
            raw_data: Raw results from fetch_raw
        
        Returns:
            Parsed DataFrame
        """
        results_data = raw_data.get("content", {})
        
        if not results_data:
            self.logger.error("No results data to parse")
            return pd.DataFrame()
        
        # Dune API returns results in different formats
        # Format 1: { "result": { "rows": [...], "metadata": {...} } }
        # Format 2: { "rows": [...], "metadata": {...} }
        
        rows = None
        if "result" in results_data and "rows" in results_data["result"]:
            rows = results_data["result"]["rows"]
        elif "rows" in results_data:
            rows = results_data["rows"]
        elif isinstance(results_data, list):
            rows = results_data
        
        if not rows:
            self.logger.error("No rows found in results")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(rows)
        
        # Apply field mappings if available
        if self.config and self.config.field_mappings:
            rename_map = {v: k for k, v in self.config.field_mappings.items()}
            
            # Only rename columns that exist in the DataFrame
            existing_rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
            if existing_rename_map:
                df = df.rename(columns=existing_rename_map)
                self.logger.debug(f"Renamed columns: {existing_rename_map}")
            else:
                # If no exact matches, try fuzzy matching (case-insensitive, partial matches)
                for target_col, source_col in rename_map.items():
                    if source_col not in df.columns:
                        # Try case-insensitive match
                        for col in df.columns:
                            if col.lower() == source_col.lower():
                                df = df.rename(columns={col: target_col})
                                self.logger.debug(f"Fuzzy matched: {col} -> {target_col}")
                                break
                        else:
                            # Try partial match
                            for col in df.columns:
                                if source_col.lower() in col.lower() or col.lower() in source_col.lower():
                                    df = df.rename(columns={col: target_col})
                                    self.logger.debug(f"Partial matched: {col} -> {target_col}")
                                    break
        
        self.logger.info(f"Parsed {len(df)} rows from Dune query")
        return df

