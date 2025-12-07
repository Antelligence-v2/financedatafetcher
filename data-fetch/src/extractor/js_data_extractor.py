"""
JavaScript data extractor for the data-fetch framework.
Extracts data from JavaScript variables and objects on web pages.
"""

import json
import re
from typing import Optional, Dict, Any, List

import pandas as pd

from ..utils.logger import get_logger


class JsDataExtractor:
    """
    Extractor for JavaScript data embedded in web pages.
    Extracts data from window objects, script tags, and JavaScript variables.
    """
    
    def __init__(self):
        self.logger = get_logger()
    
    def extract_from_html(
        self,
        html: str,
        variable_name: Optional[str] = None,
        script_selector: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Extract JavaScript data from HTML content.
        
        Args:
            html: HTML content
            variable_name: JavaScript variable name to extract (e.g., "window.__INITIAL_STATE__")
            script_selector: CSS selector for script tag containing data
        
        Returns:
            Extracted DataFrame
        """
        # Try different extraction methods
        data = None
        
        # Method 1: Extract from specific variable name
        if variable_name:
            data = self._extract_variable(html, variable_name)
        
        # Method 2: Extract from common data patterns
        if data is None:
            data = self._extract_common_patterns(html)
        
        # Method 3: Extract from script tags
        if data is None:
            data = self._extract_from_scripts(html, script_selector)
        
        if data is None:
            self.logger.warning("No JavaScript data found in HTML")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = self._data_to_dataframe(data)
        
        self.logger.info(f"Extracted JavaScript data with {len(df)} rows")
        return df
    
    def _extract_variable(self, html: str, variable_name: str) -> Optional[Any]:
        """
        Extract data from a JavaScript variable.
        
        Args:
            html: HTML content
            variable_name: Variable name (e.g., "window.__INITIAL_STATE__")
        
        Returns:
            Extracted data or None
        """
        # Remove namespace if present
        var_name = variable_name.replace("window.", "").replace("global.", "")
        
        # Pattern 1: var variableName = {...}
        pattern1 = rf"{var_name}\s*=\s*({{.*?}});"
        match = re.search(pattern1, html, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass
        
        # Pattern 2: window.variableName = {...}
        pattern2 = rf"window\.{var_name}\s*=\s*({{.*?}});"
        match = re.search(pattern2, html, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass
        
        # Pattern 3: variableName: {...} (in object literal)
        pattern3 = rf'"{var_name}"\s*:\s*({{.*?}})'
        match = re.search(pattern3, html, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass
        
        return None
    
    def _extract_common_patterns(self, html: str) -> Optional[Any]:
        """
        Extract data from common JavaScript patterns.
        
        Args:
            html: HTML content
        
        Returns:
            Extracted data or None
        """
        # Common variable names to check (including financial site patterns)
        common_vars = [
            "__INITIAL_STATE__",
            "__DATA__",
            "__INITIAL_DATA__",
            "window.data",
            "window.chartData",
            "window.seriesData",
            "initialData",
            "chartData",
            "seriesData",
            # Financial site patterns
            "window.quoteData",
            "window.marketData",
            "window.stockData",
            "quoteData",
            "marketData",
            "stockData",
            "instrumentData",
            "tickerData",
            "priceData",
            # Reuters/Financial news patterns
            "__REDUX_STATE__",
            "__APOLLO_STATE__",
            "window.__REACT_QUERY_STATE__",
            "window.__NEXT_DATA__",
            "__NEXT_DATA__",  # Next.js data (without window prefix)
            # Bloomberg patterns
            "bloombergData",
            "bbgData",
            "marketData",
            # Generic data patterns
            "pageData",
            "componentData",
            "props",
            "state",
            # React/Redux patterns
            "window.__PRELOADED_STATE__",
            "__PRELOADED_STATE__",
        ]
        
        for var_name in common_vars:
            data = self._extract_variable(html, var_name)
            if data:
                return data
        
        return None
    
    def _extract_from_scripts(self, html: str, selector: Optional[str] = None) -> Optional[Any]:
        """
        Extract data from script tags.
        
        Args:
            html: HTML content
            selector: CSS selector for script tag (not used in regex extraction)
        
        Returns:
            Extracted data or None
        """
        from bs4 import BeautifulSoup
        
        soup = BeautifulSoup(html, "lxml")
        scripts = soup.find_all("script")
        
        # First, check for script tags with type="application/json" (common in Next.js/React apps)
        for script in scripts:
            if script.get("type") == "application/json" and script.string:
                try:
                    data = json.loads(script.string)
                    self.logger.debug("Found JSON data in script tag with type='application/json'")
                    return data
                except json.JSONDecodeError:
                    continue
        
        # Also check for script tags with id containing "data" or "__NEXT_DATA__"
        for script in scripts:
            script_id = script.get("id", "").lower()
            if ("data" in script_id or "__next" in script_id) and script.string:
                try:
                    data = json.loads(script.string)
                    self.logger.debug(f"Found JSON data in script tag with id='{script.get('id')}'")
                    return data
                except json.JSONDecodeError:
                    continue
        
        for script in scripts:
            if not script.string:
                continue
            
            script_content = script.string
            
            # Look for JSON data in script tags
            # Pattern: data = {...} or var data = {...}
            # Also look for financial-specific patterns
            patterns = [
                r"data\s*=\s*({.*?});",
                r"var\s+data\s*=\s*({.*?});",
                r"const\s+data\s*=\s*({.*?});",
                r"let\s+data\s*=\s*({.*?});",
                # Financial data patterns
                r"quoteData\s*=\s*({.*?});",
                r"marketData\s*=\s*({.*?});",
                r"stockData\s*=\s*({.*?});",
                r"instrumentData\s*=\s*({.*?});",
                r"tickerData\s*=\s*({.*?});",
                # React/Next.js patterns (Reuters, Bloomberg often use these)
                r"__NEXT_DATA__\s*=\s*({.*?});",
                r"__REDUX_STATE__\s*=\s*({.*?});",
                r"__APOLLO_STATE__\s*=\s*({.*?});",
                r"window\.__NEXT_DATA__\s*=\s*({.*?});",
                # Generic object patterns
                r"window\.__INITIAL_STATE__\s*=\s*({.*?});",
                r"window\.__DATA__\s*=\s*({.*?});",
                # Bloomberg patterns
                r"bloombergData\s*=\s*({.*?});",
                r"bbgData\s*=\s*({.*?});",
            ]
            
            # Also look for script tags with type="application/json" (common in Next.js)
            for script in scripts:
                if script.get("type") == "application/json" and script.string:
                    try:
                        data = json.loads(script.string)
                        return data
                    except json.JSONDecodeError:
                        continue
            
            for pattern in patterns:
                matches = re.finditer(pattern, script_content, re.DOTALL)
                for match in matches:
                    try:
                        data = json.loads(match.group(1))
                        return data
                    except json.JSONDecodeError:
                        continue
            
            # Look for array data
            array_patterns = [
                r"data\s*=\s*(\[.*?\]);",
                r"var\s+data\s*=\s*(\[.*?\]);",
            ]
            
            for pattern in array_patterns:
                matches = re.finditer(pattern, script_content, re.DOTALL)
                for match in matches:
                    try:
                        data = json.loads(match.group(1))
                        return data
                    except json.JSONDecodeError:
                        continue
        
        return None
    
    def _data_to_dataframe(self, data: Any) -> pd.DataFrame:
        """
        Convert extracted JavaScript data to DataFrame.
        
        Args:
            data: Extracted data (dict, list, etc.)
        
        Returns:
            DataFrame
        """
        if isinstance(data, list):
            if data and isinstance(data[0], dict):
                return pd.DataFrame(data)
            else:
                return pd.DataFrame({"value": data})
        
        elif isinstance(data, dict):
            # Check for nested data arrays
            for key, value in data.items():
                if isinstance(value, list) and value:
                    if isinstance(value[0], dict):
                        df = pd.DataFrame(value)
                        # Add parent key as prefix to column names
                        df.columns = [f"{key}_{col}" for col in df.columns]
                        return df
            
            # Flatten dict structure
            flattened = {}
            self._flatten_dict(data, flattened)
            return pd.DataFrame([flattened])
        
        else:
            return pd.DataFrame({"value": [data]})
    
    def _flatten_dict(self, d: Dict, result: Dict, prefix: str = ""):
        """Recursively flatten nested dictionary."""
        for key, value in d.items():
            new_key = f"{prefix}_{key}" if prefix else key
            
            if isinstance(value, dict):
                self._flatten_dict(value, result, new_key)
            elif isinstance(value, list):
                # Convert list to string representation
                result[new_key] = json.dumps(value)
            else:
                result[new_key] = value
    
    def extract_from_browser(
        self,
        browser_manager,
        url: str,
        variable_name: str,
        wait_for_selector: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Extract JavaScript data directly from browser.
        
        Args:
            browser_manager: BrowserManager instance
            url: URL to load
            variable_name: JavaScript variable name to extract
            wait_for_selector: CSS selector to wait for
        
        Returns:
            Extracted DataFrame
        """
        import asyncio
        
        async def _extract():
            async with browser_manager:
                page = await browser_manager._context.new_page()
                try:
                    await page.goto(url, timeout=30000, wait_until="networkidle")
                    
                    if wait_for_selector:
                        await page.wait_for_selector(wait_for_selector, timeout=10000)
                    
                    # Evaluate JavaScript to get variable
                    script = f"JSON.stringify({variable_name})"
                    result = await page.evaluate(script)
                    
                    if result:
                        data = json.loads(result)
                        return self._data_to_dataframe(data)
                    
                finally:
                    await page.close()
            
            return pd.DataFrame()
        
        return asyncio.run(_extract())

