"""
JSON data extractor for the data-fetch framework.
Normalizes JSON timeseries data into DataFrames.
"""

import json
import re
from typing import List, Dict, Any, Optional, Union
from datetime import datetime

import pandas as pd

from ..utils.logger import get_logger


class JsonExtractor:
    """
    Extractor for JSON data.
    Handles various JSON structures and normalizes to DataFrames.
    """
    
    def __init__(self):
        self.logger = get_logger()
    
    def extract(
        self,
        data: Union[str, bytes, dict, list],
        data_path: Optional[str] = None,
        field_mappings: Optional[Dict[str, str]] = None,
    ) -> pd.DataFrame:
        """
        Extract data from JSON and return as DataFrame.
        
        Args:
            data: JSON data (string, bytes, dict, or list)
            data_path: JSONPath-like path to the data array (e.g., "data.chart.series")
            field_mappings: Optional mapping from JSON fields to output columns
        
        Returns:
            Extracted DataFrame
        """
        # Parse JSON if needed
        if isinstance(data, (str, bytes)):
            if isinstance(data, bytes):
                data = data.decode("utf-8")
            data = json.loads(data)
        
        # Navigate to data path if specified
        if data_path:
            data = self._navigate_path(data, data_path)
        
        # Handle different data structures
        df = self._extract_data(data)
        
        # Apply field mappings
        if field_mappings and not df.empty:
            df = self._apply_mappings(df, field_mappings)
        
        # Convert date columns
        df = self._convert_dates(df)
        
        self.logger.info(f"Extracted JSON data with {len(df)} rows")
        return df
    
    def _navigate_path(self, data: Any, path: str) -> Any:
        """
        Navigate to a nested path in JSON data.
        
        Args:
            data: JSON data structure
            path: Dot-separated path (e.g., "data.chart.series")
        
        Returns:
            Data at the specified path
        """
        parts = path.split(".")
        
        for part in parts:
            if not part:
                continue
            
            # Handle array index
            if part.isdigit():
                idx = int(part)
                if isinstance(data, list) and idx < len(data):
                    data = data[idx]
                else:
                    raise KeyError(f"Array index {idx} out of range")
            elif isinstance(data, dict):
                if part in data:
                    data = data[part]
                else:
                    raise KeyError(f"Key '{part}' not found in data")
            else:
                raise KeyError(f"Cannot navigate '{part}' in {type(data)}")
        
        return data
    
    def _extract_data(self, data: Any) -> pd.DataFrame:
        """
        Extract data from various JSON structures.
        
        Handles:
        - Array of objects: [{"date": ..., "value": ...}, ...]
        - Object with arrays: {"dates": [...], "values": [...]}
        - Nested structures
        """
        if isinstance(data, list):
            # Array of objects
            if data and isinstance(data[0], dict):
                return pd.DataFrame(data)
            # Array of arrays
            elif data and isinstance(data[0], list):
                return pd.DataFrame(data)
            # Simple array
            else:
                return pd.DataFrame({"value": data})
        
        elif isinstance(data, dict):
            # Check for arrays that could be parallel
            array_keys = [k for k, v in data.items() if isinstance(v, list)]
            
            if array_keys:
                # Check if arrays are the same length (parallel arrays)
                lengths = [len(data[k]) for k in array_keys]
                if len(set(lengths)) == 1:
                    # Parallel arrays - each key becomes a column
                    # Flatten any nested dicts in the arrays to avoid unhashable type errors
                    flattened_data = {}
                    for k in array_keys:
                        flattened_data[k] = [
                            json.dumps(v) if isinstance(v, (dict, list)) else v
                            for v in data[k]
                        ]
                    return pd.DataFrame(flattened_data)
            
            # Look for nested data arrays
            for key, value in data.items():
                if isinstance(value, list) and value and isinstance(value[0], dict):
                    # Flatten nested dicts in the list
                    flattened_list = []
                    for item in value:
                        if isinstance(item, dict):
                            flattened_item = {}
                            for k, v in item.items():
                                if isinstance(v, (dict, list)):
                                    flattened_item[k] = json.dumps(v)
                                else:
                                    flattened_item[k] = v
                            flattened_list.append(flattened_item)
                        else:
                            flattened_list.append(item)
                    return pd.DataFrame(flattened_list)
            
            # Single object - flatten nested structures to avoid unhashable type errors
            flattened = {}
            for k, v in data.items():
                if isinstance(v, (dict, list)):
                    flattened[k] = json.dumps(v)
                else:
                    flattened[k] = v
            return pd.DataFrame([flattened])
        
        else:
            return pd.DataFrame()
    
    def _apply_mappings(
        self,
        df: pd.DataFrame,
        mappings: Dict[str, str],
    ) -> pd.DataFrame:
        """
        Apply field mappings to rename and select columns.
        
        Args:
            df: Input DataFrame
            mappings: Dict mapping output column names to input paths
        
        Returns:
            DataFrame with mapped columns
        """
        result = pd.DataFrame()
        
        for output_col, input_path in mappings.items():
            # Handle nested paths
            if "." in input_path:
                # Need to extract from nested structures
                parts = input_path.split(".")
                col_name = parts[0]
                
                if col_name in df.columns:
                    def extract_nested(row, parts):
                        value = row
                        for part in parts:
                            if isinstance(value, dict) and part in value:
                                value = value[part]
                            else:
                                return None
                        return value
                    
                    result[output_col] = df.apply(
                        lambda row: extract_nested(row[col_name], parts[1:])
                        if col_name in row else None,
                        axis=1
                    )
            else:
                if input_path in df.columns:
                    result[output_col] = df[input_path]
        
        # Keep unmapped columns too
        for col in df.columns:
            if col not in result.columns and col not in mappings.values():
                result[col] = df[col]
        
        return result
    
    def _convert_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert date-like columns to datetime."""
        date_patterns = ["date", "time", "timestamp", "created", "updated"]
        
        for col in df.columns:
            # Check if column name suggests dates
            if any(p in col.lower() for p in date_patterns):
                try:
                    df[col] = self._parse_dates(df[col])
                except Exception:
                    pass
            # Check if values look like dates/timestamps
            elif df[col].dtype == object:
                sample = df[col].dropna().head(5)
                if len(sample) > 0 and self._looks_like_dates(sample):
                    try:
                        df[col] = self._parse_dates(df[col])
                    except Exception:
                        pass
        
        return df
    
    def _looks_like_dates(self, series: pd.Series) -> bool:
        """Check if a series looks like date values."""
        sample = str(series.iloc[0]) if len(series) > 0 else ""
        
        # Check for common date patterns
        date_patterns = [
            r"^\d{4}-\d{2}-\d{2}",  # YYYY-MM-DD
            r"^\d{2}/\d{2}/\d{4}",  # MM/DD/YYYY
            r"^\d{10,13}$",  # Unix timestamp
        ]
        
        return any(re.match(p, sample) for p in date_patterns)
    
    def _parse_dates(self, series: pd.Series) -> pd.Series:
        """Parse various date formats."""
        # Check for Unix timestamps
        sample = series.dropna().head(1)
        if len(sample) > 0:
            val = sample.iloc[0]
            if isinstance(val, (int, float)) and val > 1e9:
                # Unix timestamp (seconds or milliseconds)
                if val > 1e12:
                    return pd.to_datetime(series, unit="ms")
                else:
                    return pd.to_datetime(series, unit="s")
        
        # Try standard date parsing
        return pd.to_datetime(series, errors="coerce")
    
    def detect_structure(self, data: Union[str, bytes, dict, list]) -> Dict[str, Any]:
        """
        Detect the structure of JSON data.
        
        Args:
            data: JSON data
        
        Returns:
            Dictionary describing the structure
        """
        # Parse if needed
        if isinstance(data, (str, bytes)):
            if isinstance(data, bytes):
                data = data.decode("utf-8")
            data = json.loads(data)
        
        structure = {
            "type": type(data).__name__,
            "is_timeseries": False,
            "data_paths": [],
            "field_names": [],
            "row_count": 0,
        }
        
        if isinstance(data, list):
            structure["row_count"] = len(data)
            if data and isinstance(data[0], dict):
                structure["is_timeseries"] = True
                structure["field_names"] = list(data[0].keys())
                structure["data_paths"] = [""]
        
        elif isinstance(data, dict):
            structure["field_names"] = list(data.keys())
            
            # Find nested arrays
            for key, value in data.items():
                if isinstance(value, list):
                    if value and isinstance(value[0], dict):
                        structure["data_paths"].append(key)
                        structure["is_timeseries"] = True
                        structure["row_count"] = len(value)
                        structure["nested_fields"] = list(value[0].keys())
        
        return structure
    
    def find_data_arrays(self, data: Any, path: str = "") -> List[Dict[str, Any]]:
        """
        Recursively find all data arrays in JSON structure.
        
        Args:
            data: JSON data
            path: Current path
        
        Returns:
            List of dicts with path and metadata for each array found
        """
        results = []
        
        if isinstance(data, list) and len(data) > 0:
            if isinstance(data[0], dict):
                results.append({
                    "path": path,
                    "type": "object_array",
                    "count": len(data),
                    "fields": list(data[0].keys()),
                })
            elif isinstance(data[0], (int, float)):
                results.append({
                    "path": path,
                    "type": "value_array",
                    "count": len(data),
                })
        
        elif isinstance(data, dict):
            for key, value in data.items():
                new_path = f"{path}.{key}" if path else key
                results.extend(self.find_data_arrays(value, new_path))
        
        return results

