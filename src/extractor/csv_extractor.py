"""
CSV data extractor for the data-fetch framework.
Handles CSV endpoints and embedded CSV data.
"""

import csv
import io
import re
from typing import Union, Optional, List, Dict
from pathlib import Path

import pandas as pd

from ..utils.logger import get_logger


class CsvExtractor:
    """
    Extractor for CSV data.
    Handles various CSV delimiters, encodings, and formats.
    """
    
    def __init__(self):
        self.logger = get_logger()
    
    def extract(
        self,
        data: Union[str, bytes, Path],
        delimiter: Optional[str] = None,
        encoding: str = "utf-8",
        has_header: Optional[bool] = None,
        skip_rows: int = 0,
    ) -> pd.DataFrame:
        """
        Extract data from CSV and return as DataFrame.
        
        Args:
            data: CSV data (string, bytes, or file path)
            delimiter: CSV delimiter (auto-detect if None)
            encoding: Text encoding (default: utf-8)
            has_header: Whether CSV has header row (auto-detect if None)
            skip_rows: Number of rows to skip at the start
        
        Returns:
            Extracted DataFrame
        """
        # Load data
        if isinstance(data, Path):
            with open(data, "r", encoding=encoding) as f:
                csv_content = f.read()
        elif isinstance(data, bytes):
            csv_content = data.decode(encoding, errors="replace")
        else:
            csv_content = data
        
        # Auto-detect delimiter if not provided
        if delimiter is None:
            delimiter = self._detect_delimiter(csv_content)
        
        # Auto-detect header if not specified
        if has_header is None:
            has_header = self._detect_header(csv_content, delimiter)
        
        # Parse CSV
        try:
            df = pd.read_csv(
                io.StringIO(csv_content),
                delimiter=delimiter,
                encoding=encoding,
                header=0 if has_header else None,
                skiprows=skip_rows,
                on_bad_lines="skip",
                engine="python",
            )
        except Exception as e:
            self.logger.warning(f"Error parsing CSV with pandas: {e}, trying manual parse")
            df = self._manual_parse(csv_content, delimiter, has_header, skip_rows)
        
        # Clean column names
        if has_header:
            df.columns = [self._clean_column_name(col) for col in df.columns]
        
        self.logger.info(f"Extracted CSV data with {len(df)} rows, {len(df.columns)} columns")
        return df
    
    def _detect_delimiter(self, content: str) -> str:
        """
        Auto-detect CSV delimiter.
        
        Args:
            content: CSV content
        
        Returns:
            Detected delimiter character
        """
        # Common delimiters to try
        delimiters = [",", ";", "\t", "|"]
        
        # Count occurrences of each delimiter in first few lines
        first_lines = content.split("\n")[:10]
        delimiter_counts = {}
        
        for delim in delimiters:
            count = sum(line.count(delim) for line in first_lines)
            delimiter_counts[delim] = count
        
        # Return delimiter with highest count
        if delimiter_counts:
            return max(delimiter_counts, key=delimiter_counts.get)
        
        return ","  # Default
    
    def _detect_header(self, content: str, delimiter: str) -> bool:
        """
        Detect if CSV has a header row.
        
        Args:
            content: CSV content
            delimiter: CSV delimiter
        
        Returns:
            True if header detected
        """
        lines = content.split("\n")[:3]
        if len(lines) < 2:
            return False
        
        # Check if first line looks like headers (non-numeric values)
        first_line = lines[0].split(delimiter)
        second_line = lines[1].split(delimiter) if len(lines) > 1 else []
        
        if len(first_line) != len(second_line):
            return False
        
        # If first line has mostly non-numeric values and second has mostly numeric, likely header
        first_numeric = sum(1 for val in first_line if self._is_numeric(val.strip()))
        second_numeric = sum(1 for val in second_line if self._is_numeric(val.strip()))
        
        return first_numeric < second_numeric
    
    def _is_numeric(self, value: str) -> bool:
        """Check if a string value is numeric."""
        try:
            float(value.replace(",", "").replace("$", "").replace("%", ""))
            return True
        except (ValueError, AttributeError):
            return False
    
    def _clean_column_name(self, name: str) -> str:
        """Clean column name (remove whitespace, special chars)."""
        if not isinstance(name, str):
            name = str(name)
        # Remove leading/trailing whitespace
        name = name.strip()
        # Replace spaces with underscores
        name = re.sub(r"\s+", "_", name)
        # Remove special characters
        name = re.sub(r"[^\w_]", "", name)
        # Lowercase
        return name.lower()
    
    def _manual_parse(
        self,
        content: str,
        delimiter: str,
        has_header: bool,
        skip_rows: int,
    ) -> pd.DataFrame:
        """
        Manually parse CSV using Python's csv module.
        
        Args:
            content: CSV content
            delimiter: CSV delimiter
            has_header: Whether header exists
            skip_rows: Rows to skip
        
        Returns:
            DataFrame
        """
        lines = content.split("\n")
        
        # Skip rows
        if skip_rows > 0:
            lines = lines[skip_rows:]
        
        # Parse with csv module
        reader = csv.reader(io.StringIO("\n".join(lines)), delimiter=delimiter)
        rows = list(reader)
        
        if not rows:
            return pd.DataFrame()
        
        # Extract header if present
        if has_header and rows:
            headers = rows[0]
            data_rows = rows[1:]
        else:
            headers = [f"col_{i}" for i in range(len(rows[0]) if rows else 0)]
            data_rows = rows
        
        # Create DataFrame
        df = pd.DataFrame(data_rows, columns=headers[:len(data_rows[0])] if data_rows else [])
        
        return df
    
    def extract_from_url(self, url: str, **kwargs) -> pd.DataFrame:
        """
        Extract CSV data from a URL.
        
        Args:
            url: URL to fetch CSV from
            **kwargs: Additional arguments for extract()
        
        Returns:
            Extracted DataFrame
        """
        import requests
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Try to detect encoding from headers
            encoding = response.encoding or "utf-8"
            
            return self.extract(response.content, encoding=encoding, **kwargs)
        except Exception as e:
            self.logger.error(f"Error fetching CSV from URL {url}: {e}")
            return pd.DataFrame()




