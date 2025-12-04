"""
HTML table extractor for the data-fetch framework.
Extracts data from HTML tables and converts to DataFrames.
"""

import re
from typing import List, Optional, Dict, Any
from dataclasses import dataclass

import pandas as pd
from bs4 import BeautifulSoup

from ..utils.logger import get_logger


@dataclass
class TableInfo:
    """Information about a detected HTML table."""
    index: int
    selector: str
    num_rows: int
    num_cols: int
    headers: List[str]
    sample_row: List[str]
    has_numeric_data: bool


class TableExtractor:
    """
    Extractor for HTML tables.
    Converts HTML tables to pandas DataFrames.
    """
    
    def __init__(self):
        self.logger = get_logger()
    
    def find_tables(self, html: str) -> List[TableInfo]:
        """
        Find all tables in HTML and return information about them.
        
        Args:
            html: HTML content
        
        Returns:
            List of TableInfo objects describing each table
        """
        soup = BeautifulSoup(html, "lxml")
        tables = soup.find_all("table")
        
        table_infos = []
        
        for i, table in enumerate(tables):
            # Get headers
            headers = []
            header_row = table.find("thead")
            if header_row:
                headers = [th.get_text(strip=True) for th in header_row.find_all(["th", "td"])]
            else:
                # Try first row
                first_row = table.find("tr")
                if first_row:
                    headers = [cell.get_text(strip=True) for cell in first_row.find_all(["th", "td"])]
            
            # Get row count
            rows = table.find_all("tr")
            num_rows = len(rows) - 1  # Exclude header row
            num_cols = len(headers) if headers else 0
            
            # Get sample row
            sample_row = []
            if len(rows) > 1:
                sample_cells = rows[1].find_all(["th", "td"])
                sample_row = [cell.get_text(strip=True) for cell in sample_cells]
            
            # Check for numeric data
            has_numeric = any(
                self._looks_numeric(cell)
                for cell in sample_row
            )
            
            # Create selector
            table_id = table.get("id")
            table_class = table.get("class")
            if table_id:
                selector = f"#{table_id}"
            elif table_class:
                selector = f"table.{'.'.join(table_class)}"
            else:
                selector = f"table:nth-of-type({i + 1})"
            
            table_infos.append(TableInfo(
                index=i,
                selector=selector,
                num_rows=num_rows,
                num_cols=num_cols,
                headers=headers,
                sample_row=sample_row,
                has_numeric_data=has_numeric,
            ))
        
        self.logger.info(f"Found {len(table_infos)} tables in HTML")
        return table_infos
    
    def extract_table(
        self,
        html: str,
        selector: Optional[str] = None,
        table_index: int = 0,
        header_row: int = 0,
        skip_rows: int = 0,
    ) -> pd.DataFrame:
        """
        Extract a table from HTML as a DataFrame.
        
        Args:
            html: HTML content
            selector: CSS selector for the table (optional)
            table_index: Index of table to extract if no selector
            header_row: Row index to use as headers
            skip_rows: Number of rows to skip after headers
        
        Returns:
            Extracted DataFrame
        """
        soup = BeautifulSoup(html, "lxml")
        
        # Find the table
        if selector:
            table = soup.select_one(selector)
        else:
            tables = soup.find_all("table")
            if table_index < len(tables):
                table = tables[table_index]
            else:
                raise ValueError(f"Table index {table_index} out of range")
        
        if not table:
            raise ValueError(f"Table not found with selector: {selector}")
        
        # Extract rows
        rows = table.find_all("tr")
        if not rows:
            return pd.DataFrame()
        
        # Get headers
        header_cells = rows[header_row].find_all(["th", "td"])
        headers = [self._clean_text(cell.get_text()) for cell in header_cells]
        
        # Make headers unique
        headers = self._make_unique_headers(headers)
        
        # Extract data rows
        data = []
        start_row = header_row + 1 + skip_rows
        
        for row in rows[start_row:]:
            cells = row.find_all(["th", "td"])
            row_data = [self._clean_text(cell.get_text()) for cell in cells]
            
            # Pad or trim to match header count
            if len(row_data) < len(headers):
                row_data.extend([""] * (len(headers) - len(row_data)))
            elif len(row_data) > len(headers):
                row_data = row_data[:len(headers)]
            
            data.append(row_data)
        
        # Create DataFrame
        df = pd.DataFrame(data, columns=headers)
        
        # Try to convert numeric columns
        for col in df.columns:
            df[col] = self._convert_to_numeric(df[col])
        
        self.logger.info(f"Extracted table with {len(df)} rows and {len(df.columns)} columns")
        return df
    
    def extract_all_tables(self, html: str) -> List[pd.DataFrame]:
        """
        Extract all tables from HTML.
        
        Args:
            html: HTML content
        
        Returns:
            List of DataFrames, one per table
        """
        table_infos = self.find_tables(html)
        
        dfs = []
        for info in table_infos:
            try:
                df = self.extract_table(html, table_index=info.index)
                if not df.empty:
                    dfs.append(df)
            except Exception as e:
                self.logger.warning(f"Failed to extract table {info.index}: {e}")
        
        return dfs
    
    def extract_best_table(
        self,
        html: str,
        min_rows: int = 5,
        require_numeric: bool = True,
    ) -> Optional[pd.DataFrame]:
        """
        Extract the best table from HTML based on heuristics.
        
        Args:
            html: HTML content
            min_rows: Minimum number of rows required
            require_numeric: Whether table must have numeric data
        
        Returns:
            Best matching DataFrame or None
        """
        table_infos = self.find_tables(html)
        
        # Filter and score tables
        scored_tables = []
        for info in table_infos:
            score = 0
            
            # Skip tables that don't meet requirements
            if info.num_rows < min_rows:
                continue
            if require_numeric and not info.has_numeric_data:
                continue
            
            # Score by row count
            score += min(info.num_rows / 100, 1.0)
            
            # Score by column count (prefer more columns up to a point)
            score += min(info.num_cols / 10, 0.5)
            
            # Bonus for having date-like headers
            date_keywords = ["date", "time", "day", "month", "year"]
            if any(kw in " ".join(info.headers).lower() for kw in date_keywords):
                score += 0.5
            
            scored_tables.append((info, score))
        
        if not scored_tables:
            return None
        
        # Get best table
        scored_tables.sort(key=lambda x: x[1], reverse=True)
        best_info = scored_tables[0][0]
        
        return self.extract_table(html, table_index=best_info.index)
    
    def _clean_text(self, text: str) -> str:
        """Clean extracted text."""
        # Remove extra whitespace
        text = " ".join(text.split())
        # Remove non-printable characters
        text = "".join(c for c in text if c.isprintable())
        return text.strip()
    
    def _make_unique_headers(self, headers: List[str]) -> List[str]:
        """Make header names unique by adding suffixes."""
        seen = {}
        unique = []
        
        for h in headers:
            if not h:
                h = "column"
            
            if h in seen:
                seen[h] += 1
                unique.append(f"{h}_{seen[h]}")
            else:
                seen[h] = 0
                unique.append(h)
        
        return unique
    
    def _looks_numeric(self, text: str) -> bool:
        """Check if text looks like a number."""
        # Remove common number formatting
        cleaned = text.replace(",", "").replace("$", "").replace("%", "")
        cleaned = cleaned.replace("B", "e9").replace("M", "e6").replace("K", "e3")
        
        try:
            float(cleaned)
            return True
        except (ValueError, TypeError):
            return False
    
    def _convert_to_numeric(self, series: pd.Series) -> pd.Series:
        """Try to convert a series to numeric values."""
        def parse_value(val):
            if pd.isna(val) or val == "":
                return val
            
            val_str = str(val).strip()
            
            # Handle common suffixes
            multipliers = {
                "B": 1e9, "b": 1e9,
                "M": 1e6, "m": 1e6,
                "K": 1e3, "k": 1e3,
            }
            
            for suffix, mult in multipliers.items():
                if val_str.endswith(suffix):
                    try:
                        num = float(val_str[:-1].replace(",", "").replace("$", ""))
                        return num * mult
                    except ValueError:
                        pass
            
            # Try direct conversion
            try:
                cleaned = val_str.replace(",", "").replace("$", "").replace("%", "")
                return float(cleaned)
            except ValueError:
                return val
        
        return series.apply(parse_value)

