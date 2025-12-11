"""
Financial data normalizer for the data-fetch framework.
Standardizes financial formats, currencies, and data structures.
"""

import re
from typing import Union, Optional, Dict, Any
from decimal import Decimal, InvalidOperation

import pandas as pd
import numpy as np

from ..utils.logger import get_logger


class FinancialNormalizer:
    """
    Normalizer for financial data.
    Handles currency symbols, large numbers, percentages, and date formats.
    """
    
    # Currency symbols and their codes
    CURRENCY_SYMBOLS = {
        "$": "USD",
        "€": "EUR",
        "£": "GBP",
        "¥": "JPY",
        "₹": "INR",
        "₿": "BTC",
        "¢": "USD",  # Cents
    }
    
    # Number suffixes
    NUMBER_SUFFIXES = {
        "K": 1_000,
        "M": 1_000_000,
        "B": 1_000_000_000,
        "T": 1_000_000_000_000,
        "k": 1_000,
        "m": 1_000_000,
        "b": 1_000_000_000,
        "t": 1_000_000_000_000,
    }
    
    def __init__(self):
        self.logger = get_logger()
    
    def normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize a DataFrame containing financial data.
        
        Args:
            df: DataFrame to normalize
        
        Returns:
            Normalized DataFrame
        """
        df = df.copy()
        
        # Normalize each column
        for col in df.columns:
            # Detect column type and normalize accordingly
            if self._is_price_column(col):
                df[col] = df[col].apply(self.normalize_price)
            elif self._is_percentage_column(col):
                df[col] = df[col].apply(self.normalize_percentage)
            elif self._is_volume_column(col):
                df[col] = df[col].apply(self.normalize_number)
            elif self._is_ticker_column(col):
                df[col] = df[col].apply(self.normalize_ticker)
            else:
                # Try to normalize as number if it looks numeric
                if df[col].dtype == "object":
                    df[col] = df[col].apply(self._try_normalize_number)
        
        return df
    
    def normalize_price(self, value: Union[str, float, int]) -> Optional[float]:
        """
        Normalize a price value.
        
        Args:
            value: Price value (string or number)
        
        Returns:
            Normalized price as float
        """
        if pd.isna(value):
            return None
        
        if isinstance(value, (int, float)):
            return float(value)
        
        if not isinstance(value, str):
            value = str(value)
        
        # Remove currency symbols and whitespace
        currency_code = None
        for symbol, code in self.CURRENCY_SYMBOLS.items():
            if symbol in value:
                currency_code = code
                value = value.replace(symbol, "")
        
        value = value.strip()
        
        # Remove commas
        value = value.replace(",", "")
        
        # Handle negative values in parentheses
        if value.startswith("(") and value.endswith(")"):
            value = "-" + value[1:-1]
        
        # Parse number
        try:
            return float(value)
        except ValueError:
            self.logger.warning(f"Could not parse price: {value}")
            return None
    
    def normalize_percentage(self, value: Union[str, float, int]) -> Optional[float]:
        """
        Normalize a percentage value.
        
        Args:
            value: Percentage value (string or number)
        
        Returns:
            Normalized percentage as float (0-100 scale)
        """
        if pd.isna(value):
            return None
        
        if isinstance(value, (int, float)):
            # Assume already in 0-100 scale
            return float(value)
        
        if not isinstance(value, str):
            value = str(value)
        
        value = value.strip()
        
        # Remove % symbol
        value = value.replace("%", "")
        
        # Remove commas
        value = value.replace(",", "")
        
        # Handle negative values
        if value.startswith("(") and value.endswith(")"):
            value = "-" + value[1:-1]
        
        try:
            return float(value)
        except ValueError:
            self.logger.warning(f"Could not parse percentage: {value}")
            return None
    
    def normalize_number(
        self,
        value: Union[str, float, int],
        handle_suffixes: bool = True,
    ) -> Optional[float]:
        """
        Normalize a number value (handles K, M, B suffixes).
        
        Args:
            value: Number value (string or number)
            handle_suffixes: Whether to handle K/M/B suffixes
        
        Returns:
            Normalized number as float
        """
        if pd.isna(value):
            return None
        
        if isinstance(value, (int, float)):
            return float(value)
        
        if not isinstance(value, str):
            value = str(value)
        
        value = value.strip()
        
        # Remove currency symbols
        for symbol in self.CURRENCY_SYMBOLS.keys():
            value = value.replace(symbol, "")
        
        # Remove commas
        value = value.replace(",", "")
        
        # Handle suffixes (K, M, B)
        multiplier = 1.0
        if handle_suffixes:
            for suffix, mult in self.NUMBER_SUFFIXES.items():
                if value.endswith(suffix):
                    multiplier = mult
                    value = value[:-len(suffix)]
                    break
        
        # Handle negative values
        if value.startswith("(") and value.endswith(")"):
            value = "-" + value[1:-1]
        
        try:
            return float(value) * multiplier
        except ValueError:
            self.logger.warning(f"Could not parse number: {value}")
            return None
    
    def normalize_ticker(self, value: Union[str, Any]) -> Optional[str]:
        """
        Normalize a ticker symbol.
        
        Args:
            value: Ticker symbol
        
        Returns:
            Normalized ticker (uppercase, no special chars)
        """
        if pd.isna(value):
            return None
        
        if not isinstance(value, str):
            value = str(value)
        
        # Uppercase and remove whitespace
        ticker = value.strip().upper()
        
        # Remove common prefixes/suffixes
        ticker = re.sub(r"^[A-Z]+:", "", ticker)  # Remove exchange prefix (e.g., "NASDAQ:AAPL")
        ticker = re.sub(r"\.[A-Z]+$", "", ticker)  # Remove exchange suffix (e.g., "AAPL.NASDAQ")
        
        # Remove special characters (keep only alphanumeric)
        ticker = re.sub(r"[^A-Z0-9]", "", ticker)
        
        return ticker if ticker else None
    
    def _is_price_column(self, col_name: str) -> bool:
        """Check if column name suggests price data."""
        col_lower = col_name.lower()
        return any(x in col_lower for x in ["price", "close", "open", "high", "low", "bid", "ask"])
    
    def _is_percentage_column(self, col_name: str) -> bool:
        """Check if column name suggests percentage data."""
        col_lower = col_name.lower()
        return any(x in col_lower for x in ["percent", "pct", "change", "return", "yield"])
    
    def _is_volume_column(self, col_name: str) -> bool:
        """Check if column name suggests volume data."""
        col_lower = col_name.lower()
        return any(x in col_lower for x in ["volume", "vol", "amount", "quantity"])
    
    def _is_ticker_column(self, col_name: str) -> bool:
        """Check if column name suggests ticker/symbol data."""
        col_lower = col_name.lower()
        return any(x in col_lower for x in ["ticker", "symbol", "symbol", "asset", "coin"])
    
    def _try_normalize_number(self, value: Any) -> Any:
        """Try to normalize a value as a number."""
        if pd.isna(value):
            return value
        
        if isinstance(value, (int, float)):
            return value
        
        # Try to parse as number
        normalized = self.normalize_number(value, handle_suffixes=True)
        return normalized if normalized is not None else value
    
    def detect_currency(self, value: Union[str, float, int]) -> Optional[str]:
        """
        Detect currency from a value.
        
        Args:
            value: Value that may contain currency symbol
        
        Returns:
            Currency code (USD, EUR, etc.) or None
        """
        if pd.isna(value) or isinstance(value, (int, float)):
            return None
        
        value_str = str(value)
        
        for symbol, code in self.CURRENCY_SYMBOLS.items():
            if symbol in value_str:
                return code
        
        return None




