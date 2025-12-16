"""
Canonical schema definition for financial time-series data.
Provides standardized data formats and conversion utilities.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
import pandas as pd
import numpy as np


# Standard column names
COLUMN_DATE = "date"
COLUMN_ASSET = "asset"
COLUMN_METRIC = "metric"
COLUMN_VALUE = "value"
COLUMN_SOURCE = "source"

# Common metric names
METRIC_VOLUME = "volume"
METRIC_VOLUME_7DMA = "volume_7dma"
METRIC_PRICE = "price"
METRIC_MARKET_CAP = "market_cap"


@dataclass
class FinancialDataSchema:
    """
    Schema definition for financial time-series data.
    Supports both long format (normalized) and wide format.
    """
    
    # Required columns
    date_column: str = COLUMN_DATE
    
    # Long format columns (optional)
    asset_column: Optional[str] = COLUMN_ASSET
    metric_column: Optional[str] = COLUMN_METRIC
    value_column: Optional[str] = COLUMN_VALUE
    
    # Wide format columns (alternative to long format)
    value_columns: List[str] = field(default_factory=list)
    
    # Metadata columns
    source_column: str = COLUMN_SOURCE
    
    # Data types
    date_format: str = "%Y-%m-%d"
    numeric_columns: List[str] = field(default_factory=list)
    
    def get_required_columns(self) -> List[str]:
        """Get list of required columns."""
        if self.value_columns:
            # Wide format
            return [self.date_column] + self.value_columns
        else:
            # Long format
            return [
                self.date_column,
                self.asset_column,
                self.metric_column,
                self.value_column,
            ]
    
    def validate_dataframe(self, df: pd.DataFrame) -> List[str]:
        """
        Validate that a DataFrame matches this schema.
        
        Args:
            df: DataFrame to validate
        
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        # Check required columns
        required = self.get_required_columns()
        missing = [col for col in required if col not in df.columns]
        if missing:
            errors.append(f"Missing required columns: {missing}")
        
        # Check date column type
        if self.date_column in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df[self.date_column]):
                try:
                    pd.to_datetime(df[self.date_column])
                except Exception:
                    errors.append(f"Column '{self.date_column}' cannot be parsed as datetime")
        
        # Check numeric columns
        for col in self.numeric_columns or self.value_columns:
            if col in df.columns:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    try:
                        pd.to_numeric(df[col])
                    except Exception:
                        errors.append(f"Column '{col}' cannot be converted to numeric")
        
        return errors


def to_long_format(
    df: pd.DataFrame,
    date_column: str = COLUMN_DATE,
    value_columns: List[str] = None,
    asset_name: Optional[str] = None,
    source: Optional[str] = None,
) -> pd.DataFrame:
    """
    Convert a wide-format DataFrame to long format.
    
    Args:
        df: DataFrame in wide format
        date_column: Name of the date column
        value_columns: Columns to melt (if None, uses all non-date columns)
        asset_name: Optional asset name to add
        source: Optional source name to add
    
    Returns:
        DataFrame in long format with columns: date, asset, metric, value, source
    
    Example:
        Input (wide):
            date       | btc_volume | eth_volume
            2024-01-01 | 1.2B      | 500M
        
        Output (long):
            date       | asset | metric | value | source
            2024-01-01 | BTC   | volume | 1.2B  | theblock
            2024-01-01 | ETH   | volume | 500M  | theblock
    """
    if value_columns is None:
        value_columns = [col for col in df.columns if col != date_column]
    
    # Melt the DataFrame
    melted = df.melt(
        id_vars=[date_column],
        value_vars=value_columns,
        var_name="metric_raw",
        value_name=COLUMN_VALUE,
    )
    
    # Parse asset and metric from column names
    # Assumes format like "btc_volume" or "eth_volume_7dma"
    def parse_metric(metric_raw: str) -> tuple:
        parts = metric_raw.lower().split("_")
        if len(parts) >= 2:
            asset = parts[0].upper()
            metric = "_".join(parts[1:])
        else:
            asset = asset_name or "UNKNOWN"
            metric = metric_raw
        return asset, metric
    
    parsed = melted["metric_raw"].apply(parse_metric)
    melted[COLUMN_ASSET] = [p[0] for p in parsed]
    melted[COLUMN_METRIC] = [p[1] for p in parsed]
    
    # Add source
    melted[COLUMN_SOURCE] = source or "unknown"
    
    # Clean up
    melted = melted.drop(columns=["metric_raw"])
    melted = melted.rename(columns={date_column: COLUMN_DATE})
    
    # Reorder columns
    return melted[[COLUMN_DATE, COLUMN_ASSET, COLUMN_METRIC, COLUMN_VALUE, COLUMN_SOURCE]]


def to_wide_format(
    df: pd.DataFrame,
    date_column: str = COLUMN_DATE,
    asset_column: str = COLUMN_ASSET,
    metric_column: str = COLUMN_METRIC,
    value_column: str = COLUMN_VALUE,
) -> pd.DataFrame:
    """
    Convert a long-format DataFrame to wide format.
    
    Args:
        df: DataFrame in long format
        date_column: Name of the date column
        asset_column: Name of the asset column
        metric_column: Name of the metric column
        value_column: Name of the value column
    
    Returns:
        DataFrame in wide format
    
    Example:
        Input (long):
            date       | asset | metric | value
            2024-01-01 | BTC   | volume | 1.2B
            2024-01-01 | ETH   | volume | 500M
        
        Output (wide):
            date       | btc_volume | eth_volume
            2024-01-01 | 1.2B      | 500M
    """
    # Create combined column name
    df = df.copy()
    df["_col_name"] = df[asset_column].str.lower() + "_" + df[metric_column].str.lower()
    
    # Pivot
    pivoted = df.pivot_table(
        index=date_column,
        columns="_col_name",
        values=value_column,
        aggfunc="first",
    ).reset_index()
    
    pivoted.columns.name = None
    return pivoted


def normalize_dataframe(
    df: pd.DataFrame,
    date_column: str = None,
    date_format: str = None,
    numeric_columns: List[str] = None,
    drop_na: bool = False,
    sort_by_date: bool = True,
) -> pd.DataFrame:
    """
    Normalize a DataFrame to standard format.
    
    Args:
        df: DataFrame to normalize
        date_column: Name of the date column (auto-detect if None)
        date_format: Expected date format (auto-detect if None)
        numeric_columns: Columns to convert to numeric
        drop_na: Whether to drop rows with NaN values
        sort_by_date: Whether to sort by date
    
    Returns:
        Normalized DataFrame
    """
    df = df.copy()
    
    # Auto-detect date column
    if date_column is None:
        date_candidates = ["date", "timestamp", "time", "datetime", "Date", "Timestamp"]
        for col in date_candidates:
            if col in df.columns:
                date_column = col
                break
    
    # Convert date column
    if date_column and date_column in df.columns:
        try:
            df[date_column] = pd.to_datetime(df[date_column], format=date_format)
        except Exception:
            df[date_column] = pd.to_datetime(df[date_column])
    
    # Convert numeric columns
    if numeric_columns:
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
    
    # Drop NaN
    if drop_na:
        df = df.dropna()
    
    # Sort by date
    if sort_by_date and date_column and date_column in df.columns:
        df = df.sort_values(date_column).reset_index(drop=True)
    
    return df


def create_empty_dataframe(schema: FinancialDataSchema) -> pd.DataFrame:
    """Create an empty DataFrame with the schema's columns."""
    columns = schema.get_required_columns()
    if schema.source_column:
        columns.append(schema.source_column)
    return pd.DataFrame(columns=columns)


def merge_dataframes(
    dfs: List[pd.DataFrame],
    date_column: str = COLUMN_DATE,
    how: str = "outer",
) -> pd.DataFrame:
    """
    Merge multiple DataFrames on date column.
    
    Args:
        dfs: List of DataFrames to merge
        date_column: Column to merge on
        how: Merge type ("outer", "inner", "left", "right")
    
    Returns:
        Merged DataFrame
    """
    if not dfs:
        return pd.DataFrame()
    
    if len(dfs) == 1:
        return dfs[0]
    
    result = dfs[0]
    for df in dfs[1:]:
        result = pd.merge(result, df, on=date_column, how=how)
    
    return result.sort_values(date_column).reset_index(drop=True)

