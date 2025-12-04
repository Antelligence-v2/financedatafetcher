"""
Data validation module for the data-fetch framework.
Provides quality checks for extracted financial data.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Callable
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

from ..utils.logger import get_logger


@dataclass
class ValidationResult:
    """Result of data validation."""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    stats: Dict[str, Any] = field(default_factory=dict)
    
    def add_error(self, message: str):
        """Add an error (makes result invalid)."""
        self.errors.append(message)
        self.is_valid = False
    
    def add_warning(self, message: str):
        """Add a warning (doesn't affect validity)."""
        self.warnings.append(message)
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "is_valid": self.is_valid,
            "errors": self.errors,
            "warnings": self.warnings,
            "stats": self.stats,
        }


class DataValidator:
    """
    Validator for financial time-series data.
    Performs various quality checks on DataFrames.
    """
    
    def __init__(
        self,
        strict_mode: bool = False,
        date_column: str = "date",
        numeric_columns: Optional[List[str]] = None,
    ):
        """
        Initialize the validator.
        
        Args:
            strict_mode: If True, warnings become errors
            date_column: Name of the date column
            numeric_columns: List of numeric column names to validate
        """
        self.strict_mode = strict_mode
        self.date_column = date_column
        self.numeric_columns = numeric_columns or []
        self.logger = get_logger()
        
        # Custom validators
        self._custom_validators: List[Callable[[pd.DataFrame], List[str]]] = []
    
    def add_validator(self, validator: Callable[[pd.DataFrame], List[str]]):
        """Add a custom validator function."""
        self._custom_validators.append(validator)
    
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        """
        Validate a DataFrame.
        
        Args:
            df: DataFrame to validate
        
        Returns:
            ValidationResult with errors, warnings, and stats
        """
        result = ValidationResult(is_valid=True)
        
        if df is None or df.empty:
            result.add_error("DataFrame is empty or None")
            return result
        
        # Collect stats
        result.stats = {
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": list(df.columns),
        }
        
        # Run all validations
        self._check_required_columns(df, result)
        self._check_duplicates(df, result)
        self._check_null_values(df, result)
        self._check_date_column(df, result)
        self._check_numeric_columns(df, result)
        self._check_outliers(df, result)
        self._check_date_continuity(df, result)
        
        # Run custom validators
        for validator in self._custom_validators:
            try:
                issues = validator(df)
                for issue in issues:
                    if self.strict_mode:
                        result.add_error(issue)
                    else:
                        result.add_warning(issue)
            except Exception as e:
                result.add_warning(f"Custom validator failed: {e}")
        
        self.logger.info(
            f"Validation complete: valid={result.is_valid}, "
            f"errors={len(result.errors)}, warnings={len(result.warnings)}"
        )
        
        return result
    
    def _check_required_columns(self, df: pd.DataFrame, result: ValidationResult):
        """Check that required columns exist."""
        if self.date_column and self.date_column not in df.columns:
            # Try to find a date-like column
            date_candidates = [c for c in df.columns if "date" in c.lower() or "time" in c.lower()]
            if date_candidates:
                result.add_warning(
                    f"Expected date column '{self.date_column}' not found. "
                    f"Found candidates: {date_candidates}"
                )
            else:
                result.add_warning(f"No date column found (expected '{self.date_column}')")
    
    def _check_duplicates(self, df: pd.DataFrame, result: ValidationResult):
        """Check for duplicate rows."""
        try:
            dup_count = df.duplicated().sum()
            if dup_count > 0:
                result.add_warning(f"Found {dup_count} duplicate rows")
                result.stats["duplicate_count"] = dup_count
        except (TypeError, ValueError) as e:
            # DataFrame contains unhashable types (lists, dicts)
            result.add_warning(f"Cannot check for duplicates: DataFrame contains unhashable types")
            self.logger.debug(f"Duplicate check failed: {e}")
        
        # Check for duplicate dates
        if self.date_column and self.date_column in df.columns:
            try:
                date_dups = df[self.date_column].duplicated().sum()
                if date_dups > 0:
                    result.add_warning(f"Found {date_dups} duplicate dates")
                    result.stats["duplicate_dates"] = date_dups
            except (TypeError, ValueError):
                # Date column might contain unhashable types
                pass
    
    def _check_null_values(self, df: pd.DataFrame, result: ValidationResult):
        """Check for null/NaN values."""
        null_counts = df.isnull().sum()
        cols_with_nulls = null_counts[null_counts > 0]
        
        if not cols_with_nulls.empty:
            result.stats["null_counts"] = cols_with_nulls.to_dict()
            
            for col, count in cols_with_nulls.items():
                pct = count / len(df) * 100
                if pct > 50:
                    result.add_error(f"Column '{col}' has {pct:.1f}% null values")
                elif pct > 10:
                    result.add_warning(f"Column '{col}' has {pct:.1f}% null values ({count} rows)")
    
    def _check_date_column(self, df: pd.DataFrame, result: ValidationResult):
        """Validate the date column."""
        if self.date_column not in df.columns:
            return
        
        date_col = df[self.date_column]
        
        # Check if it's datetime type
        if not pd.api.types.is_datetime64_any_dtype(date_col):
            try:
                pd.to_datetime(date_col)
            except Exception:
                result.add_warning(f"Column '{self.date_column}' cannot be parsed as datetime")
                return
        
        # Get date range
        try:
            dates = pd.to_datetime(date_col.dropna())
            if len(dates) > 0:
                result.stats["date_range"] = {
                    "min": str(dates.min()),
                    "max": str(dates.max()),
                    "span_days": (dates.max() - dates.min()).days,
                }
                
                # Check for future dates
                future_dates = dates[dates > datetime.now()]
                if len(future_dates) > 0:
                    result.add_warning(f"Found {len(future_dates)} future dates")
                
                # Check for very old dates (before 2000)
                old_dates = dates[dates < datetime(2000, 1, 1)]
                if len(old_dates) > 0:
                    result.add_warning(f"Found {len(old_dates)} dates before 2000")
        except Exception as e:
            result.add_warning(f"Error analyzing dates: {e}")
    
    def _check_numeric_columns(self, df: pd.DataFrame, result: ValidationResult):
        """Validate numeric columns."""
        numeric_cols = self.numeric_columns or df.select_dtypes(include=[np.number]).columns.tolist()
        
        for col in numeric_cols:
            if col not in df.columns:
                continue
            
            series = df[col]
            
            # Try to convert to numeric if not already
            if not pd.api.types.is_numeric_dtype(series):
                try:
                    series = pd.to_numeric(series, errors="coerce")
                except Exception:
                    continue
            
            # Check for negative values (often invalid for volumes, prices)
            neg_count = (series < 0).sum()
            if neg_count > 0:
                result.add_warning(f"Column '{col}' has {neg_count} negative values")
            
            # Collect stats
            if col not in result.stats.get("numeric_stats", {}):
                result.stats.setdefault("numeric_stats", {})[col] = {
                    "min": float(series.min()) if not pd.isna(series.min()) else None,
                    "max": float(series.max()) if not pd.isna(series.max()) else None,
                    "mean": float(series.mean()) if not pd.isna(series.mean()) else None,
                    "std": float(series.std()) if not pd.isna(series.std()) else None,
                }
    
    def _check_outliers(self, df: pd.DataFrame, result: ValidationResult):
        """Check for outliers in numeric columns using IQR method."""
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        
        for col in numeric_cols:
            series = df[col].dropna()
            if len(series) < 10:
                continue
            
            Q1 = series.quantile(0.25)
            Q3 = series.quantile(0.75)
            IQR = Q3 - Q1
            
            lower_bound = Q1 - 3 * IQR
            upper_bound = Q3 + 3 * IQR
            
            outliers = series[(series < lower_bound) | (series > upper_bound)]
            
            if len(outliers) > 0:
                pct = len(outliers) / len(series) * 100
                if pct > 5:
                    result.add_warning(
                        f"Column '{col}' has {len(outliers)} potential outliers ({pct:.1f}%)"
                    )
                result.stats.setdefault("outliers", {})[col] = len(outliers)
    
    def _check_date_continuity(self, df: pd.DataFrame, result: ValidationResult):
        """Check for gaps in date sequence."""
        if self.date_column not in df.columns:
            return
        
        try:
            dates = pd.to_datetime(df[self.date_column].dropna()).sort_values()
            if len(dates) < 2:
                return
            
            # Calculate expected frequency
            diffs = dates.diff().dropna()
            median_diff = diffs.median()
            
            # Find gaps larger than 2x median
            gaps = diffs[diffs > 2 * median_diff]
            
            if len(gaps) > 0:
                gap_count = len(gaps)
                result.add_warning(f"Found {gap_count} date gaps (>2x expected frequency)")
                result.stats["date_gaps"] = gap_count
                
                # Report largest gaps
                if len(gaps) <= 5:
                    for idx, gap in gaps.items():
                        result.stats.setdefault("largest_gaps", []).append({
                            "after_date": str(dates.loc[idx - 1] if idx - 1 in dates.index else "N/A"),
                            "gap_days": gap.days if hasattr(gap, "days") else str(gap),
                        })
        except Exception as e:
            self.logger.debug(f"Error checking date continuity: {e}")


def validate_financial_data(
    df: pd.DataFrame,
    date_column: str = "date",
    strict: bool = False,
) -> ValidationResult:
    """
    Convenience function to validate financial data.
    
    Args:
        df: DataFrame to validate
        date_column: Name of the date column
        strict: If True, use strict validation mode
    
    Returns:
        ValidationResult
    """
    validator = DataValidator(
        strict_mode=strict,
        date_column=date_column,
    )
    return validator.validate(df)

