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
class ValidationProfile:
    """Validation profile for site-specific validation rules."""
    name: str
    require_date_column: bool = False
    allow_negative_volumes: bool = False
    allow_negative_oi: bool = False
    allow_negative_prices: bool = False
    min_rows: int = 1
    max_rows: Optional[int] = None
    skip_date_continuity: bool = False
    custom_validators: List[Callable] = field(default_factory=list)
    
    def __post_init__(self):
        """Initialize custom validators list if not provided."""
        if self.custom_validators is None:
            self.custom_validators = []


# Predefined validation profiles
SNAPSHOT_PROFILE = ValidationProfile(
    name="snapshot",
    require_date_column=False,
    allow_negative_volumes=False,
    allow_negative_oi=False,
    allow_negative_prices=False,
    min_rows=1,
    max_rows=1,
    skip_date_continuity=True,
)

TIME_SERIES_PROFILE = ValidationProfile(
    name="time_series",
    require_date_column=True,
    allow_negative_volumes=False,
    allow_negative_oi=False,
    allow_negative_prices=False,
    min_rows=1,
    max_rows=None,
    skip_date_continuity=False,
)

CRYPTO_METRICS_PROFILE = ValidationProfile(
    name="crypto_metrics",
    require_date_column=False,
    allow_negative_volumes=False,
    allow_negative_oi=False,
    allow_negative_prices=False,
    min_rows=1,
    max_rows=1,
    skip_date_continuity=True,
    # Allow negative net flows
    custom_validators=[],
)

# Market sentiment profile (for FRED economic indicators)
MARKET_SENTIMENT_PROFILE = ValidationProfile(
    name="market_sentiment",
    require_date_column=True,
    allow_negative_volumes=True,  # Economic indicators can be negative
    allow_negative_oi=True,
    allow_negative_prices=True,  # Some indicators can be negative
    min_rows=1,
    max_rows=None,  # Time series can have many rows
    skip_date_continuity=False,  # Check date continuity for time series
    custom_validators=[],
)

# Profile mapping by site_id prefix
PROFILE_MAP = {
    "coinglass": CRYPTO_METRICS_PROFILE,
    "dune": SNAPSHOT_PROFILE,  # Dune queries can return time-series or snapshot
    "theblock": TIME_SERIES_PROFILE,
    "coingecko": TIME_SERIES_PROFILE,
    "cryptocompare": TIME_SERIES_PROFILE,
    "alphavantage": SNAPSHOT_PROFILE,
    "invezz": CRYPTO_METRICS_PROFILE,
    "bitcoin_com": CRYPTO_METRICS_PROFILE,
    "fred": MARKET_SENTIMENT_PROFILE,
}


def get_validation_profile(site_id: Optional[str] = None) -> ValidationProfile:
    """
    Get validation profile for a site.
    
    Args:
        site_id: Site ID to get profile for
    
    Returns:
        ValidationProfile instance
    """
    if not site_id:
        return SNAPSHOT_PROFILE
    
    # Check profile map by prefix
    for prefix, profile in PROFILE_MAP.items():
        if site_id.startswith(prefix):
            return profile
    
    # Default to snapshot profile
    return SNAPSHOT_PROFILE


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
        date_column: Optional[str] = "date",
        numeric_columns: Optional[List[str]] = None,
        require_date_column: bool = False,
        validation_profile: Optional[ValidationProfile] = None,
    ):
        """
        Initialize the validator.
        
        Args:
            strict_mode: If True, warnings become errors
            date_column: Name of the date column (None to skip date checks)
            numeric_columns: List of numeric column names to validate
            require_date_column: If True, missing date column is an error
            validation_profile: Validation profile to use (overrides other settings)
        """
        self.strict_mode = strict_mode
        self.date_column = date_column
        self.numeric_columns = numeric_columns or []
        self.require_date_column = require_date_column
        self.logger = get_logger()
        
        # Use validation profile if provided
        self.profile = validation_profile
        if self.profile:
            self.require_date_column = self.profile.require_date_column
            if not self.date_column and self.profile.require_date_column:
                self.date_column = "date"
        
        # Custom validators
        self._custom_validators: List[Callable[[pd.DataFrame], List[str]]] = []
        if self.profile and self.profile.custom_validators:
            self._custom_validators.extend(self.profile.custom_validators)
    
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
        
        # Check row count constraints from profile
        if self.profile:
            if len(df) < self.profile.min_rows:
                result.add_error(
                    f"DataFrame has {len(df)} rows, minimum required: {self.profile.min_rows}"
                )
            if self.profile.max_rows and len(df) > self.profile.max_rows:
                result.add_warning(
                    f"DataFrame has {len(df)} rows, expected maximum: {self.profile.max_rows}"
                )
        
        # Run all validations
        self._check_required_columns(df, result)
        self._check_duplicates(df, result)
        self._check_null_values(df, result)
        self._check_date_column(df, result)
        self._check_numeric_columns(df, result)
        self._check_outliers(df, result)
        
        # Skip date continuity if profile says so
        if not (self.profile and self.profile.skip_date_continuity):
            self._check_date_continuity(df, result)
        
        # Financial-specific validations
        self._validate_price_ranges(df, result)
        self._validate_currency_formats(df, result)
        self._validate_volumes(df, result)
        self._validate_percentages(df, result)
        self._validate_ohlc_data(df, result)
        self._detect_anomalies(df, result)
        
        # Calculate data quality score
        result.stats["quality_score"] = self._calculate_quality_score(result)
        
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
            f"errors={len(result.errors)}, warnings={len(result.warnings)}, "
            f"quality_score={result.stats.get('quality_score', 'N/A')}"
        )
        
        return result
    
    def _check_required_columns(self, df: pd.DataFrame, result: ValidationResult):
        """Check that required columns exist."""
        # Only check for date column if it's explicitly set and required
        if self.date_column and self.date_column not in df.columns:
            # Try to find a date-like column
            date_candidates = [c for c in df.columns if "date" in c.lower() or "time" in c.lower()]
            if date_candidates:
                if self.require_date_column:
                    result.add_error(
                        f"Required date column '{self.date_column}' not found. "
                        f"Found candidates: {date_candidates}"
                    )
                else:
                    result.add_warning(
                        f"Expected date column '{self.date_column}' not found. "
                        f"Found candidates: {date_candidates}"
                    )
            else:
                # Only error/warn if date_column is required
                if self.require_date_column:
                    result.add_error(f"Required date column '{self.date_column}' not found")
                # For non-time-series data, this is expected - only info level
                elif self.date_column != "date":  # Custom date column name
                    result.add_warning(f"No date column found (expected '{self.date_column}')")
                # For default "date" column in non-time-series data, this is normal
    
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
        # Skip if no date column is expected
        if not self.date_column or self.date_column not in df.columns:
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
            # Use profile settings if available
            allow_negative = False
            if self.profile:
                if "volume" in col.lower():
                    allow_negative = self.profile.allow_negative_volumes
                elif "oi" in col.lower() or "open" in col.lower() and "interest" in col.lower():
                    allow_negative = self.profile.allow_negative_oi
                elif "price" in col.lower():
                    allow_negative = self.profile.allow_negative_prices
                # Net inflow/outflow can be negative
                elif "inflow" in col.lower() or "outflow" in col.lower():
                    allow_negative = True
            
            neg_count = (series < 0).sum()
            if neg_count > 0:
                if allow_negative:
                    result.add_warning(
                        f"Column '{col}' has {neg_count} negative values (allowed by profile)"
                    )
                else:
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
    
    def _validate_price_ranges(self, df: pd.DataFrame, result: ValidationResult):
        """Validate price ranges and detect suspicious price jumps."""
        price_cols = [col for col in df.columns if any(kw in col.lower() for kw in ["price", "close", "open", "high", "low"])]
        
        for col in price_cols:
            if col not in df.columns:
                continue
            
            series = pd.to_numeric(df[col], errors="coerce").dropna()
            if len(series) < 2:
                continue
            
            # Check for negative prices (usually invalid)
            neg_count = (series < 0).sum()
            if neg_count > 0:
                result.add_warning(f"Column '{col}' has {neg_count} negative prices")
            
            # Detect suspicious price jumps (>50% change)
            pct_change = series.pct_change().abs()
            large_jumps = (pct_change > 0.5).sum()
            if large_jumps > 0:
                result.add_warning(
                    f"Column '{col}' has {large_jumps} suspicious price jumps (>50% change)"
                )
                result.stats.setdefault("price_jumps", {})[col] = large_jumps
            
            # Check for prices that are too high/low compared to historical data
            if len(series) > 10:
                mean_price = series.mean()
                std_price = series.std()
                outliers = series[(series < mean_price - 3 * std_price) | (series > mean_price + 3 * std_price)]
                if len(outliers) > len(series) * 0.1:  # More than 10% outliers
                    result.add_warning(
                        f"Column '{col}' has {len(outliers)} extreme price outliers"
                    )
    
    def _validate_currency_formats(self, df: pd.DataFrame, result: ValidationResult):
        """Validate currency formats and consistency."""
        price_cols = [col for col in df.columns if any(kw in col.lower() for kw in ["price", "amount", "value", "cost"])]
        
        if not price_cols:
            return
        
        # Check for mixed currency symbols (if data is still in string format)
        currency_symbols = set()
        for col in price_cols:
            if df[col].dtype == object:
                sample = df[col].dropna().head(10)
                for val in sample:
                    val_str = str(val)
                    for symbol in ["$", "€", "£", "¥", "₹"]:
                        if symbol in val_str:
                            currency_symbols.add(symbol)
        
        if len(currency_symbols) > 1:
            result.add_warning(
                f"Mixed currency symbols detected: {currency_symbols}. "
                "Data may need normalization."
            )
            result.stats["mixed_currencies"] = list(currency_symbols)
    
    def _validate_volumes(self, df: pd.DataFrame, result: ValidationResult):
        """Validate volume data (should be positive)."""
        volume_cols = [col for col in df.columns if "volume" in col.lower()]
        
        # Check if negative volumes are allowed by profile
        allow_negative = False
        if self.profile:
            allow_negative = self.profile.allow_negative_volumes
        
        for col in volume_cols:
            if col not in df.columns:
                continue
            
            series = pd.to_numeric(df[col], errors="coerce").dropna()
            if len(series) == 0:
                continue
            
            # Check for negative volumes
            neg_count = (series < 0).sum()
            if neg_count > 0:
                if allow_negative:
                    result.add_warning(
                        f"Column '{col}' has {neg_count} negative volumes (allowed by profile)"
                    )
                else:
                    result.add_error(f"Column '{col}' has {neg_count} negative volumes (invalid)")
            
            # Check for zero volumes (might indicate data issues)
            zero_count = (series == 0).sum()
            if zero_count > len(series) * 0.1:  # More than 10% zeros
                result.add_warning(
                    f"Column '{col}' has {zero_count} zero volumes ({zero_count/len(series)*100:.1f}%)"
                )
    
    def _validate_percentages(self, df: pd.DataFrame, result: ValidationResult):
        """Validate percentage values are within reasonable bounds."""
        pct_cols = [col for col in df.columns if any(kw in col.lower() for kw in ["percent", "pct", "change", "return", "yield"])]
        
        for col in pct_cols:
            if col not in df.columns:
                continue
            
            series = pd.to_numeric(df[col], errors="coerce").dropna()
            if len(series) == 0:
                continue
            
            # Check for extreme percentages (outside -100% to +1000% range)
            extreme_low = (series < -100).sum()
            extreme_high = (series > 1000).sum()
            
            if extreme_low > 0:
                result.add_warning(
                    f"Column '{col}' has {extreme_low} values below -100%"
                )
            if extreme_high > 0:
                result.add_warning(
                    f"Column '{col}' has {extreme_high} values above 1000%"
                )
    
    def _validate_ohlc_data(self, df: pd.DataFrame, result: ValidationResult):
        """Validate OHLC (Open/High/Low/Close) data relationships."""
        # Find OHLC columns
        ohlc_map = {}
        for col in df.columns:
            col_lower = col.lower()
            if "open" in col_lower and "open" not in ohlc_map:
                ohlc_map["open"] = col
            elif "high" in col_lower and "high" not in ohlc_map:
                ohlc_map["high"] = col
            elif "low" in col_lower and "low" not in ohlc_map:
                ohlc_map["low"] = col
            elif "close" in col_lower and "close" not in ohlc_map:
                ohlc_map["close"] = col
        
        # Need at least High and Low for basic validation
        if "high" not in ohlc_map or "low" not in ohlc_map:
            return
        
        high_col = ohlc_map["high"]
        low_col = ohlc_map["low"]
        
        high_series = pd.to_numeric(df[high_col], errors="coerce")
        low_series = pd.to_numeric(df[low_col], errors="coerce")
        
        # High >= Low (always)
        invalid = (high_series < low_series).sum()
        if invalid > 0:
            result.add_error(
                f"OHLC validation failed: {invalid} rows where High < Low (impossible)"
            )
            result.stats["ohlc_errors"] = invalid
        
        # Open/Close should be within High/Low range
        if "open" in ohlc_map:
            open_series = pd.to_numeric(df[ohlc_map["open"]], errors="coerce")
            invalid_open = (
                (open_series > high_series) | (open_series < low_series)
            ).sum()
            if invalid_open > 0:
                result.add_warning(
                    f"OHLC validation: {invalid_open} rows where Open is outside High/Low range"
                )
        
        if "close" in ohlc_map:
            close_series = pd.to_numeric(df[ohlc_map["close"]], errors="coerce")
            invalid_close = (
                (close_series > high_series) | (close_series < low_series)
            ).sum()
            if invalid_close > 0:
                result.add_warning(
                    f"OHLC validation: {invalid_close} rows where Close is outside High/Low range"
                )
    
    def _detect_anomalies(self, df: pd.DataFrame, result: ValidationResult):
        """Detect anomalies in financial time series data."""
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        
        for col in numeric_cols:
            if col == self.date_column:
                continue
            
            series = df[col].dropna()
            if len(series) < 10:
                continue
            
            # Use Z-score for anomaly detection
            mean = series.mean()
            std = series.std()
            
            if std == 0:
                continue
            
            z_scores = (series - mean) / std
            anomalies = (z_scores.abs() > 3).sum()  # 3 standard deviations
            
            if anomalies > 0:
                pct = anomalies / len(series) * 100
                if pct > 5:  # More than 5% anomalies
                    result.add_warning(
                        f"Column '{col}' has {anomalies} potential anomalies ({pct:.1f}%)"
                    )
                    result.stats.setdefault("anomalies", {})[col] = anomalies
    
    def _calculate_quality_score(self, result: ValidationResult) -> float:
        """
        Calculate data quality score (0-100).
        Based on completeness, consistency, and validation results.
        """
        score = 100.0
        
        # Deduct points for errors (10 points each)
        score -= len(result.errors) * 10
        
        # Deduct points for warnings (2 points each)
        score -= len(result.warnings) * 2
        
        # Deduct points for null values
        if "null_counts" in result.stats:
            total_rows = result.stats.get("row_count", 1)
            for col, null_count in result.stats["null_counts"].items():
                null_pct = (null_count / total_rows) * 100
                score -= min(null_pct * 0.5, 10)  # Max 10 points per column
        
        # Deduct points for duplicates
        if "duplicate_count" in result.stats:
            dup_pct = (result.stats["duplicate_count"] / result.stats.get("row_count", 1)) * 100
            score -= min(dup_pct * 0.3, 5)
        
        # Ensure score is between 0 and 100
        return max(0.0, min(100.0, score))


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

