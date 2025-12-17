"""
AI-powered validation agent for data normalization.
Uses OpenAI API to validate that normalization doesn't lose data.
"""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import pandas as pd
import os
import json

from .data_normalizer import NormalizedDataPoint
from ..utils.logger import get_logger


@dataclass
class ValidationReport:
    """Validation report from AI agent."""
    passed: bool
    warnings: List[str]
    errors: List[str]
    raw_field_count: int
    normalized_metric_count: int
    missing_fields: List[str]
    suggestions: List[str]
    raw_sample: Optional[Dict[str, Any]] = None
    normalized_sample: Optional[Dict[str, Any]] = None


class AIValidator:
    """
    AI-powered validator for data normalization.
    Compares raw DataFrames to normalized data points to ensure no data loss.
    """
    
    def __init__(self, openai_api_key: Optional[str] = None):
        """
        Initialize AI validator.
        
        Args:
            openai_api_key: OpenAI API key (reads from env if not provided)
        """
        self.logger = get_logger()
        self.api_key = openai_api_key or os.getenv("OPENAI_API_KEY")
        self._client = None
        
        if not self.api_key:
            self.logger.warning("OpenAI API key not found. AI validation will be disabled.")
        else:
            try:
                import openai
                self._client = openai.OpenAI(api_key=self.api_key)
                self.logger.info("AI validator initialized with OpenAI API")
            except ImportError:
                self.logger.warning("OpenAI package not installed. Install with: pip install openai")
            except Exception as e:
                self.logger.warning(f"Failed to initialize OpenAI client: {str(e)}")
    
    def is_available(self) -> bool:
        """Check if AI validation is available."""
        return self._client is not None
    
    def validate_normalization(
        self,
        raw_df: pd.DataFrame,
        normalized_points: List[NormalizedDataPoint],
        source: str
    ) -> ValidationReport:
        """
        Validate that normalization doesn't lose data.
        
        Args:
            raw_df: Raw DataFrame from scraper
            normalized_points: List of normalized data points
            source: Source name (e.g., 'coinglass')
            
        Returns:
            ValidationReport with validation results
        """
        # Basic validation (always performed)
        basic_report = self._basic_validation(raw_df, normalized_points, source)
        
        # AI validation (if available)
        if self.is_available():
            try:
                ai_report = self._ai_validation(raw_df, normalized_points, source)
                # Merge reports
                return ValidationReport(
                    passed=basic_report.passed and ai_report.passed,
                    warnings=basic_report.warnings + ai_report.warnings,
                    errors=basic_report.errors + ai_report.errors,
                    raw_field_count=basic_report.raw_field_count,
                    normalized_metric_count=basic_report.normalized_metric_count,
                    missing_fields=list(set(basic_report.missing_fields + ai_report.missing_fields)),
                    suggestions=basic_report.suggestions + ai_report.suggestions,
                    raw_sample=basic_report.raw_sample,
                    normalized_sample=basic_report.normalized_sample,
                )
            except Exception as e:
                self.logger.error(f"AI validation failed: {str(e)}")
                # Return basic validation with warning
                basic_report.warnings.append(f"AI validation failed: {str(e)}. Using basic validation only.")
                return basic_report
        else:
            # Only basic validation
            basic_report.warnings.append("AI validation unavailable. Using basic validation only.")
            return basic_report
    
    def _basic_validation(
        self,
        raw_df: pd.DataFrame,
        normalized_points: List[NormalizedDataPoint],
        source: str
    ) -> ValidationReport:
        """
        Perform basic validation without AI.
        
        Args:
            raw_df: Raw DataFrame
            normalized_points: Normalized points
            source: Source name
            
        Returns:
            ValidationReport
        """
        warnings = []
        errors = []
        missing_fields = []
        suggestions = []
        
        # Count fields
        raw_field_count = len(raw_df.columns) if not raw_df.empty else 0
        normalized_metric_count = len(set(p.metric for p in normalized_points))
        
        # Check if we have data
        if raw_df.empty:
            errors.append("Raw DataFrame is empty")
            return ValidationReport(
                passed=False,
                warnings=warnings,
                errors=errors,
                raw_field_count=0,
                normalized_metric_count=0,
                missing_fields=missing_fields,
                suggestions=suggestions,
            )
        
        if not normalized_points:
            errors.append("No normalized data points created")
            return ValidationReport(
                passed=False,
                warnings=warnings,
                errors=errors,
                raw_field_count=raw_field_count,
                normalized_metric_count=0,
                missing_fields=missing_fields,
                suggestions=suggestions,
            )
        
        # Get sample data
        raw_sample = raw_df.iloc[0].to_dict() if len(raw_df) > 0 else {}
        normalized_sample = {
            'source': normalized_points[0].source,
            'asset': normalized_points[0].asset,
            'metric': normalized_points[0].metric,
            'value': normalized_points[0].value,
        } if normalized_points else {}
        
        # Check for numeric columns that might not be normalized
        numeric_cols = raw_df.select_dtypes(include=['number']).columns.tolist()
        timestamp_cols = [col for col in raw_df.columns if 'timestamp' in col.lower() or 'date' in col.lower()]
        excluded_cols = set(timestamp_cols)
        
        # Count potentially missing numeric fields
        normalized_metrics = set(p.metric for p in normalized_points)
        raw_numeric_fields = set(numeric_cols) - excluded_cols
        
        # Simple heuristic: if we have many raw fields but few normalized metrics, warn
        if len(raw_numeric_fields) > normalized_metric_count * 2:
            warnings.append(
                f"Raw data has {len(raw_numeric_fields)} numeric fields but only "
                f"{normalized_metric_count} normalized metrics. Some fields may not be normalized."
            )
        
        # Check if key fields are present
        if 'timestamp' not in raw_df.columns and 'date' not in raw_df.columns:
            warnings.append("No timestamp/date column found in raw data")
        
        passed = len(errors) == 0
        
        return ValidationReport(
            passed=passed,
            warnings=warnings,
            errors=errors,
            raw_field_count=raw_field_count,
            normalized_metric_count=normalized_metric_count,
            missing_fields=missing_fields,
            suggestions=suggestions,
            raw_sample=raw_sample,
            normalized_sample=normalized_sample,
        )
    
    def _ai_validation(
        self,
        raw_df: pd.DataFrame,
        normalized_points: List[NormalizedDataPoint],
        source: str
    ) -> ValidationReport:
        """
        Perform AI-powered validation.
        
        Args:
            raw_df: Raw DataFrame
            normalized_points: Normalized points
            source: Source name
            
        Returns:
            ValidationReport
        """
        if not self._client:
            return ValidationReport(
                passed=True,
                warnings=[],
                errors=[],
                raw_field_count=len(raw_df.columns),
                normalized_metric_count=len(set(p.metric for p in normalized_points)),
                missing_fields=[],
                suggestions=[],
            )
        
        # Prepare data for AI analysis
        raw_summary = self._summarize_dataframe(raw_df)
        normalized_summary = self._summarize_normalized_points(normalized_points)
        
        # Create prompt
        prompt = f"""You are a data quality validator. Analyze if data normalization has lost any information.

RAW DATA SUMMARY:
{json.dumps(raw_summary, indent=2)}

NORMALIZED DATA SUMMARY:
{json.dumps(normalized_summary, indent=2)}

SOURCE: {source}

Please analyze:
1. Are all important numeric fields from raw data represented in normalized metrics?
2. Are there any fields that seem to be missing?
3. Are the value ranges reasonable (no unexpected nulls or zeros)?
4. Are there any data quality issues?

Respond in JSON format:
{{
    "passed": true/false,
    "warnings": ["warning1", "warning2"],
    "errors": ["error1", "error2"],
    "missing_fields": ["field1", "field2"],
    "suggestions": ["suggestion1", "suggestion2"]
}}"""
        
        try:
            response = self._client.chat.completions.create(
                model="gpt-4o-mini",  # Use cheaper model for validation
                messages=[
                    {"role": "system", "content": "You are a data quality validator. Respond only with valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,  # Low temperature for consistent validation
                max_tokens=500,
            )
            
            # Parse response
            response_text = response.choices[0].message.content.strip()
            
            # Try to extract JSON from response
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0].strip()
            
            ai_result = json.loads(response_text)
            
            return ValidationReport(
                passed=ai_result.get("passed", True),
                warnings=ai_result.get("warnings", []),
                errors=ai_result.get("errors", []),
                raw_field_count=len(raw_df.columns),
                normalized_metric_count=len(set(p.metric for p in normalized_points)),
                missing_fields=ai_result.get("missing_fields", []),
                suggestions=ai_result.get("suggestions", []),
            )
        
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse AI response as JSON: {str(e)}")
            return ValidationReport(
                passed=True,
                warnings=[f"AI validation response parsing failed: {str(e)}"],
                errors=[],
                raw_field_count=len(raw_df.columns),
                normalized_metric_count=len(set(p.metric for p in normalized_points)),
                missing_fields=[],
                suggestions=[],
            )
        except Exception as e:
            self.logger.error(f"AI validation error: {str(e)}")
            raise
    
    def _summarize_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Summarize DataFrame for AI analysis."""
        if df.empty:
            return {"empty": True}
        
        summary = {
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": list(df.columns),
            "numeric_columns": df.select_dtypes(include=['number']).columns.tolist(),
            "sample_values": {},
        }
        
        # Add sample values for first row
        if len(df) > 0:
            first_row = df.iloc[0]
            for col in df.columns[:10]:  # Limit to first 10 columns
                value = first_row[col]
                if pd.notna(value):
                    summary["sample_values"][col] = str(value)[:100]  # Truncate long values
        
        return summary
    
    def _summarize_normalized_points(self, points: List[NormalizedDataPoint]) -> Dict[str, Any]:
        """Summarize normalized points for AI analysis."""
        if not points:
            return {"empty": True}
        
        metrics = set(p.metric for p in points)
        assets = set(p.asset for p in points)
        categories = set(p.category for p in points)
        
        summary = {
            "point_count": len(points),
            "unique_metrics": sorted(list(metrics)),
            "unique_assets": sorted(list(assets)),
            "unique_categories": sorted(list(categories)),
            "sample_point": {
                "source": points[0].source,
                "asset": points[0].asset,
                "metric": points[0].metric,
                "category": points[0].category,
                "value": points[0].value,
                "unit": points[0].unit,
            } if points else None,
        }
        
        return summary

