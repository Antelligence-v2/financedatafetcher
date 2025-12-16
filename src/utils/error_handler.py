"""
Error handler with classification and recovery strategies.
Provides intelligent error handling for web scraping operations.
"""

from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from enum import Enum

from .logger import get_logger


class ErrorType(Enum):
    """Types of errors that can occur during scraping."""
    NETWORK = "network"
    AUTH = "auth"
    RATE_LIMIT = "rate_limit"
    BOT_DETECTION = "bot_detection"
    PARSING = "parsing"
    VALIDATION = "validation"
    UNKNOWN = "unknown"


@dataclass
class ErrorRecovery:
    """Recovery strategy for an error."""
    error_type: ErrorType
    retry: bool
    retry_delay: float
    recovery_action: Optional[str] = None
    max_retries: int = 3
    suggestions: List[str] = None
    
    def __post_init__(self):
        if self.suggestions is None:
            self.suggestions = []


class ErrorHandler:
    """
    Error handler with classification and recovery strategies.
    """
    
    def __init__(self):
        self.logger = get_logger()
    
    def classify_error(self, error: Exception) -> ErrorType:
        """
        Classify an error to determine recovery strategy.
        
        Args:
            error: The exception that occurred
        
        Returns:
            ErrorType classification
        """
        error_str = str(error).lower()
        error_type = type(error).__name__
        
        # Network errors
        if any(x in error_type for x in ["Connection", "Timeout", "HTTPError", "RequestException"]):
            if "429" in error_str or "rate limit" in error_str:
                return ErrorType.RATE_LIMIT
            if "403" in error_str or "forbidden" in error_str:
                return ErrorType.BOT_DETECTION
            if "401" in error_str or "unauthorized" in error_str:
                return ErrorType.AUTH
            return ErrorType.NETWORK
        
        # Authentication errors
        if "401" in error_str or "unauthorized" in error_str or "authentication" in error_str:
            return ErrorType.AUTH
        
        # Rate limit errors
        if "429" in error_str or "rate limit" in error_str or "too many requests" in error_str:
            return ErrorType.RATE_LIMIT
        
        # Bot detection errors
        if any(x in error_str for x in ["cloudflare", "captcha", "blocked", "403", "forbidden"]):
            return ErrorType.BOT_DETECTION
        
        # Query timeout errors (for Dune)
        if "timeout" in error_str and ("query" in error_str or "execution" in error_str):
            return ErrorType.RATE_LIMIT  # Treat as rate limit issue
        
        # Selector not found errors (for DOM extraction)
        if "selector" in error_str and ("not found" in error_str or "timeout" in error_str):
            return ErrorType.PARSING
        
        # Parsing errors
        if any(x in error_type for x in ["JSONDecode", "ValueError", "KeyError", "AttributeError", "ParseError"]):
            return ErrorType.PARSING
        
        # Validation errors
        if "validation" in error_str or "invalid" in error_str:
            return ErrorType.VALIDATION
        
        return ErrorType.UNKNOWN
    
    def get_recovery_strategy(self, error_type: ErrorType) -> ErrorRecovery:
        """
        Get recovery strategy for an error type.
        
        Args:
            error_type: Classified error type
        
        Returns:
            ErrorRecovery strategy
        """
        strategies = {
            ErrorType.NETWORK: ErrorRecovery(
                error_type=ErrorType.NETWORK,
                retry=True,
                retry_delay=2.0,
                max_retries=3,
                recovery_action="retry_with_backoff",
                suggestions=[
                    "Check internet connection",
                    "Verify URL is accessible",
                    "Try again later",
                ],
            ),
            ErrorType.AUTH: ErrorRecovery(
                error_type=ErrorType.AUTH,
                retry=True,
                retry_delay=5.0,
                max_retries=2,
                recovery_action="refresh_auth",
                suggestions=[
                    "Check API key is valid",
                    "Refresh authentication token",
                    "Verify credentials in configuration",
                ],
            ),
            ErrorType.RATE_LIMIT: ErrorRecovery(
                error_type=ErrorType.RATE_LIMIT,
                retry=True,
                retry_delay=60.0,
                max_retries=1,
                recovery_action="wait_and_retry",
                suggestions=[
                    "Wait before retrying",
                    "Reduce request frequency",
                    "Check rate limit configuration",
                ],
            ),
            ErrorType.BOT_DETECTION: ErrorRecovery(
                error_type=ErrorType.BOT_DETECTION,
                retry=True,
                retry_delay=30.0,
                max_retries=2,
                recovery_action="enable_stealth",
                suggestions=[
                    "Enable stealth mode",
                    "Use a proxy",
                    "Add delays between requests",
                    "Check robots.txt compliance",
                ],
            ),
            ErrorType.PARSING: ErrorRecovery(
                error_type=ErrorType.PARSING,
                retry=False,
                retry_delay=0.0,
                recovery_action="try_alternative_extractor",
                suggestions=[
                    "Try different extraction method",
                    "Check data format matches expected structure",
                    "Verify response content type",
                ],
            ),
            ErrorType.VALIDATION: ErrorRecovery(
                error_type=ErrorType.VALIDATION,
                retry=False,
                retry_delay=0.0,
                recovery_action="fix_data",
                suggestions=[
                    "Review extracted data",
                    "Check field mappings",
                    "Verify data schema",
                ],
            ),
            ErrorType.UNKNOWN: ErrorRecovery(
                error_type=ErrorType.UNKNOWN,
                retry=True,
                retry_delay=2.0,
                max_retries=2,
                recovery_action="retry",
                suggestions=[
                    "Check error message for details",
                    "Review logs for more information",
                ],
            ),
        }
        
        return strategies.get(error_type, strategies[ErrorType.UNKNOWN])
    
    def handle_error(
        self,
        error: Exception,
        context: Optional[Dict[str, Any]] = None,
    ) -> ErrorRecovery:
        """
        Handle an error and return recovery strategy.
        
        Args:
            error: The exception that occurred
            context: Additional context (URL, site_id, etc.)
        
        Returns:
            ErrorRecovery strategy
        """
        error_type = self.classify_error(error)
        recovery = self.get_recovery_strategy(error_type)
        
        self.logger.error(
            f"Error classified as {error_type.value}: {error}. "
            f"Recovery: {recovery.recovery_action}"
        )
        
        if recovery.suggestions:
            self.logger.info(f"Suggestions: {', '.join(recovery.suggestions)}")
        
        return recovery




