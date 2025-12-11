"""
Structured logging module for the data-fetch framework.
Provides consistent logging across all modules with optional JSON output.
"""

import logging
import sys
import json
from datetime import datetime
from typing import Optional
from pathlib import Path


class JsonFormatter(logging.Formatter):
    """Custom formatter that outputs logs as JSON."""
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "message": record.getMessage(),
        }
        
        # Add extra fields if present
        if hasattr(record, "extra_data"):
            log_data["data"] = record.extra_data
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_data)


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colored output for terminal."""
    
    COLORS = {
        "DEBUG": "\033[36m",      # Cyan
        "INFO": "\033[32m",       # Green
        "WARNING": "\033[33m",    # Yellow
        "ERROR": "\033[31m",      # Red
        "CRITICAL": "\033[35m",   # Magenta
    }
    RESET = "\033[0m"
    
    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, self.RESET)
        record.levelname = f"{color}{record.levelname}{self.RESET}"
        return super().format(record)


# Global logger instance
_logger: Optional[logging.Logger] = None


def setup_logging(
    level: str = "INFO",
    json_output: bool = False,
    log_file: Optional[Path] = None,
) -> logging.Logger:
    """
    Setup the global logger for the data-fetch framework.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_output: If True, output logs as JSON (useful for production)
        log_file: Optional path to write logs to a file
    
    Returns:
        Configured logger instance
    """
    global _logger
    
    # Create logger
    logger = logging.getLogger("data_fetch")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    
    if json_output:
        console_handler.setFormatter(JsonFormatter())
    else:
        # Use colored formatter for terminal
        format_str = "%(asctime)s | %(levelname)-8s | %(module)s:%(funcName)s:%(lineno)d | %(message)s"
        console_handler.setFormatter(ColoredFormatter(format_str, datefmt="%Y-%m-%d %H:%M:%S"))
    
    logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        log_file = Path(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(JsonFormatter())
        logger.addHandler(file_handler)
    
    _logger = logger
    return logger


def get_logger() -> logging.Logger:
    """
    Get the global logger instance.
    Creates a default logger if not already configured.
    
    Returns:
        Logger instance
    """
    global _logger
    
    if _logger is None:
        _logger = setup_logging()
    
    return _logger


class LoggerAdapter(logging.LoggerAdapter):
    """
    Logger adapter that allows adding extra context to log messages.
    """
    
    def process(self, msg, kwargs):
        # Add extra data to the log record
        extra = kwargs.get("extra", {})
        if self.extra:
            extra.update(self.extra)
        kwargs["extra"] = extra
        return msg, kwargs


def get_context_logger(context: dict) -> LoggerAdapter:
    """
    Get a logger adapter with additional context.
    Useful for adding site_id, url, or other context to all log messages.
    
    Args:
        context: Dictionary of context to add to all log messages
    
    Returns:
        LoggerAdapter with context
    """
    return LoggerAdapter(get_logger(), context)

