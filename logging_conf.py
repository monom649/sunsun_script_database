#!/usr/bin/env python3
"""
Secure logging configuration for Google Sheets reader.

Ensures no sensitive information (credentials, tokens, IDs) is logged.
Provides structured logging with appropriate levels.
"""

import logging
import sys
import re
from typing import Dict, Any
from pathlib import Path


class SecureFormatter(logging.Formatter):
    """Custom formatter that sanitizes sensitive information."""
    
    # Patterns for sensitive data that should be redacted
    SENSITIVE_PATTERNS = [
        # Google Sheets IDs (alphanumeric strings of 30+ chars)
        (r'\b[a-zA-Z0-9_-]{30,}\b', '***SHEET_ID***'),
        # JSON keys and credentials
        (r'"[^"]*(?:key|token|secret|credential)[^"]*":\s*"[^"]*"', '"***REDACTED***"'),
        # File paths containing credentials
        (r'/[^/\s]*(?:key|credential|secret)[^/\s]*\.json', '/***CREDENTIALS***.json'),
        # URLs with tokens or sensitive parameters
        (r'https://[^/\s]*google[^/\s]*/[^\s]*[?&](?:key|token|access_token)=[^&\s]*', 'https://***REDACTED***/'),
        # Email addresses in service account format
        (r'[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+\.iam\.gserviceaccount\.com', '***SERVICE_ACCOUNT***@***.iam.gserviceaccount.com')
    ]
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record with sensitive data redaction."""
        # Get the original formatted message
        message = super().format(record)
        
        # Apply sensitive data redaction
        for pattern, replacement in self.SENSITIVE_PATTERNS:
            message = re.sub(pattern, replacement, message, flags=re.IGNORECASE)
        
        return message


class SecurityFilter(logging.Filter):
    """Filter to block messages containing sensitive patterns."""
    
    BLOCKED_PATTERNS = [
        # Block any message that might contain a full JSON credential
        r'{\s*"type":\s*"service_account"',
        # Block messages with actual credential file contents
        r'"private_key":\s*"-----BEGIN',
        # Block full OAuth tokens
        r'ya29\.[a-zA-Z0-9_-]+',
    ]
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Return False if the record contains sensitive patterns."""
        message = record.getMessage()
        
        for pattern in self.BLOCKED_PATTERNS:
            if re.search(pattern, message, re.IGNORECASE):
                # Log a warning about blocked sensitive content
                logging.getLogger('security').warning(
                    "Blocked log message containing sensitive credential data"
                )
                return False
        
        return True


def setup_logging(
    level: str = 'INFO',
    log_file: str = None,
    enable_console: bool = True
) -> logging.Logger:
    """
    Setup secure logging configuration.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file: Optional log file path
        enable_console: Whether to enable console logging
    
    Returns:
        Configured logger instance
    """
    # Create main logger
    logger = logging.getLogger('sheets_reader')
    logger.setLevel(getattr(logging, level.upper()))
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Create secure formatter
    formatter = SecureFormatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Create security filter
    security_filter = SecurityFilter()
    
    # Console handler
    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        console_handler.addFilter(security_filter)
        logger.addHandler(console_handler)
    
    # File handler
    if log_file:
        try:
            # Ensure log directory exists
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            file_handler.addFilter(security_filter)
            logger.addHandler(file_handler)
        except Exception as e:
            logger.warning(f"Failed to setup file logging: {e}")
    
    # Separate security logger for security events
    security_logger = logging.getLogger('security')
    security_logger.setLevel(logging.WARNING)
    
    if enable_console:
        security_handler = logging.StreamHandler(sys.stderr)
        security_handler.setFormatter(SecureFormatter(
            fmt='[SECURITY] %(asctime)s - %(levelname)s - %(message)s'
        ))
        security_logger.addHandler(security_handler)
    
    return logger


def get_logger(name: str = 'sheets_reader') -> logging.Logger:
    """Get a configured logger instance."""
    return logging.getLogger(name)


def log_config_summary(config_info: Dict[str, Any]) -> None:
    """Log configuration summary without sensitive data."""
    logger = get_logger()
    
    logger.info("Configuration loaded successfully")
    logger.info(f"Sheet ID prefix: {config_info.get('sheet_id_prefix', 'N/A')}")
    logger.info(f"Worksheet: {config_info.get('worksheet_name', 'N/A')}")
    logger.info(f"Scope: {config_info.get('scope', 'N/A')}")
    logger.info(f"Timeout: {config_info.get('timeout', 'N/A')}s")
    logger.info(f"Retry attempts: {config_info.get('retry_attempts', 'N/A')}")


def log_operation_start(operation: str, **kwargs) -> None:
    """Log the start of an operation with sanitized parameters."""
    logger = get_logger()
    
    # Sanitize parameters
    safe_params = {}
    for key, value in kwargs.items():
        if key in ['limit', 'page', 'filter', 'columns']:
            safe_params[key] = value
        else:
            safe_params[key] = '***REDACTED***'
    
    param_str = ', '.join(f"{k}={v}" for k, v in safe_params.items())
    logger.info(f"Starting {operation}" + (f" with {param_str}" if param_str else ""))


def log_operation_result(operation: str, success: bool, count: int = None, error: str = None) -> None:
    """Log the result of an operation."""
    logger = get_logger()
    
    if success:
        count_str = f" ({count} records)" if count is not None else ""
        logger.info(f"Completed {operation} successfully{count_str}")
    else:
        # Ensure error message doesn't contain sensitive data
        safe_error = error if error else "Unknown error"
        for pattern, replacement in SecureFormatter.SENSITIVE_PATTERNS:
            safe_error = re.sub(pattern, replacement, safe_error, flags=re.IGNORECASE)
        
        logger.error(f"Failed {operation}: {safe_error}")


def log_retry_attempt(attempt: int, max_attempts: int, delay: float, error: str) -> None:
    """Log retry attempt with sanitized error."""
    logger = get_logger()
    
    # Sanitize error message
    safe_error = error
    for pattern, replacement in SecureFormatter.SENSITIVE_PATTERNS:
        safe_error = re.sub(pattern, replacement, safe_error, flags=re.IGNORECASE)
    
    logger.warning(
        f"Attempt {attempt}/{max_attempts} failed, retrying in {delay:.1f}s: {safe_error}"
    )


def log_watch_cycle(interval: int, next_run: str, records_processed: int = None) -> None:
    """Log watch mode cycle information."""
    logger = get_logger()
    
    if records_processed is not None:
        logger.info(f"Watch cycle complete - processed {records_processed} records, next run: {next_run}")
    else:
        logger.info(f"Watch cycle starting - interval: {interval}s, next run: {next_run}")


# Configure third-party loggers to reduce noise
def configure_third_party_loggers():
    """Configure third-party library loggers."""
    # Reduce Google API client logging
    logging.getLogger('googleapiclient.discovery').setLevel(logging.WARNING)
    logging.getLogger('google.auth').setLevel(logging.WARNING)
    logging.getLogger('google_auth_httplib2').setLevel(logging.WARNING)
    logging.getLogger('httplib2').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)


# Auto-configure when module is imported
configure_third_party_loggers()