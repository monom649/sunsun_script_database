#!/usr/bin/env python3
"""
Configuration module for secure Google Sheets access.

Validates environment variables without exposing sensitive values.
Ensures minimum required configuration for spreadsheet access.
"""

import os
import sys
from pathlib import Path
from typing import Dict, Optional


class ConfigError(Exception):
    """Configuration validation error."""
    pass


class Config:
    """Secure configuration management."""
    
    # Required environment variables
    REQUIRED_VARS = {
        'GOOGLE_APPLICATION_CREDENTIALS': 'Path to service account JSON file',
        'SHEET_ID': 'Google Sheets document ID',
        'WORKSHEET_NAME': 'Worksheet name within the spreadsheet'
    }
    
    # Optional environment variables with defaults
    OPTIONAL_VARS = {
        'SHEETS_SCOPE': 'https://www.googleapis.com/auth/spreadsheets.readonly',
        'REQUEST_TIMEOUT': '10',
        'RETRY_MAX_ATTEMPTS': '3',
        'RETRY_INITIAL_DELAY': '5',
        'WATCH_INTERVAL': '300',
        'JITTER_PERCENT': '10'
    }
    
    def __init__(self):
        """Initialize configuration from environment variables."""
        self._config = {}
        self._validate_environment()
        
    def _validate_environment(self) -> None:
        """Validate all required environment variables."""
        missing_vars = []
        
        # Check required variables
        for var_name, description in self.REQUIRED_VARS.items():
            value = os.getenv(var_name)
            if not value:
                missing_vars.append(f"{var_name} ({description})")
            else:
                self._config[var_name] = value
        
        if missing_vars:
            raise ConfigError(
                f"Missing required environment variables:\n" +
                "\n".join(f"  - {var}" for var in missing_vars) +
                "\n\nPlease set these variables or use .env file."
            )
        
        # Set optional variables with defaults
        for var_name, default_value in self.OPTIONAL_VARS.items():
            self._config[var_name] = os.getenv(var_name, default_value)
        
        # Additional validation
        self._validate_credentials_file()
        self._validate_numeric_settings()
    
    def _validate_credentials_file(self) -> None:
        """Validate service account credentials file exists."""
        creds_path = self._config['GOOGLE_APPLICATION_CREDENTIALS']
        
        if not Path(creds_path).exists():
            raise ConfigError(
                f"Service account credentials file not found. "
                f"Please ensure the file exists and GOOGLE_APPLICATION_CREDENTIALS "
                f"points to the correct path."
            )
        
        if not creds_path.endswith('.json'):
            raise ConfigError(
                "Service account credentials file must be a JSON file."
            )
    
    def _validate_numeric_settings(self) -> None:
        """Validate numeric configuration values."""
        numeric_vars = {
            'REQUEST_TIMEOUT': (1, 300),
            'RETRY_MAX_ATTEMPTS': (1, 10),
            'RETRY_INITIAL_DELAY': (1, 60),
            'WATCH_INTERVAL': (30, 3600),
            'JITTER_PERCENT': (0, 50)
        }
        
        for var_name, (min_val, max_val) in numeric_vars.items():
            try:
                value = int(self._config[var_name])
                if not (min_val <= value <= max_val):
                    raise ConfigError(
                        f"{var_name} must be between {min_val} and {max_val}, "
                        f"got: {value}"
                    )
                self._config[var_name] = value
            except ValueError:
                raise ConfigError(
                    f"{var_name} must be a valid integer, "
                    f"got: {self._config[var_name]}"
                )
    
    def get(self, key: str, default: Optional[str] = None) -> str:
        """Get configuration value safely."""
        return self._config.get(key, default)
    
    def get_int(self, key: str, default: Optional[int] = None) -> int:
        """Get integer configuration value."""
        value = self._config.get(key, default)
        return int(value) if value is not None else None
    
    @property
    def credentials_path(self) -> str:
        """Get service account credentials file path."""
        return self._config['GOOGLE_APPLICATION_CREDENTIALS']
    
    @property
    def sheet_id(self) -> str:
        """Get Google Sheets document ID."""
        return self._config['SHEET_ID']
    
    @property
    def worksheet_name(self) -> str:
        """Get worksheet name."""
        return self._config['WORKSHEET_NAME']
    
    @property
    def sheets_scope(self) -> str:
        """Get Google Sheets API scope."""
        return self._config['SHEETS_SCOPE']
    
    @property
    def request_timeout(self) -> int:
        """Get request timeout in seconds."""
        return self._config['REQUEST_TIMEOUT']
    
    @property
    def retry_max_attempts(self) -> int:
        """Get maximum retry attempts."""
        return self._config['RETRY_MAX_ATTEMPTS']
    
    @property
    def retry_initial_delay(self) -> int:
        """Get initial retry delay in seconds."""
        return self._config['RETRY_INITIAL_DELAY']
    
    @property
    def watch_interval(self) -> int:
        """Get watch mode interval in seconds."""
        return self._config['WATCH_INTERVAL']
    
    @property
    def jitter_percent(self) -> int:
        """Get jitter percentage for watch intervals."""
        return self._config['JITTER_PERCENT']
    
    def validate_sheet_access(self) -> Dict[str, str]:
        """Return sanitized configuration info for logging."""
        return {
            'sheet_id_prefix': self.sheet_id[:8] + '...',
            'worksheet_name': self.worksheet_name,
            'scope': self.sheets_scope,
            'timeout': str(self.request_timeout),
            'retry_attempts': str(self.retry_max_attempts)
        }


def load_config() -> Config:
    """Load and validate configuration."""
    try:
        return Config()
    except ConfigError as e:
        print(f"Configuration Error: {e}", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"Unexpected configuration error: {e}", file=sys.stderr)
        sys.exit(1)


def check_environment() -> bool:
    """Check if environment is properly configured."""
    try:
        Config()
        return True
    except ConfigError:
        return False