#!/usr/bin/env python3
"""
Secure Google Sheets data extraction module.

Provides minimal-privilege access to Google Sheets with proper error handling
and data filtering capabilities.
"""

import time
import random
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta

import gspread
from google.auth.exceptions import RefreshError, DefaultCredentialsError
from gspread.exceptions import APIError, SpreadsheetNotFound, WorksheetNotFound
from google.oauth2.service_account import Credentials

from config import Config
from logging_conf import get_logger, log_operation_start, log_operation_result, log_retry_attempt


class ExtractionError(Exception):
    """Base exception for extraction errors."""
    pass


class AuthenticationError(ExtractionError):
    """Authentication-related errors."""
    pass


class NetworkError(ExtractionError):
    """Network-related errors."""
    pass


class DataError(ExtractionError):
    """Data processing errors."""
    pass


class SheetsExtractor:
    """Secure Google Sheets data extractor."""
    
    def __init__(self, config: Config):
        """Initialize extractor with configuration."""
        self.config = config
        self.logger = get_logger()
        self._client = None
        self._worksheet = None
        self._last_access_time = None
        
    def _get_credentials(self) -> Credentials:
        """Get service account credentials."""
        try:
            credentials = Credentials.from_service_account_file(
                self.config.credentials_path,
                scopes=[self.config.sheets_scope]
            )
            return credentials
        except FileNotFoundError:
            raise AuthenticationError("Service account credentials file not found")
        except Exception as e:
            raise AuthenticationError(f"Failed to load credentials: {str(e)}")
    
    def _initialize_client(self) -> None:
        """Initialize Google Sheets client."""
        if self._client is not None:
            return
        
        try:
            credentials = self._get_credentials()
            self._client = gspread.authorize(credentials)
            self.logger.info("Google Sheets client initialized successfully")
        except (DefaultCredentialsError, RefreshError) as e:
            raise AuthenticationError(f"Authentication failed: {str(e)}")
        except Exception as e:
            raise ExtractionError(f"Failed to initialize client: {str(e)}")
    
    def _get_worksheet(self) -> gspread.Worksheet:
        """Get the target worksheet."""
        if self._worksheet is not None:
            return self._worksheet
        
        try:
            self._initialize_client()
            
            # Open spreadsheet
            spreadsheet = self._client.open_by_key(self.config.sheet_id)
            
            # Get specific worksheet
            self._worksheet = spreadsheet.worksheet(self.config.worksheet_name)
            
            self.logger.info(f"Opened worksheet: {self.config.worksheet_name}")
            return self._worksheet
            
        except SpreadsheetNotFound:
            raise DataError("Spreadsheet not found or access denied")
        except WorksheetNotFound:
            raise DataError(f"Worksheet '{self.config.worksheet_name}' not found")
        except APIError as e:
            if "PERMISSION_DENIED" in str(e):
                raise AuthenticationError("Permission denied - check service account access")
            elif "QUOTA_EXCEEDED" in str(e):
                raise NetworkError("API quota exceeded")
            else:
                raise NetworkError(f"Google Sheets API error: {str(e)}")
        except Exception as e:
            raise ExtractionError(f"Failed to access worksheet: {str(e)}")
    
    def _retry_with_backoff(
        self, 
        operation: Callable, 
        operation_name: str,
        *args, 
        **kwargs
    ) -> Any:
        """Execute operation with exponential backoff retry."""
        last_exception = None
        
        for attempt in range(1, self.config.retry_max_attempts + 1):
            try:
                result = operation(*args, **kwargs)
                if attempt > 1:
                    self.logger.info(f"Operation '{operation_name}' succeeded on attempt {attempt}")
                return result
                
            except AuthenticationError:
                # Don't retry authentication errors
                raise
            except (NetworkError, APIError, Exception) as e:
                last_exception = e
                
                if attempt < self.config.retry_max_attempts:
                    # Calculate delay with exponential backoff
                    delay = self.config.retry_initial_delay * (3 ** (attempt - 1))
                    
                    # Add jitter
                    jitter = random.uniform(0.8, 1.2)
                    delay *= jitter
                    
                    log_retry_attempt(attempt, self.config.retry_max_attempts, delay, str(e))
                    time.sleep(delay)
                else:
                    self.logger.error(f"Operation '{operation_name}' failed after {attempt} attempts")
        
        # All retries exhausted
        if isinstance(last_exception, APIError):
            if "QUOTA_EXCEEDED" in str(last_exception):
                raise NetworkError("API quota exceeded after retries")
            else:
                raise NetworkError(f"API error after retries: {str(last_exception)}")
        else:
            raise ExtractionError(f"Operation failed after retries: {str(last_exception)}")
    
    def _get_all_records(self) -> List[Dict[str, str]]:
        """Get all records from worksheet as dictionaries."""
        worksheet = self._get_worksheet()
        
        # Get all values including headers
        all_values = worksheet.get_all_values()
        
        if not all_values:
            raise DataError("Worksheet is empty")
        
        if len(all_values) < 2:
            raise DataError("Worksheet contains only headers")
        
        # First row is headers
        headers = all_values[0]
        
        # Convert rows to dictionaries
        records = []
        for row_values in all_values[1:]:
            # Pad row with empty strings if shorter than headers
            padded_row = row_values + [''] * (len(headers) - len(row_values))
            record = dict(zip(headers, padded_row))
            records.append(record)
        
        return records
    
    def extract_data(
        self,
        columns: Optional[List[str]] = None,
        filter_func: Optional[Callable[[Dict], bool]] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, str]]:
        """
        Extract data from Google Sheets with filtering.
        
        Args:
            columns: List of column names to include (None for all)
            filter_func: Function to filter records (None for no filtering)
            limit: Maximum number of records to return (None for no limit)
        
        Returns:
            List of dictionaries representing filtered records
        """
        log_operation_start(
            "data extraction",
            columns=columns,
            has_filter=filter_func is not None,
            limit=limit
        )
        
        try:
            # Get all records with retry logic
            records = self._retry_with_backoff(
                self._get_all_records,
                "get_all_records"
            )
            
            self.logger.info(f"Retrieved {len(records)} total records")
            
            # Apply filtering
            if filter_func:
                filtered_records = [record for record in records if filter_func(record)]
                self.logger.info(f"Filtered to {len(filtered_records)} records")
                records = filtered_records
            
            # Apply column selection
            if columns:
                # Validate column names
                if records:
                    available_columns = set(records[0].keys())
                    missing_columns = set(columns) - available_columns
                    if missing_columns:
                        raise DataError(f"Columns not found: {', '.join(missing_columns)}")
                
                # Filter columns
                records = [{col: record.get(col, '') for col in columns} for record in records]
                self.logger.info(f"Selected {len(columns)} columns")
            
            # Apply limit
            if limit and len(records) > limit:
                records = records[:limit]
                self.logger.info(f"Limited to {limit} records")
            
            # Update last access time
            self._last_access_time = datetime.now()
            
            log_operation_result("data extraction", True, len(records))
            return records
            
        except (AuthenticationError, DataError, NetworkError):
            raise
        except Exception as e:
            error_msg = f"Unexpected error during extraction: {str(e)}"
            log_operation_result("data extraction", False, error=error_msg)
            raise ExtractionError(error_msg)
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on the connection.
        
        Returns:
            Dictionary with health status information
        """
        log_operation_start("health check")
        
        health_status = {
            'timestamp': datetime.now().isoformat(),
            'status': 'unknown',
            'last_access': None,
            'error': None
        }
        
        try:
            # Try to access worksheet metadata
            worksheet = self._retry_with_backoff(
                self._get_worksheet,
                "health_check_access"
            )
            
            # Get basic worksheet info
            row_count = worksheet.row_count
            col_count = worksheet.col_count
            
            health_status.update({
                'status': 'healthy',
                'row_count': row_count,
                'col_count': col_count,
                'last_access': self._last_access_time.isoformat() if self._last_access_time else None
            })
            
            log_operation_result("health check", True)
            
        except AuthenticationError as e:
            health_status.update({
                'status': 'auth_error',
                'error': 'Authentication failed'
            })
            log_operation_result("health check", False, error=str(e))
            
        except (DataError, NetworkError) as e:
            health_status.update({
                'status': 'error',
                'error': str(e)
            })
            log_operation_result("health check", False, error=str(e))
            
        except Exception as e:
            error_msg = f"Health check failed: {str(e)}"
            health_status.update({
                'status': 'error',
                'error': 'Unexpected error'
            })
            log_operation_result("health check", False, error=error_msg)
        
        return health_status
    
    def close(self) -> None:
        """Clean up resources."""
        self._client = None
        self._worksheet = None
        self.logger.info("Extractor resources cleaned up")


# Utility functions for common filtering operations

def create_status_filter(status: str) -> Callable[[Dict], bool]:
    """Create a filter function for a specific status."""
    def filter_func(record: Dict) -> bool:
        return record.get('status', '').lower() == status.lower()
    return filter_func


def create_date_range_filter(
    date_column: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    date_format: str = '%Y-%m-%d'
) -> Callable[[Dict], bool]:
    """Create a filter function for date ranges."""
    def filter_func(record: Dict) -> bool:
        date_str = record.get(date_column, '')
        if not date_str:
            return False
        
        try:
            record_date = datetime.strptime(date_str, date_format)
            
            if start_date:
                start = datetime.strptime(start_date, date_format)
                if record_date < start:
                    return False
            
            if end_date:
                end = datetime.strptime(end_date, date_format)
                if record_date > end:
                    return False
            
            return True
        except ValueError:
            return False
    
    return filter_func


def create_text_search_filter(column: str, search_term: str, case_sensitive: bool = False) -> Callable[[Dict], bool]:
    """Create a filter function for text search."""
    def filter_func(record: Dict) -> bool:
        value = record.get(column, '')
        if not case_sensitive:
            value = value.lower()
            search_term_lower = search_term.lower()
            return search_term_lower in value
        else:
            return search_term in value
    return filter_func


def create_multi_value_filter(column: str, values: List[str]) -> Callable[[Dict], bool]:
    """Create a filter function that matches any of the provided values."""
    def filter_func(record: Dict) -> bool:
        record_value = record.get(column, '')
        return record_value in values
    return filter_func