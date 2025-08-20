#!/usr/bin/env python3
"""
Tests for extractor module.

Tests spreadsheet extraction logic with mocked Google Sheets API.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound, APIError

from config import Config
from extractor import (
    SheetsExtractor, 
    ExtractionError, 
    AuthenticationError, 
    NetworkError, 
    DataError,
    create_status_filter,
    create_date_range_filter,
    create_text_search_filter,
    create_multi_value_filter
)


class TestSheetsExtractor:
    """Test SheetsExtractor class."""
    
    @pytest.fixture
    def mock_config(self):
        """Create a mock configuration."""
        config = Mock(spec=Config)
        config.credentials_path = '/path/to/credentials.json'
        config.sheet_id = 'test_sheet_id'
        config.worksheet_name = 'test_worksheet'
        config.sheets_scope = 'https://www.googleapis.com/auth/spreadsheets.readonly'
        config.request_timeout = 10
        config.retry_max_attempts = 3
        config.retry_initial_delay = 5
        return config
    
    @pytest.fixture
    def extractor(self, mock_config):
        """Create extractor instance with mock config."""
        return SheetsExtractor(mock_config)
    
    def test_initialization(self, mock_config):
        """Test extractor initialization."""
        extractor = SheetsExtractor(mock_config)
        assert extractor.config == mock_config
        assert extractor._client is None
        assert extractor._worksheet is None
    
    @patch('extractor.Credentials')
    def test_get_credentials_success(self, mock_credentials, extractor):
        """Test successful credentials loading."""
        mock_creds = Mock()
        mock_credentials.from_service_account_file.return_value = mock_creds
        
        result = extractor._get_credentials()
        
        assert result == mock_creds
        mock_credentials.from_service_account_file.assert_called_once_with(
            '/path/to/credentials.json',
            scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
        )
    
    @patch('extractor.Credentials')
    def test_get_credentials_file_not_found(self, mock_credentials, extractor):
        """Test credentials loading with missing file."""
        mock_credentials.from_service_account_file.side_effect = FileNotFoundError()
        
        with pytest.raises(AuthenticationError) as exc_info:
            extractor._get_credentials()
        
        assert "credentials file not found" in str(exc_info.value)
    
    @patch('extractor.gspread')
    @patch('extractor.Credentials')
    def test_initialize_client_success(self, mock_credentials, mock_gspread, extractor):
        """Test successful client initialization."""
        mock_creds = Mock()
        mock_credentials.from_service_account_file.return_value = mock_creds
        mock_client = Mock()
        mock_gspread.authorize.return_value = mock_client
        
        extractor._initialize_client()
        
        assert extractor._client == mock_client
        mock_gspread.authorize.assert_called_once_with(mock_creds)
    
    def test_get_worksheet_success(self, extractor):
        """Test successful worksheet access."""
        mock_client = Mock()
        mock_spreadsheet = Mock()
        mock_worksheet = Mock()
        
        extractor._client = mock_client
        mock_client.open_by_key.return_value = mock_spreadsheet
        mock_spreadsheet.worksheet.return_value = mock_worksheet
        
        result = extractor._get_worksheet()
        
        assert result == mock_worksheet
        assert extractor._worksheet == mock_worksheet
        mock_client.open_by_key.assert_called_once_with('test_sheet_id')
        mock_spreadsheet.worksheet.assert_called_once_with('test_worksheet')
    
    def test_get_worksheet_spreadsheet_not_found(self, extractor):
        """Test worksheet access with spreadsheet not found."""
        mock_client = Mock()
        mock_client.open_by_key.side_effect = SpreadsheetNotFound()
        extractor._client = mock_client
        
        with pytest.raises(DataError) as exc_info:
            extractor._get_worksheet()
        
        assert "not found or access denied" in str(exc_info.value)
    
    def test_get_worksheet_worksheet_not_found(self, extractor):
        """Test worksheet access with worksheet not found."""
        mock_client = Mock()
        mock_spreadsheet = Mock()
        mock_spreadsheet.worksheet.side_effect = WorksheetNotFound()
        mock_client.open_by_key.return_value = mock_spreadsheet
        extractor._client = mock_client
        
        with pytest.raises(DataError) as exc_info:
            extractor._get_worksheet()
        
        assert "Worksheet 'test_worksheet' not found" in str(exc_info.value)
    
    def test_get_worksheet_permission_denied(self, extractor):
        """Test worksheet access with permission denied."""
        mock_client = Mock()
        mock_client.open_by_key.side_effect = APIError(
            "PERMISSION_DENIED", "Permission denied"
        )
        extractor._client = mock_client
        
        with pytest.raises(AuthenticationError) as exc_info:
            extractor._get_worksheet()
        
        assert "Permission denied" in str(exc_info.value)
    
    def test_get_all_records_success(self, extractor):
        """Test successful record retrieval."""
        mock_worksheet = Mock()
        mock_worksheet.get_all_values.return_value = [
            ['Name', 'Status', 'Date'],
            ['Item 1', 'ACTIVE', '2023-01-01'],
            ['Item 2', 'INACTIVE', '2023-01-02']
        ]
        extractor._worksheet = mock_worksheet
        
        result = extractor._get_all_records()
        
        expected = [
            {'Name': 'Item 1', 'Status': 'ACTIVE', 'Date': '2023-01-01'},
            {'Name': 'Item 2', 'Status': 'INACTIVE', 'Date': '2023-01-02'}
        ]
        assert result == expected
    
    def test_get_all_records_empty_worksheet(self, extractor):
        """Test record retrieval from empty worksheet."""
        mock_worksheet = Mock()
        mock_worksheet.get_all_values.return_value = []
        extractor._worksheet = mock_worksheet
        
        with pytest.raises(DataError) as exc_info:
            extractor._get_all_records()
        
        assert "Worksheet is empty" in str(exc_info.value)
    
    def test_get_all_records_headers_only(self, extractor):
        """Test record retrieval with headers only."""
        mock_worksheet = Mock()
        mock_worksheet.get_all_values.return_value = [['Name', 'Status']]
        extractor._worksheet = mock_worksheet
        
        with pytest.raises(DataError) as exc_info:
            extractor._get_all_records()
        
        assert "contains only headers" in str(exc_info.value)
    
    @patch.object(SheetsExtractor, '_retry_with_backoff')
    def test_extract_data_basic(self, mock_retry, extractor):
        """Test basic data extraction."""
        mock_records = [
            {'Name': 'Item 1', 'Status': 'ACTIVE'},
            {'Name': 'Item 2', 'Status': 'INACTIVE'}
        ]
        mock_retry.return_value = mock_records
        
        result = extractor.extract_data()
        
        assert result == mock_records
        mock_retry.assert_called_once()
    
    @patch.object(SheetsExtractor, '_retry_with_backoff')
    def test_extract_data_with_columns(self, mock_retry, extractor):
        """Test data extraction with column selection."""
        mock_records = [
            {'Name': 'Item 1', 'Status': 'ACTIVE', 'Date': '2023-01-01'},
            {'Name': 'Item 2', 'Status': 'INACTIVE', 'Date': '2023-01-02'}
        ]
        mock_retry.return_value = mock_records
        
        result = extractor.extract_data(columns=['Name', 'Status'])
        
        expected = [
            {'Name': 'Item 1', 'Status': 'ACTIVE'},
            {'Name': 'Item 2', 'Status': 'INACTIVE'}
        ]
        assert result == expected
    
    @patch.object(SheetsExtractor, '_retry_with_backoff')
    def test_extract_data_with_filter(self, mock_retry, extractor):
        """Test data extraction with filtering."""
        mock_records = [
            {'Name': 'Item 1', 'Status': 'ACTIVE'},
            {'Name': 'Item 2', 'Status': 'INACTIVE'}
        ]
        mock_retry.return_value = mock_records
        
        def filter_active(record):
            return record.get('Status') == 'ACTIVE'
        
        result = extractor.extract_data(filter_func=filter_active)
        
        expected = [{'Name': 'Item 1', 'Status': 'ACTIVE'}]
        assert result == expected
    
    @patch.object(SheetsExtractor, '_retry_with_backoff')
    def test_extract_data_with_limit(self, mock_retry, extractor):
        """Test data extraction with limit."""
        mock_records = [
            {'Name': 'Item 1', 'Status': 'ACTIVE'},
            {'Name': 'Item 2', 'Status': 'INACTIVE'},
            {'Name': 'Item 3', 'Status': 'ACTIVE'}
        ]
        mock_retry.return_value = mock_records
        
        result = extractor.extract_data(limit=2)
        
        assert len(result) == 2
        assert result == mock_records[:2]
    
    @patch.object(SheetsExtractor, '_retry_with_backoff')
    def test_extract_data_missing_columns(self, mock_retry, extractor):
        """Test data extraction with missing columns."""
        mock_records = [{'Name': 'Item 1', 'Status': 'ACTIVE'}]
        mock_retry.return_value = mock_records
        
        with pytest.raises(DataError) as exc_info:
            extractor.extract_data(columns=['Name', 'MissingColumn'])
        
        assert "Columns not found: MissingColumn" in str(exc_info.value)


class TestFilterFunctions:
    """Test filter utility functions."""
    
    def test_create_status_filter(self):
        """Test status filter creation."""
        filter_func = create_status_filter('ACTIVE')
        
        assert filter_func({'status': 'ACTIVE'}) is True
        assert filter_func({'status': 'INACTIVE'}) is False
        assert filter_func({'status': 'active'}) is True  # Case insensitive
        assert filter_func({}) is False
    
    def test_create_date_range_filter(self):
        """Test date range filter creation."""
        filter_func = create_date_range_filter(
            'date', 
            start_date='2023-01-01', 
            end_date='2023-01-31'
        )
        
        assert filter_func({'date': '2023-01-15'}) is True
        assert filter_func({'date': '2022-12-31'}) is False
        assert filter_func({'date': '2023-02-01'}) is False
        assert filter_func({'date': 'invalid'}) is False
        assert filter_func({}) is False
    
    def test_create_text_search_filter(self):
        """Test text search filter creation."""
        filter_func = create_text_search_filter('name', 'test')
        
        assert filter_func({'name': 'test item'}) is True
        assert filter_func({'name': 'TEST ITEM'}) is True  # Case insensitive
        assert filter_func({'name': 'other item'}) is False
        assert filter_func({}) is False
        
        # Case sensitive
        filter_func_case = create_text_search_filter('name', 'Test', case_sensitive=True)
        assert filter_func_case({'name': 'Test item'}) is True
        assert filter_func_case({'name': 'test item'}) is False
    
    def test_create_multi_value_filter(self):
        """Test multi-value filter creation."""
        filter_func = create_multi_value_filter('status', ['ACTIVE', 'PENDING'])
        
        assert filter_func({'status': 'ACTIVE'}) is True
        assert filter_func({'status': 'PENDING'}) is True
        assert filter_func({'status': 'INACTIVE'}) is False
        assert filter_func({}) is False