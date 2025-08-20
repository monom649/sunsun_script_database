#!/usr/bin/env python3
"""
Tests for logging configuration.

Tests secure logging functionality and sensitive data redaction.
"""

import logging
import re
import pytest
from io import StringIO

from logging_conf import SecureFormatter, SecurityFilter, setup_logging, get_logger


class TestSecureFormatter:
    """Test SecureFormatter class."""
    
    def test_sheet_id_redaction(self):
        """Test that Google Sheets IDs are redacted."""
        formatter = SecureFormatter()
        
        # Create a log record with a sheets ID
        record = logging.LogRecord(
            name='test',
            level=logging.INFO,
            pathname='',
            lineno=0,
            msg='Processing sheet: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms',
            args=(),
            exc_info=None
        )
        
        formatted = formatter.format(record)
        assert '***SHEET_ID***' in formatted
        assert '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms' not in formatted
    
    def test_json_key_redaction(self):
        """Test that JSON credentials are redacted."""
        formatter = SecureFormatter()
        
        record = logging.LogRecord(
            name='test',
            level=logging.INFO,
            pathname='',
            lineno=0,
            msg='Config: {"private_key": "secret_value"}',
            args=(),
            exc_info=None
        )
        
        formatted = formatter.format(record)
        assert '***REDACTED***' in formatted
        assert 'secret_value' not in formatted
    
    def test_service_account_email_redaction(self):
        """Test that service account emails are redacted."""
        formatter = SecureFormatter()
        
        record = logging.LogRecord(
            name='test',
            level=logging.INFO,
            pathname='',
            lineno=0,
            msg='Using account: sheets-reader@my-project-123456.iam.gserviceaccount.com',
            args=(),
            exc_info=None
        )
        
        formatted = formatter.format(record)
        assert '***SERVICE_ACCOUNT***@***.iam.gserviceaccount.com' in formatted
        assert 'sheets-reader@my-project-123456.iam.gserviceaccount.com' not in formatted
    
    def test_file_path_redaction(self):
        """Test that credential file paths are redacted."""
        formatter = SecureFormatter()
        
        record = logging.LogRecord(
            name='test',
            level=logging.INFO,
            pathname='',
            lineno=0,
            msg='Loading credentials from /path/to/service-account-key.json',
            args=(),
            exc_info=None
        )
        
        formatted = formatter.format(record)
        assert '/***CREDENTIALS***.json' in formatted
        assert '/path/to/service-account-key.json' not in formatted
    
    def test_normal_message_unchanged(self):
        """Test that normal messages are not modified."""
        formatter = SecureFormatter()
        
        record = logging.LogRecord(
            name='test',
            level=logging.INFO,
            pathname='',
            lineno=0,
            msg='Normal log message without sensitive data',
            args=(),
            exc_info=None
        )
        
        formatted = formatter.format(record)
        assert 'Normal log message without sensitive data' in formatted
        assert '***' not in formatted


class TestSecurityFilter:
    """Test SecurityFilter class."""
    
    def test_blocks_service_account_json(self):
        """Test that service account JSON is blocked."""
        security_filter = SecurityFilter()
        
        record = logging.LogRecord(
            name='test',
            level=logging.INFO,
            pathname='',
            lineno=0,
            msg='{"type": "service_account", "project_id": "test"}',
            args=(),
            exc_info=None
        )
        
        assert security_filter.filter(record) is False
    
    def test_blocks_private_key(self):
        """Test that private keys are blocked."""
        security_filter = SecurityFilter()
        
        record = logging.LogRecord(
            name='test',
            level=logging.INFO,
            pathname='',
            lineno=0,
            msg='{"private_key": "-----BEGIN PRIVATE KEY-----..."}',
            args=(),
            exc_info=None
        )
        
        assert security_filter.filter(record) is False
    
    def test_blocks_oauth_token(self):
        """Test that OAuth tokens are blocked."""
        security_filter = SecurityFilter()
        
        record = logging.LogRecord(
            name='test',
            level=logging.INFO,
            pathname='',
            lineno=0,
            msg='Token: ya29.a0AfH6SMC_test_token_here',
            args=(),
            exc_info=None
        )
        
        assert security_filter.filter(record) is False
    
    def test_allows_normal_messages(self):
        """Test that normal messages are allowed."""
        security_filter = SecurityFilter()
        
        record = logging.LogRecord(
            name='test',
            level=logging.INFO,
            pathname='',
            lineno=0,
            msg='Normal log message',
            args=(),
            exc_info=None
        )
        
        assert security_filter.filter(record) is True


class TestLoggingSetup:
    """Test logging setup functions."""
    
    def test_setup_logging_default(self):
        """Test default logging setup."""
        logger = setup_logging()
        
        assert logger.name == 'sheets_reader'
        assert logger.level == logging.INFO
        assert len(logger.handlers) > 0
    
    def test_setup_logging_custom_level(self):
        """Test logging setup with custom level."""
        logger = setup_logging(level='DEBUG')
        
        assert logger.level == logging.DEBUG
    
    def test_setup_logging_with_file(self, tmp_path):
        """Test logging setup with file output."""
        log_file = tmp_path / 'test.log'
        logger = setup_logging(log_file=str(log_file))
        
        # Check that file handler was added
        file_handlers = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]
        assert len(file_handlers) > 0
    
    def test_setup_logging_no_console(self):
        """Test logging setup without console output."""
        logger = setup_logging(enable_console=False)
        
        # Check that no StreamHandler was added
        stream_handlers = [h for h in logger.handlers if isinstance(h, logging.StreamHandler)]
        assert len(stream_handlers) == 0
    
    def test_get_logger(self):
        """Test get_logger function."""
        logger = get_logger()
        assert logger.name == 'sheets_reader'
        
        custom_logger = get_logger('custom')
        assert custom_logger.name == 'custom'


class TestLogMessageFunctions:
    """Test logging utility functions."""
    
    def test_log_config_summary(self, caplog):
        """Test configuration summary logging."""
        from logging_conf import log_config_summary
        
        config_info = {
            'sheet_id_prefix': 'test123...',
            'worksheet_name': 'TestSheet',
            'scope': 'readonly',
            'timeout': '10',
            'retry_attempts': '3'
        }
        
        with caplog.at_level(logging.INFO):
            log_config_summary(config_info)
        
        assert 'Configuration loaded successfully' in caplog.text
        assert 'TestSheet' in caplog.text
        assert 'test123...' in caplog.text
    
    def test_log_operation_start(self, caplog):
        """Test operation start logging."""
        from logging_conf import log_operation_start
        
        with caplog.at_level(logging.INFO):
            log_operation_start('test_operation', limit=100, sensitive_param='secret')
        
        assert 'Starting test_operation' in caplog.text
        assert 'limit=100' in caplog.text
        assert '***REDACTED***' in caplog.text
        assert 'secret' not in caplog.text
    
    def test_log_operation_result_success(self, caplog):
        """Test successful operation result logging."""
        from logging_conf import log_operation_result
        
        with caplog.at_level(logging.INFO):
            log_operation_result('test_operation', True, count=50)
        
        assert 'Completed test_operation successfully (50 records)' in caplog.text
    
    def test_log_operation_result_failure(self, caplog):
        """Test failed operation result logging."""
        from logging_conf import log_operation_result
        
        with caplog.at_level(logging.ERROR):
            log_operation_result('test_operation', False, error='Test error message')
        
        assert 'Failed test_operation: Test error message' in caplog.text
    
    def test_log_retry_attempt(self, caplog):
        """Test retry attempt logging."""
        from logging_conf import log_retry_attempt
        
        with caplog.at_level(logging.WARNING):
            log_retry_attempt(2, 3, 5.0, 'Network timeout')
        
        assert 'Attempt 2/3 failed, retrying in 5.0s: Network timeout' in caplog.text