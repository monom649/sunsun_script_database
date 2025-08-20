#!/usr/bin/env python3
"""
Tests for configuration module.

Tests environment variable validation without using real credentials.
"""

import os
import tempfile
import pytest
from unittest.mock import patch
from pathlib import Path

from config import Config, ConfigError, check_environment


class TestConfig:
    """Test configuration validation."""
    
    def test_missing_required_variables(self):
        """Test error when required variables are missing."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ConfigError) as exc_info:
                Config()
            
            assert "Missing required environment variables" in str(exc_info.value)
            assert "GOOGLE_APPLICATION_CREDENTIALS" in str(exc_info.value)
            assert "SHEET_ID" in str(exc_info.value)
            assert "WORKSHEET_NAME" in str(exc_info.value)
    
    def test_missing_credentials_file(self):
        """Test error when credentials file doesn't exist."""
        env_vars = {
            'GOOGLE_APPLICATION_CREDENTIALS': '/nonexistent/file.json',
            'SHEET_ID': 'test_sheet_id',
            'WORKSHEET_NAME': 'test_worksheet'
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(ConfigError) as exc_info:
                Config()
            
            assert "Service account credentials file not found" in str(exc_info.value)
    
    def test_non_json_credentials_file(self):
        """Test error when credentials file is not JSON."""
        with tempfile.NamedTemporaryFile(suffix='.txt', delete=False) as temp_file:
            temp_path = temp_file.name
        
        try:
            env_vars = {
                'GOOGLE_APPLICATION_CREDENTIALS': temp_path,
                'SHEET_ID': 'test_sheet_id',
                'WORKSHEET_NAME': 'test_worksheet'
            }
            
            with patch.dict(os.environ, env_vars, clear=True):
                with pytest.raises(ConfigError) as exc_info:
                    Config()
                
                assert "must be a JSON file" in str(exc_info.value)
        finally:
            os.unlink(temp_path)
    
    def test_invalid_numeric_values(self):
        """Test error with invalid numeric configuration."""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as temp_file:
            temp_path = temp_file.name
        
        try:
            env_vars = {
                'GOOGLE_APPLICATION_CREDENTIALS': temp_path,
                'SHEET_ID': 'test_sheet_id',
                'WORKSHEET_NAME': 'test_worksheet',
                'REQUEST_TIMEOUT': 'invalid'
            }
            
            with patch.dict(os.environ, env_vars, clear=True):
                with pytest.raises(ConfigError) as exc_info:
                    Config()
                
                assert "must be a valid integer" in str(exc_info.value)
        finally:
            os.unlink(temp_path)
    
    def test_numeric_values_out_of_range(self):
        """Test error with numeric values out of valid range."""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as temp_file:
            temp_path = temp_file.name
        
        try:
            env_vars = {
                'GOOGLE_APPLICATION_CREDENTIALS': temp_path,
                'SHEET_ID': 'test_sheet_id',
                'WORKSHEET_NAME': 'test_worksheet',
                'REQUEST_TIMEOUT': '999'  # Too high
            }
            
            with patch.dict(os.environ, env_vars, clear=True):
                with pytest.raises(ConfigError) as exc_info:
                    Config()
                
                assert "must be between" in str(exc_info.value)
        finally:
            os.unlink(temp_path)
    
    def test_valid_configuration(self):
        """Test successful configuration with valid values."""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as temp_file:
            temp_path = temp_file.name
            temp_file.write(b'{"type": "service_account"}')
        
        try:
            env_vars = {
                'GOOGLE_APPLICATION_CREDENTIALS': temp_path,
                'SHEET_ID': 'test_sheet_id_12345',
                'WORKSHEET_NAME': 'test_worksheet',
                'REQUEST_TIMEOUT': '15',
                'RETRY_MAX_ATTEMPTS': '5'
            }
            
            with patch.dict(os.environ, env_vars, clear=True):
                config = Config()
                
                assert config.credentials_path == temp_path
                assert config.sheet_id == 'test_sheet_id_12345'
                assert config.worksheet_name == 'test_worksheet'
                assert config.request_timeout == 15
                assert config.retry_max_attempts == 5
        finally:
            os.unlink(temp_path)
    
    def test_default_values(self):
        """Test that default values are applied correctly."""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as temp_file:
            temp_path = temp_file.name
            temp_file.write(b'{"type": "service_account"}')
        
        try:
            env_vars = {
                'GOOGLE_APPLICATION_CREDENTIALS': temp_path,
                'SHEET_ID': 'test_sheet_id',
                'WORKSHEET_NAME': 'test_worksheet'
            }
            
            with patch.dict(os.environ, env_vars, clear=True):
                config = Config()
                
                assert config.sheets_scope == 'https://www.googleapis.com/auth/spreadsheets.readonly'
                assert config.request_timeout == 10
                assert config.retry_max_attempts == 3
                assert config.retry_initial_delay == 5
                assert config.watch_interval == 300
                assert config.jitter_percent == 10
        finally:
            os.unlink(temp_path)
    
    def test_validate_sheet_access(self):
        """Test sanitized configuration info."""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as temp_file:
            temp_path = temp_file.name
            temp_file.write(b'{"type": "service_account"}')
        
        try:
            env_vars = {
                'GOOGLE_APPLICATION_CREDENTIALS': temp_path,
                'SHEET_ID': 'very_long_sheet_id_12345',
                'WORKSHEET_NAME': 'test_worksheet'
            }
            
            with patch.dict(os.environ, env_vars, clear=True):
                config = Config()
                info = config.validate_sheet_access()
                
                assert 'sheet_id_prefix' in info
                assert info['sheet_id_prefix'] == 'very_lon...'
                assert info['worksheet_name'] == 'test_worksheet'
                assert 'scope' in info
                assert 'timeout' in info
        finally:
            os.unlink(temp_path)


class TestCheckEnvironment:
    """Test environment checking function."""
    
    def test_check_environment_false(self):
        """Test that check_environment returns False for invalid config."""
        with patch.dict(os.environ, {}, clear=True):
            assert check_environment() is False
    
    def test_check_environment_true(self):
        """Test that check_environment returns True for valid config."""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as temp_file:
            temp_path = temp_file.name
            temp_file.write(b'{"type": "service_account"}')
        
        try:
            env_vars = {
                'GOOGLE_APPLICATION_CREDENTIALS': temp_path,
                'SHEET_ID': 'test_sheet_id',
                'WORKSHEET_NAME': 'test_worksheet'
            }
            
            with patch.dict(os.environ, env_vars, clear=True):
                assert check_environment() is True
        finally:
            os.unlink(temp_path)