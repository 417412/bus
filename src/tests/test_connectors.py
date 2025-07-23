#!/usr/bin/env python3
"""
Test suite for database connectors.
Tests connection functionality and password decryption.
"""

import os
import sys
import pytest
import logging
import subprocess  # Add this import
import socket     # Add this import
import requests   # Add this import
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

# Add the parent directory to the path so Python can find the modules
parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent_dir)

from src.connectors.postgres_connector import PostgresConnector
from src.connectors.firebird_connector import FirebirdConnector
from src.connectors.yottadb_connector import YottaDBConnector
from src.config.settings import get_decrypted_database_config
from src.utils.password_manager import get_password_manager

class TestConnectors:
    """Test suite for database connectors."""
    
    def setup_method(self):
        """Setup for each test method."""
        self.logger = logging.getLogger(__name__)
        
        # Sample test configurations
        self.test_pg_config = {
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "user": "test_user",
            "password": "test_password"
        }
        
        self.test_fb_config = {
            "host": "localhost",
            "port": 3050,
            "database": "test.fdb",
            "user": "SYSDBA",
            "password": "masterkey",
            "charset": "UTF8"
        }
        
        self.test_ydb_config = {
            "api_url": "http://192.168.156.43/cgi-bin/qms_export_pat",  # Realistic URL
            "timeout": 300,
            "connect_timeout": 300,
            "max_retries": 2,
            "delimiter": "|"
        }
    
    def test_postgres_connector_init_with_config(self):
        """Test PostgreSQL connector initialization with explicit config."""
        connector = PostgresConnector(self.test_pg_config)
        
        assert connector.config == self.test_pg_config
        assert connector.connection is None
        assert connector.logger is not None
    
    def test_postgres_connector_init_default_config(self):
        """Test PostgreSQL connector initialization with default decrypted config."""
        with patch('src.connectors.postgres_connector.get_decrypted_database_config') as mock_config:
            mock_config.return_value = {"PostgreSQL": self.test_pg_config}
            
            connector = PostgresConnector()
            
            assert connector.config == self.test_pg_config
            mock_config.assert_called_once()
    
    def test_firebird_connector_init_with_config(self):
        """Test Firebird connector initialization with explicit config."""
        connector = FirebirdConnector(self.test_fb_config)
        
        assert connector.config == self.test_fb_config
        assert connector.connection is None
        assert connector.logger is not None
        assert connector.server_name == "infoclinica_server"
        assert connector.db_name == "infoclinica_db"
    
    def test_firebird_connector_init_default_config(self):
        """Test Firebird connector initialization with default decrypted config."""
        with patch('src.connectors.firebird_connector.get_decrypted_database_config') as mock_config:
            mock_config.return_value = {"Firebird": self.test_fb_config}
            
            connector = FirebirdConnector()
            
            assert connector.config == self.test_fb_config
            mock_config.assert_called_once()
    
    def test_yottadb_connector_init_with_config(self):
        """Test YottaDB connector initialization with explicit config."""
        connector = YottaDBConnector(self.test_ydb_config)
        
        assert connector.config == self.test_ydb_config
        assert connector.logger is not None
    
    def test_yottadb_connector_init_default_config(self):
        """Test YottaDB connector initialization with default decrypted config."""
        with patch('src.connectors.yottadb_connector.get_decrypted_database_config') as mock_config:
            mock_config.return_value = {"YottaDB": self.test_ydb_config}
            
            connector = YottaDBConnector()
            
            assert connector.config == self.test_ydb_config
            mock_config.assert_called_once()
    
    def test_yottadb_url_parsing(self):
        """Test YottaDB URL parsing functionality."""
        connector = YottaDBConnector(self.test_ydb_config)
        
        # Test URL parsing
        host = connector._extract_host_from_url("http://192.168.156.43/cgi-bin/qms_export_pat")
        assert host == "192.168.156.43"
        
        host = connector._extract_host_from_url("https://example.com:8080/api")
        assert host == "example.com"
        
        # Test invalid URL
        host = connector._extract_host_from_url("invalid-url")
        assert host is None
    
    @patch('psycopg2.connect')
    def test_postgres_connection_success(self, mock_connect):
        """Test successful PostgreSQL connection."""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        connector = PostgresConnector(self.test_pg_config)
        result = connector.connect()
        
        assert result is True
        assert connector.connection == mock_connection
        mock_connect.assert_called_once()
    
    @patch('psycopg2.connect')
    def test_postgres_connection_failure(self, mock_connect):
        """Test PostgreSQL connection failure."""
        mock_connect.side_effect = Exception("Connection failed")
        
        connector = PostgresConnector(self.test_pg_config)
        result = connector.connect()
        
        assert result is False
        assert connector.connection is None
    
    @patch('src.connectors.firebird_connector.connect')
    @patch('src.connectors.firebird_connector.driver_config')
    def test_firebird_connection_success(self, mock_driver_config, mock_connect):
        """Test successful Firebird connection."""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        connector = FirebirdConnector(self.test_fb_config)
        
        with patch.object(connector, '_diagnose_connectivity'):
            result = connector.connect()
        
        assert result is True
        assert connector.connection == mock_connection
    
    @patch('src.connectors.firebird_connector.connect')
    def test_firebird_connection_failure(self, mock_connect):
        """Test Firebird connection failure."""
        mock_connect.side_effect = Exception("Connection failed")
        
        connector = FirebirdConnector(self.test_fb_config)
        
        with patch.object(connector, '_diagnose_connectivity'):
            result = connector.connect()
        
        assert result is False
        assert connector.connection is None
    
    @patch('subprocess.run')
    def test_yottadb_connection_ping_timeout(self, mock_subprocess):
        """Test YottaDB connection when ping times out."""
        # Mock ping timeout - now subprocess is properly imported
        mock_subprocess.side_effect = subprocess.TimeoutExpired(['ping'], 10)
        
        # Mock socket success as fallback
        with patch('socket.socket') as mock_socket_class:
            mock_socket = Mock()
            mock_socket.connect_ex.return_value = 0  # Success
            mock_socket_class.return_value = mock_socket
            
            connector = YottaDBConnector(self.test_ydb_config)
            result = connector.connect()
            
            assert result is True
            mock_subprocess.assert_called_once()
            mock_socket.connect_ex.assert_called_once()

    @patch('subprocess.run')
    def test_yottadb_connection_ping_command_not_found(self, mock_subprocess):
        """Test YottaDB connection when ping command is not found."""
        # Mock ping command not found
        mock_subprocess.side_effect = FileNotFoundError("ping command not found")
        
        # Mock socket success as fallback
        with patch('socket.socket') as mock_socket_class:
            mock_socket = Mock()
            mock_socket.connect_ex.return_value = 0  # Success
            mock_socket_class.return_value = mock_socket
            
            connector = YottaDBConnector(self.test_ydb_config)
            result = connector.connect()
            
            assert result is True
            mock_subprocess.assert_called_once()
            mock_socket.connect_ex.assert_called_once()

    @patch('subprocess.run')
    def test_yottadb_connection_success(self, mock_subprocess):
        """Test successful YottaDB API connection via ping."""
        # Mock successful ping
        mock_result = Mock()
        mock_result.returncode = 0
        mock_subprocess.return_value = mock_result
        
        connector = YottaDBConnector(self.test_ydb_config)
        result = connector.connect()
        
        assert result is True
        mock_subprocess.assert_called_once()
        
        # Verify ping command was called correctly
        call_args = mock_subprocess.call_args[0][0]  # First positional argument
        assert 'ping' in call_args
        assert '192.168.156.43' in call_args  # Should use the actual host from config

    @patch('subprocess.run')
    def test_yottadb_connection_ping_failure_socket_success(self, mock_subprocess):
        """Test YottaDB connection when ping fails but socket test succeeds."""
        # Mock ping failure
        mock_result = Mock()
        mock_result.returncode = 1
        mock_subprocess.return_value = mock_result
        
        # Mock socket success
        with patch('socket.socket') as mock_socket_class:
            mock_socket = Mock()
            mock_socket.connect_ex.return_value = 0  # Success
            mock_socket_class.return_value = mock_socket
            
            connector = YottaDBConnector(self.test_ydb_config)
            result = connector.connect()
            
            assert result is True
            mock_subprocess.assert_called_once()
            mock_socket.connect_ex.assert_called_once_with(('192.168.156.43', 80))

    @patch('subprocess.run')
    def test_yottadb_connection_failure(self, mock_subprocess):
        """Test YottaDB API connection failure when both ping and socket fail."""
        # Mock ping failure
        mock_result = Mock()
        mock_result.returncode = 1
        mock_subprocess.return_value = mock_result
        
        # Mock socket failure
        with patch('socket.socket') as mock_socket_class:
            mock_socket = Mock()
            mock_socket.connect_ex.return_value = 1  # Failure
            mock_socket_class.return_value = mock_socket
            
            connector = YottaDBConnector(self.test_ydb_config)
            result = connector.connect()
            
            assert result is False
            mock_subprocess.assert_called_once()
            mock_socket.connect_ex.assert_called_once()
    
    def test_password_decryption_integration(self):
        """Test that encrypted passwords are properly decrypted."""
        password_manager = get_password_manager()
        
        # Create config with encrypted password
        encrypted_password = password_manager.encrypt_password("test_password")
        encrypted_config = {
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "user": "test_user",
            "password": encrypted_password
        }
        
        # Mock the decrypted config function
        with patch('src.connectors.postgres_connector.get_decrypted_database_config') as mock_config:
            decrypted_config = password_manager.decrypt_config_passwords({"PostgreSQL": encrypted_config})
            mock_config.return_value = decrypted_config
            
            connector = PostgresConnector()
            
            # Verify password was decrypted
            assert connector.config["password"] == "test_password"
            assert not password_manager.is_encrypted(connector.config["password"])
    
    def test_password_logging_security(self):
        """Test that passwords are not logged in plain text."""
        config_with_password = {
            "host": "localhost",
            "password": "secret_password",
            "user": "test_user"
        }
        
        with patch('src.connectors.postgres_connector.setup_logger') as mock_logger:
            mock_logger_instance = Mock()
            mock_logger.return_value = mock_logger_instance
            
            connector = PostgresConnector(config_with_password)
            
            # Check that debug log was called
            mock_logger_instance.debug.assert_called()
            
            # Verify password was masked in the log call
            log_call_args = mock_logger_instance.debug.call_args[0][0]
            assert "secret_password" not in log_call_args
            assert "********" in log_call_args
    
    def test_postgres_query_execution(self):
        """Test PostgreSQL query execution."""
        with patch('psycopg2.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            
            # Setup the mock chain
            mock_connect.return_value = mock_connection
            mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
            mock_connection.cursor.return_value.__exit__ = Mock(return_value=None)
            
            # Setup cursor mock
            mock_cursor.description = [('id',), ('name',)]
            mock_cursor.fetchall.return_value = [(1, 'test'), (2, 'test2')]
            
            connector = PostgresConnector(self.test_pg_config)
            connector.connection = mock_connection
            
            rows, columns = connector.execute_query("SELECT * FROM test")
            
            assert columns == ['id', 'name']
            assert rows == [(1, 'test'), (2, 'test2')]
            mock_cursor.execute.assert_called_once_with("SELECT * FROM test", ())
    
    def test_firebird_query_execution(self):
        """Test Firebird query execution."""
        mock_connection = Mock()
        mock_cursor = Mock()
        
        # Setup the mock chain for context manager
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=None)
        
        # Setup cursor mock
        mock_cursor.description = [('ID',), ('NAME',)]
        mock_cursor.fetchall.return_value = [(1, 'test'), (2, 'test2')]
        
        connector = FirebirdConnector(self.test_fb_config)
        connector.connection = mock_connection
        
        rows, columns = connector.execute_query("SELECT * FROM test")
        
        assert columns == ['id', 'name']  # Should be lowercase
        assert rows == [(1, 'test'), (2, 'test2')]
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test", ())
    
    def test_connection_cleanup(self):
        """Test proper connection cleanup."""
        # Test PostgreSQL cleanup
        mock_pg_connection = Mock()
        pg_connector = PostgresConnector(self.test_pg_config)
        pg_connector.connection = mock_pg_connection
        
        pg_connector.disconnect()
        mock_pg_connection.close.assert_called_once()
        
        # Test Firebird cleanup
        mock_fb_connection = Mock()
        fb_connector = FirebirdConnector(self.test_fb_config)
        fb_connector.connection = mock_fb_connection
        
        fb_connector.disconnect()
        mock_fb_connection.close.assert_called_once()


class TestPasswordManager:
    """Test suite for password manager functionality."""
    
    def test_password_encryption_decryption(self):
        """Test basic password encryption and decryption."""
        password_manager = get_password_manager()
        original_password = "test_password_123"
        
        # Encrypt password
        encrypted = password_manager.encrypt_password(original_password)
        
        # Verify it's encrypted
        assert password_manager.is_encrypted(encrypted)
        assert encrypted.startswith("ENC:")
        assert encrypted != original_password
        
        # Decrypt password
        decrypted = password_manager.decrypt_password(encrypted)
        assert decrypted == original_password
    
    def test_config_password_encryption(self):
        """Test configuration password encryption."""
        password_manager = get_password_manager()
        
        config = {
            "database1": {
                "host": "localhost",
                "password": "secret1"
            },
            "database2": {
                "host": "remote",
                "password": "secret2"
            }
        }
        
        # Encrypt passwords
        encrypted_config = password_manager.encrypt_config_passwords(config)
        
        # Verify passwords are encrypted
        assert password_manager.is_encrypted(encrypted_config["database1"]["password"])
        assert password_manager.is_encrypted(encrypted_config["database2"]["password"])
        assert encrypted_config["database1"]["host"] == "localhost"  # Non-passwords unchanged
        
        # Decrypt passwords
        decrypted_config = password_manager.decrypt_config_passwords(encrypted_config)
        
        # Verify passwords are decrypted
        assert decrypted_config["database1"]["password"] == "secret1"
        assert decrypted_config["database2"]["password"] == "secret2"
    
    def test_backward_compatibility(self):
        """Test that plaintext passwords work (backward compatibility)."""
        password_manager = get_password_manager()
        plaintext_password = "plaintext_password"
        
        # Should not be considered encrypted
        assert not password_manager.is_encrypted(plaintext_password)
        
        # Decrypting plaintext should return as-is
        result = password_manager.decrypt_password(plaintext_password)
        assert result == plaintext_password


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])