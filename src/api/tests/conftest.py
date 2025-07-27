"""
Pytest configuration and fixtures for API tests.
"""

import pytest
import asyncio
import os
import sys
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timedelta

# Add the parent directories to the path
parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(parent_dir)

from fastapi.testclient import TestClient
from src.api.main import app, pg_connector, oauth_tokens

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
def client():
    """Create a test client for the FastAPI app."""
    return TestClient(app)

@pytest.fixture
def sample_patient_request():
    """Sample patient credential request data."""
    return {
        "lastname": "Smith",
        "firstname": "John", 
        "midname": "William",
        "bdate": "1990-01-15",
        "cllogin": "jsmith_login",
        "clpassword": "secure_password123"
    }

@pytest.fixture
def sample_patient_request_no_midname():
    """Sample patient credential request data without middle name."""
    return {
        "lastname": "Doe",
        "firstname": "Jane", 
        "midname": None,
        "bdate": "1985-05-20",
        "cllogin": "jdoe_login",
        "clpassword": "another_password456"
    }

@pytest.fixture
def sample_patient_db_record():
    """Sample patient record from database."""
    return {
        'uuid': 'test-uuid-123',
        'lastname': 'Smith',
        'name': 'John',
        'surname': 'William',
        'birthdate': '1990-01-15',
        'hisnumber_qms': 'QMS123456',
        'hisnumber_infoclinica': 'IC789012',
        'login_qms': 'jsmith_login',
        'login_infoclinica': None
    }

@pytest.fixture
def sample_patient_db_record_partial():
    """Sample patient record with only one HIS number."""
    return {
        'uuid': 'test-uuid-456',
        'lastname': 'Doe',
        'name': 'Jane',
        'surname': None,
        'birthdate': '1985-05-20',
        'hisnumber_qms': 'QMS789012',
        'hisnumber_infoclinica': None,
        'login_qms': 'jdoe_login',
        'login_infoclinica': None
    }

@pytest.fixture
def mock_oauth_token_response():
    """Mock OAuth token response."""
    return {
        "access_token": "mock_access_token_12345",
        "token_type": "Bearer",
        "expires_in": 3600,
        "scope": "patient_update"
    }

@pytest.fixture
def mock_pg_connector():
    """Mock PostgreSQL connector."""
    mock_connector = Mock()
    mock_connector.connection = True
    mock_connector.connect.return_value = True
    mock_connector.disconnect.return_value = None
    return mock_connector

@pytest.fixture(autouse=True)
def clear_oauth_cache():
    """Clear OAuth token cache before each test."""
    oauth_tokens.clear()
    yield
    oauth_tokens.clear()

@pytest.fixture
def mock_environment():
    """Mock environment variables for testing."""
    env_vars = {
        "YOTTADB_API_BASE": "http://test-yottadb.com",
        "YOTTADB_TOKEN_URL": "http://test-yottadb.com/oauth/token",
        "YOTTADB_CLIENT_ID": "test_yottadb_client",
        "YOTTADB_CLIENT_SECRET": "test_yottadb_secret",
        "YOTTADB_USERNAME": "test_yottadb_user",
        "YOTTADB_PASSWORD": "test_yottadb_pass",
        "FIREBIRD_API_BASE": "http://test-firebird.com",
        "FIREBIRD_TOKEN_URL": "http://test-firebird.com/oauth/token",
        "FIREBIRD_CLIENT_ID": "test_firebird_client",
        "FIREBIRD_CLIENT_SECRET": "test_firebird_secret",
        "FIREBIRD_USERNAME": "test_firebird_user",
        "FIREBIRD_PASSWORD": "test_firebird_pass",
    }
    
    with patch.dict(os.environ, env_vars):
        yield env_vars

class MockAsyncResponse:
    """Mock async HTTP response."""
    def __init__(self, status_code: int, json_data: dict = None, text: str = ""):
        self.status_code = status_code
        self._json_data = json_data or {}
        self.text = text
    
    def json(self):
        return self._json_data

@pytest.fixture
def mock_httpx_client():
    """Mock httpx async client."""
    return AsyncMock()