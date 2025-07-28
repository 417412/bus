"""
Pytest configuration and fixtures for API tests.
"""

import pytest
import pytest_asyncio
import asyncio
import os
import sys
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timedelta

# Add the parent directories to the path
parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(parent_dir)

from fastapi.testclient import TestClient
from src.api.main import app, oauth_tokens
from src.api.database import db_pool, PatientRepository

# Configure pytest-asyncio
pytest_plugins = ('pytest_asyncio',)

# REMOVED: Custom event_loop fixture - let pytest-asyncio handle it

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
        'login_infoclinica': None,
        'registered_via_mobile': False,
        'matching_locked': False
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
        'login_infoclinica': None,
        'registered_via_mobile': False,
        'matching_locked': False
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
def mock_db_pool():
    """Mock database pool."""
    mock_pool = Mock()
    mock_pool.execute_query = AsyncMock()
    mock_pool.execute_insert = AsyncMock()
    mock_pool.execute_update = AsyncMock()
    mock_pool.check_health = AsyncMock(return_value=True)
    return mock_pool

@pytest.fixture
def mock_patient_repo():
    """Mock patient repository."""
    mock_repo = Mock(spec=PatientRepository)
    mock_repo.find_patient_by_credentials = AsyncMock()
    mock_repo.register_mobile_app_user = AsyncMock()
    mock_repo.get_mobile_app_stats = AsyncMock()
    mock_repo.get_patient_matching_stats = AsyncMock()
    mock_repo.lock_patient_matching = AsyncMock()
    mock_repo.unlock_patient_matching = AsyncMock()
    return mock_repo

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
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "test_medical_system",
        "POSTGRES_USER": "test_user",
        "POSTGRES_PASSWORD": "test_password",
        "MOBILE_APP_REGISTRATION_ENABLED": "true",
        "MOBILE_APP_AUTO_REGISTER": "true",
        "MOBILE_APP_REQUIRE_BOTH_HIS": "false"
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

@pytest.fixture(autouse=True)
def mock_database_initialization():
    """Mock database initialization for all tests."""
    with patch('src.api.main.initialize_database', return_value=True), \
         patch('src.api.main.close_database'), \
         patch('src.api.main.get_database_health', return_value={"status": "healthy", "database": "test_db"}):
        yield

@pytest.fixture
def mock_patient_repo_dependency():
    """Mock the patient repository dependency."""
    def _mock_get_patient_repo():
        mock_repo = Mock(spec=PatientRepository)
        mock_repo.find_patient_by_credentials = AsyncMock()
        mock_repo.register_mobile_app_user = AsyncMock()
        mock_repo.get_mobile_app_stats = AsyncMock(return_value={
            "total_mobile_users": 10,
            "both_his_registered": 5,
            "qms_only": 3,
            "infoclinica_only": 2
        })
        mock_repo.get_patient_matching_stats = AsyncMock(return_value=[
            {"match_type": "NEW_WITH_DOCUMENT", "count": 10, "new_patients_created": 10, "mobile_app_matches": 0},
            {"match_type": "MOBILE_APP_NEW", "count": 5, "new_patients_created": 5, "mobile_app_matches": 5}
        ])
        mock_repo.lock_patient_matching = AsyncMock(return_value=True)
        mock_repo.unlock_patient_matching = AsyncMock(return_value=True)
        return mock_repo
    
    with patch('src.api.main.get_patient_repo', side_effect=_mock_get_patient_repo):
        yield

# Helper functions for tests
def create_mock_patient_creation_response(success=True, hisnumber="TEST123"):
    """Create a mock patient creation response - FIXED."""
    if success:
        return {
            "success": True,
            "hisnumber": hisnumber,
            "fullname": "Test Patient",
            "message": "Patient created successfully"
        }
    else:
        return {
            "success": False,
            "error": "Creation failed"
        }

def create_mock_database_result(data_list):
    """Create a mock database result from a list of dictionaries."""
    return data_list