"""
Pytest configuration and fixtures for API tests - ENHANCED VERSION.
"""

import pytest
import asyncio
import os
import sys
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# Add the parent directories to the path
parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(parent_dir)

from fastapi.testclient import TestClient
from src.api.main import app, oauth_tokens

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

# =============================================================================
# MOCK RESPONSE UTILITY FUNCTIONS
# =============================================================================

def create_mock_oauth_token_response(
    access_token: str = "mock_access_token_12345",
    token_type: str = "Bearer",
    expires_in: int = 3600,
    scope: str = "patient_update"
) -> Dict[str, Any]:
    """Create a mock OAuth token response."""
    return {
        "access_token": access_token,
        "token_type": token_type,
        "expires_in": expires_in,
        "scope": scope
    }

def create_mock_patient_creation_response(
    success: bool,
    hisnumber: Optional[str] = "TEST123",
    fullname: str = "Test Patient",
    message: str = None,
    error: str = None
) -> Dict[str, Any]:
    """Create a mock patient creation response."""
    if success:
        return {
            "success": True,
            "hisnumber": hisnumber,
            "fullname": fullname,
            "message": message or "Patient created successfully"
        }
    else:
        return {
            "success": False,
            "error": error or "Creation failed"
        }

def create_mock_patient_record(
    uuid: str = "test-uuid-123",
    lastname: str = "Smith",
    firstname: str = "John",
    midname: Optional[str] = "William",
    hisnumber_qms: Optional[str] = "QMS123456",
    hisnumber_infoclinica: Optional[str] = "IC789012",
    registered_via_mobile: bool = False,
    matching_locked: bool = False
) -> Dict[str, Any]:
    """Create a mock patient database record."""
    return {
        'uuid': uuid,
        'lastname': lastname,
        'name': firstname,  # Database uses 'name' for firstname
        'surname': midname,  # Database uses 'surname' for midname
        'birthdate': '1990-01-15',
        'hisnumber_qms': hisnumber_qms,
        'hisnumber_infoclinica': hisnumber_infoclinica,
        'login_qms': 'test_login',
        'login_infoclinica': None,
        'registered_via_mobile': registered_via_mobile,
        'matching_locked': matching_locked
    }

def create_mock_http_response(status_code: int, json_data: Dict = None, text: str = ""):
    """Create a mock HTTP response."""
    mock_response = Mock()
    mock_response.status_code = status_code
    mock_response.json.return_value = json_data or {}
    mock_response.text = text
    return mock_response

# =============================================================================
# PATIENT DATA FIXTURES
# =============================================================================

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
    return create_mock_patient_record()

@pytest.fixture
def sample_patient_db_record_partial():
    """Sample patient record with only one HIS number."""
    return create_mock_patient_record(
        uuid='test-uuid-456',
        lastname='Doe',
        firstname='Jane',
        midname=None,
        hisnumber_qms='QMS789012',
        hisnumber_infoclinica=None
    )

@pytest.fixture
def sample_mobile_patient_record():
    """Sample patient record registered via mobile."""
    return create_mock_patient_record(
        uuid='mobile-uuid-789',
        registered_via_mobile=True
    )

# =============================================================================
# MOCK REPOSITORIES AND DEPENDENCIES
# =============================================================================

@pytest.fixture
def mock_patient_repo():
    """Mock patient repository for basic operations."""
    mock_repo = Mock()
    mock_repo.find_patient_by_credentials = AsyncMock(return_value=None)
    mock_repo.register_mobile_app_user = AsyncMock(return_value="test-mobile-uuid")
    mock_repo.lock_patient_matching = AsyncMock(return_value=True)
    mock_repo.unlock_patient_matching = AsyncMock(return_value=True)
    mock_repo.get_mobile_app_stats = AsyncMock(return_value={
        "total_mobile_users": 0,
        "both_his_registered": 0,
        "qms_only": 0,
        "infoclinica_only": 0
    })
    mock_repo.get_patient_matching_stats = AsyncMock(return_value=[])
    return mock_repo

@pytest.fixture
def mock_repo_with_patient(mock_patient_repo, sample_patient_db_record):
    """Mock patient repository that returns a patient."""
    mock_patient_repo.find_patient_by_credentials = AsyncMock(return_value=sample_patient_db_record)
    return mock_patient_repo

@pytest.fixture
def mock_repo_no_patient(mock_patient_repo):
    """Mock patient repository that returns no patient."""
    mock_patient_repo.find_patient_by_credentials = AsyncMock(return_value=None)
    return mock_patient_repo

@pytest.fixture
def mock_repo_locked_patient(mock_patient_repo):
    """Mock patient repository with locked patient."""
    locked_patient = create_mock_patient_record(matching_locked=True)
    mock_patient_repo.find_patient_by_credentials = AsyncMock(return_value=locked_patient)
    return mock_patient_repo

@pytest.fixture
def mock_patient_repo_dependency(mock_patient_repo):
    """Override the patient repository dependency - SHORTENED NAME."""
    def get_mock_patient_repo():
        return mock_patient_repo
    
    with patch('src.api.main.get_patient_repo', side_effect=get_mock_patient_repo):
        yield mock_patient_repo

# =============================================================================
# DATABASE AND HTTP MOCKING
# =============================================================================

@pytest.fixture
def mock_db_pool():
    """Mock the database pool for tests that need database access."""
    with patch('src.api.database.db_pool') as mock_pool:
        mock_pool.pool = Mock()
        mock_pool.initialize_pool.return_value = True
        mock_pool.check_health = AsyncMock(return_value=True)
        yield mock_pool

@pytest.fixture
def mock_httpx_client():
    """Mock httpx async client."""
    return AsyncMock()

@pytest.fixture
def mock_successful_oauth():
    """Mock successful OAuth responses for both systems."""
    def mock_post_side_effect(url, **kwargs):
        if 'yottadb' in url or '192.168.156.43' in url:
            return create_mock_http_response(200, create_mock_oauth_token_response("yottadb_token"))
        elif 'firebird' in url or '192.168.160.141' in url:
            return create_mock_http_response(200, create_mock_oauth_token_response("firebird_token"))
        else:
            return create_mock_http_response(200, create_mock_oauth_token_response())
    
    return mock_post_side_effect

@pytest.fixture
def mock_failed_oauth():
    """Mock failed OAuth responses."""
    def mock_post_side_effect(url, **kwargs):
        return create_mock_http_response(401, {"error": "invalid_credentials"}, "Unauthorized")
    
    return mock_post_side_effect

# =============================================================================
# TEST ENVIRONMENT SETUP
# =============================================================================

@pytest.fixture(autouse=True)
def clear_oauth_cache():
    """Clear OAuth token cache before each test."""
    oauth_tokens.clear()
    # Also clear the locks if they exist
    try:
        from src.api.main import oauth_locks
        oauth_locks.clear()
    except ImportError:
        pass  # oauth_locks might not exist yet
    
    yield
    
    oauth_tokens.clear()
    try:
        from src.api.main import oauth_locks
        oauth_locks.clear()
    except ImportError:
        pass

@pytest.fixture
def mock_environment():
    """Mock environment variables for testing."""
    env_vars = {
        "ENVIRONMENT": "testing",
        "MOBILE_APP_REGISTRATION_ENABLED": "true",
        "MOBILE_APP_AUTO_REGISTER": "true",
        "MOBILE_APP_REQUIRE_BOTH_HIS": "false",
        "YOTTADB_API_BASE": "http://test-yottadb.com",
        "YOTTADB_TOKEN_URL": "http://test-yottadb.com/token",
        "YOTTADB_CLIENT_ID": "admin",
        "YOTTADB_CLIENT_SECRET": "secret",
        "YOTTADB_USERNAME": "admin",
        "YOTTADB_PASSWORD": "secret",
        "FIREBIRD_API_BASE": "http://test-firebird.com",
        "FIREBIRD_TOKEN_URL": "http://test-firebird.com/token",
        "FIREBIRD_CLIENT_ID": "admin",
        "FIREBIRD_CLIENT_SECRET": "secret",
        "FIREBIRD_USERNAME": "admin",
        "FIREBIRD_PASSWORD": "secret",
    }
    
    with patch.dict(os.environ, env_vars):
        yield env_vars

# =============================================================================
# TEST DATA GENERATORS
# =============================================================================

class TestDataGenerator:
    """Utility class for generating test data."""
    
    @staticmethod
    def invalid_dates():
        """Generate invalid date test cases."""
        return [
            ("1990/01/15", "Invalid separator"),
            ("15-01-1990", "Wrong format order"),
            ("1990-13-01", "Invalid month"),
            ("1990-01-32", "Invalid day"),
            ("invalid", "Non-date string"),
            ("", "Empty string"),
            ("1990-1-1", "Missing zero padding"),
            ("90-01-15", "Two digit year")
        ]
    
    @staticmethod
    def valid_dates():
        """Generate valid date test cases."""
        return [
            ("1990-01-15", "Standard format"),
            ("2000-12-31", "Year 2000"),
            ("1985-06-20", "Mid-year date"),
            ("2023-02-28", "February non-leap year"),
            ("2020-02-29", "February leap year")
        ]
    
    @staticmethod
    def oauth_error_scenarios():
        """Generate OAuth error test scenarios."""
        return [
            (400, {"error": "invalid_request"}, "Bad request"),
            (401, {"error": "invalid_client"}, "Invalid client"),
            (403, {"error": "access_denied"}, "Access denied"),
            (500, {"error": "server_error"}, "Server error"),
            (503, {"error": "service_unavailable"}, "Service unavailable")
        ]

@pytest.fixture
def test_data_generator():
    """Provide test data generator."""
    return TestDataGenerator()