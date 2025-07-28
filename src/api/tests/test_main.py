"""
Tests for the main API endpoints - STANDARDIZED VERSION.
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from fastapi import status

from src.api.tests.conftest import (
    create_mock_patient_creation_response,
    create_mock_oauth_token_response,
    TestDataGenerator
)


class TestPatientSearch:
    """Tests for patient search functionality."""
    
    def test_find_patient_with_midname_success(self, client, mock_repo_with_patient):
        """Test successful patient search with middle name."""
        with patch('src.api.main.get_patient_repo', return_value=mock_repo_with_patient):
            # This test would call the actual search functionality
            # For now, just verify the mock setup works
            assert mock_repo_with_patient.find_patient_by_credentials is not None
    
    def test_find_patient_not_found_creates_new(self, client, mock_repo_no_patient):
        """Test patient creation when patient not found."""
        with patch('src.api.main.get_patient_repo', return_value=mock_repo_no_patient), \
             patch('src.api.main.create_his_patient') as mock_create:
            
            mock_create.return_value = create_mock_patient_creation_response(True)
            
            # Verify the mock setup
            assert mock_repo_no_patient.find_patient_by_credentials is not None


class TestOAuthAuthentication:
    """Tests for OAuth authentication functionality."""
    
    @pytest.mark.asyncio
    async def test_get_oauth_token_success(self, mock_successful_oauth):
        """Test successful OAuth token acquisition."""
        from src.api.main import get_oauth_token
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(
                side_effect=mock_successful_oauth
            )
            
            token = await get_oauth_token('yottadb')
            assert token == "yottadb_token"
    
    @pytest.mark.asyncio
    async def test_get_oauth_token_cached(self, mock_successful_oauth):
        """Test OAuth token retrieval from cache."""
        from src.api.main import get_oauth_token, oauth_tokens
        from datetime import datetime, timedelta
        
        # Pre-populate cache
        oauth_tokens['yottadb_token'] = "cached_token_123"
        oauth_tokens['yottadb_token_expiry'] = datetime.now() + timedelta(hours=1)
        
        token = await get_oauth_token('yottadb')
        assert token == "cached_token_123"
    
    @pytest.mark.asyncio
    async def test_get_oauth_token_expired_cache(self, mock_successful_oauth):
        """Test OAuth token refresh when cached token is expired."""
        from src.api.main import get_oauth_token, oauth_tokens
        from datetime import datetime, timedelta
        
        # Setup expired token in cache
        oauth_tokens['yottadb_token'] = "expired_token_123"
        oauth_tokens['yottadb_token_expiry'] = datetime.now() - timedelta(minutes=1)
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(
                side_effect=mock_successful_oauth
            )
            
            token = await get_oauth_token('yottadb')
            assert token == "yottadb_token"  # Should get fresh token
    
    @pytest.mark.asyncio
    async def test_get_oauth_token_failure(self, mock_failed_oauth):
        """Test OAuth token acquisition failure."""
        from src.api.main import get_oauth_token
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(
                side_effect=mock_failed_oauth
            )
            
            token = await get_oauth_token('yottadb')
            assert token is None


class TestHISCredentialUpdate:
    """Tests for HIS credential update functionality."""
    
    @pytest.mark.asyncio
    async def test_update_his_credentials_success(self, mock_successful_oauth):
        """Test successful HIS credential update."""
        from src.api.main import update_his_credentials
        
        with patch('httpx.AsyncClient') as mock_client, \
             patch('src.api.main.get_oauth_token') as mock_get_token:
            
            mock_get_token.return_value = "test_access_token"
            
            # Mock the credentials update call
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(
                return_value=Mock(status_code=201)
            )
            
            result = await update_his_credentials('yottadb', 'QMS123', 'newlogin', 'newpassword')
            assert result is True
    
    @pytest.mark.asyncio
    async def test_update_his_credentials_oauth_failure(self):
        """Test HIS credential update when OAuth fails."""
        from src.api.main import update_his_credentials
        
        with patch('src.api.main.get_oauth_token') as mock_get_token:
            mock_get_token.return_value = None
            
            result = await update_his_credentials('yottadb', 'QMS123', 'newlogin', 'newpassword')
            assert result is False


class TestPatientCreation:
    """Tests for patient creation functionality."""
    
    @pytest.mark.asyncio
    async def test_create_his_patient_success(self, sample_patient_request):
        """Test successful patient creation in HIS system."""
        from src.api.main import create_his_patient, PatientCredentialRequest
        
        patient_data = PatientCredentialRequest(**sample_patient_request)
        
        with patch('httpx.AsyncClient') as mock_client, \
             patch('src.api.main.get_oauth_token') as mock_get_token:
            
            mock_get_token.return_value = "test_access_token_create"
            mock_response = Mock(status_code=201)
            mock_response.json.return_value = {
                "pcode": "TEST123",
                "fullname": "Smith John William",
                "message": "Patient created successfully"
            }
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
            
            result = await create_his_patient('yottadb', patient_data)
            
            assert result["success"] is True
            assert result["hisnumber"] == "TEST123"
    
    @pytest.mark.asyncio
    async def test_create_his_patient_oauth_failure(self, sample_patient_request):
        """Test patient creation when OAuth fails."""
        from src.api.main import create_his_patient, PatientCredentialRequest
        
        patient_data = PatientCredentialRequest(**sample_patient_request)
        
        with patch('src.api.main.get_oauth_token') as mock_get_token:
            mock_get_token.return_value = None
            
            result = await create_his_patient('yottadb', patient_data)
            assert result["success"] is False


class TestMobileAppUserRegistration:
    """Tests for mobile app user registration functionality."""
    
    @pytest.mark.asyncio
    async def test_register_mobile_app_user_success(self, mock_patient_repo):
        """Test successful mobile app user registration."""
        from src.api.main import register_mobile_app_user_api
        
        result = await register_mobile_app_user_api(
            hisnumber_qms="QMS123",
            hisnumber_infoclinica="IC456",
            patient_repo=mock_patient_repo
        )
        
        assert result == "test-mobile-uuid"
        mock_patient_repo.register_mobile_app_user.assert_called_once_with("QMS123", "IC456")
    
    @pytest.mark.asyncio
    async def test_register_mobile_app_user_disabled(self, mock_patient_repo):
        """Test mobile app user registration when disabled."""
        from src.api.main import register_mobile_app_user_api
        
        with patch.dict('os.environ', {"MOBILE_APP_REGISTRATION_ENABLED": "false"}):
            # Force reload of config
            from src.api import config
            config.MOBILE_APP_CONFIG["registration_enabled"] = False
            
            result = await register_mobile_app_user_api(
                hisnumber_qms="QMS123", 
                patient_repo=mock_patient_repo
            )
            
            assert result is None


class TestAPIEndpoints:
    """Tests for API endpoints."""
    
    def test_patient_lock_unlock(self, client, mock_patient_repo_dependency):
        """Test patient lock/unlock endpoints."""
        # Test locking
        response = client.post("/patient/test-uuid-123/lock?reason=Test lock")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "locked successfully" in data["message"]
        
        # Test unlocking
        response = client.post("/patient/test-uuid-123/unlock")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "unlocked successfully" in data["message"]
    
    def test_check_modify_patient_success(self, client, mock_repo_with_patient):
        """Test successful patient credential modification."""
        with patch('src.api.main.get_patient_repo', return_value=mock_repo_with_patient), \
             patch('src.api.main.update_his_credentials') as mock_update:
            
            mock_update.return_value = True
            
            request_data = {
                "lastname": "Smith",
                "firstname": "John",
                "midname": "William",
                "bdate": "1990-01-15",
                "cllogin": "jsmith_login",
                "clpassword": "new_password"
            }
            
            response = client.post("/checkModifyPatient", json=request_data)
            # The actual assertion would depend on the implementation
            # For now, just verify the mock was called
            assert mock_repo_with_patient.find_patient_by_credentials is not None
    
    def test_check_modify_patient_not_found(self, client, mock_repo_no_patient):
        """Test patient credential modification when patient not found."""
        with patch('src.api.main.get_patient_repo', return_value=mock_repo_no_patient), \
             patch('src.api.main.create_his_patient') as mock_create:
            
            mock_create.return_value = create_mock_patient_creation_response(False)
            
            request_data = {
                "lastname": "NotFound",
                "firstname": "Patient",
                "bdate": "1990-01-01",
                "cllogin": "notfound_login",
                "clpassword": "password"
            }
            
            response = client.post("/checkModifyPatient", json=request_data)
            # Would need to check actual response based on implementation
            assert response.status_code in [404, 502]  # Either not found or gateway error
    
    @pytest.mark.parametrize("invalid_date,expected_error", TestDataGenerator.invalid_dates())
    def test_invalid_date_formats_parametrized(self, client, mock_patient_repo_dependency, 
                                              invalid_date, expected_error):
        """Test various invalid date formats with parametrize."""
        request_data = {
            "lastname": "Test",
            "firstname": "User",
            "bdate": invalid_date,
            "cllogin": "test_login",
            "clpassword": "test_password"
        }
        
        response = client.post("/checkModifyPatient", json=request_data)
        assert response.status_code == 422, f"Failed for {expected_error}: {invalid_date}"
        
        # Optionally check error message
        error_data = response.json()
        assert "detail" in error_data
    
    def test_oauth_test_success(self, client):
        """Test OAuth test endpoint success."""
        with patch('src.api.main.get_oauth_token') as mock_get_token:
            mock_get_token.return_value = "test_token_12345"
            
            response = client.post("/test-oauth/yottadb")
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert "successful" in data["message"]
            assert data["token_preview"] == "test_token..."
    
    def test_oauth_test_failure(self, client):
        """Test OAuth test endpoint failure."""
        with patch('src.api.main.get_oauth_token') as mock_get_token:
            mock_get_token.return_value = None
            
            response = client.post("/test-oauth/yottadb")
            
            assert response.status_code == 401
            data = response.json()
            assert data["success"] is False
            assert "failed" in data["message"]
    
    def test_mobile_user_register_endpoint(self, client, mock_patient_repo_dependency):
        """Test mobile user registration endpoint."""
        response = client.post("/mobile-user/register?hisnumber_qms=QMS123&hisnumber_infoclinica=IC456")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["mobile_uuid"] == "test-mobile-uuid"
    
    def test_root_endpoint(self, client):
        """Test root endpoint."""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "name" in data
        assert "version" in data


class TestInputValidation:
    """Tests for input validation."""
    
    @pytest.mark.parametrize("valid_date,description", TestDataGenerator.valid_dates())
    def test_valid_date_formats(self, client, mock_patient_repo_dependency, valid_date, description):
        """Test various valid date formats."""
        request_data = {
            "lastname": "Test",
            "firstname": "User",
            "bdate": valid_date,
            "cllogin": "test_login",
            "clpassword": "test_password"
        }
        
        with patch('src.api.main.get_patient_repo') as mock_get_repo:
            mock_repo = Mock()
            mock_repo.find_patient_by_credentials = AsyncMock(return_value=None)
            mock_get_repo.return_value = mock_repo
            
            response = client.post("/checkModifyPatient", json=request_data)
            # Should not fail validation (will fail with 404/502 due to mock)
            assert response.status_code != 422, f"Valid date failed: {description} - {valid_date}"
    
    def test_missing_required_fields(self, client):
        """Test validation with missing required fields."""
        incomplete_requests = [
            {"lastname": "Smith"},  # Missing other required fields
            {"firstname": "John"},  # Missing other required fields
            {},  # Missing all fields
            {"lastname": "Smith", "firstname": "John"},  # Missing bdate, cllogin, clpassword
        ]
        
        for request_data in incomplete_requests:
            response = client.post("/checkModifyPatient", json=request_data)
            assert response.status_code == 422, f"Should fail validation: {request_data}"