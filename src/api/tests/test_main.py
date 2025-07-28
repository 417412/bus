"""
Tests for the main API endpoints - Fixed version.
"""

import pytest
import json
import os
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime, timedelta
from fastapi import status

from src.api.main import (
    get_oauth_token, 
    update_his_credentials,
    create_his_patient,
    register_mobile_app_user_api,
    oauth_tokens
)
from src.api.tests.conftest import MockAsyncResponse, create_mock_patient_creation_response


class TestPatientSearch:
    """Tests for patient search functionality."""
    
    def test_find_patient_with_midname_success(self, client, mock_patient_repo_dependency, 
                                             sample_patient_request, sample_patient_db_record):
        """Test successful patient search with middle name."""
        with patch('src.api.main.get_patient_repository') as mock_get_repo:
            mock_repo = Mock()
            mock_repo.find_patient_by_credentials = AsyncMock(return_value=sample_patient_db_record)
            mock_get_repo.return_value = mock_repo
            
            with patch('src.api.main.update_his_credentials', return_value=True):
                response = client.post("/checkModifyPatient", json=sample_patient_request)
                
                assert response.status_code == 200
                data = response.json()
                assert data["success"] == "true"
                assert data["action"] == "update"
                assert "updated successfully" in data["message"]
    
    def test_find_patient_not_found_creates_new(self, client, mock_patient_repo_dependency, 
                                              sample_patient_request):
        """Test patient creation when patient not found."""
        with patch('src.api.main.get_patient_repository') as mock_get_repo:
            mock_repo = Mock()
            mock_repo.find_patient_by_credentials = AsyncMock(return_value=None)
            mock_repo.register_mobile_app_user = AsyncMock(return_value="test-mobile-uuid")
            mock_get_repo.return_value = mock_repo
            
            with patch('src.api.main.create_his_patient') as mock_create:
                mock_create.return_value = create_mock_patient_creation_response(True, "TEST123")
                
                response = client.post("/checkModifyPatient", json=sample_patient_request)
                
                assert response.status_code == 200
                data = response.json()
                assert data["success"] == "true"
                assert data["action"] == "create"
                assert "created successfully" in data["message"]
                assert data["mobile_uuid"] == "test-mobile-uuid"


class TestOAuthAuthentication:
    """Tests for OAuth authentication functionality."""
    
    @pytest.mark.asyncio
    async def test_get_oauth_token_success(self, mock_oauth_token_response, mock_environment):
        """Test successful OAuth token acquisition."""
        with patch('httpx.AsyncClient') as mock_client:
            # Setup
            mock_response = MockAsyncResponse(200, mock_oauth_token_response)
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
            
            # Execute
            token = await get_oauth_token('yottadb')
            
            # Assert
            assert token == "mock_access_token_12345"
            assert 'yottadb_token' in oauth_tokens
            assert 'yottadb_token_expiry' in oauth_tokens
            assert oauth_tokens['yottadb_token'] == "mock_access_token_12345"
    
    @pytest.mark.asyncio
    async def test_get_oauth_token_cached(self, mock_environment):
        """Test OAuth token retrieval from cache."""
        # Setup - put token in cache
        oauth_tokens['yottadb_token'] = "cached_token_123"
        oauth_tokens['yottadb_token_expiry'] = datetime.now() + timedelta(hours=1)
        
        # Execute
        token = await get_oauth_token('yottadb')
        
        # Assert
        assert token == "cached_token_123"
    
    @pytest.mark.asyncio
    async def test_get_oauth_token_expired_cache(self, mock_oauth_token_response, mock_environment):
        """Test OAuth token refresh when cached token is expired."""
        # Setup - put expired token in cache
        oauth_tokens['yottadb_token'] = "expired_token_123"
        oauth_tokens['yottadb_token_expiry'] = datetime.now() - timedelta(minutes=1)
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = MockAsyncResponse(200, mock_oauth_token_response)
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
            
            # Execute
            token = await get_oauth_token('yottadb')
            
            # Assert
            assert token == "mock_access_token_12345"  # New token, not cached one
    
    @pytest.mark.asyncio
    async def test_get_oauth_token_failure(self, mock_environment):
        """Test OAuth token acquisition failure."""
        with patch('httpx.AsyncClient') as mock_client:
            # Setup
            mock_response = MockAsyncResponse(401, {}, "Unauthorized")
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
            
            # Execute
            token = await get_oauth_token('yottadb')
            
            # Assert
            assert token is None


class TestHISCredentialUpdate:
    """Tests for HIS credential update functionality."""
    
    @pytest.mark.asyncio
    async def test_update_his_credentials_success(self, mock_oauth_token_response, mock_environment):
        """Test successful HIS credential update."""
        with patch('httpx.AsyncClient') as mock_client, \
             patch('src.api.main.get_oauth_token') as mock_get_token:
            
            # Setup
            mock_get_token.return_value = "test_access_token"
            mock_response = MockAsyncResponse(201)
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
            
            # Execute
            result = await update_his_credentials('yottadb', 'QMS123', 'newlogin', 'newpassword')
            
            # Assert
            assert result is True
            mock_get_token.assert_called_once_with('yottadb')
    
    @pytest.mark.asyncio
    async def test_update_his_credentials_oauth_failure(self, mock_environment):
        """Test HIS credential update when OAuth fails."""
        with patch('src.api.main.get_oauth_token') as mock_get_token:
            # Setup
            mock_get_token.return_value = None
            
            # Execute
            result = await update_his_credentials('yottadb', 'QMS123', 'newlogin', 'newpassword')
            
            # Assert
            assert result is False


class TestPatientCreation:
    """Tests for patient creation functionality."""
    
    @pytest.mark.asyncio
    async def test_create_his_patient_success(self, sample_patient_request, mock_environment):
        """Test successful patient creation in HIS system."""
        from src.api.main import PatientCredentialRequest
        
        patient_data = PatientCredentialRequest(**sample_patient_request)
        
        with patch('httpx.AsyncClient') as mock_client, \
             patch('src.api.main.get_oauth_token') as mock_get_token:
            
            # Setup
            mock_get_token.return_value = "test_access_token_create"
            mock_response = MockAsyncResponse(201, {
                "pcode": "TEST123",
                "fullname": "Smith John William",
                "message": "Patient created successfully"
            })
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
            
            # Execute
            result = await create_his_patient('yottadb', patient_data)
            
            # Assert
            assert result["success"] is True
            assert result["hisnumber"] == "TEST123"
            assert result["fullname"] == "Smith John William"
            mock_get_token.assert_called_once_with('yottadb')
    
    @pytest.mark.asyncio
    async def test_create_his_patient_oauth_failure(self, sample_patient_request, mock_environment):
        """Test patient creation when OAuth fails."""
        from src.api.main import PatientCredentialRequest
        
        patient_data = PatientCredentialRequest(**sample_patient_request)
        
        with patch('src.api.main.get_oauth_token') as mock_get_token:
            # Setup
            mock_get_token.return_value = None
            
            # Execute
            result = await create_his_patient('yottadb', patient_data)
            
            # Assert
            assert result["success"] is False
            assert "OAuth authentication failed" in result["error"]


class TestMobileAppUserRegistration:
    """Tests for mobile app user registration."""
    
    @pytest.mark.asyncio
    async def test_register_mobile_app_user_success(self, mock_environment):
        """Test successful mobile app user registration."""
        mock_repo = Mock()
        mock_repo.register_mobile_app_user = AsyncMock(return_value="test-mobile-uuid")
        
        result = await register_mobile_app_user_api(
            hisnumber_qms="QMS123",
            hisnumber_infoclinica="IC456",
            patient_repo=mock_repo
        )
        
        assert result == "test-mobile-uuid"
        mock_repo.register_mobile_app_user.assert_called_once_with("QMS123", "IC456")
    
    @pytest.mark.asyncio
    async def test_register_mobile_app_user_disabled(self, mock_environment):
        """Test mobile app user registration when disabled."""
        with patch.dict(os.environ, {"MOBILE_APP_REGISTRATION_ENABLED": "false"}):
            # Force reload of config
            from src.api import config
            config.MOBILE_APP_CONFIG["registration_enabled"] = False
            
            result = await register_mobile_app_user_api(
                hisnumber_qms="QMS123",
                hisnumber_infoclinica="IC456"
            )
            
            assert result is None


class TestAPIEndpoints:
    """Tests for API endpoints - Updated for new architecture."""
    
    def test_health_check_healthy(self, client, mock_patient_repo_dependency):
        """Test health check endpoint when system is healthy."""
        with patch('src.api.main.get_database_health') as mock_health:
            mock_health.return_value = {"status": "healthy", "database": "test_db"}
            
            response = client.get("/health")
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "healthy"
            assert "timestamp" in data
            assert "database" in data
            assert "his_endpoints" in data
    
    def test_stats_endpoint(self, client, mock_patient_repo_dependency):
        """Test statistics endpoint."""
        with patch('src.api.main.get_patient_repository') as mock_get_repo:
            mock_repo = Mock()
            mock_repo.get_mobile_app_stats = AsyncMock(return_value={
                "total_mobile_users": 10,
                "both_his_registered": 5,
                "qms_only": 3,
                "infoclinica_only": 2
            })
            mock_repo.get_patient_matching_stats = AsyncMock(return_value=[
                {"match_type": "NEW_WITH_DOCUMENT", "count": 10, "new_patients_created": 10, "mobile_app_matches": 0}
            ])
            mock_get_repo.return_value = mock_repo
            
            response = client.get("/stats")
            
            assert response.status_code == 200
            data = response.json()
            assert "mobile_app_users" in data
            assert "patient_matching_24h" in data
            assert "timestamp" in data
    
class TestAPIEndpoints:
    """Tests for API endpoints - Fixed for database issues."""
    
    def test_patient_lock_unlock(self, client, mock_patient_repo_dependency):
        """Test patient lock and unlock endpoints - FIXED."""
        with patch('src.api.main.get_patient_repository') as mock_get_repo:
            mock_repo = Mock()
            mock_repo.lock_patient_matching = AsyncMock(return_value=True)
            mock_repo.unlock_patient_matching = AsyncMock(return_value=True)
            mock_get_repo.return_value = mock_repo
            
            # Test lock
            response = client.post("/patient/test-uuid-123/lock?reason=Test lock")
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            
            # Test unlock
            response = client.post("/patient/test-uuid-123/unlock")
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
    
    def test_check_modify_patient_success(self, client, mock_patient_repo_dependency, 
                                         sample_patient_request, sample_patient_db_record):
        """Test successful patient credential modification."""
        with patch('src.api.main.get_patient_repository') as mock_get_repo:
            mock_repo = Mock()
            mock_repo.find_patient_by_credentials = AsyncMock(return_value=sample_patient_db_record)
            mock_get_repo.return_value = mock_repo
            
            with patch('src.api.main.update_his_credentials', return_value=True):
                response = client.post("/checkModifyPatient", json=sample_patient_request)
                
                assert response.status_code == 200
                data = response.json()
                assert data["success"] == "true"
                assert data["action"] == "update"
    
    def test_check_modify_patient_not_found(self, client, mock_patient_repo_dependency, 
                                          sample_patient_request):
        """Test patient credential modification when patient not found."""
        with patch('src.api.main.get_patient_repository') as mock_get_repo:
            mock_repo = Mock()
            mock_repo.find_patient_by_credentials = AsyncMock(return_value=None)
            mock_repo.register_mobile_app_user = AsyncMock(return_value="test-uuid")
            mock_get_repo.return_value = mock_repo
            
            with patch('src.api.main.create_his_patient') as mock_create:
                mock_create.return_value = create_mock_patient_creation_response(True, "TEST123")
                
                response = client.post("/checkModifyPatient", json=sample_patient_request)
                
                assert response.status_code == 200
                data = response.json()
                assert data["success"] == "true"
                assert data["action"] == "create"
    
    def test_check_modify_patient_invalid_date_format(self, client, mock_patient_repo_dependency, 
                                                    sample_patient_request):
        """Test patient credential modification with invalid date format."""
        sample_patient_request["bdate"] = "invalid-date"
        
        response = client.post("/checkModifyPatient", json=sample_patient_request)
        
        assert response.status_code == 422  # Validation error
    
    def test_oauth_test_success(self, client, mock_patient_repo_dependency):
        """Test OAuth test endpoint success."""
        with patch('src.api.main.get_oauth_token') as mock_get_token:
            mock_get_token.return_value = "test_token_12345"
            
            response = client.post("/test-oauth/yottadb")
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert "successful" in data["message"]
            assert data["token_preview"] == "test_token..."
    
    def test_oauth_test_failure(self, client, mock_patient_repo_dependency):
        """Test OAuth test endpoint failure."""
        with patch('src.api.main.get_oauth_token') as mock_get_token:
            mock_get_token.return_value = None
            
            response = client.post("/test-oauth/yottadb")
            
            assert response.status_code == 401
            data = response.json()
            assert data["success"] is False
            assert "failed" in data["message"]
    
    def test_patient_lock_unlock(self, client, mock_patient_repo_dependency):
        """Test patient lock and unlock endpoints."""
        # Test lock
        response = client.post("/patient/test-uuid-123/lock?reason=Test lock")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        
        # Test unlock
        response = client.post("/patient/test-uuid-123/unlock")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
    
    def test_mobile_user_register_endpoint(self, client, mock_patient_repo_dependency):
        """Test mobile user registration endpoint."""
        with patch('src.api.main.register_mobile_app_user_api') as mock_register:
            mock_register.return_value = "test-mobile-uuid"
            
            response = client.post("/mobile-user/register?hisnumber_qms=QMS123&hisnumber_infoclinica=IC456")
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert data["mobile_uuid"] == "test-mobile-uuid"
    
    def test_root_endpoint(self, client, mock_patient_repo_dependency):
        """Test root endpoint."""
        response = client.get("/")
        
        assert response.status_code == 200
        data = response.json()
        assert "name" in data
        assert "version" in data
        assert "description" in data


class TestInputValidation:
    """Tests for input validation."""
    
    def test_valid_date_formats(self, client, mock_patient_repo_dependency):
        """Test various valid date formats."""
        valid_dates = ["1990-01-15", "2000-12-31", "1985-06-20"]
        
        for date in valid_dates:
            request_data = {
                "lastname": "Test",
                "firstname": "User",
                "bdate": date,
                "cllogin": "test_login",
                "clpassword": "test_password"
            }
            
            with patch('src.api.main.get_patient_repository') as mock_get_repo:
                mock_repo = Mock()
                mock_repo.find_patient_by_credentials = AsyncMock(return_value=None)
                mock_repo.register_mobile_app_user = AsyncMock(return_value="test-uuid")
                mock_get_repo.return_value = mock_repo
                
                with patch('src.api.main.create_his_patient') as mock_create:
                    mock_create.return_value = create_mock_patient_creation_response(False)
                    
                    response = client.post("/checkModifyPatient", json=request_data)
                    # Should not fail validation (may fail later due to mocks)
                    assert response.status_code != 422
    
    def test_invalid_date_formats(self, client, mock_patient_repo_dependency):
        """Test various invalid date formats."""
        invalid_dates = ["1990/01/15", "15-01-1990", "1990-13-01", "invalid", ""]
        
        for date in invalid_dates:
            request_data = {
                "lastname": "Test",
                "firstname": "User",
                "bdate": date,
                "cllogin": "test_login",
                "clpassword": "test_password"
            }
            
            response = client.post("/checkModifyPatient", json=request_data)
            assert response.status_code == 422  # Validation error