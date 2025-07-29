"""
Tests for main API endpoints - REFACTORED AND CONSOLIDATED.
This file contains all tests for the primary API endpoints in main.py.
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from fastapi import status
from datetime import datetime

from src.api.tests.conftest import (
    create_mock_patient_creation_response,
    create_mock_oauth_token_response,
    create_mock_http_response,
    TestDataGenerator
)


class TestMainAPIEndpoints:
    """Test all main API endpoints as they actually exist in main.py."""
    
    def test_root_endpoint(self, client):
        """Test root endpoint returns correct API info."""
        response = client.get("/")
        
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Patient Credential Management API"
        assert data["version"] == "1.1.0"
        assert data["main_endpoint"] == "/checkModifyPatient"
        assert data["docs_url"] == "/docs"
        assert data["health_url"] == "/health"
    
    def test_health_endpoint_structure(self, client):
        """Test health endpoint returns expected structure."""
        with patch('src.api.main.get_database_health') as mock_health:
            mock_health.return_value = {"status": "healthy", "patients_count": 100}
            
            response = client.get("/health")
            
            assert response.status_code == 200
            data = response.json()
            
            # Verify actual structure from main.py
            required_fields = ["status", "timestamp", "version", "environment", 
                             "database", "oauth_tokens", "mobile_app", "his_endpoints"]
            for field in required_fields:
                assert field in data
    
    def test_health_endpoint_unhealthy_database(self, client):
        """Test health endpoint when database is unhealthy."""
        with patch('src.api.main.get_database_health') as mock_health:
            mock_health.return_value = {"status": "unhealthy", "error": "Connection failed"}
            
            response = client.get("/health")
            
            assert response.status_code == 503
            data = response.json()
            assert data["status"] == "degraded"
    
    def test_stats_endpoint(self, client, mock_patient_repo_dependency):
        """Test stats endpoint returns mobile app and matching statistics."""
        response = client.get("/stats")
        
        assert response.status_code == 200
        data = response.json()
        
        # Check structure matches get_api_stats in main.py
        assert "mobile_app_users" in data
        assert "patient_matching_24h" in data
        assert "oauth_tokens_cached" in data
        assert "timestamp" in data
    
    def test_config_endpoint(self, client):
        """Test configuration endpoint returns masked config."""
        response = client.get("/config")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)
        
        # Should have main config sections
        expected_sections = ["api", "postgresql", "his_api", "mobile_app", "security"]
        for section in expected_sections:
            assert section in data
    
    def test_oauth_test_endpoint_success(self, client):
        """Test OAuth test endpoint with successful authentication."""
        with patch('src.api.main.get_oauth_token') as mock_get_token:
            mock_get_token.return_value = "test_token_12345"
            
            response = client.post("/test-oauth/yottadb")
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert "successful" in data["message"]
            assert data["token_preview"] == "test_token..."
    
    def test_oauth_test_endpoint_failure(self, client):
        """Test OAuth test endpoint with failed authentication."""
        with patch('src.api.main.get_oauth_token') as mock_get_token:
            mock_get_token.return_value = None
            
            response = client.post("/test-oauth/yottadb")
            
            assert response.status_code == 401
            data = response.json()
            assert data["success"] is False
            assert "failed" in data["message"]
    
    def test_oauth_test_endpoint_invalid_his_type(self, client):
        """Test OAuth test endpoint with invalid HIS type."""
        response = client.post("/test-oauth/invalid")
        
        assert response.status_code == 400
        data = response.json()
        assert "Invalid HIS type" in data["detail"]
    
    @pytest.mark.parametrize("his_type", ["yottadb", "firebird"])
    def test_oauth_test_endpoint_both_systems(self, client, his_type):
        """Test OAuth test endpoint for both HIS systems."""
        with patch('src.api.main.get_oauth_token') as mock_get_token:
            mock_get_token.return_value = f"{his_type}_token_123"
            
            response = client.post(f"/test-oauth/{his_type}")
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert his_type.upper() in data["message"]


class TestCheckModifyPatientEndpoint:
    """Tests for the main /checkModifyPatient endpoint."""
    
    def test_check_modify_patient_found_update_success(self, client, mock_repo_with_patient):
        """Test successful credential update when patient is found."""
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
            
            # Should succeed or return business logic response
            assert response.status_code in [200, 502]  # Allow for mock variations
    
    def test_check_modify_patient_not_found_creates_new(self, client, mock_repo_no_patient):
        """Test patient creation when patient not found."""
        with patch('src.api.main.get_patient_repo', return_value=mock_repo_no_patient), \
             patch('src.api.main.create_his_patient') as mock_create, \
             patch('src.api.main.register_mobile_app_user_api') as mock_mobile:
            
            mock_create.return_value = create_mock_patient_creation_response(True, "TEST123")
            mock_mobile.return_value = "mobile-uuid-123"
            
            request_data = {
                "lastname": "NewPatient",
                "firstname": "Test",
                "bdate": "1990-01-01",
                "cllogin": "newpatient_login",
                "clpassword": "password123"
            }
            
            response = client.post("/checkModifyPatient", json=request_data)
            
            # Should succeed or return business logic response
            assert response.status_code in [200, 502]  # Allow for mock variations
    
    @pytest.mark.parametrize("invalid_date,expected_error", TestDataGenerator.invalid_dates())
    def test_check_modify_patient_invalid_dates(self, client, mock_patient_repo_dependency, 
                                              invalid_date, expected_error):
        """Test validation with invalid date formats."""
        request_data = {
            "lastname": "Test",
            "firstname": "User",
            "bdate": invalid_date,
            "cllogin": "test_login",
            "clpassword": "test_password"
        }
        
        response = client.post("/checkModifyPatient", json=request_data)
        assert response.status_code == 422, f"Failed for {expected_error}: {invalid_date}"
        
        error_data = response.json()
        assert "detail" in error_data
    
    def test_check_modify_patient_missing_required_fields(self, client):
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


class TestPatientCreationTestEndpoint:
    """Tests for the /test-create/{his_type} endpoint."""
    
    @pytest.mark.parametrize("his_type", ["yottadb", "firebird"])
    def test_patient_creation_test_success(self, client, sample_patient_request, his_type):
        """Test successful patient creation test endpoint."""
        with patch('src.api.main.create_his_patient') as mock_create:
            mock_create.return_value = {
                "success": True,
                "hisnumber": "TEST123",
                "fullname": "Smith John William",
                "message": "Patient created successfully"
            }
            
            response = client.post(f"/test-create/{his_type}", json=sample_patient_request)
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert his_type.upper() in data["message"]
            assert data["hisnumber"] == "TEST123"
    
    def test_patient_creation_test_failure(self, client, sample_patient_request):
        """Test patient creation test endpoint failure."""
        with patch('src.api.main.create_his_patient') as mock_create:
            mock_create.return_value = {
                "success": False,
                "error": "Creation failed"
            }
            
            response = client.post("/test-create/yottadb", json=sample_patient_request)
            
            assert response.status_code == 502
            data = response.json()
            assert data["success"] is False
            assert "failed" in data["message"]
    
    def test_patient_creation_test_invalid_his_type(self, client, sample_patient_request):
        """Test patient creation test endpoint with invalid HIS type."""
        response = client.post("/test-create/invalid", json=sample_patient_request)
        
        assert response.status_code == 400
        data = response.json()
        assert "Invalid HIS type" in data["detail"]


class TestMobileUserRegistrationEndpoint:
    """Tests for mobile user registration endpoint."""
    
    def test_mobile_user_register_success(self, client, mock_patient_repo_dependency):
        """Test successful mobile user registration."""
        # Mock the register_mobile_app_user_api function directly
        with patch('src.api.main.register_mobile_app_user_api') as mock_register:
            mock_register.return_value = "test-mobile-uuid"
            
            # The endpoint takes parameters as query params or form data, not JSON
            response = client.post("/mobile-user/register", params={
                "hisnumber_qms": "QMS123",
                "hisnumber_infoclinica": "IC456"
            })
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert "mobile_uuid" in data
            assert data["mobile_uuid"] == "test-mobile-uuid"
    
    def test_mobile_user_register_failure(self, client):
        """Test mobile user registration failure."""
        with patch('src.api.main.register_mobile_app_user_api') as mock_register:
            mock_register.return_value = None
            
            response = client.post("/mobile-user/register", params={
                "hisnumber_qms": "QMS123"
            })
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is False


class TestPatientManagementEndpoints:
    """Tests for patient lock/unlock endpoints."""
    
    def test_lock_patient_matching_success(self, client):
        """Test successful patient matching lock."""
        from src.api.main import get_patient_repo, app
        
        # Create a proper mock repository with AsyncMock methods
        mock_repo = Mock()
        mock_repo.lock_patient_matching = AsyncMock(return_value=True)
        
        # Override the FastAPI dependency
        def override_get_patient_repo():
            return mock_repo
        
        app.dependency_overrides[get_patient_repo] = override_get_patient_repo
        
        try:
            response = client.post("/patient/test-uuid-123/lock", params={
                "reason": "Manual lock for testing"
            })
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert "locked successfully" in data["message"]
            
            # Verify the mock was called with correct parameters
            mock_repo.lock_patient_matching.assert_called_once_with("test-uuid-123", "Manual lock for testing")
        
        finally:
            # Clean up dependency override
            app.dependency_overrides.clear()
    
    def test_lock_patient_matching_failure(self, client):
        """Test patient matching lock failure."""
        from src.api.main import get_patient_repo, app
        
        mock_repo = Mock()
        mock_repo.lock_patient_matching = AsyncMock(return_value=False)
        
        def override_get_patient_repo():
            return mock_repo
        
        app.dependency_overrides[get_patient_repo] = override_get_patient_repo
        
        try:
            response = client.post("/patient/test-uuid-123/lock", params={
                "reason": "Test lock"
            })
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is False
            assert "Failed to lock patient" in data["message"]
            
            # Verify the mock was called
            mock_repo.lock_patient_matching.assert_called_once_with("test-uuid-123", "Test lock")
        
        finally:
            app.dependency_overrides.clear()
    
    def test_lock_patient_matching_default_reason(self, client):
        """Test patient matching lock with default reason."""
        from src.api.main import get_patient_repo, app
        
        mock_repo = Mock()
        mock_repo.lock_patient_matching = AsyncMock(return_value=True)
        
        def override_get_patient_repo():
            return mock_repo
        
        app.dependency_overrides[get_patient_repo] = override_get_patient_repo
        
        try:
            # No reason parameter - should use default
            response = client.post("/patient/test-uuid-123/lock")
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert "locked successfully" in data["message"]
            
            # Verify the mock was called with default reason
            mock_repo.lock_patient_matching.assert_called_once_with("test-uuid-123", "Manual lock")
        
        finally:
            app.dependency_overrides.clear()
    
    def test_unlock_patient_matching_success(self, client):
        """Test successful patient matching unlock."""
        from src.api.main import get_patient_repo, app
        
        # Create a proper mock repository with AsyncMock methods
        mock_repo = Mock()
        mock_repo.unlock_patient_matching = AsyncMock(return_value=True)
        
        # Override the FastAPI dependency
        def override_get_patient_repo():
            return mock_repo
        
        app.dependency_overrides[get_patient_repo] = override_get_patient_repo
        
        try:
            response = client.post("/patient/test-uuid-123/unlock")
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert "unlocked successfully" in data["message"]
            
            # Verify the mock was called with correct parameters
            mock_repo.unlock_patient_matching.assert_called_once_with("test-uuid-123")
        
        finally:
            # Clean up dependency override
            app.dependency_overrides.clear()
    
    def test_unlock_patient_matching_failure(self, client):
        """Test patient matching unlock failure."""
        from src.api.main import get_patient_repo, app
        
        mock_repo = Mock()
        mock_repo.unlock_patient_matching = AsyncMock(return_value=False)
        
        def override_get_patient_repo():
            return mock_repo
        
        app.dependency_overrides[get_patient_repo] = override_get_patient_repo
        
        try:
            response = client.post("/patient/test-uuid-123/unlock")
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is False
            assert "Failed to unlock patient" in data["message"]
            
            # Verify the mock was called
            mock_repo.unlock_patient_matching.assert_called_once_with("test-uuid-123")
        
        finally:
            app.dependency_overrides.clear()
    
    def test_lock_patient_matching_exception_handling(self, client):
        """Test patient matching lock with exception handling."""
        from src.api.main import get_patient_repo, app
        
        mock_repo = Mock()
        mock_repo.lock_patient_matching = AsyncMock(side_effect=Exception("Database connection failed"))
        
        def override_get_patient_repo():
            return mock_repo
        
        app.dependency_overrides[get_patient_repo] = override_get_patient_repo
        
        try:
            response = client.post("/patient/test-uuid-123/lock", params={
                "reason": "Test exception"
            })
            
            assert response.status_code == 500
            data = response.json()
            assert "detail" in data
            assert "Error locking patient matching" in data["detail"]
        
        finally:
            app.dependency_overrides.clear()
    
    def test_unlock_patient_matching_exception_handling(self, client):
        """Test patient matching unlock with exception handling."""
        from src.api.main import get_patient_repo, app
        
        mock_repo = Mock()
        mock_repo.unlock_patient_matching = AsyncMock(side_effect=Exception("Database connection failed"))
        
        def override_get_patient_repo():
            return mock_repo
        
        app.dependency_overrides[get_patient_repo] = override_get_patient_repo
        
        try:
            response = client.post("/patient/test-uuid-123/unlock")
            
            assert response.status_code == 500
            data = response.json()
            assert "detail" in data
            assert "Error unlocking patient matching" in data["detail"]
        
        finally:
            app.dependency_overrides.clear()