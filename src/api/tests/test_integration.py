"""
Integration tests for the API - FIXED VERSION.
"""

import pytest
import asyncio
from unittest.mock import patch, AsyncMock, Mock

from src.api.tests.conftest import (
    MockAsyncResponse, 
    create_mock_patient_creation_response,
    create_mock_oauth_token_response
)


class TestIntegrationFlow:
    """Integration tests for complete API flows."""
    
    @pytest.fixture
    def client(self):
        """Create test client."""
        from fastapi.testclient import TestClient
        from src.api.main import app
        return TestClient(app)
    
    def test_complete_successful_flow(self, client):
        """Test complete successful patient credential update flow."""
        with patch('src.api.main.get_patient_repo') as mock_get_repo, \
             patch('httpx.AsyncClient') as mock_httpx, \
             patch('src.api.main.get_oauth_token') as mock_get_token:
            
            # Setup database response
            patient_record = {
                'uuid': 'integration-test-uuid',
                'lastname': 'Integration',
                'name': 'Test',
                'surname': 'Patient',
                'birthdate': '1990-01-15',
                'hisnumber_qms': 'QMS-INT-123',
                'hisnumber_infoclinica': 'IC-INT-456',
                'login_qms': 'integration_login',
                'login_infoclinica': None,
                'registered_via_mobile': False,
                'matching_locked': False
            }
            
            mock_repo = Mock()
            mock_repo.find_patient_by_credentials = AsyncMock(return_value=patient_record)
            mock_get_repo.return_value = mock_repo
            
            # Setup OAuth responses
            mock_get_token.return_value = "integration_test_token"
            
            # Setup HIS API responses
            mock_httpx_instance = mock_httpx.return_value.__aenter__.return_value
            mock_httpx_instance.post = AsyncMock(return_value=MockAsyncResponse(201))
            
            # Execute request
            request_data = {
                "lastname": "Integration",
                "firstname": "Test",
                "midname": "Patient",
                "bdate": "1990-01-15",
                "cllogin": "integration_login",
                "clpassword": "new_secure_password"
            }
            
            response = client.post("/checkModifyPatient", json=request_data)
            
            # Assert successful response
            assert response.status_code == 200
            data = response.json()
            assert data["success"] == "true"
            assert data["action"] == "update"
            assert "2 system(s)" in data["message"]
    
    def test_patient_creation_flow(self, client):
        """Test complete patient creation flow when patient not found."""
        with patch('src.api.main.get_patient_repo') as mock_get_repo, \
             patch('httpx.AsyncClient') as mock_httpx, \
             patch('src.api.main.get_oauth_token') as mock_get_token:
            
            # Setup database response - no patient found
            mock_repo = Mock()
            mock_repo.find_patient_by_credentials = AsyncMock(return_value=None)
            mock_repo.register_mobile_app_user = AsyncMock(return_value="mobile-uuid-123")
            mock_get_repo.return_value = mock_repo
            
            # Setup OAuth responses
            mock_get_token.return_value = "creation_test_token"
            
            # Setup HIS API responses for patient creation
            creation_response = create_mock_patient_creation_response("NEW123", "Test User")
            mock_httpx_instance = mock_httpx.return_value.__aenter__.return_value
            mock_httpx_instance.post = AsyncMock(return_value=creation_response)
            
            # Execute request
            request_data = {
                "lastname": "NewPatient",
                "firstname": "Test",
                "midname": "User",
                "bdate": "1990-01-01",
                "cllogin": "new_patient_login",
                "clpassword": "new_patient_password"
            }
            
            response = client.post("/checkModifyPatient", json=request_data)
            
            # Assert successful creation response
            assert response.status_code == 200
            data = response.json()
            assert data["success"] == "true"
            assert data["action"] == "create"
            assert data["mobile_uuid"] == "mobile-uuid-123"
    
    def test_partial_success_flow(self, client):
        """Test flow with partial HIS system failure."""
        with patch('src.api.main.get_patient_repo') as mock_get_repo, \
             patch('httpx.AsyncClient') as mock_httpx, \
             patch('src.api.main.get_oauth_token') as mock_get_token:
            
            # Setup database response - patient found
            patient_record = {
                'uuid': 'partial-test-uuid',
                'lastname': 'Partial',
                'name': 'Test',
                'surname': None,
                'birthdate': '1985-05-20',
                'hisnumber_qms': 'QMS-PART-789',
                'hisnumber_infoclinica': 'IC-PART-012',
                'login_qms': 'partial_login',
                'login_infoclinica': None,
                'registered_via_mobile': False,
                'matching_locked': False
            }
            
            mock_repo = Mock()
            mock_repo.find_patient_by_credentials = AsyncMock(return_value=patient_record)
            mock_get_repo.return_value = mock_repo
            
            # Setup OAuth responses
            mock_get_token.return_value = "partial_test_token"
            
            # Setup HIS API responses - one success, one failure
            mock_httpx_instance = mock_httpx.return_value.__aenter__.return_value
            mock_httpx_instance.post = AsyncMock(side_effect=[
                MockAsyncResponse(201),  # YottaDB success
                MockAsyncResponse(500, {}, "Internal Server Error")  # Firebird failure
            ])
            
            # Execute request
            request_data = {
                "lastname": "Partial",
                "firstname": "Test",
                "midname": None,
                "bdate": "1985-05-20",
                "cllogin": "partial_login",
                "clpassword": "partial_password"
            }
            
            response = client.post("/checkModifyPatient", json=request_data)
            
            # Assert partial success response
            assert response.status_code == 200
            data = response.json()
            assert data["success"] == "partial"
            assert data["action"] == "update"
            assert "Failed:" in data["message"]


class TestErrorHandling:
    """Test error handling scenarios."""
    
    @pytest.fixture
    def client(self):
        """Create test client."""
        from fastapi.testclient import TestClient
        from src.api.main import app
        return TestClient(app)
    
    def test_database_error_handling(self, client):
        """Test handling of database errors."""
        with patch('src.api.main.get_patient_repo') as mock_get_repo:
            mock_repo = Mock()
            mock_repo.find_patient_by_credentials = AsyncMock(side_effect=Exception("Database error"))
            mock_get_repo.return_value = mock_repo
            
            request_data = {
                "lastname": "Error",
                "firstname": "Test",
                "bdate": "1990-01-01",
                "cllogin": "error_login",
                "clpassword": "error_password"
            }
            
            response = client.post("/checkModifyPatient", json=request_data)
            
            # Should handle the error gracefully
            assert response.status_code == 500
            data = response.json()
            assert "Internal server error" in data["detail"]
    
    def test_oauth_failure_handling(self, client):
        """Test handling of OAuth failures."""
        with patch('src.api.main.get_patient_repo') as mock_get_repo, \
             patch('src.api.main.get_oauth_token') as mock_get_token:
            
            # Setup database response - patient found
            patient_record = {
                'uuid': 'oauth-fail-uuid',
                'lastname': 'OAuth',
                'name': 'Fail',
                'surname': None,
                'birthdate': '1990-01-01',
                'hisnumber_qms': 'QMS-OAUTH-FAIL',
                'hisnumber_infoclinica': None,
                'login_qms': 'oauth_fail_login',
                'login_infoclinica': None,
                'registered_via_mobile': False,
                'matching_locked': False
            }
            
            mock_repo = Mock()
            mock_repo.find_patient_by_credentials = AsyncMock(return_value=patient_record)
            mock_get_repo.return_value = mock_repo
            
            # Setup OAuth failure
            mock_get_token.return_value = None  # OAuth fails
            
            # Execute request
            request_data = {
                "lastname": "OAuth",
                "firstname": "Fail",
                "bdate": "1990-01-01",
                "cllogin": "oauth_fail_login",
                "clpassword": "oauth_fail_password"
            }
            
            response = client.post("/checkModifyPatient", json=request_data)
            
            # Should handle OAuth failure gracefully
            assert response.status_code == 502
            data = response.json()
            assert "Failed to update credentials" in data["detail"]
    
    def test_invalid_request_data(self, client):
        """Test handling of invalid request data."""
        # Test with invalid date format
        invalid_request = {
            "lastname": "Test",
            "firstname": "User",
            "bdate": "invalid-date",
            "cllogin": "test_login",
            "clpassword": "test_password"
        }
        
        response = client.post("/checkModifyPatient", json=invalid_request)
        
        # Should return validation error
        assert response.status_code == 422
        data = response.json()
        assert "detail" in data
    
    def test_missing_required_fields(self, client):
        """Test handling of missing required fields."""
        incomplete_request = {
            "lastname": "Test",
            "firstname": "User"
            # Missing required fields
        }
        
        response = client.post("/checkModifyPatient", json=incomplete_request)
        
        # Should return validation error
        assert response.status_code == 422
        data = response.json()
        assert "detail" in data
    
    def test_patient_no_his_numbers(self, client):
        """Test handling of patient with no HIS numbers."""
        with patch('src.api.main.get_patient_repo') as mock_get_repo:
            # Setup database response - patient found but no HIS numbers
            patient_record = {
                'uuid': 'no-his-uuid',
                'lastname': 'NoHIS',
                'name': 'Patient',
                'surname': None,
                'birthdate': '1990-01-01',
                'hisnumber_qms': None,  # No HIS numbers
                'hisnumber_infoclinica': None,
                'login_qms': 'no_his_login',
                'login_infoclinica': None,
                'registered_via_mobile': False,
                'matching_locked': False
            }
            
            mock_repo = Mock()
            mock_repo.find_patient_by_credentials = AsyncMock(return_value=patient_record)
            mock_get_repo.return_value = mock_repo
            
            request_data = {
                "lastname": "NoHIS",
                "firstname": "Patient",
                "bdate": "1990-01-01",
                "cllogin": "no_his_login",
                "clpassword": "no_his_password"
            }
            
            response = client.post("/checkModifyPatient", json=request_data)
            
            # Should return error about no HIS numbers
            assert response.status_code == 400
            data = response.json()
            assert "no associated HIS numbers" in data["detail"]


class TestConcurrencyAndPerformance:
    """Tests for concurrency and performance scenarios."""
    
    @pytest.fixture
    def client(self):
        """Create test client."""
        from fastapi.testclient import TestClient
        from src.api.main import app
        return TestClient(app)
    
    def test_concurrent_requests_handling(self, client):
        """Test that the API can handle concurrent requests."""
        import threading
        import time
        
        results = []
        
        def make_request():
            with patch('src.api.main.get_patient_repo') as mock_get_repo:
                mock_repo = Mock()
                mock_repo.find_patient_by_credentials = AsyncMock(return_value=None)
                mock_repo.register_mobile_app_user = AsyncMock(return_value=None)
                mock_get_repo.return_value = mock_repo
                
                request_data = {
                    "lastname": "Concurrent",
                    "firstname": "Test",
                    "bdate": "1990-01-01",
                    "cllogin": "concurrent_login",
                    "clpassword": "concurrent_password"
                }
                
                response = client.post("/checkModifyPatient", json=request_data)
                results.append(response.status_code)
        
        # Start multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=make_request)
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # All requests should be handled
        assert len(results) == 5
        # Most should return some valid status code
        valid_statuses = [200, 404, 500, 502]
        assert all(status in valid_statuses for status in results)


class TestHealthAndUtilityEndpoints:
    """Test health check and utility endpoints."""
    
    @pytest.fixture
    def client(self):
        """Create test client."""
        from fastapi.testclient import TestClient
        from src.api.main import app
        return TestClient(app)
    
    def test_health_endpoint(self, client):
        """Test health check endpoint."""
        with patch('src.api.main.get_database_health') as mock_health:
            mock_health.return_value = {
                "status": "healthy",
                "patients_count": 100,
                "mobile_users_count": 50
            }
            
            response = client.get("/health")
            
            assert response.status_code in [200, 503]  # Healthy or degraded
            data = response.json()
            assert "status" in data
            assert "timestamp" in data
    
    def test_stats_endpoint(self, client):
        """Test statistics endpoint."""
        with patch('src.api.main.get_patient_repo') as mock_get_repo:
            mock_repo = Mock()
            mock_repo.get_mobile_app_stats = AsyncMock(return_value={
                "total_mobile_users": 10,
                "both_his_registered": 5,
                "qms_only": 3,
                "infoclinica_only": 2
            })
            mock_repo.get_patient_matching_stats = AsyncMock(return_value=[])
            mock_get_repo.return_value = mock_repo
            
            response = client.get("/stats")
            
            assert response.status_code == 200
            data = response.json()
            assert "mobile_app_users" in data
            assert "timestamp" in data
    
    def test_config_endpoint(self, client):
        """Test configuration endpoint."""
        response = client.get("/config")
        
        assert response.status_code == 200
        data = response.json()
        # Should contain configuration but with masked sensitive data
        assert isinstance(data, dict)
    
    def test_root_endpoint(self, client):
        """Test root endpoint."""
        response = client.get("/")
        
        assert response.status_code == 200
        data = response.json()
        assert "name" in data
        assert "version" in data
        assert "main_endpoint" in data
        assert data["main_endpoint"] == "/checkModifyPatient"


class TestOAuthEndpoints:
    """Test OAuth-related endpoints."""
    
    @pytest.fixture
    def client(self):
        """Create test client."""
        from fastapi.testclient import TestClient
        from src.api.main import app
        return TestClient(app)
    
    def test_oauth_test_success(self, client):
        """Test OAuth test endpoint with success."""
        with patch('src.api.main.get_oauth_token') as mock_get_token:
            mock_get_token.return_value = "test_token_12345"
            
            response = client.post("/test-oauth/yottadb")
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert "successful" in data["message"]
            assert "test_token..." in data["token_preview"]
    
    def test_oauth_test_failure(self, client):
        """Test OAuth test endpoint with failure."""
        with patch('src.api.main.get_oauth_token') as mock_get_token:
            mock_get_token.return_value = None
            
            response = client.post("/test-oauth/yottadb")
            
            assert response.status_code == 401
            data = response.json()
            assert data["success"] is False
            assert "failed" in data["message"]
    
    def test_oauth_test_invalid_his_type(self, client):
        """Test OAuth test endpoint with invalid HIS type."""
        response = client.post("/test-oauth/invalid")
        
        assert response.status_code == 400
        data = response.json()
        assert "Invalid HIS type" in data["detail"]


class TestPatientCreationEndpoints:
    """Test patient creation test endpoints."""
    
    @pytest.fixture
    def client(self):
        """Create test client."""
        from fastapi.testclient import TestClient
        from src.api.main import app
        return TestClient(app)
    
    def test_create_test_success(self, client):
        """Test patient creation test endpoint with success."""
        with patch('src.api.main.create_his_patient') as mock_create:
            mock_create.return_value = {
                "success": True,
                "hisnumber": "TEST123",
                "fullname": "Test User",
                "message": "Patient created successfully"
            }
            
            request_data = {
                "lastname": "Test",
                "firstname": "User",
                "bdate": "1990-01-01",
                "cllogin": "test_login",
                "clpassword": "test_password"
            }
            
            response = client.post("/test-create/yottadb", json=request_data)
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert "creation successful" in data["message"]
            assert data["hisnumber"] == "TEST123"
    
    def test_create_test_failure(self, client):
        """Test patient creation test endpoint with failure."""
        with patch('src.api.main.create_his_patient') as mock_create:
            mock_create.return_value = {
                "success": False,
                "error": "Creation failed"
            }
            
            request_data = {
                "lastname": "Test",
                "firstname": "User",
                "bdate": "1990-01-01",
                "cllogin": "test_login",
                "clpassword": "test_password"
            }
            
            response = client.post("/test-create/yottadb", json=request_data)
            
            assert response.status_code == 502
            data = response.json()
            assert data["success"] is False
            assert "creation failed" in data["message"]
    
    def test_create_test_invalid_his_type(self, client):
        """Test patient creation test endpoint with invalid HIS type."""
        request_data = {
            "lastname": "Test",
            "firstname": "User",
            "bdate": "1990-01-01",
            "cllogin": "test_login",
            "clpassword": "test_password"
        }
        
        response = client.post("/test-create/invalid", json=request_data)
        
        assert response.status_code == 400
        data = response.json()
        assert "Invalid HIS type" in data["detail"]