"""
Integration tests for the API.
"""

import pytest
import json
from unittest.mock import patch, AsyncMock, Mock
from fastapi.testclient import TestClient

from src.api.main import app
from src.api.tests.conftest import MockAsyncResponse, create_mock_patient_creation_response


class TestIntegrationFlow:
    """Integration tests for complete API flows."""
    
    @pytest.fixture
    def client(self):
        """Create test client."""
        return TestClient(app)
    
    def test_complete_successful_flow(self, client, mock_patient_repo_dependency):
        """Test complete successful patient credential update flow."""
        # Setup patient record
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
        
        with patch('src.api.main.get_patient_repository') as mock_get_repo:
            mock_repo = Mock()
            mock_repo.find_patient_by_credentials = AsyncMock(return_value=patient_record)
            mock_get_repo.return_value = mock_repo
            
            with patch('src.api.main.get_oauth_token') as mock_get_token, \
                 patch('httpx.AsyncClient') as mock_httpx:
                
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
                
                # Assert response
                assert response.status_code == 200
                data = response.json()
                assert data["success"] == "true"
                assert data["action"] == "update"
                assert "2 system(s)" in data["message"]
                
                # Verify patient repository was called
                mock_repo.find_patient_by_credentials.assert_called_once()
                
                # Verify OAuth tokens were requested (twice for both systems)
                assert mock_get_token.call_count == 2
                
                # Verify HIS APIs were called (twice for both systems)
                assert mock_httpx_instance.post.call_count == 2
    
    def test_partial_failure_flow(self, client, mock_patient_repo_dependency):
        """Test flow with partial HIS system failure."""
        # Setup patient record
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
        
        with patch('src.api.main.get_patient_repository') as mock_get_repo:
            mock_repo = Mock()
            mock_repo.find_patient_by_credentials = AsyncMock(return_value=patient_record)
            mock_get_repo.return_value = mock_repo
            
            with patch('src.api.main.get_oauth_token') as mock_get_token, \
                 patch('httpx.AsyncClient') as mock_httpx:
                
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
                
                # Assert response
                assert response.status_code == 200
                data = response.json()
                assert data["success"] == "partial"
                assert data["action"] == "update"
                assert "Failed:" in data["message"]
    
    def test_database_error_flow(self, client, mock_patient_repo_dependency):
        """Test flow with database error."""
        with patch('src.api.main.get_patient_repository') as mock_get_repo:
            mock_repo = Mock()
            mock_repo.find_patient_by_credentials = AsyncMock(
                side_effect=Exception("Database connection failed")
            )
            mock_get_repo.return_value = mock_repo
            
            # Execute request
            request_data = {
                "lastname": "Error",
                "firstname": "Test",
                "bdate": "1990-01-01",
                "cllogin": "error_login",
                "clpassword": "error_password"
            }
            
            response = client.post("/checkModifyPatient", json=request_data)
            
            # Assert response
            assert response.status_code == 500
            data = response.json()
            assert "Internal server error" in data["detail"]
    
    def test_oauth_failure_flow(self, client, mock_patient_repo_dependency):
        """Test flow with OAuth authentication failure."""
        # Setup patient record with only QMS number
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
        
        with patch('src.api.main.get_patient_repository') as mock_get_repo:
            mock_repo = Mock()
            mock_repo.find_patient_by_credentials = AsyncMock(return_value=patient_record)
            mock_get_repo.return_value = mock_repo
            
            with patch('src.api.main.get_oauth_token') as mock_get_token:
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
                
                # Assert response
                assert response.status_code == 502
                data = response.json()
                assert "Failed to update credentials" in data["detail"]
    
    def test_patient_creation_flow(self, client, mock_patient_repo_dependency):
        """Test complete patient creation flow."""
        with patch('src.api.main.get_patient_repository') as mock_get_repo:
            mock_repo = Mock()
            mock_repo.find_patient_by_credentials = AsyncMock(return_value=None)  # Patient not found
            mock_repo.register_mobile_app_user = AsyncMock(return_value="test-mobile-uuid")
            mock_get_repo.return_value = mock_repo
            
            with patch('src.api.main.create_his_patient') as mock_create:
                # Setup successful creation responses
                mock_create.side_effect = [
                    create_mock_patient_creation_response(True, "QMS123"),
                    create_mock_patient_creation_response(True, "IC456")
                ]
                
                # Execute request
                request_data = {
                    "lastname": "NewPatient",
                    "firstname": "Test",
                    "midname": "Create",
                    "bdate": "1990-01-01",
                    "cllogin": "new_patient_login",
                    "clpassword": "new_patient_password"
                }
                
                response = client.post("/checkModifyPatient", json=request_data)
                
                # Assert response
                assert response.status_code == 200
                data = response.json()
                assert data["success"] == "true"
                assert data["action"] == "create"
                assert data["mobile_uuid"] == "test-mobile-uuid"
                
                # Verify patient repository was called
                mock_repo.find_patient_by_credentials.assert_called_once()
                mock_repo.register_mobile_app_user.assert_called_once()
                
                # Verify create was called for both systems
                assert mock_create.call_count == 2


class TestConcurrencyAndPerformance:
    """Tests for concurrency and performance scenarios."""
    
    @pytest.fixture
    def client(self):
        """Create test client."""
        return TestClient(app)
    
    def test_concurrent_his_updates(self, client, mock_patient_repo_dependency):
        """Test that HIS updates are called concurrently."""
        import asyncio
        from unittest.mock import call
        
        # Setup patient record with both HIS numbers
        patient_record = {
            'uuid': 'concurrent-test-uuid',
            'lastname': 'Concurrent',
            'name': 'Test',
            'surname': None,
            'birthdate': '1990-01-01',
            'hisnumber_qms': 'QMS-CONCURRENT',
            'hisnumber_infoclinica': 'IC-CONCURRENT',
            'login_qms': 'concurrent_login',
            'login_infoclinica': None,
            'registered_via_mobile': False,
            'matching_locked': False
        }
        
        with patch('src.api.main.get_patient_repository') as mock_get_repo:
            mock_repo = Mock()
            mock_repo.find_patient_by_credentials = AsyncMock(return_value=patient_record)
            mock_get_repo.return_value = mock_repo
            
            with patch('src.api.main.get_oauth_token') as mock_get_token, \
                 patch('httpx.AsyncClient') as mock_httpx:
                
                # Setup OAuth responses
                mock_get_token.return_value = "concurrent_token"
                
                # Setup HIS API responses with slight delays to test concurrency
                async def delayed_response(*args, **kwargs):
                    await asyncio.sleep(0.01)  # Small delay
                    return MockAsyncResponse(201)
                
                mock_httpx_instance = mock_httpx.return_value.__aenter__.return_value
                mock_httpx_instance.post = AsyncMock(side_effect=delayed_response)
                
                # Execute request
                request_data = {
                    "lastname": "Concurrent",
                    "firstname": "Test",
                    "bdate": "1990-01-01",
                    "cllogin": "concurrent_login",
                    "clpassword": "concurrent_password"
                }
                
                response = client.post("/checkModifyPatient", json=request_data)
                
                # Assert response
                assert response.status_code == 200
                data = response.json()
                assert data["success"] == "true"
                assert data["action"] == "update"
                
                # Verify both systems were called
                assert mock_httpx_instance.post.call_count == 2
                
                # Verify OAuth was called for both systems
                assert mock_get_token.call_count == 2
                mock_get_token.assert_has_calls([call('yottadb'), call('firebird')], any_order=True)
    
    def test_multiple_concurrent_requests(self, client, mock_patient_repo_dependency):
        """Test handling multiple concurrent requests."""
        import concurrent.futures
        import time
        
        with patch('src.api.main.get_patient_repository') as mock_get_repo:
            mock_repo = Mock()
            mock_repo.find_patient_by_credentials = AsyncMock(return_value=None)
            mock_repo.register_mobile_app_user = AsyncMock(return_value="test-uuid")
            mock_get_repo.return_value = mock_repo
            
            with patch('src.api.main.create_his_patient') as mock_create:
                mock_create.return_value = create_mock_patient_creation_response(False)  # Quick failure
                
                def make_request(i):
                    request_data = {
                        "lastname": f"User{i}",
                        "firstname": "Test",
                        "bdate": "1990-01-01",
                        "cllogin": f"user{i}_login",
                        "clpassword": "password"
                    }
                    return client.post("/checkModifyPatient", json=request_data)
                
                # Execute multiple concurrent requests
                start_time = time.time()
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    futures = [executor.submit(make_request, i) for i in range(10)]
                    responses = [f.result() for f in concurrent.futures.as_completed(futures)]
                end_time = time.time()
                
                # All should return some response (success or failure)
                assert len(responses) == 10
                
                # Should handle requests in reasonable time
                assert end_time - start_time < 5.0  # 5 seconds for 10 requests