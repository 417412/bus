"""
Integration tests for the API.
"""

import pytest
import json
from unittest.mock import patch, AsyncMock
from fastapi.testclient import TestClient

from src.api.main import app
from src.api.tests.conftest import MockAsyncResponse


class TestIntegrationFlow:
    """Integration tests for complete API flows."""
    
    @pytest.fixture
    def client(self):
        """Create test client."""
        return TestClient(app)
    
    @patch('src.api.main.pg_connector')
    @patch('httpx.AsyncClient')
    @patch('src.api.main.get_oauth_token')
    def test_complete_successful_flow(self, mock_get_token, mock_httpx, mock_pg, client):
        """Test complete successful patient credential update flow."""
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
            'login_infoclinica': None
        }
        
        mock_pg.execute_query.return_value = (
            [tuple(patient_record.values())],
            list(patient_record.keys())
        )
        
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
        assert "2 system(s)" in data["message"]
        
        # Verify database was queried
        mock_pg.execute_query.assert_called()
        
        # Verify OAuth tokens were requested (twice for both systems)
        assert mock_get_token.call_count == 2
        
        # Verify HIS APIs were called (twice for both systems)
        assert mock_httpx_instance.post.call_count == 2
    
    @patch('src.api.main.pg_connector')
    @patch('httpx.AsyncClient')
    @patch('src.api.main.get_oauth_token')
    def test_partial_failure_flow(self, mock_get_token, mock_httpx, mock_pg, client):
        """Test flow with partial HIS system failure."""
        # Setup database response
        patient_record = {
            'uuid': 'partial-test-uuid',
            'lastname': 'Partial',
            'name': 'Test',
            'surname': None,
            'birthdate': '1985-05-20',
            'hisnumber_qms': 'QMS-PART-789',
            'hisnumber_infoclinica': 'IC-PART-012',
            'login_qms': 'partial_login',
            'login_infoclinica': None
        }
        
        mock_pg.execute_query.return_value = (
            [tuple(patient_record.values())],
            list(patient_record.keys())
        )
        
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
        assert "Failed:" in data["message"]
    
    @patch('src.api.main.pg_connector')
    def test_database_error_flow(self, mock_pg, client):
        """Test flow with database error."""
        # Setup database error
        mock_pg.execute_query.side_effect = Exception("Database connection failed")
        
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
        assert "Database error" in data["detail"]
    
    @patch('src.api.main.pg_connector')
    @patch('httpx.AsyncClient')
    def test_oauth_failure_flow(self, mock_httpx, mock_pg, client):
        """Test flow with OAuth authentication failure."""
        # Setup database response
        patient_record = {
            'uuid': 'oauth-fail-uuid',
            'lastname': 'OAuth',
            'name': 'Fail',
            'surname': None,
            'birthdate': '1990-01-01',
            'hisnumber_qms': 'QMS-OAUTH-FAIL',
            'hisnumber_infoclinica': None,
            'login_qms': 'oauth_fail_login',
            'login_infoclinica': None
        }
        
        mock_pg.execute_query.return_value = (
            [tuple(patient_record.values())],
            list(patient_record.keys())
        )
        
        # Setup OAuth failure
        mock_httpx_instance = mock_httpx.return_value.__aenter__.return_value
        mock_httpx_instance.post = AsyncMock(return_value=MockAsyncResponse(401, {}, "Unauthorized"))
        
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


class TestConcurrencyAndPerformance:
    """Tests for concurrency and performance scenarios."""
    
    @pytest.fixture
    def client(self):
        """Create test client."""
        return TestClient(app)
    
    @patch('src.api.main.pg_connector')
    @patch('httpx.AsyncClient')
    @patch('src.api.main.get_oauth_token')
    def test_concurrent_his_updates(self, mock_get_token, mock_httpx, mock_pg, client):
        """Test that HIS updates are called concurrently."""
        import asyncio
        from unittest.mock import call
        
        # Setup database response with both HIS numbers
        patient_record = {
            'uuid': 'concurrent-test-uuid',
            'lastname': 'Concurrent',
            'name': 'Test',
            'surname': None,
            'birthdate': '1990-01-01',
            'hisnumber_qms': 'QMS-CONCURRENT',
            'hisnumber_infoclinica': 'IC-CONCURRENT',
            'login_qms': 'concurrent_login',
            'login_infoclinica': None
        }
        
        mock_pg.execute_query.return_value = (
            [tuple(patient_record.values())],
            list(patient_record.keys())
        )
        
        # Setup OAuth responses
        mock_get_token.return_value = "concurrent_token"
        
        # Setup HIS API responses with slight delays to test concurrency
        async def delayed_response(url, **kwargs):
            await asyncio.sleep(0.1)  # Small delay
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
        
        # Verify both systems were called
        assert mock_httpx_instance.post.call_count == 2
        
        # Verify OAuth was called for both systems
        assert mock_get_token.call_count == 2
        mock_get_token.assert_has_calls([call('yottadb'), call('firebird')], any_order=True)