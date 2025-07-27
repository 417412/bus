"""
Tests for patient creation functionality.
"""

import pytest
from unittest.mock import patch, AsyncMock
from fastapi import status

from src.api.main import create_his_patient
from src.api.tests.conftest import MockAsyncResponse


class TestPatientCreation:
    """Tests for patient creation in HIS systems."""
    
    @pytest.mark.asyncio
    async def test_create_his_patient_success(self, sample_patient_request, mock_environment):
        """Test successful patient creation in HIS system."""
        from src.api.main import PatientCredentialRequest
        
        patient_data = PatientCredentialRequest(**sample_patient_request)
        
        with patch('httpx.AsyncClient') as mock_client, \
             patch('src.api.main.get_oauth_token') as mock_get_token:
            
            # Setup
            mock_get_token.return_value = "test_access_token_create"
            mock_response = MockAsyncResponse(201)
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
            
            # Execute
            result = await create_his_patient('yottadb', patient_data)
            
            # Assert
            assert result is True
            mock_get_token.assert_called_once_with('yottadb')
            
            # Verify API call was made with correct data
            call_args = mock_client.return_value.__aenter__.return_value.post.call_args
            assert '/createPatients' in call_args[0][0]  # URL contains create endpoint
            
            # Verify payload structure
            payload = call_args[1]['json']
            assert payload['lastname'] == sample_patient_request['lastname']
            assert payload['firstname'] == sample_patient_request['firstname']
            assert payload['midname'] == sample_patient_request['midname']
            assert payload['bdate'] == sample_patient_request['bdate']
            assert payload['cllogin'] == sample_patient_request['cllogin']
            assert payload['clpassword'] == sample_patient_request['clpassword']
    
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
            assert result is False
    
    @pytest.mark.asyncio
    async def test_create_his_patient_api_failure(self, sample_patient_request, mock_environment):
        """Test patient creation when API call fails."""
        from src.api.main import PatientCredentialRequest
        
        patient_data = PatientCredentialRequest(**sample_patient_request)
        
        with patch('httpx.AsyncClient') as mock_client, \
             patch('src.api.main.get_oauth_token') as mock_get_token:
            
            # Setup
            mock_get_token.return_value = "test_access_token"
            mock_response = MockAsyncResponse(400, {}, "Bad Request")
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
            
            # Execute
            result = await create_his_patient('yottadb', patient_data)
            
            # Assert
            assert result is False
    
    @pytest.mark.asyncio
    async def test_create_his_patient_token_expired_retry(self, sample_patient_request, mock_environment):
        """Test patient creation with token expiry and retry."""
        from src.api.main import PatientCredentialRequest
        
        patient_data = PatientCredentialRequest(**sample_patient_request)
        
        with patch('httpx.AsyncClient') as mock_client, \
             patch('src.api.main.get_oauth_token') as mock_get_token:
            
            # Setup
            mock_get_token.side_effect = ["expired_token", "new_token"]
            
            # First call returns 401, second call returns 201
            mock_client_instance = mock_client.return_value.__aenter__.return_value
            mock_client_instance.post = AsyncMock(side_effect=[
                MockAsyncResponse(401, {}, "Unauthorized"),
                MockAsyncResponse(201)
            ])
            
            # Execute
            result = await create_his_patient('yottadb', patient_data)
            
            # Assert
            assert result is True
            assert mock_get_token.call_count == 2  # Called twice due to retry


class TestCheckModifyPatientWithCreation:
    """Tests for the main endpoint with patient creation."""
    
    @patch('src.api.main.find_patient_by_credentials')
    @patch('src.api.main.create_his_patient')
    def test_patient_not_found_create_success(self, mock_create, mock_find, client, sample_patient_request):
        """Test patient creation when patient not found."""
        # Setup
        mock_find.return_value = None  # Patient not found
        mock_create.return_value = True  # Creation succeeds
        
        # Execute
        response = client.post("/checkModifyPatient", json=sample_patient_request)
        
        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["success"] == "true"
        assert data["action"] == "create"
        assert "created successfully" in data["message"]
        assert "2 system(s)" in data["message"]  # Both YottaDB and Firebird
        
        # Verify create was called for both systems
        assert mock_create.call_count == 2
        
        # Verify calls were made for both systems
        call_args = [call[0] for call in mock_create.call_args_list]
        his_types = [args[0] for args in call_args]
        assert 'yottadb' in his_types
        assert 'firebird' in his_types
    
    @patch('src.api.main.find_patient_by_credentials')
    @patch('src.api.main.create_his_patient')
    def test_patient_not_found_create_partial_success(self, mock_create, mock_find, client, sample_patient_request):
        """Test patient creation with partial success."""
        # Setup
        mock_find.return_value = None  # Patient not found
        mock_create.side_effect = [True, False]  # First succeeds, second fails
        
        # Execute
        response = client.post("/checkModifyPatient", json=sample_patient_request)
        
        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["success"] == "partial"
        assert data["action"] == "create"
        assert "created in:" in data["message"]
        assert "Failed:" in data["message"]
    
    @patch('src.api.main.find_patient_by_credentials')
    @patch('src.api.main.create_his_patient')
    def test_patient_not_found_create_failure(self, mock_create, mock_find, client, sample_patient_request):
        """Test patient creation when all creations fail."""
        # Setup
        mock_find.return_value = None  # Patient not found
        mock_create.return_value = False  # All creations fail
        
        # Execute
        response = client.post("/checkModifyPatient", json=sample_patient_request)
        
        # Assert
        assert response.status_code == 502
        data = response.json()
        assert "Failed to create patient" in data["detail"]
    
    @patch('src.api.main.find_patient_by_credentials')
    @patch('src.api.main.update_his_credentials')
    def test_patient_found_update_vs_create(self, mock_update, mock_find, client, 
                                           sample_patient_request, sample_patient_db_record):
        """Test that when patient is found, update is called instead of create."""
        # Setup
        mock_find.return_value = sample_patient_db_record  # Patient found
        mock_update.return_value = True
        
        # Execute
        response = client.post("/checkModifyPatient", json=sample_patient_request)
        
        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["success"] == "true"
        assert data["action"] == "update"  # Should be update, not create
        assert "updated successfully" in data["message"]


class TestPatientCreationIntegration:
    """Integration tests for patient creation flow."""
    
    @patch('src.api.main.pg_connector')
    @patch('httpx.AsyncClient')
    @patch('src.api.main.get_oauth_token')
    def test_complete_patient_creation_flow(self, mock_get_token, mock_httpx, mock_pg, client):
        """Test complete patient creation flow."""
        # Setup database response - no patient found
        mock_pg.execute_query.return_value = ([], [])  # Empty result
        
        # Setup OAuth responses
        mock_get_token.return_value = "creation_test_token"
        
        # Setup HIS API responses for creation
        mock_httpx_instance = mock_httpx.return_value.__aenter__.return_value
        mock_httpx_instance.post = AsyncMock(return_value=MockAsyncResponse(201))
        
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
        assert "2 system(s)" in data["message"]
        
        # Verify database was queried
        mock_pg.execute_query.assert_called()
        
        # Verify OAuth tokens were requested (twice for both systems)
        assert mock_get_token.call_count == 2
        
        # Verify HIS create APIs were called (twice for both systems)
        assert mock_httpx_instance.post.call_count == 2
        
        # Verify create endpoints were called
        call_args = [call[0] for call in mock_httpx_instance.post.call_args_list]
        urls = [args[0] for args in call_args]
        assert all('/createPatients' in url for url in urls)
        
        # Verify payload structure in API calls
        payloads = [call[1]['json'] for call in mock_httpx_instance.post.call_args_list]
        for payload in payloads:
            assert payload['lastname'] == 'NewPatient'
            assert payload['firstname'] == 'Test'
            assert payload['midname'] == 'Create'
            assert payload['bdate'] == '1990-01-01'
            assert payload['cllogin'] == 'new_patient_login'
            assert payload['clpassword'] == 'new_patient_password'


class TestPatientCreationTestEndpoint:
    """Tests for the patient creation test endpoint."""
    
    @patch('src.api.main.create_his_patient')
    def test_test_patient_creation_success(self, mock_create, client, sample_patient_request):
        """Test patient creation test endpoint success."""
        # Setup
        mock_create.return_value = True
        
        # Execute
        response = client.post("/test-create/yottadb", json=sample_patient_request)
        
        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "creation successful" in data["message"]
        assert "Smith, John" in data["patient"]
    
    @patch('src.api.main.create_his_patient')
    def test_test_patient_creation_failure(self, mock_create, client, sample_patient_request):
        """Test patient creation test endpoint failure."""
        # Setup
        mock_create.return_value = False
        
        # Execute
        response = client.post("/test-create/yottadb", json=sample_patient_request)
        
        # Assert
        assert response.status_code == 502
        data = response.json()
        assert data["success"] is False
        assert "creation failed" in data["message"]
    
    def test_test_patient_creation_invalid_his_type(self, client, sample_patient_request):
        """Test patient creation test endpoint with invalid HIS type."""
        # Execute
        response = client.post("/test-create/invalid", json=sample_patient_request)
        
        # Assert
        assert response.status_code == 400
        data = response.json()
        assert "Invalid HIS type" in data["detail"]
    
    def test_test_patient_creation_invalid_data(self, client):
        """Test patient creation test endpoint with invalid data."""
        invalid_data = {
            "lastname": "Test",
            "firstname": "User",
            "bdate": "invalid-date",  # Invalid date format
            "cllogin": "test_login",
            "clpassword": "test_password"
        }
        
        # Execute
        response = client.post("/test-create/yottadb", json=invalid_data)
        
        # Assert
        assert response.status_code == 422  # Validation error


class TestConcurrentPatientOperations:
    """Tests for concurrent patient operations."""
    
    @pytest.mark.asyncio
    async def test_concurrent_patient_creation(self):
        """Test concurrent patient creation in both HIS systems."""
        from src.api.main import create_his_patient, PatientCredentialRequest
        
        patient_data = PatientCredentialRequest(
            lastname="Concurrent",
            firstname="Test",
            midname="Patient",
            bdate="1990-01-01",
            cllogin="concurrent_login",
            clpassword="concurrent_password"
        )
        
        with patch('httpx.AsyncClient') as mock_client, \
             patch('src.api.main.get_oauth_token') as mock_get_token:
            
            # Setup
            mock_get_token.return_value = "concurrent_token"
            
            async def delayed_create_response(*args, **kwargs):
                import asyncio
                await asyncio.sleep(0.1)  # Small delay
                return MockAsyncResponse(201)
            
            mock_httpx_instance = mock_client.return_value.__aenter__.return_value
            mock_httpx_instance.post = AsyncMock(side_effect=delayed_create_response)
            
            # Execute concurrent creations
            import time
            start_time = time.time()
            tasks = [
                create_his_patient('yottadb', patient_data),
                create_his_patient('firebird', patient_data)
            ]
            results = await asyncio.gather(*tasks)
            end_time = time.time()
            
            # Assert
            assert all(result is True for result in results)
            
            # Should complete faster than 200ms (2 * 100ms) due to concurrency
            assert end_time - start_time < 0.15  # Allow some overhead
            
            # Verify both systems were called
            assert mock_httpx_instance.post.call_count == 2