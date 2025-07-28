"""
Tests for patient creation functionality - Fixed version.
"""
import pytest
import os
from unittest.mock import patch, AsyncMock, Mock
from fastapi import status

from src.api.main import create_his_patient, register_mobile_app_user_api
from src.api.tests.conftest import MockAsyncResponse, create_mock_patient_creation_response


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


class TestCheckModifyPatientWithCreation:
    """Tests for the main endpoint with patient creation - FIXED."""
    
    def test_patient_not_found_create_partial_success(self, client, mock_patient_repo_dependency, 
                                                     sample_patient_request):
        """Test patient creation with partial success - FIXED."""
        # Force mobile app registration to be enabled
        with patch.dict(os.environ, {
            "MOBILE_APP_REGISTRATION_ENABLED": "true",
            "MOBILE_APP_AUTO_REGISTER": "true"
        }):
            # Force reload of config
            from src.api import config
            config.MOBILE_APP_CONFIG["registration_enabled"] = True
            config.MOBILE_APP_CONFIG["auto_register_on_create"] = True
            
            with patch('src.api.main.get_patient_repository') as mock_get_repo:
                mock_repo = Mock()
                mock_repo.find_patient_by_credentials = AsyncMock(return_value=None)
                mock_repo.register_mobile_app_user = AsyncMock(return_value="test-mobile-uuid")
                mock_get_repo.return_value = mock_repo
                
                with patch('src.api.main.create_his_patient') as mock_create:
                    # FIXED: First succeeds with proper dict format, second fails properly
                    mock_create.side_effect = [
                        {"success": True, "hisnumber": "TEST123", "message": "Created successfully"},
                        {"success": False, "error": "Creation failed"}
                    ]
                    
                    response = client.post("/checkModifyPatient", json=sample_patient_request)
                    
                    assert response.status_code == 200
                    data = response.json()
                    assert data["success"] == "partial"
                    assert data["action"] == "create"
                    assert "created in:" in data["message"]
                    assert "Failed:" in data["message"]
    
    def test_patient_not_found_create_failure(self, client, mock_patient_repo_dependency, 
                                            sample_patient_request):
        """Test patient creation when all creations fail - FIXED."""
        with patch('src.api.main.get_patient_repository') as mock_get_repo:
            mock_repo = Mock()
            mock_repo.find_patient_by_credentials = AsyncMock(return_value=None)
            mock_get_repo.return_value = mock_repo
            
            with patch('src.api.main.create_his_patient') as mock_create:
                # FIXED: Both return failure properly in dict format
                mock_create.return_value = {"success": False, "error": "Creation failed"}
                
                response = client.post("/checkModifyPatient", json=sample_patient_request)
                
                assert response.status_code == 502
                data = response.json()
                assert "Failed to create patient" in data["detail"]
    
    def test_patient_not_found_create_success(self, client, mock_patient_repo_dependency, 
                                             sample_patient_request):
        """Test patient creation when patient not found - FIXED."""
        # Force mobile app registration to be enabled
        with patch.dict(os.environ, {
            "MOBILE_APP_REGISTRATION_ENABLED": "true",
            "MOBILE_APP_AUTO_REGISTER": "true"
        }):
            # Force reload of config
            from src.api import config
            config.MOBILE_APP_CONFIG["registration_enabled"] = True
            config.MOBILE_APP_CONFIG["auto_register_on_create"] = True
            
            with patch('src.api.main.get_patient_repository') as mock_get_repo:
                mock_repo = Mock()
                mock_repo.find_patient_by_credentials = AsyncMock(return_value=None)
                mock_repo.register_mobile_app_user = AsyncMock(return_value="test-mobile-uuid")
                mock_get_repo.return_value = mock_repo
                
                with patch('src.api.main.create_his_patient') as mock_create:
                    mock_create.return_value = {"success": True, "hisnumber": "TEST123", "message": "Created successfully"}
                    
                    response = client.post("/checkModifyPatient", json=sample_patient_request)
                    
                    assert response.status_code == 200
                    data = response.json()
                    assert data["success"] == "true"
                    assert data["action"] == "create"
                    assert "created successfully" in data["message"]
                    assert data["mobile_uuid"] == "test-mobile-uuid"
                    
                    # Verify create was called for both systems
                    assert mock_create.call_count == 2


class TestMobileAppUserRegistration:
    """Tests for mobile app user registration - FIXED."""
    
    @pytest.mark.asyncio
    async def test_register_mobile_app_user_success(self):
        """Test successful mobile app user registration - FIXED."""
        # Force mobile app registration to be enabled
        with patch.dict(os.environ, {
            "MOBILE_APP_REGISTRATION_ENABLED": "true"
        }):
            # Force reload of config
            from src.api import config
            config.MOBILE_APP_CONFIG["registration_enabled"] = True
            
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
    async def test_register_mobile_app_user_no_his_numbers(self, mock_environment):
        """Test mobile app user registration with no HIS numbers."""
        result = await register_mobile_app_user_api()
        
        assert result is None
    
    @pytest.mark.asyncio 
    async def test_register_mobile_app_user_requires_both_his(self, mock_environment):
        """Test mobile app user registration when both HIS numbers are required."""
        with patch.dict('os.environ', {"MOBILE_APP_REQUIRE_BOTH_HIS": "true"}):
            # Force reload of config
            from src.api import config
            config.MOBILE_APP_CONFIG["require_both_his"] = True
            
            result = await register_mobile_app_user_api(hisnumber_qms="QMS123")
            
            assert result is None


class TestPatientCreationTestEndpoint:
    """Tests for the patient creation test endpoint."""
    
    def test_test_patient_creation_success(self, client, mock_patient_repo_dependency, 
                                         sample_patient_request):
        """Test patient creation test endpoint success."""
        with patch('src.api.main.create_his_patient') as mock_create:
            mock_create.return_value = create_mock_patient_creation_response(True, "TEST123")
            
            response = client.post("/test-create/yottadb", json=sample_patient_request)
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert "creation successful" in data["message"]
            assert "Smith, John" in data["patient"]
            assert data["hisnumber"] == "TEST123"
    
    def test_test_patient_creation_failure(self, client, mock_patient_repo_dependency, 
                                         sample_patient_request):
        """Test patient creation test endpoint failure."""
        with patch('src.api.main.create_his_patient') as mock_create:
            mock_create.return_value = create_mock_patient_creation_response(False)
            
            response = client.post("/test-create/yottadb", json=sample_patient_request)
            
            assert response.status_code == 502
            data = response.json()
            assert data["success"] is False
            assert "creation failed" in data["message"]
    
    def test_test_patient_creation_invalid_his_type(self, client, mock_patient_repo_dependency, 
                                                   sample_patient_request):
        """Test patient creation test endpoint with invalid HIS type."""
        response = client.post("/test-create/invalid", json=sample_patient_request)
        
        assert response.status_code == 400
        data = response.json()
        assert "Invalid HIS type" in data["detail"]