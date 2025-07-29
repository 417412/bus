"""
Tests for patient creation functionality - ENHANCED VERSION.
"""

import pytest
from unittest.mock import patch, AsyncMock, Mock
from fastapi import status

from src.api.main import create_his_patient, register_mobile_app_user_api
from src.api.tests.conftest import create_mock_patient_creation_response


class TestPatientCreation:
    """Tests for patient creation in HIS systems."""
    
    @pytest.mark.asyncio
    async def test_create_his_patient_success(self, sample_patient_request):
        """Test successful patient creation in HIS system."""
        from src.api.main import PatientCredentialRequest
        
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
            assert result["fullname"] == "Smith John William"
            mock_get_token.assert_called_once_with('yottadb')


class TestCheckModifyPatientWithCreation:
    """Tests for the main endpoint with patient creation."""
    
    def test_patient_not_found_create_success(self, client, sample_patient_request):
        """Test patient creation when patient not found."""
        with patch('src.api.main.get_patient_repo') as mock_get_repo, \
             patch('src.api.main.create_his_patient') as mock_create, \
             patch('src.api.main.register_mobile_app_user_api') as mock_register:
            
            # Setup mocks
            mock_repo = Mock()
            mock_repo.find_patient_by_credentials = AsyncMock(return_value=None)
            mock_get_repo.return_value = mock_repo
            
            mock_create.return_value = create_mock_patient_creation_response(
                True, hisnumber="TEST123", fullname="Smith John William"
            )
            mock_register.return_value = "test-mobile-uuid"
            
            response = client.post("/checkModifyPatient", json=sample_patient_request)
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] == "true"
            assert data["action"] == "create"
            assert "created successfully" in data["message"]
            
            # Verify create was called for both systems
            assert mock_create.call_count == 2
    
    def test_patient_not_found_create_partial_success(self, client, sample_patient_request):
        """Test patient creation with partial success."""
        with patch('src.api.main.get_patient_repo') as mock_get_repo, \
             patch('src.api.main.create_his_patient') as mock_create:
            
            # Setup mocks
            mock_repo = Mock()
            mock_repo.find_patient_by_credentials = AsyncMock(return_value=None)
            mock_get_repo.return_value = mock_repo
            
            # First succeeds, second fails
            mock_create.side_effect = [
                create_mock_patient_creation_response(True, hisnumber="TEST123"),
                create_mock_patient_creation_response(False, error="Creation failed")
            ]
            
            response = client.post("/checkModifyPatient", json=sample_patient_request)
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] == "partial"
            assert data["action"] == "create"
            assert "created in:" in data["message"]
            assert "Failed:" in data["message"]
    
    def test_patient_not_found_create_failure(self, client, sample_patient_request):
        """Test patient creation when all creations fail."""
        with patch('src.api.main.get_patient_repo') as mock_get_repo, \
             patch('src.api.main.create_his_patient') as mock_create:
            
            # Setup mocks
            mock_repo = Mock()
            mock_repo.find_patient_by_credentials = AsyncMock(return_value=None)
            mock_get_repo.return_value = mock_repo
            
            mock_create.return_value = create_mock_patient_creation_response(False)
            
            response = client.post("/checkModifyPatient", json=sample_patient_request)
            
            assert response.status_code == 502
            data = response.json()
            assert "Failed to create patient" in data["detail"]


class TestMobileAppUserRegistration:
    """Tests for mobile app user registration."""
    
    @pytest.mark.asyncio
    async def test_register_mobile_app_user_success(self, mock_patient_repo):
        """Test successful mobile app user registration."""
        with patch('src.api.main.MOBILE_APP_CONFIG', {
            "registration_enabled": True,
            "require_both_his": False,
            "auto_register_on_create": True
        }):
            result = await register_mobile_app_user_api(
                hisnumber_qms="QMS123",
                hisnumber_infoclinica="IC456",
                patient_repo=mock_patient_repo
            )
            
            assert result == "test-mobile-uuid"
            mock_patient_repo.register_mobile_app_user.assert_called_once_with("QMS123", "IC456")
    
    @pytest.mark.asyncio
    async def test_register_mobile_app_user_success_with_both_his_required(self, mock_patient_repo):
        """Test successful mobile app user registration when both HIS numbers are required and provided."""
        with patch('src.api.main.MOBILE_APP_CONFIG', {
            "registration_enabled": True,
            "require_both_his": True,
            "auto_register_on_create": True
        }):
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
        with patch('src.api.main.MOBILE_APP_CONFIG', {
            "registration_enabled": False,
            "require_both_his": False,
            "auto_register_on_create": True
        }):
            result = await register_mobile_app_user_api(
                hisnumber_qms="QMS123",
                hisnumber_infoclinica="IC456",
                patient_repo=mock_patient_repo
            )
            
            assert result is None
            mock_patient_repo.register_mobile_app_user.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_register_mobile_app_user_no_his_numbers(self, mock_patient_repo):
        """Test mobile app user registration without HIS numbers."""
        with patch('src.api.main.MOBILE_APP_CONFIG', {
            "registration_enabled": True,
            "require_both_his": False,
            "auto_register_on_create": True
        }):
            result = await register_mobile_app_user_api(patient_repo=mock_patient_repo)
            assert result is None
            mock_patient_repo.register_mobile_app_user.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_register_mobile_app_user_requires_both_his_only_qms(self, mock_patient_repo):
        """Test mobile app user registration when both HIS numbers required but only QMS provided."""
        with patch('src.api.main.MOBILE_APP_CONFIG', {
            "registration_enabled": True,
            "require_both_his": True,
            "auto_register_on_create": True
        }):
            # Only QMS HIS number provided
            result = await register_mobile_app_user_api(
                hisnumber_qms="QMS123",
                patient_repo=mock_patient_repo
            )
            assert result is None
            mock_patient_repo.register_mobile_app_user.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_register_mobile_app_user_requires_both_his_only_infoclinica(self, mock_patient_repo):
        """Test mobile app user registration when both HIS numbers required but only Infoclinica provided."""
        with patch('src.api.main.MOBILE_APP_CONFIG', {
            "registration_enabled": True,
            "require_both_his": True,
            "auto_register_on_create": True
        }):
            # Only Infoclinica HIS number provided
            result = await register_mobile_app_user_api(
                hisnumber_infoclinica="IC456",
                patient_repo=mock_patient_repo
            )
            assert result is None
            mock_patient_repo.register_mobile_app_user.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_register_mobile_app_user_single_his_when_not_required(self, mock_patient_repo):
        """Test mobile app user registration with single HIS number when both not required."""
        with patch('src.api.main.MOBILE_APP_CONFIG', {
            "registration_enabled": True,
            "require_both_his": False,
            "auto_register_on_create": True
        }):
            # Only QMS HIS number provided, but both not required
            result = await register_mobile_app_user_api(
                hisnumber_qms="QMS123",
                patient_repo=mock_patient_repo
            )
            
            assert result == "test-mobile-uuid"
            mock_patient_repo.register_mobile_app_user.assert_called_once_with("QMS123", None)
    
    @pytest.mark.asyncio
    async def test_register_mobile_app_user_repository_failure(self, mock_patient_repo):
        """Test mobile app user registration when repository returns None."""
        with patch('src.api.main.MOBILE_APP_CONFIG', {
            "registration_enabled": True,
            "require_both_his": False,
            "auto_register_on_create": True
        }):
            # Mock repository to return None (registration failed)
            mock_patient_repo.register_mobile_app_user = AsyncMock(return_value=None)
            
            result = await register_mobile_app_user_api(
                hisnumber_qms="QMS123",
                hisnumber_infoclinica="IC456",
                patient_repo=mock_patient_repo
            )
            
            assert result is None
            mock_patient_repo.register_mobile_app_user.assert_called_once_with("QMS123", "IC456")
    
    @pytest.mark.asyncio
    async def test_register_mobile_app_user_repository_exception(self, mock_patient_repo):
        """Test mobile app user registration when repository raises exception."""
        with patch('src.api.main.MOBILE_APP_CONFIG', {
            "registration_enabled": True,
            "require_both_his": False,
            "auto_register_on_create": True
        }):
            # Mock repository to raise exception
            mock_patient_repo.register_mobile_app_user = AsyncMock(
                side_effect=Exception("Database connection failed")
            )
            
            result = await register_mobile_app_user_api(
                hisnumber_qms="QMS123",
                hisnumber_infoclinica="IC456",
                patient_repo=mock_patient_repo
            )
            
            assert result is None
            mock_patient_repo.register_mobile_app_user.assert_called_once_with("QMS123", "IC456")
    
    @pytest.mark.asyncio
    async def test_register_mobile_app_user_no_patient_repo(self):
        """Test mobile app user registration when no patient repo provided."""
        with patch('src.api.main.MOBILE_APP_CONFIG', {
            "registration_enabled": True,
            "require_both_his": False,
            "auto_register_on_create": True
        }), patch('src.api.main.get_patient_repository') as mock_get_repo:
            
            # Mock the get_patient_repository function
            mock_repo = Mock()
            mock_repo.register_mobile_app_user = AsyncMock(return_value="auto-repo-uuid")
            mock_get_repo.return_value = mock_repo
            
            result = await register_mobile_app_user_api(
                hisnumber_qms="QMS123",
                hisnumber_infoclinica="IC456"
            )
            
            assert result == "auto-repo-uuid"
            mock_get_repo.assert_called_once()
            mock_repo.register_mobile_app_user.assert_called_once_with("QMS123", "IC456")

class TestPatientCreationTestEndpoint:
    """Tests for the patient creation test endpoint."""
    
    def test_test_patient_creation_success(self, client, sample_patient_request):
        """Test patient creation test endpoint success."""
        with patch('src.api.main.create_his_patient') as mock_create:
            mock_create.return_value = create_mock_patient_creation_response(
                True, hisnumber="TEST123", fullname="Smith John William"
            )
            
            response = client.post("/test-create/yottadb", json=sample_patient_request)
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert "creation successful" in data["message"]
            assert "Smith, John" in data["patient"]
    
    def test_test_patient_creation_failure(self, client, sample_patient_request):
        """Test patient creation test endpoint failure."""
        with patch('src.api.main.create_his_patient') as mock_create:
            mock_create.return_value = create_mock_patient_creation_response(False)
            
            response = client.post("/test-create/yottadb", json=sample_patient_request)
            
            assert response.status_code == 502
            data = response.json()
            assert data["success"] is False
            assert "creation failed" in data["message"]
    
    def test_test_patient_creation_invalid_his_type(self, client, sample_patient_request):
        """Test patient creation test endpoint with invalid HIS type."""
        response = client.post("/test-create/invalid", json=sample_patient_request)
        
        assert response.status_code == 400
        data = response.json()
        assert "Invalid HIS type" in data["detail"]