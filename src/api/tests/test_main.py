"""
Tests for the main API endpoints.
"""

import pytest
import json
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime, timedelta
from fastapi import status

from src.api.main import (
    find_patient_by_credentials, 
    get_oauth_token, 
    update_his_credentials,
    oauth_tokens
)
from src.api.tests.conftest import MockAsyncResponse


class TestPatientSearch:
    """Tests for patient search functionality."""
    
    @patch('src.api.main.pg_connector')
    def test_find_patient_with_midname_success(self, mock_connector, sample_patient_db_record):
        """Test successful patient search with middle name."""
        # Setup
        mock_connector.execute_query.return_value = (
            [tuple(sample_patient_db_record.values())],
            list(sample_patient_db_record.keys())
        )
        
        # Execute
        result = find_patient_by_credentials(
            lastname="Smith",
            firstname="John", 
            midname="William",
            bdate="1990-01-15",
            cllogin="jsmith_login"
        )
        
        # Assert
        assert result is not None
        assert result['uuid'] == 'test-uuid-123'
        assert result['lastname'] == 'Smith'
        assert result['hisnumber_qms'] == 'QMS123456'
        assert result['hisnumber_infoclinica'] == 'IC789012'
        
        # Verify query was called with correct parameters
        mock_connector.execute_query.assert_called_once()
        call_args = mock_connector.execute_query.call_args
        assert "surname = %s" in call_args[0][0]  # Query should include surname check
        assert call_args[0][1] == ("Smith", "John", "William", "1990-01-15", "jsmith_login", "jsmith_login")
    
    @patch('src.api.main.pg_connector')
    def test_find_patient_without_midname_success(self, mock_connector, sample_patient_db_record_partial):
        """Test successful patient search without middle name."""
        # Setup
        mock_connector.execute_query.return_value = (
            [tuple(sample_patient_db_record_partial.values())],
            list(sample_patient_db_record_partial.keys())
        )
        
        # Execute
        result = find_patient_by_credentials(
            lastname="Doe",
            firstname="Jane", 
            midname=None,
            bdate="1985-05-20",
            cllogin="jdoe_login"
        )
        
        # Assert
        assert result is not None
        assert result['uuid'] == 'test-uuid-456'
        assert result['lastname'] == 'Doe'
        assert result['hisnumber_qms'] == 'QMS789012'
        assert result['hisnumber_infoclinica'] is None
        
        # Verify query was called with correct parameters
        mock_connector.execute_query.assert_called_once()
        call_args = mock_connector.execute_query.call_args
        assert "surname IS NULL OR surname = ''" in call_args[0][0]  # Query should check for NULL surname
        assert call_args[0][1] == ("Doe", "Jane", "1985-05-20", "jdoe_login", "jdoe_login")
    
    @patch('src.api.main.pg_connector')
    def test_find_patient_not_found(self, mock_connector):
        """Test patient search when no patient is found."""
        # Setup
        mock_connector.execute_query.return_value = ([], [])
        
        # Execute
        result = find_patient_by_credentials(
            lastname="NotFound",
            firstname="Patient", 
            midname=None,
            bdate="2000-01-01",
            cllogin="nonexistent_login"
        )
        
        # Assert
        assert result is None
    
    @patch('src.api.main.pg_connector')
    def test_find_patient_multiple_results_warning(self, mock_connector, sample_patient_db_record):
        """Test patient search when multiple patients are found."""
        # Setup - return two identical records
        mock_connector.execute_query.return_value = (
            [tuple(sample_patient_db_record.values()), tuple(sample_patient_db_record.values())],
            list(sample_patient_db_record.keys())
        )
        
        # Execute
        result = find_patient_by_credentials(
            lastname="Smith",
            firstname="John", 
            midname="William",
            bdate="1990-01-15",
            cllogin="jsmith_login"
        )
        
        # Assert - should return first result
        assert result is not None
        assert result['uuid'] == 'test-uuid-123'


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
    
    @pytest.mark.asyncio
    async def test_get_oauth_token_missing_access_token(self, mock_environment):
        """Test OAuth response missing access_token field."""
        with patch('httpx.AsyncClient') as mock_client:
            # Setup
            mock_response = MockAsyncResponse(200, {"token_type": "Bearer"})  # Missing access_token
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
    
    @pytest.mark.asyncio
    async def test_update_his_credentials_api_failure(self, mock_environment):
        """Test HIS credential update when API call fails."""
        with patch('httpx.AsyncClient') as mock_client, \
             patch('src.api.main.get_oauth_token') as mock_get_token:
            
            # Setup
            mock_get_token.return_value = "test_access_token"
            mock_response = MockAsyncResponse(400, {}, "Bad Request")
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
            
            # Execute
            result = await update_his_credentials('yottadb', 'QMS123', 'newlogin', 'newpassword')
            
            # Assert
            assert result is False
    
    @pytest.mark.asyncio
    async def test_update_his_credentials_token_expired_retry(self, mock_oauth_token_response, mock_environment):
        """Test HIS credential update with token expiry and retry."""
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
            result = await update_his_credentials('yottadb', 'QMS123', 'newlogin', 'newpassword')
            
            # Assert
            assert result is True
            assert mock_get_token.call_count == 2  # Called twice due to retry


class TestAPIEndpoints:
    """Tests for API endpoints."""
    
    @patch('src.api.main.pg_connector')
    def test_health_check_healthy(self, mock_connector, client):
        """Test health check endpoint when system is healthy."""
        # Setup
        mock_connector.connection = True
        mock_connector.execute_query.return_value = ([1], ['column'])
        
        # Execute
        response = client.get("/health")
        
        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["database"] == "connected"
        assert "timestamp" in data
        assert "his_endpoints" in data
    
    @patch('src.api.main.pg_connector')
    def test_health_check_unhealthy(self, mock_connector, client):
        """Test health check endpoint when system is unhealthy."""
        # Setup
        mock_connector.connection = False
        
        # Execute
        response = client.get("/health")
        
        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy" 
        assert data["database"] == "disconnected"
    
    @patch('src.api.main.find_patient_by_credentials')
    @patch('src.api.main.update_his_credentials') 
    def test_check_modify_patient_success(self, mock_update, mock_find, client, 
                                         sample_patient_request, sample_patient_db_record):
        """Test successful patient credential modification."""
        # Setup
        mock_find.return_value = sample_patient_db_record
        mock_update.return_value = True
        
        # Execute
        response = client.post("/checkModifyPatient", json=sample_patient_request)
        
        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["success"] == "true"
        assert "updated successfully" in data["message"]
    
    @patch('src.api.main.find_patient_by_credentials')
    def test_check_modify_patient_not_found(self, mock_find, client, sample_patient_request):
        """Test patient credential modification when patient not found."""
        # Setup
        mock_find.return_value = None
        
        # Execute
        response = client.post("/checkModifyPatient", json=sample_patient_request)
        
        # Assert
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"].lower()
    
    @patch('src.api.main.find_patient_by_credentials')
    def test_check_modify_patient_no_his_numbers(self, mock_find, client, sample_patient_request):
        """Test patient credential modification when patient has no HIS numbers."""
        # Setup
        patient_no_his = {
            'uuid': 'test-uuid-123',
            'lastname': 'Smith',
            'name': 'John',
            'surname': 'William',
            'birthdate': '1990-01-15',
            'hisnumber_qms': None,
            'hisnumber_infoclinica': None,
            'login_qms': 'jsmith_login',
            'login_infoclinica': None
        }
        mock_find.return_value = patient_no_his
        
        # Execute
        response = client.post("/checkModifyPatient", json=sample_patient_request)
        
        # Assert
        assert response.status_code == 400
        data = response.json()
        assert "no associated HIS numbers" in data["detail"]
    
    @patch('src.api.main.find_patient_by_credentials')
    @patch('src.api.main.update_his_credentials')
    def test_check_modify_patient_partial_success(self, mock_update, mock_find, client, 
                                                 sample_patient_request, sample_patient_db_record):
        """Test patient credential modification with partial success."""
        # Setup
        mock_find.return_value = sample_patient_db_record
        mock_update.side_effect = [True, False]  # First succeeds, second fails
        
        # Execute
        response = client.post("/checkModifyPatient", json=sample_patient_request)
        
        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["success"] == "partial"
        assert "Failed:" in data["message"]
    
    def test_check_modify_patient_invalid_date_format(self, client, sample_patient_request):
        """Test patient credential modification with invalid date format."""
        # Setup
        sample_patient_request["bdate"] = "invalid-date"
        
        # Execute
        response = client.post("/checkModifyPatient", json=sample_patient_request)
        
        # Assert
        assert response.status_code == 422  # Validation error
    
    def test_check_modify_patient_missing_fields(self, client):
        """Test patient credential modification with missing required fields."""
        # Setup
        incomplete_request = {
            "lastname": "Smith",
            "firstname": "John"
            # Missing required fields
        }
        
        # Execute
        response = client.post("/checkModifyPatient", json=incomplete_request)
        
        # Assert
        assert response.status_code == 422  # Validation error


class TestOAuthTestEndpoint:
    """Tests for OAuth testing endpoint."""
    
    @patch('src.api.main.get_oauth_token')
    def test_oauth_test_success(self, mock_get_token, client):
        """Test OAuth test endpoint success."""
        # Setup
        mock_get_token.return_value = "test_token_12345"
        
        # Execute
        response = client.post("/test-oauth/yottadb")
        
        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "successful" in data["message"]
        assert data["token_preview"] == "test_token..."
    
    @patch('src.api.main.get_oauth_token')
    def test_oauth_test_failure(self, mock_get_token, client):
        """Test OAuth test endpoint failure."""
        # Setup
        mock_get_token.return_value = None
        
        # Execute
        response = client.post("/test-oauth/yottadb")
        
        # Assert
        assert response.status_code == 401
        data = response.json()
        assert data["success"] is False
        assert "failed" in data["message"]
    
    def test_oauth_test_invalid_his_type(self, client):
        """Test OAuth test endpoint with invalid HIS type."""
        # Execute
        response = client.post("/test-oauth/invalid")
        
        # Assert
        assert response.status_code == 400
        data = response.json()
        assert "Invalid HIS type" in data["detail"]


class TestInputValidation:
    """Tests for input validation."""
    
    def test_valid_date_formats(self, client):
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
            
            with patch('src.api.main.find_patient_by_credentials') as mock_find:
                mock_find.return_value = None
                response = client.post("/checkModifyPatient", json=request_data)
                # Should not fail validation (will fail with 404 due to mock)
                assert response.status_code != 422
    
    def test_invalid_date_formats(self, client):
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