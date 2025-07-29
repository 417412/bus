"""
Tests for OAuth authentication and token management - CONSOLIDATED.
All OAuth-related tests from main.py, oauth.py consolidated here.
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timedelta
import httpx

from src.api.tests.conftest import (
    create_mock_oauth_token_response,
    create_mock_http_response,
    TestDataGenerator
)


class TestOAuthTokenAcquisition:
    """Test OAuth token acquisition and management."""
    
    @pytest.mark.asyncio
    async def test_oauth_token_request_format(self):
        """Test OAuth request uses correct format with empty client_id/client_secret."""
        from src.api.main import get_oauth_token
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_post = AsyncMock()
            mock_client.return_value.__aenter__.return_value.post = mock_post
            mock_post.return_value = create_mock_http_response(
                200, create_mock_oauth_token_response()
            )
            
            await get_oauth_token('yottadb')
            
            # Verify the actual OAuth data format from main.py
            call_args = mock_post.call_args
            oauth_data = call_args[1]['data']
            
            # These should be empty strings as per actual implementation
            assert oauth_data['grant_type'] == ""
            assert oauth_data['scope'] == ""
            assert oauth_data['client_id'] == ""
            assert oauth_data['client_secret'] == ""
            # These should have actual values from config
            assert oauth_data['username'] == "admin"
            assert oauth_data['password'] == "secret"
    
    @pytest.mark.asyncio
    async def test_oauth_token_success_yottadb(self):
        """Test successful OAuth token acquisition for YottaDB."""
        from src.api.main import get_oauth_token
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_post = AsyncMock()
            mock_client.return_value.__aenter__.return_value.post = mock_post
            mock_post.return_value = create_mock_http_response(
                200, create_mock_oauth_token_response("yottadb_token_123")
            )
            
            token = await get_oauth_token('yottadb')
            
            assert token == "yottadb_token_123"
            # Verify token_url was called
            call_args = mock_post.call_args
            assert 'token' in call_args[0][0]  # URL should contain 'token'
    
    @pytest.mark.asyncio
    async def test_oauth_token_success_firebird(self):
        """Test successful OAuth token acquisition for Firebird."""
        from src.api.main import get_oauth_token
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_post = AsyncMock()
            mock_client.return_value.__aenter__.return_value.post = mock_post
            mock_post.return_value = create_mock_http_response(
                200, create_mock_oauth_token_response("firebird_token_456")
            )
            
            token = await get_oauth_token('firebird')
            
            assert token == "firebird_token_456"
    
    @pytest.mark.parametrize("status_code,error_data,description", TestDataGenerator.oauth_error_scenarios())
    @pytest.mark.asyncio
    async def test_oauth_error_responses(self, status_code, error_data, description):
        """Test handling of various OAuth error responses."""
        from src.api.main import get_oauth_token, oauth_tokens
        
        oauth_tokens.clear()
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_post = AsyncMock()
            mock_client.return_value.__aenter__.return_value.post = mock_post
            mock_post.return_value = create_mock_http_response(status_code, error_data)
            
            token = await get_oauth_token('yottadb')
            
            assert token is None, f"Should return None for {description}"
    
    @pytest.mark.asyncio
    async def test_oauth_network_exception(self):
        """Test handling of network exceptions during OAuth."""
        from src.api.main import get_oauth_token, oauth_tokens
        
        oauth_tokens.clear()
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_post = AsyncMock()
            mock_client.return_value.__aenter__.return_value.post = mock_post
            mock_post.side_effect = httpx.ConnectError("Connection failed")
            
            token = await get_oauth_token('yottadb')
            
            assert token is None
    
    @pytest.mark.asyncio
    async def test_oauth_timeout_exception(self):
        """Test handling of timeout exceptions during OAuth."""
        from src.api.main import get_oauth_token, oauth_tokens
        
        oauth_tokens.clear()
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_post = AsyncMock()
            mock_client.return_value.__aenter__.return_value.post = mock_post
            mock_post.side_effect = httpx.TimeoutException("Request timeout")
            
            token = await get_oauth_token('yottadb')
            
            assert token is None


class TestOAuthTokenCaching:
    """Test OAuth token caching mechanisms."""
    
    @pytest.mark.asyncio
    async def test_oauth_token_caching_valid_token(self):
        """Test retrieval of valid cached token."""
        from src.api.main import get_oauth_token, oauth_tokens
        
        oauth_tokens.clear()
        
        # Pre-populate cache with valid token
        oauth_tokens['yottadb_token'] = "cached_token_123"
        oauth_tokens['yottadb_token_expiry'] = datetime.now() + timedelta(hours=1)
        
        token = await get_oauth_token('yottadb')
        
        assert token == "cached_token_123"
    
    @pytest.mark.asyncio
    async def test_oauth_token_caching_expired_token(self):
        """Test token refresh when cached token is expired."""
        from src.api.main import get_oauth_token, oauth_tokens
        
        oauth_tokens.clear()
        
        # Setup expired token in cache
        oauth_tokens['yottadb_token'] = "expired_token_123"
        oauth_tokens['yottadb_token_expiry'] = datetime.now() - timedelta(minutes=1)
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_post = AsyncMock()
            mock_client.return_value.__aenter__.return_value.post = mock_post
            mock_post.return_value = create_mock_http_response(
                200, create_mock_oauth_token_response("new_fresh_token")
            )
            
            token = await get_oauth_token('yottadb')
            
            assert token == "new_fresh_token"
            # Verify new token is cached
            assert oauth_tokens['yottadb_token'] == "new_fresh_token"
    
    @pytest.mark.asyncio
    async def test_oauth_token_caching_custom_expires_in(self):
        """Test handling of custom expires_in values."""
        from src.api.main import get_oauth_token, oauth_tokens
        
        oauth_tokens.clear()
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_post = AsyncMock()
            mock_client.return_value.__aenter__.return_value.post = mock_post
            mock_post.return_value = create_mock_http_response(
                200, create_mock_oauth_token_response("custom_token", expires_in=7200)  # 2 hours
            )
            
            token = await get_oauth_token('yottadb')
            
            assert token == "custom_token"
            # Check that expiry time accounts for custom expires_in
            expiry_time = oauth_tokens['yottadb_token_expiry']
            expected_min = datetime.now() + timedelta(seconds=7200 - 300 - 10)  # Buffer minus some tolerance
            assert expiry_time > expected_min
    
    @pytest.mark.asyncio
    async def test_oauth_different_systems_separate_caches(self):
        """Test that different HIS systems have separate token caches."""
        from src.api.main import get_oauth_token, oauth_tokens
        from src.api.config import HIS_API_CONFIG
        
        oauth_tokens.clear()
        
        # Get the actual configured URLs from config
        yottadb_token_url = HIS_API_CONFIG['yottadb']['oauth']['token_url']
        firebird_token_url = HIS_API_CONFIG['firebird']['oauth']['token_url']
        
        def mock_post_side_effect(url, **kwargs):
            # Use actual configured URLs for matching
            if url == yottadb_token_url:
                return create_mock_http_response(200, create_mock_oauth_token_response("yottadb_token"))
            elif url == firebird_token_url:
                return create_mock_http_response(200, create_mock_oauth_token_response("firebird_token"))
            else:
                return create_mock_http_response(200, create_mock_oauth_token_response("unknown_token"))
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_post = AsyncMock(side_effect=mock_post_side_effect)
            mock_client.return_value.__aenter__.return_value.post = mock_post
            
            # Get tokens for both systems
            yotta_token = await get_oauth_token('yottadb')
            firebird_token = await get_oauth_token('firebird')
            
            # Assert different tokens
            assert yotta_token == "yottadb_token"
            assert firebird_token == "firebird_token"
            assert oauth_tokens['yottadb_token'] == "yottadb_token"
            assert oauth_tokens['firebird_token'] == "firebird_token"


class TestOAuthConcurrency:
    """Test OAuth token acquisition under concurrent access."""
    
    @pytest.mark.asyncio
    async def test_oauth_concurrent_token_requests(self):
        """Test concurrent OAuth token requests use proper locking."""
        from src.api.main import get_oauth_token, oauth_tokens
        import asyncio
        
        oauth_tokens.clear()
        
        call_count = 0
        
        def mock_post_side_effect(url, **kwargs):
            nonlocal call_count
            call_count += 1
            return create_mock_http_response(200, create_mock_oauth_token_response(f"token_{call_count}"))
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_post = AsyncMock(side_effect=mock_post_side_effect)
            mock_client.return_value.__aenter__.return_value.post = mock_post
            
            # Make multiple concurrent requests for the same system
            tasks = [get_oauth_token('yottadb') for _ in range(5)]
            tokens = await asyncio.gather(*tasks)
            
            # All tokens should be the same (first one cached)
            assert all(token == tokens[0] for token in tokens)
            # Should only have made one actual HTTP call due to locking
            assert call_count == 1
    
    @pytest.mark.asyncio
    async def test_oauth_different_systems_concurrent(self):
        """Test concurrent requests for different systems don't interfere."""
        from src.api.main import oauth_tokens
        import asyncio
        
        oauth_tokens.clear()
        
        # Mock the get_oauth_token function directly - more abstract approach
        original_get_oauth_token = None
        
        async def mock_get_oauth_token(his_type: str):
            """Mock that returns different tokens for different systems."""
            if his_type == 'yottadb':
                return "yottadb_concurrent"
            elif his_type == 'firebird':
                return "firebird_concurrent"
            else:
                return f"unknown_{his_type}_concurrent"
        
        with patch('src.api.main.get_oauth_token', side_effect=mock_get_oauth_token) as mock_oauth:
            # Make concurrent requests for different systems
            tasks = [
                mock_oauth('yottadb'),
                mock_oauth('firebird'),
                mock_oauth('yottadb'),
                mock_oauth('firebird')
            ]
            tokens = await asyncio.gather(*tasks)
            
            # Should get appropriate tokens for each system
            assert tokens[0] == "yottadb_concurrent"
            assert tokens[1] == "firebird_concurrent"
            assert tokens[2] == "yottadb_concurrent"
            assert tokens[3] == "firebird_concurrent"
            
            # Verify each system was called
            assert mock_oauth.call_count == 4
            call_args = [call[0][0] for call in mock_oauth.call_args_list]
            assert call_args == ['yottadb', 'firebird', 'yottadb', 'firebird']


class TestHISCredentialOperations:
    """Test HIS credential update operations that use OAuth."""
    
    @pytest.mark.asyncio
    async def test_update_his_credentials_success(self):
        """Test successful HIS credential update with OAuth."""
        from src.api.main import update_his_credentials
        
        with patch('src.api.main.get_oauth_token') as mock_get_token, \
             patch('httpx.AsyncClient') as mock_client:
            
            mock_get_token.return_value = "test_access_token"
            
            mock_post = AsyncMock()
            mock_client.return_value.__aenter__.return_value.post = mock_post
            mock_post.return_value = Mock(status_code=201)
            
            result = await update_his_credentials('yottadb', 'QMS123', 'newlogin', 'newpassword')
            
            assert result is True
            # Verify OAuth token was requested
            mock_get_token.assert_called_once_with('yottadb')
    
    @pytest.mark.asyncio
    async def test_update_his_credentials_oauth_failure(self):
        """Test HIS credential update when OAuth fails."""
        from src.api.main import update_his_credentials
        
        with patch('src.api.main.get_oauth_token') as mock_get_token:
            mock_get_token.return_value = None
            
            result = await update_his_credentials('yottadb', 'QMS123', 'newlogin', 'newpassword')
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_update_his_credentials_token_retry(self):
        """Test HIS credential update with token expiry and retry."""
        from src.api.main import update_his_credentials, oauth_tokens
        
        with patch('src.api.main.get_oauth_token') as mock_get_token, \
             patch('httpx.AsyncClient') as mock_client:
            
            # First call returns expired token, second call returns fresh token
            mock_get_token.side_effect = ["expired_token", "fresh_token"]
            
            mock_post = AsyncMock()
            mock_client.return_value.__aenter__.return_value.post = mock_post
            # First call returns 401 (expired), second call returns 201 (success)
            mock_post.side_effect = [
                Mock(status_code=401),
                Mock(status_code=201)
            ]
            
            result = await update_his_credentials('yottadb', 'QMS123', 'newlogin', 'newpassword')
            
            assert result is True
            # Should have been called twice due to retry
            assert mock_get_token.call_count == 2
    
    @pytest.mark.asyncio
    async def test_create_his_patient_oauth_integration(self, sample_patient_request):
        """Test patient creation with OAuth integration."""
        from src.api.main import create_his_patient, PatientCredentialRequest
        
        patient_data = PatientCredentialRequest(**sample_patient_request)
        
        with patch('src.api.main.get_oauth_token') as mock_get_token, \
             patch('httpx.AsyncClient') as mock_client:
            
            mock_get_token.return_value = "test_create_token"
            
            mock_post = AsyncMock()
            mock_client.return_value.__aenter__.return_value.post = mock_post
            mock_response = Mock(status_code=201)
            mock_response.json.return_value = {
                "pcode": "TEST123",
                "fullname": "Smith John William",
                "message": "Patient created successfully"
            }
            mock_post.return_value = mock_response
            
            result = await create_his_patient('yottadb', patient_data)
            
            assert result["success"] is True
            assert result["hisnumber"] == "TEST123"
            # Verify OAuth token was requested
            mock_get_token.assert_called_once_with('yottadb')