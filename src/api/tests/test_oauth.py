"""
Tests specifically for OAuth functionality.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import patch, AsyncMock

from src.api.main import get_oauth_token, oauth_tokens
from src.api.tests.conftest import MockAsyncResponse


class TestOAuthTokenManagement:
    """Tests for OAuth token management."""
    
    @pytest.mark.asyncio
    async def test_token_caching_behavior(self):
        """Test OAuth token caching behavior."""
        with patch('httpx.AsyncClient') as mock_client:
            # Setup first call
            mock_response = MockAsyncResponse(200, {
                "access_token": "first_token",
                "expires_in": 3600
            })
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
            
            # First call should fetch token
            token1 = await get_oauth_token('yottadb')
            assert token1 == "first_token"
            assert mock_client.return_value.__aenter__.return_value.post.call_count == 1
            
            # Second call should use cached token
            token2 = await get_oauth_token('yottadb')
            assert token2 == "first_token"
            assert mock_client.return_value.__aenter__.return_value.post.call_count == 1  # No additional call
    
    @pytest.mark.asyncio
    async def test_token_expiry_refresh(self):
        """Test token refresh when expired."""
        with patch('httpx.AsyncClient') as mock_client:
            # Setup expired token in cache
            oauth_tokens['yottadb_token'] = "expired_token"
            oauth_tokens['yottadb_token_expiry'] = datetime.now() - timedelta(minutes=1)
            
            # Setup fresh token response
            mock_response = MockAsyncResponse(200, {
                "access_token": "fresh_token",
                "expires_in": 3600
            })
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
            
            # Should get fresh token
            token = await get_oauth_token('yottadb')
            assert token == "fresh_token"
            assert oauth_tokens['yottadb_token'] == "fresh_token"
    
    @pytest.mark.asyncio
    async def test_token_near_expiry_refresh(self):
        """Test token refresh when near expiry (within 5 minute buffer)."""
        with patch('httpx.AsyncClient') as mock_client:
            # Setup token expiring in 2 minutes (within 5 minute buffer)
            oauth_tokens['yottadb_token'] = "expiring_token"
            oauth_tokens['yottadb_token_expiry'] = datetime.now() + timedelta(minutes=2)
            
            # Setup fresh token response
            mock_response = MockAsyncResponse(200, {
                "access_token": "refreshed_token",
                "expires_in": 3600
            })
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
            
            # Should get fresh token due to buffer
            token = await get_oauth_token('yottadb')
            assert token == "refreshed_token"
    
    @pytest.mark.asyncio
    async def test_different_systems_separate_tokens(self):
        """Test that different HIS systems have separate token caches."""
        with patch('httpx.AsyncClient') as mock_client:
            # Setup different responses for different systems
            def mock_post(url, **kwargs):
                if 'yottadb' in url:
                    return MockAsyncResponse(200, {"access_token": "yottadb_token", "expires_in": 3600})
                else:
                    return MockAsyncResponse(200, {"access_token": "firebird_token", "expires_in": 3600})
            
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(side_effect=mock_post)
            
            # Get tokens for both systems
            yotta_token = await get_oauth_token('yottadb')
            firebird_token = await get_oauth_token('firebird')
            
            # Assert different tokens
            assert yotta_token == "yottadb_token"
            assert firebird_token == "firebird_token"
            assert oauth_tokens['yottadb_token'] == "yottadb_token"
            assert oauth_tokens['firebird_token'] == "firebird_token"
    
    @pytest.mark.asyncio
    async def test_oauth_request_parameters(self):
        """Test that OAuth requests include correct parameters."""
        with patch('httpx.AsyncClient') as mock_client, \
             patch.dict('os.environ', {
                 'YOTTADB_USERNAME': 'test_user',
                 'YOTTADB_PASSWORD': 'test_pass',
                 'YOTTADB_CLIENT_ID': 'test_client',
                 'YOTTADB_CLIENT_SECRET': 'test_secret',
                 'YOTTADB_SCOPE': 'test_scope'
             }):
            
            mock_response = MockAsyncResponse(200, {
                "access_token": "test_token",
                "expires_in": 3600
            })
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
            
            # Execute
            await get_oauth_token('yottadb')
            
            # Verify call parameters
            call_args = mock_client.return_value.__aenter__.return_value.post.call_args
            assert call_args[1]['data']['grant_type'] == 'password'
            assert call_args[1]['data']['username'] == 'test_user'
            assert call_args[1]['data']['password'] == 'test_pass'
            assert call_args[1]['data']['client_id'] == 'test_client'
            assert call_args[1]['data']['client_secret'] == 'test_secret'
            assert call_args[1]['data']['scope'] == 'test_scope'
            assert call_args[1]['headers']['Content-Type'] == 'application/x-www-form-urlencoded'
    
    @pytest.mark.asyncio
    async def test_oauth_error_responses(self):
        """Test handling of various OAuth error responses."""
        error_scenarios = [
            (400, {"error": "invalid_request"}),
            (401, {"error": "invalid_client"}),
            (403, {"error": "access_denied"}),
            (500, {"error": "server_error"})
        ]
        
        for status_code, error_data in error_scenarios:
            with patch('httpx.AsyncClient') as mock_client:
                mock_response = MockAsyncResponse(status_code, error_data)
                mock_client.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
                
                token = await get_oauth_token('yottadb')
                assert token is None
    
    @pytest.mark.asyncio 
    async def test_oauth_network_exception(self):
        """Test handling of network exceptions during OAuth."""
        with patch('httpx.AsyncClient') as mock_client:
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(
                side_effect=Exception("Network error")
            )
            
            token = await get_oauth_token('yottadb')
            assert token is None
    
    @pytest.mark.asyncio
    async def test_oauth_custom_expires_in(self):
        """Test handling of custom expires_in values."""
        with patch('httpx.AsyncClient') as mock_client:
            # Test with 2-hour expiry
            mock_response = MockAsyncResponse(200, {
                "access_token": "long_lived_token",
                "expires_in": 7200  # 2 hours
            })
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
            
            token = await get_oauth_token('yottadb')
            assert token == "long_lived_token"
            
            # Check that expiry time is set correctly (approximately 2 hours - 5 minute buffer)
            expiry_time = oauth_tokens['yottadb_token_expiry']
            expected_expiry = datetime.now() + timedelta(seconds=7200 - 300)
            
            # Allow 10 second tolerance for test execution time
            assert abs((expiry_time - expected_expiry).total_seconds()) < 10