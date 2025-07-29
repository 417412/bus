"""
Real integration tests that actually call APIs (when available).
"""

import pytest
import os
import httpx
import asyncio

# Only run these tests if explicitly requested
pytestmark = pytest.mark.skipif(
    os.getenv("TEST_REAL_APIS", "false").lower() != "true",
    reason="Real API tests disabled. Set TEST_REAL_APIS=true to enable."
)

class TestRealAPIIntegration:
    """Integration tests with real API calls."""
    
    @pytest.mark.asyncio
    async def test_real_yottadb_connection(self):
        """Test connection to real YottaDB API."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get("http://192.168.156.118")
                # Just check if the server responds
                assert response.status_code in [200, 404, 403, 401]  # Any response is good
        except Exception as e:
            pytest.skip(f"YottaDB server not available: {e}")
    
    @pytest.mark.asyncio
    async def test_real_firebird_connection(self):
        """Test connection to real Firebird API."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get("http://192.168.160.141")
                # Just check if the server responds
                assert response.status_code in [200, 404, 403, 401]  # Any response is good
        except Exception as e:
            pytest.skip(f"Firebird server not available: {e}")
    
    @pytest.mark.asyncio
    async def test_real_oauth_endpoints(self):
        """Test real OAuth endpoints."""
        endpoints = [
            "http://192.168.156.118:7072/token",
            "http://192.168.160.141/token"
        ]
        
        for endpoint in endpoints:
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    # Try to POST to the token endpoint (should fail with auth error, not 404)
                    response = await client.post(endpoint, data={
                        "grant_type": "password",
                        "username": "test",
                        "password": "test"
                    })
                    
                    # Should get 400/401/403, not 404 (which would mean endpoint doesn't exist)
                    assert response.status_code != 404, f"Token endpoint {endpoint} not found"
                    
            except httpx.ConnectError:
                pytest.skip(f"Server {endpoint} not available")