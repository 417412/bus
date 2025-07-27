"""
Performance and load tests for the API.
"""

import pytest
import asyncio
import time
from unittest.mock import patch, AsyncMock
from concurrent.futures import ThreadPoolExecutor

from src.api.tests.conftest import MockAsyncResponse


class TestPerformance:
    """Performance tests for the API."""
    
    @pytest.mark.asyncio
    async def test_concurrent_oauth_requests(self):
        """Test performance of concurrent OAuth requests."""
        from src.api.main import get_oauth_token, oauth_tokens
        
        oauth_tokens.clear()  # Start fresh
        
        with patch('httpx.AsyncClient') as mock_client:
            # Setup mock to simulate network delay
            async def delayed_response(*args, **kwargs):
                await asyncio.sleep(0.1)  # 100ms delay
                return MockAsyncResponse(200, {
                    "access_token": "perf_test_token",
                    "expires_in": 3600
                })
            
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(side_effect=delayed_response)
            
            # Execute multiple concurrent requests
            start_time = time.time()
            tasks = [get_oauth_token('yottadb') for _ in range(10)]
            results = await asyncio.gather(*tasks)
            end_time = time.time()
            
            # Assertions
            assert all(result == "perf_test_token" for result in results)
            
            # Should complete much faster than 10 * 100ms due to caching
            # Only first request should hit the API, others should use cache
            assert end_time - start_time < 0.5  # Much less than 1 second
            
            # Verify only one actual API call was made due to caching
            assert mock_client.return_value.__aenter__.return_value.post.call_count == 1
    
    @pytest.mark.asyncio
    async def test_his_update_concurrency(self):
        """Test concurrent HIS system updates."""
        from src.api.main import update_his_credentials
        
        with patch('httpx.AsyncClient') as mock_client, \
             patch('src.api.main.get_oauth_token') as mock_get_token:
            
            # Setup mocks
            mock_get_token.return_value = "test_token"
            
            async def delayed_his_response(*args, **kwargs):
                await asyncio.sleep(0.1)  # 100ms delay per call
                return MockAsyncResponse(201)
            
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(side_effect=delayed_his_response)
            
            # Execute concurrent updates to both systems
            start_time = time.time()
            tasks = [
                update_his_credentials('yottadb', 'QMS123', 'login', 'pass'),
                update_his_credentials('firebird', 'IC456', 'login', 'pass')
            ]
            results = await asyncio.gather(*tasks)
            end_time = time.time()
            
            # Assertions
            assert all(result is True for result in results)
            
            # Should complete faster than 200ms (2 * 100ms) due to concurrency
            assert end_time - start_time < 0.15  # Allow some overhead
    
    def test_api_endpoint_response_time(self, client):
        """Test API endpoint response times."""
        with patch('src.api.main.pg_connector') as mock_pg:
            # Setup quick database response
            mock_pg.execute_query.return_value = ([1], ['column'])
            
            # Test health endpoint
            start_time = time.time()
            response = client.get("/health")
            end_time = time.time()
            
            assert response.status_code == 200
            assert end_time - start_time < 0.1  # Should be very fast
    
    @pytest.mark.asyncio
    async def test_token_cache_performance(self):
        """Test performance benefits of token caching."""
        from src.api.main import get_oauth_token, oauth_tokens
        
        oauth_tokens.clear()
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = MockAsyncResponse(200, {
                "access_token": "cached_token",
                "expires_in": 3600
            })
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
            
            # First call - should hit API
            start_time = time.time()
            token1 = await get_oauth_token('yottadb')
            first_call_time = time.time() - start_time
            
            # Subsequent calls - should use cache
            cache_times = []
            for _ in range(5):
                start_time = time.time()
                token = await get_oauth_token('yottadb')
                cache_times.append(time.time() - start_time)
                assert token == token1
            
            # Cache calls should be much faster
            avg_cache_time = sum(cache_times) / len(cache_times)
            assert avg_cache_time < first_call_time / 10  # At least 10x faster
            
            # Verify only one API call was made
            assert mock_client.return_value.__aenter__.return_value.post.call_count == 1


class TestStressTest:
    """Stress tests for the API."""
    
    @pytest.mark.slow
    def test_multiple_patient_requests(self, client):
        """Test handling multiple patient requests."""
        with patch('src.api.main.find_patient_by_credentials') as mock_find:
            # Setup mock to return no patient found (faster than full flow)
            mock_find.return_value = None
            
            # Execute multiple requests
            requests = []
            for i in range(50):
                request_data = {
                    "lastname": f"User{i}",
                    "firstname": "Test",
                    "bdate": "1990-01-01",
                    "cllogin": f"user{i}_login",
                    "clpassword": "password"
                }
                requests.append(request_data)
            
            start_time = time.time()
            responses = []
            for request_data in requests:
                response = client.post("/checkModifyPatient", json=request_data)
                responses.append(response)
            end_time = time.time()
            
            # All should return 404 (patient not found)
            assert all(r.status_code == 404 for r in responses)
            
            # Should handle all requests in reasonable time
            assert end_time - start_time < 5.0  # 5 seconds for 50 requests
            
            # Average response time should be reasonable
            avg_time = (end_time - start_time) / len(requests)
            assert avg_time < 0.1  # Less than 100ms per request
    
    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_oauth_token_stress(self):
        """Stress test OAuth token management with many concurrent requests."""
        from src.api.main import get_oauth_token, oauth_tokens
        
        oauth_tokens.clear()
        
        with patch('httpx.AsyncClient') as mock_client:
            call_count = 0
            
            async def counting_response(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                await asyncio.sleep(0.01)  # Small delay
                return MockAsyncResponse(200, {
                    "access_token": f"stress_token_{call_count}",
                    "expires_in": 3600
                })
            
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(side_effect=counting_response)
            
            # Execute many concurrent OAuth requests
            tasks = [get_oauth_token('yottadb') for _ in range(100)]
            results = await asyncio.gather(*tasks)
            
            # All should return the same token (from cache after first call)
            unique_tokens = set(results)
            assert len(unique_tokens) == 1  # Only one unique token
            
            # Should have made only one API call due to caching
            assert call_count == 1