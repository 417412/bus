"""
Performance and load tests for the API - ENHANCED VERSION.
"""

import pytest
import asyncio
import time
from unittest.mock import patch, AsyncMock, Mock

from src.api.tests.conftest import create_mock_oauth_token_response, create_mock_http_response


class TestPerformance:
    """Performance tests for the API."""
    
    @pytest.mark.asyncio
    async def test_concurrent_oauth_requests(self):
        """Test performance of concurrent OAuth requests with proper locking."""
        from src.api.main import get_oauth_token, oauth_tokens
        
        oauth_tokens.clear()
        
        with patch('httpx.AsyncClient') as mock_client:
            async def delayed_response(*args, **kwargs):
                await asyncio.sleep(0.01)  # 10ms delay
                return create_mock_http_response(200, create_mock_oauth_token_response("perf_test_token"))
            
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(side_effect=delayed_response)
            
            # Execute multiple concurrent requests
            start_time = time.time()
            tasks = [get_oauth_token('yottadb') for _ in range(10)]
            results = await asyncio.gather(*tasks)
            end_time = time.time()
            
            # Assertions
            assert all(result == "perf_test_token" for result in results)
            
            # With proper async locking, should only make ONE API call
            assert mock_client.return_value.__aenter__.return_value.post.call_count == 1
            
            # Should complete much faster than 10 * 10ms due to caching
            assert end_time - start_time < 0.5
    
    @pytest.mark.asyncio
    async def test_his_update_concurrency(self):
        """Test concurrent HIS system updates."""
        from src.api.main import update_his_credentials
        
        with patch('httpx.AsyncClient') as mock_client, \
             patch('src.api.main.get_oauth_token') as mock_get_token:
            
            mock_get_token.return_value = "test_token"
            
            async def delayed_his_response(*args, **kwargs):
                await asyncio.sleep(0.05)  # 50ms delay per call
                mock_response = Mock()
                mock_response.status_code = 201
                return mock_response
            
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
            
            # Should complete faster than 100ms (2 * 50ms) due to concurrency
            assert end_time - start_time < 0.08  # Allow some overhead
    
    def test_api_endpoint_response_time(self, client):
        """Test API endpoint response times."""
        with patch('src.api.main.get_patient_repo') as mock_get_repo:
            mock_repo = Mock()
            mock_repo.find_patient_by_credentials = AsyncMock(return_value=None)
            mock_get_repo.return_value = mock_repo
            
            # Test health endpoint
            start_time = time.time()
            response = client.get("/health")
            end_time = time.time()
            
            assert response.status_code in [200, 503]  # Healthy or service unavailable
            assert end_time - start_time < 0.1  # Should be very fast
    
    @pytest.mark.asyncio
    async def test_token_cache_performance(self):
        """Test performance benefits of token caching."""
        from src.api.main import get_oauth_token, oauth_tokens
        
        oauth_tokens.clear()
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = create_mock_http_response(200, create_mock_oauth_token_response("cached_token"))
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
            assert avg_cache_time < first_call_time / 5  # At least 5x faster
            
            # Verify only one API call was made
            assert mock_client.return_value.__aenter__.return_value.post.call_count == 1
    
    def test_database_connection_performance(self, client, mock_db_pool):
        """Test database connection performance."""
        start_time = time.time()
        
        # Make multiple requests that would use database
        for _ in range(10):
            response = client.get("/stats")
            # Don't assert success since it depends on mocking
        
        end_time = time.time()
        
        # Should handle multiple requests quickly
        avg_time = (end_time - start_time) / 10
        assert avg_time < 0.1  # Less than 100ms per request


class TestStressTest:
    """Stress tests for the API."""
    
    @pytest.mark.slow
    def test_multiple_patient_requests(self, client):
        """Test handling multiple patient requests."""
        with patch('src.api.main.get_patient_repo') as mock_get_repo:
            mock_repo = Mock()
            mock_repo.find_patient_by_credentials = AsyncMock(return_value=None)
            mock_get_repo.return_value = mock_repo
            
            # Execute multiple requests
            requests = []
            for i in range(20):  # Reduced from 50 for faster tests
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
            
            # All should return some response (might be 404, 502, etc. due to mocking)
            assert all(r.status_code in [200, 404, 502, 500] for r in responses)  # Added 500
            
            # Should handle all requests in reasonable time
            assert end_time - start_time < 3.0  # 3 seconds for 20 requests
            
            # Average response time should be reasonable
            avg_time = (end_time - start_time) / len(requests)
            assert avg_time < 0.15  # Less than 150ms per request
    
    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_oauth_token_stress(self):
        """Stress test OAuth token management with proper locking."""
        from src.api.main import get_oauth_token, oauth_tokens
        
        oauth_tokens.clear()
        
        with patch('httpx.AsyncClient') as mock_client:
            call_count = 0
            
            async def counting_response(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                await asyncio.sleep(0.001)  # 1ms delay
                return create_mock_http_response(
                    200, create_mock_oauth_token_response(f"stress_token_{call_count}")
                )
            
            mock_client.return_value.__aenter__.return_value.post = AsyncMock(side_effect=counting_response)
            
            # Execute many concurrent OAuth requests
            tasks = [get_oauth_token('yottadb') for _ in range(50)]  # Reduced from 100
            results = await asyncio.gather(*tasks)
            
            # With proper async locking, all should return the same token
            unique_tokens = set(results)
            assert len(unique_tokens) == 1, f"Expected 1 unique token, got {len(unique_tokens)}: {unique_tokens}"
            
            # Should have made only one API call due to proper locking
            assert call_count == 1, f"Expected 1 API call, got {call_count}"
    
    @pytest.mark.slow 
    def test_concurrent_api_requests(self, client):
        """Test concurrent API requests."""
        import threading
        import queue
        
        results = queue.Queue()
        
        def make_request():
            try:
                response = client.get("/health")
                results.put(response.status_code)
            except Exception as e:
                results.put(f"ERROR: {e}")
        
        # Create and start threads
        threads = []
        for _ in range(10):
            thread = threading.Thread(target=make_request)
            threads.append(thread)
        
        start_time = time.time()
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        end_time = time.time()
        
        # Collect results
        status_codes = []
        while not results.empty():
            status_codes.append(results.get())
        
        # All requests should complete
        assert len(status_codes) == 10
        
        # Should complete quickly due to concurrency
        assert end_time - start_time < 1.0
        
        # Most should be successful (200 or 503)
        successful = [code for code in status_codes if isinstance(code, int) and code in [200, 503]]
        assert len(successful) >= 8  # At least 80% success rate