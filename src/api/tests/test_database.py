"""
Tests for database operations and PatientRepository - CONSOLIDATED.
All database-related tests consolidated here.
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, date

from src.api.database import PatientRepository, DatabasePool, get_patient_repository
from src.api.tests.conftest import create_mock_patient_record


class TestDatabasePool:
    """Test DatabasePool functionality."""
    
    @pytest.mark.asyncio
    async def test_database_pool_initialization(self):
        """Test database pool initialization."""
        pool = DatabasePool()
        
        with patch('asyncpg.create_pool') as mock_create_pool:
            # Create a mock that behaves like an actual asyncpg pool
            mock_pool_instance = Mock()
            mock_pool_instance.get_size = Mock(return_value=5)
            mock_pool_instance.get_max_size = Mock(return_value=20)
            mock_pool_instance.close = AsyncMock()
            
            # Mock acquire to return an async context manager
            mock_connection = AsyncMock()
            mock_acquire_context = AsyncMock()
            mock_acquire_context.__aenter__ = AsyncMock(return_value=mock_connection)
            mock_acquire_context.__aexit__ = AsyncMock(return_value=None)
            mock_pool_instance.acquire = Mock(return_value=mock_acquire_context)
            
            # Make create_pool async and return the pool - this is key!
            async def mock_create_pool_async(*args, **kwargs):
                return mock_pool_instance
            
            mock_create_pool.side_effect = mock_create_pool_async
            
            result = await pool.initialize()
            
            assert result is True
            assert pool._initialized is True
            assert pool.pool == mock_pool_instance
            mock_create_pool.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_database_pool_initialization_failure(self):
        """Test database pool initialization failure."""
        pool = DatabasePool()
        
        with patch('asyncpg.create_pool') as mock_create_pool:
            mock_create_pool.side_effect = Exception("Connection failed")
            
            result = await pool.initialize()
            
            assert result is False
            assert pool._initialized is False
    
    @pytest.mark.asyncio
    async def test_database_pool_health_check_healthy(self):
        """Test database health check when healthy."""
        pool = DatabasePool()
        mock_connection = AsyncMock()
        mock_connection.fetchval.side_effect = [1, 100, 50]  # Health check, patients count, mobile users count
        
        # Create a mock pool that behaves like asyncpg pool
        mock_pool = Mock()
        mock_pool.get_size = Mock(return_value=5)
        mock_pool.get_max_size = Mock(return_value=20)
        
        # Mock acquire to return an async context manager
        mock_acquire_context = AsyncMock()
        mock_acquire_context.__aenter__ = AsyncMock(return_value=mock_connection)
        mock_acquire_context.__aexit__ = AsyncMock(return_value=None)
        mock_pool.acquire = Mock(return_value=mock_acquire_context)
        
        pool.pool = mock_pool
        
        health = await pool.check_health()
        
        assert health["status"] == "healthy"
        assert health["patients_count"] == 100
        assert health["mobile_users_count"] == 50
        assert health["pool_size"] == 5
        assert health["pool_max_size"] == 20
    
    @pytest.mark.asyncio
    async def test_database_pool_health_check_unhealthy(self):
        """Test database health check when unhealthy."""
        pool = DatabasePool()
        pool.pool = None
        
        health = await pool.check_health()
        
        assert health["status"] == "unhealthy"
        assert "error" in health
    
    @pytest.mark.asyncio
    async def test_database_pool_execute_query(self):
        """Test database query execution."""
        pool = DatabasePool()
        mock_connection = AsyncMock()
        mock_connection.fetch.return_value = [("test", "data")]
        
        # Create a mock pool that behaves like asyncpg pool
        mock_pool = Mock()
        
        # Mock acquire to return an async context manager
        mock_acquire_context = AsyncMock()
        mock_acquire_context.__aenter__ = AsyncMock(return_value=mock_connection)
        mock_acquire_context.__aexit__ = AsyncMock(return_value=None)
        mock_pool.acquire = Mock(return_value=mock_acquire_context)
        
        pool.pool = mock_pool
        
        result = await pool.execute_query("SELECT * FROM test", ("param1",))
        
        assert result == [("test", "data")]
        mock_connection.fetch.assert_called_once_with("SELECT * FROM test", "param1")


class TestPatientRepository:
    """Test PatientRepository operations."""
    
    @pytest.fixture
    def mock_pool(self):
        """Create a mock database pool."""
        pool = Mock(spec=DatabasePool)
        pool.execute_query = AsyncMock()
        pool.execute_command = AsyncMock()
        return pool
    
    @pytest.mark.asyncio
    async def test_find_patient_by_credentials_with_midname(self, mock_pool):
        """Test patient search with middle name using actual query logic."""
        # Mock the exact query result structure from database.py
        mock_pool.execute_query.return_value = [(
            'test-uuid-123', 'Smith', 'John', 'William', '1990-01-15',
            'QMS123456', 'IC789012', 'jsmith_login', None, False, False
        )]
        
        repo = PatientRepository(mock_pool)
        patient = await repo.find_patient_by_credentials(
            'Smith', 'John', 'William', '1990-01-15', 'jsmith_login'
        )
        
        assert patient is not None
        assert patient['uuid'] == 'test-uuid-123'
        assert patient['lastname'] == 'Smith'
        assert patient['name'] == 'John'  # Note: database uses 'name' for firstname
        assert patient['surname'] == 'William'  # Note: database uses 'surname' for midname
        assert patient['hisnumber_qms'] == 'QMS123456'
        assert patient['hisnumber_infoclinica'] == 'IC789012'
        
        # Verify the actual query was called with midname logic
        call_args = mock_pool.execute_query.call_args
        query = call_args[0][0]
        params = call_args[0][1]
        
        assert 'surname = $3' in query  # Checks for midname parameter
        assert params == ('Smith', 'John', 'William', '1990-01-15', 'jsmith_login')
    
    @pytest.mark.asyncio
    async def test_find_patient_by_credentials_no_midname(self, mock_pool):
        """Test patient search without middle name using actual query logic."""
        mock_pool.execute_query.return_value = [(
            'test-uuid-456', 'Doe', 'Jane', None, '1985-05-20',
            'QMS789012', None, 'jdoe_login', None, False, False
        )]
        
        repo = PatientRepository(mock_pool)
        patient = await repo.find_patient_by_credentials(
            'Doe', 'Jane', None, '1985-05-20', 'jdoe_login'
        )
        
        assert patient is not None
        assert patient['uuid'] == 'test-uuid-456'
        assert patient['surname'] is None
        assert patient['hisnumber_infoclinica'] is None
        
        # Verify the no-midname query logic
        call_args = mock_pool.execute_query.call_args
        query = call_args[0][0]
        params = call_args[0][1]
        
        assert '(surname IS NULL OR surname = \'\')' in query
        assert params == ('Doe', 'Jane', '1985-05-20', 'jdoe_login')
    
    @pytest.mark.asyncio
    async def test_find_patient_by_credentials_not_found(self, mock_pool):
        """Test patient search when no patient is found."""
        mock_pool.execute_query.return_value = []
        
        repo = PatientRepository(mock_pool)
        patient = await repo.find_patient_by_credentials(
            'NotFound', 'Patient', None, '1990-01-01', 'notfound_login'
        )
        
        assert patient is None
    
    @pytest.mark.asyncio
    async def test_find_patient_by_credentials_login_matching(self, mock_pool):
        """Test patient search matches login in either qms or infoclinica fields."""
        # Test patient with login in login_qms field
        mock_pool.execute_query.return_value = [(
            'test-uuid-qms', 'Smith', 'John', 'William', '1990-01-15',
            'QMS123456', None, 'jsmith_qms', None, False, False
        )]
        
        repo = PatientRepository(mock_pool)
        patient = await repo.find_patient_by_credentials(
            'Smith', 'John', 'William', '1990-01-15', 'jsmith_qms'
        )
        
        assert patient is not None
        assert patient['login_qms'] == 'jsmith_qms'
        
        # Verify query uses OR condition for login matching
        call_args = mock_pool.execute_query.call_args
        query = call_args[0][0]
        assert '(login_qms = $5 OR login_infoclinica = $5)' in query
    
    @pytest.mark.asyncio
    async def test_register_mobile_app_user_success(self, mock_pool):
        """Test successful mobile app user registration."""
        mock_pool.execute_query.return_value = [('mobile-uuid-123',)]
        
        repo = PatientRepository(mock_pool)
        mobile_uuid = await repo.register_mobile_app_user('QMS123', 'IC456')
        
        assert mobile_uuid == 'mobile-uuid-123'
        
        # Verify correct query and parameters
        call_args = mock_pool.execute_query.call_args
        query = call_args[0][0]
        params = call_args[0][1]
        
        assert 'INSERT INTO mobile_app_users' in query
        assert 'RETURNING uuid' in query
        assert params == ('QMS123', 'IC456')
    
    @pytest.mark.asyncio
    async def test_register_mobile_app_user_partial_data(self, mock_pool):
        """Test mobile app user registration with partial data."""
        mock_pool.execute_query.return_value = [('mobile-uuid-partial',)]
        
        repo = PatientRepository(mock_pool)
        
        # Test with only QMS number
        mobile_uuid = await repo.register_mobile_app_user('QMS123', None)
        assert mobile_uuid == 'mobile-uuid-partial'
        
        # Test with only Infoclinica number
        mobile_uuid = await repo.register_mobile_app_user(None, 'IC456')
        assert mobile_uuid == 'mobile-uuid-partial'
    
    @pytest.mark.asyncio
    async def test_register_mobile_app_user_no_data(self, mock_pool):
        """Test mobile app user registration with no HIS numbers."""
        repo = PatientRepository(mock_pool)
        mobile_uuid = await repo.register_mobile_app_user(None, None)
        
        assert mobile_uuid is None
        # Should not call database
        mock_pool.execute_query.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_lock_patient_matching_success(self, mock_pool):
        """Test successful patient matching lock."""
        mock_pool.execute_command.return_value = "UPDATE 1"
        
        repo = PatientRepository(mock_pool)
        result = await repo.lock_patient_matching('test-uuid-123', 'Test lock reason')
        
        assert result is True
        
        # Verify correct query and parameters
        call_args = mock_pool.execute_command.call_args
        query = call_args[0][0]
        params = call_args[0][1]
        
        assert 'UPDATE patients' in query
        assert 'matching_locked = TRUE' in query
        assert 'matching_locked_reason = $2' in query
        assert params == ('test-uuid-123', 'Test lock reason')
    
    @pytest.mark.asyncio
    async def test_lock_patient_matching_failure(self, mock_pool):
        """Test patient matching lock failure."""
        mock_pool.execute_command.return_value = "UPDATE 0"
        
        repo = PatientRepository(mock_pool)
        result = await repo.lock_patient_matching('nonexistent-uuid', 'Test lock')
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_unlock_patient_matching_success(self, mock_pool):
        """Test successful patient matching unlock."""
        mock_pool.execute_command.return_value = "UPDATE 1"
        
        repo = PatientRepository(mock_pool)
        result = await repo.unlock_patient_matching('test-uuid-123')
        
        assert result is True
        
        # Verify correct query
        call_args = mock_pool.execute_command.call_args
        query = call_args[0][0]
        params = call_args[0][1]
        
        assert 'matching_locked = FALSE' in query
        assert 'matching_locked_at = NULL' in query
        assert 'matching_locked_reason = NULL' in query
        assert params == ('test-uuid-123',)
    
    @pytest.mark.asyncio
    async def test_get_mobile_app_stats_success(self, mock_pool):
        """Test mobile app statistics retrieval."""
        mock_pool.execute_query.return_value = [(150, 100, 30, 20)]
        
        repo = PatientRepository(mock_pool)
        stats = await repo.get_mobile_app_stats()
        
        expected_stats = {
            "total_mobile_users": 150,
            "both_his_registered": 100,
            "qms_only": 30,
            "infoclinica_only": 20
        }
        assert stats == expected_stats
        
        # Verify query structure
        call_args = mock_pool.execute_query.call_args
        query = call_args[0][0]
        assert 'COUNT(*) as total_mobile_users' in query
        assert 'COUNT(CASE WHEN hisnumber_qms IS NOT NULL AND hisnumber_infoclinica IS NOT NULL' in query
    
    @pytest.mark.asyncio
    async def test_get_mobile_app_stats_empty_result(self, mock_pool):
        """Test mobile app statistics when no data."""
        mock_pool.execute_query.return_value = []
        
        repo = PatientRepository(mock_pool)
        stats = await repo.get_mobile_app_stats()
        
        expected_stats = {
            "total_mobile_users": 0,
            "both_his_registered": 0,
            "qms_only": 0,
            "infoclinica_only": 0
        }
        assert stats == expected_stats
    
    @pytest.mark.asyncio
    async def test_get_patient_matching_stats_success(self, mock_pool):
        """Test patient matching statistics retrieval."""
        mock_pool.execute_query.return_value = [
            ('exact_match', 50, 5, 25),
            ('fuzzy_match', 30, 10, 15),
            ('manual_match', 10, 2, 8)
        ]
        
        repo = PatientRepository(mock_pool)
        stats = await repo.get_patient_matching_stats()
        
        assert len(stats) == 3
        assert stats[0]["match_type"] == "exact_match"
        assert stats[0]["count"] == 50
        assert stats[0]["new_patients_created"] == 5
        assert stats[0]["mobile_app_matches"] == 25
        
        # Verify query includes 24-hour filter
        call_args = mock_pool.execute_query.call_args
        query = call_args[0][0]
        assert "CURRENT_TIMESTAMP - INTERVAL '24 hours'" in query
    
    @pytest.mark.asyncio
    async def test_repository_error_handling(self, mock_pool):
        """Test repository error handling."""
        mock_pool.execute_query.side_effect = Exception("Database error")
        
        repo = PatientRepository(mock_pool)
        
        # Should raise exception for critical operations
        with pytest.raises(Exception):
            await repo.find_patient_by_credentials('Test', 'User', None, '1990-01-01', 'login')
        
        # Should return None/False/empty for non-critical operations
        result = await repo.register_mobile_app_user('QMS123', 'IC456')
        assert result is None
        
        result = await repo.lock_patient_matching('uuid', 'reason')
        assert result is False
        
        stats = await repo.get_mobile_app_stats()
        assert stats == {"total_mobile_users": 0, "both_his_registered": 0, "qms_only": 0, "infoclinica_only": 0}


class TestDatabaseIntegration:
    """Test database integration functions."""
    
    @pytest.mark.asyncio
    async def test_initialize_database_success(self):
        """Test successful database initialization."""
        with patch('src.api.database.db_pool') as mock_pool:
            mock_pool.initialize = AsyncMock(return_value=True)
            
            from src.api.database import initialize_database
            result = await initialize_database()
            
            assert result is True
    
    @pytest.mark.asyncio
    async def test_initialize_database_failure(self):
        """Test database initialization failure."""
        # Need to patch both the global _connection_pool and db_pool
        with patch('src.api.database._connection_pool', None), \
             patch('src.api.database.db_pool') as mock_pool:
            mock_pool.initialize = AsyncMock(return_value=False)
            
            from src.api.database import initialize_database
            result = await initialize_database()
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_get_database_health_initialized(self):
        """Test database health when initialized."""
        with patch('src.api.database._connection_pool') as mock_pool:
            mock_pool.check_health = AsyncMock(return_value={"status": "healthy"})
            
            from src.api.database import get_database_health
            health = await get_database_health()
            
            assert health["status"] == "healthy"
    
    @pytest.mark.asyncio
    async def test_get_database_health_not_initialized(self):
        """Test database health when not initialized."""
        with patch('src.api.database._connection_pool', None):
            from src.api.database import get_database_health
            health = await get_database_health()
            
            assert health["status"] == "not_initialized"
    
    def test_get_patient_repository(self):
        """Test patient repository factory function."""
        from src.api.database import get_patient_repository
        repo = get_patient_repository()
        
        assert isinstance(repo, PatientRepository)


class TestDateHandling:
    """Test date handling in repository operations."""
    
    @pytest.fixture
    def mock_pool(self):
        """Create a mock database pool for date handling tests."""
        pool = Mock(spec=DatabasePool)
        pool.execute_query = AsyncMock()
        pool.execute_command = AsyncMock()
        return pool
    
    @pytest.mark.asyncio
    async def test_find_patient_date_conversion(self, mock_pool):
        """Test that date objects are properly handled in patient search."""
        mock_pool.execute_query.return_value = [(
            'test-uuid-date', 'Smith', 'John', None, date(1990, 1, 15),
            'QMS123', None, 'jsmith', None, False, False
        )]
        
        repo = PatientRepository(mock_pool)
        
        # Test with date object
        patient = await repo.find_patient_by_credentials(
            'Smith', 'John', None, date(1990, 1, 15), 'jsmith'
        )
        
        assert patient is not None
        assert patient['birthdate'] == date(1990, 1, 15)
        
        # Test with string date
        patient = await repo.find_patient_by_credentials(
            'Smith', 'John', None, '1990-01-15', 'jsmith'
        )
        
        assert patient is not None