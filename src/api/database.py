"""
Database connection and utilities for the API.
"""

import asyncio
import logging
from typing import Optional, Dict, Any, List, Tuple
from contextlib import asynccontextmanager
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

from src.api.config import get_postgresql_config, setup_api_logger

logger = setup_api_logger("api_database")

class PostgreSQLPool:
    """PostgreSQL connection pool for API operations."""
    
    def __init__(self):
        self.pool: Optional[psycopg2.pool.SimpleConnectionPool] = None
        self.config = get_postgresql_config()
        
    def initialize_pool(self, minconn: int = 1, maxconn: int = 10):
        """Initialize the connection pool."""
        try:
            self.pool = psycopg2.pool.SimpleConnectionPool(
                minconn=minconn,
                maxconn=maxconn,
                host=self.config["host"],
                port=self.config["port"],
                database=self.config["database"],
                user=self.config["user"],
                password=self.config["password"],
                connect_timeout=self.config["connect_timeout"],
                cursor_factory=RealDictCursor
            )
            logger.info(f"PostgreSQL connection pool initialized (min={minconn}, max={maxconn})")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL pool: {e}")
            return False
    
    def close_pool(self):
        """Close all connections in the pool."""
        if self.pool:
            self.pool.closeall()
            self.pool = None
            logger.info("PostgreSQL connection pool closed")
    
    @asynccontextmanager
    async def get_connection(self):
        """Get a connection from the pool (async context manager)."""
        if not self.pool:
            raise RuntimeError("Connection pool not initialized")
        
        connection = None
        try:
            # Get connection from pool (this might block, so we use a thread)
            loop = asyncio.get_event_loop()
            connection = await loop.run_in_executor(None, self.pool.getconn)
            
            logger.debug("Database connection acquired from pool")
            yield connection
            
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            if connection:
                # Rollback any pending transaction
                try:
                    connection.rollback()
                except:
                    pass
            raise
        finally:
            if connection:
                try:
                    # Return connection to pool
                    await loop.run_in_executor(None, self.pool.putconn, connection)
                    logger.debug("Database connection returned to pool")
                except Exception as e:
                    logger.error(f"Error returning connection to pool: {e}")
    
    def get_sync_connection(self):
        """Get a synchronous connection from the pool."""
        if not self.pool:
            raise RuntimeError("Connection pool not initialized")
        
        return self.pool.getconn()
    
    def return_sync_connection(self, connection):
        """Return a synchronous connection to the pool."""
        if self.pool and connection:
            self.pool.putconn(connection)
    
    async def execute_query(self, query: str, params: Tuple = None) -> List[Dict[str, Any]]:
        """Execute a query and return results."""
        async with self.get_connection() as conn:
            loop = asyncio.get_event_loop()
            
            def _execute():
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    if cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                        rows = cursor.fetchall()
                        return [dict(zip(columns, row)) for row in rows]
                    return []
            
            return await loop.run_in_executor(None, _execute)
    
    async def execute_insert(self, query: str, params: Tuple = None) -> Optional[Any]:
        """Execute an insert query and return the inserted ID or result."""
        async with self.get_connection() as conn:
            loop = asyncio.get_event_loop()
            
            def _execute():
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    conn.commit()
                    
                    # Try to get the returned value (for RETURNING clauses)
                    if cursor.description:
                        result = cursor.fetchone()
                        return dict(result) if result else None
                    return cursor.rowcount
            
            return await loop.run_in_executor(None, _execute)
    
    async def execute_update(self, query: str, params: Tuple = None) -> int:
        """Execute an update query and return the number of affected rows."""
        async with self.get_connection() as conn:
            loop = asyncio.get_event_loop()
            
            def _execute():
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    conn.commit()
                    return cursor.rowcount
            
            return await loop.run_in_executor(None, _execute)
    
    async def check_health(self) -> bool:
        """Check if the database connection is healthy."""
        try:
            result = await self.execute_query("SELECT 1 as health_check")
            return len(result) > 0 and result[0].get("health_check") == 1
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False

# Global connection pool instance
db_pool = PostgreSQLPool()

async def initialize_database():
    """Initialize the database connection pool."""
    return db_pool.initialize_pool()

async def close_database():
    """Close the database connection pool."""
    db_pool.close_pool()

async def get_database_health() -> Dict[str, Any]:
    """Get database health information."""
    try:
        is_healthy = await db_pool.check_health()
        
        if is_healthy:
            # Get additional database info
            stats = await db_pool.execute_query("""
                SELECT 
                    current_database() as database_name,
                    current_user as current_user,
                    version() as version
            """)
            
            return {
                "status": "healthy",
                "database": stats[0]["database_name"] if stats else "unknown",
                "user": stats[0]["current_user"] if stats else "unknown",
                "version": stats[0]["version"][:50] + "..." if stats and len(stats[0]["version"]) > 50 else stats[0]["version"] if stats else "unknown"
            }
        else:
            return {
                "status": "unhealthy",
                "error": "Health check query failed"
            }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }

class PatientRepository:
    """Repository for patient-related database operations."""
    
    def __init__(self, pool: PostgreSQLPool):
        self.pool = pool
    
    async def find_patient_by_credentials(self, lastname: str, firstname: str, 
                                        midname: Optional[str], bdate: str, 
                                        cllogin: str) -> Optional[Dict[str, Any]]:
        """Find patient by demographic data and login."""
        if midname:
            query = """
                SELECT 
                    uuid, lastname, name, surname, birthdate,
                    hisnumber_qms, hisnumber_infoclinica,
                    login_qms, login_infoclinica,
                    registered_via_mobile, matching_locked
                FROM patients 
                WHERE lastname = %s 
                AND name = %s 
                AND surname = %s
                AND birthdate = %s
                AND (login_qms = %s OR login_infoclinica = %s)
                AND matching_locked = FALSE
                LIMIT 1
            """
            params = (lastname, firstname, midname, bdate, cllogin, cllogin)
        else:
            query = """
                SELECT 
                    uuid, lastname, name, surname, birthdate,
                    hisnumber_qms, hisnumber_infoclinica,
                    login_qms, login_infoclinica,
                    registered_via_mobile, matching_locked
                FROM patients 
                WHERE lastname = %s 
                AND name = %s 
                AND (surname IS NULL OR surname = '')
                AND birthdate = %s
                AND (login_qms = %s OR login_infoclinica = %s)
                AND matching_locked = FALSE
                LIMIT 1
            """
            params = (lastname, firstname, bdate, cllogin, cllogin)
        
        results = await self.pool.execute_query(query, params)
        return results[0] if results else None
    
    async def register_mobile_app_user(self, hisnumber_qms: Optional[str] = None,
                                     hisnumber_infoclinica: Optional[str] = None) -> Optional[str]:
        """Register a mobile app user."""
        if not hisnumber_qms and not hisnumber_infoclinica:
            return None
        
        query = """
            INSERT INTO mobile_app_users (hisnumber_qms, hisnumber_infoclinica)
            VALUES (%s, %s)
            RETURNING uuid
        """
        
        result = await self.pool.execute_insert(query, (hisnumber_qms, hisnumber_infoclinica))
        return str(result["uuid"]) if result else None
    
    async def get_mobile_app_stats(self) -> Dict[str, Any]:
        """Get mobile app user statistics."""
        query = """
            SELECT 
                COUNT(*) as total_mobile_users,
                COUNT(CASE WHEN hisnumber_qms IS NOT NULL AND hisnumber_infoclinica IS NOT NULL THEN 1 END) as both_his_registered,
                COUNT(CASE WHEN hisnumber_qms IS NOT NULL AND hisnumber_infoclinica IS NULL THEN 1 END) as qms_only,
                COUNT(CASE WHEN hisnumber_qms IS NULL AND hisnumber_infoclinica IS NOT NULL THEN 1 END) as infoclinica_only
            FROM mobile_app_users
        """
        
        results = await self.pool.execute_query(query)
        return results[0] if results else {}
    
    async def get_patient_matching_stats(self) -> List[Dict[str, Any]]:
        """Get patient matching statistics."""
        query = """
            SELECT 
                match_type,
                COUNT(*) as count,
                COUNT(CASE WHEN created_uuid THEN 1 END) as new_patients_created,
                COUNT(CASE WHEN mobile_app_uuid IS NOT NULL THEN 1 END) as mobile_app_matches
            FROM patient_matching_log
            WHERE match_time >= NOW() - INTERVAL '24 hours'
            GROUP BY match_type
            ORDER BY count DESC
        """
        
        return await self.pool.execute_query(query)
    
    async def lock_patient_matching(self, patient_uuid: str, reason: str = "API lock") -> bool:
        """Lock patient from further matching."""
        query = """
            UPDATE patients
            SET 
                matching_locked = TRUE,
                matching_locked_at = CURRENT_TIMESTAMP,
                matching_locked_reason = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE uuid = %s
        """
        
        affected_rows = await self.pool.execute_update(query, (reason, patient_uuid))
        return affected_rows > 0
    
    async def unlock_patient_matching(self, patient_uuid: str) -> bool:
        """Unlock patient matching."""
        query = """
            UPDATE patients
            SET 
                matching_locked = FALSE,
                matching_locked_at = NULL,
                matching_locked_reason = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE uuid = %s
        """
        
        affected_rows = await self.pool.execute_update(query, (patient_uuid,))
        return affected_rows > 0

# Global patient repository instance
patient_repo = PatientRepository(db_pool)

def get_patient_repository() -> PatientRepository:
    """Get the patient repository instance."""
    return patient_repo