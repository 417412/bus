"""
Database connection and repository management for the API.
"""

import asyncio
import asyncpg
from datetime import datetime
from typing import Dict, Any, Optional, List
import logging
from contextlib import asynccontextmanager

from src.api.config import get_postgresql_config, setup_api_logger

logger = setup_api_logger("api_database")

# Global connection pool
_connection_pool = None
_pool_lock = asyncio.Lock()

class DatabasePool:
    """Database connection pool manager."""
    
    def __init__(self):
        self.pool = None
        self._initialized = False
    
    async def initialize(self) -> bool:
        """Initialize the connection pool."""
        if self._initialized:
            return True
        
        try:
            pg_config = get_postgresql_config()
            
            # Create connection pool
            self.pool = await asyncpg.create_pool(
                host=pg_config["host"],
                port=pg_config["port"],
                database=pg_config["database"],
                user=pg_config["user"],
                password=pg_config["password"],
                min_size=5,
                max_size=20,
                command_timeout=pg_config.get("command_timeout", 60)
            )
            
            self._initialized = True
            logger.info("Database connection pool initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            return False
    
    async def close(self):
        """Close the connection pool."""
        if self.pool:
            await self.pool.close()
            self._initialized = False
            logger.info("Database connection pool closed")
    
    async def execute_query(self, query: str, params: tuple = None) -> List[tuple]:
        """Execute a query and return results."""
        if not self.pool:
            raise Exception("Database pool not initialized")
        
        async with self.pool.acquire() as connection:
            if params:
                result = await connection.fetch(query, *params)
            else:
                result = await connection.fetch(query)
            
            return [tuple(row) for row in result]
    
    async def execute_command(self, command: str, params: tuple = None) -> str:
        """Execute a command and return status."""
        if not self.pool:
            raise Exception("Database pool not initialized")
        
        async with self.pool.acquire() as connection:
            if params:
                result = await connection.execute(command, *params)
            else:
                result = await connection.execute(command)
            
            return result
    
    async def check_health(self) -> Dict[str, Any]:
        """Check database health."""
        try:
            if not self.pool:
                return {"status": "unhealthy", "error": "Pool not initialized"}
            
            async with self.pool.acquire() as connection:
                await connection.fetchval("SELECT 1")
                
                # Get some basic stats
                patients_count = await connection.fetchval("SELECT COUNT(*) FROM patients")
                mobile_users_count = await connection.fetchval("SELECT COUNT(*) FROM mobile_app_users")
                
                return {
                    "status": "healthy",
                    "patients_count": patients_count,
                    "mobile_users_count": mobile_users_count,
                    "pool_size": self.pool.get_size(),
                    "pool_max_size": self.pool.get_max_size()
                }
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}

# Global database pool instance
db_pool = DatabasePool()

async def initialize_database() -> bool:
    """Initialize the database connection."""
    global _connection_pool
    
    async with _pool_lock:
        if _connection_pool is None:
            _connection_pool = db_pool
        
        return await _connection_pool.initialize()

async def close_database():
    """Close the database connection."""
    global _connection_pool
    
    if _connection_pool:
        await _connection_pool.close()
        _connection_pool = None

async def get_database_health() -> Dict[str, Any]:
    """Get database health status."""
    if _connection_pool:
        return await _connection_pool.check_health()
    else:
        return {"status": "not_initialized"}

class PatientRepository:
    """Repository for patient-related database operations."""
    
    def __init__(self, pool: DatabasePool = None):
        self.pool = pool or db_pool
    
    async def find_patient_by_credentials(self, lastname: str, firstname: str, 
                                        midname: Optional[str], bdate: str, 
                                        cllogin: str, clpassword: str) -> Optional[Dict]:
        """Find patient by comprehensive credentials matching."""
        try:
            # First try to find by login/password
            query_login = """
                SELECT uuid, hisnumber_qms, hisnumber_infoclinica, 
                    lastname, name, surname, birthdate,
                    login_qms, login_infoclinica
                FROM patients 
                WHERE (login_qms = $1 AND password_qms = $2) 
                OR (login_infoclinica = $1 AND password_infoclinica = $2)
                LIMIT 1
            """
            results = await self.pool.execute_query(query_login, (cllogin, clpassword))
            
            if results:
                row = results[0]
                return {
                    'uuid': str(row[0]),
                    'hisnumber_qms': row[1],
                    'hisnumber_infoclinica': row[2],
                    'lastname': row[3],
                    'name': row[4],
                    'surname': row[5],
                    'birthdate': row[6],
                    'login_qms': row[7],
                    'login_infoclinica': row[8]
                }
            
            # If not found by credentials, try by personal data
            bdate_obj = datetime.strptime(bdate, '%Y-%m-%d').date()
            
            if midname:
                query_personal = """
                    SELECT uuid, hisnumber_qms, hisnumber_infoclinica, 
                        lastname, name, surname, birthdate,
                        login_qms, login_infoclinica
                    FROM patients 
                    WHERE lastname = $1 AND name = $2 AND surname = $3 AND birthdate = $4
                    LIMIT 1
                """
                results = await self.pool.execute_query(query_personal, (lastname, firstname, midname, bdate_obj))
            else:
                query_personal = """
                    SELECT uuid, hisnumber_qms, hisnumber_infoclinica, 
                        lastname, name, surname, birthdate,
                        login_qms, login_infoclinica
                    FROM patients 
                    WHERE lastname = $1 AND name = $2 AND birthdate = $3 AND surname IS NULL
                    LIMIT 1
                """
                results = await self.pool.execute_query(query_personal, (lastname, firstname, bdate_obj))
            
            if results:
                row = results[0]
                return {
                    'uuid': str(row[0]),
                    'hisnumber_qms': row[1],
                    'hisnumber_infoclinica': row[2],
                    'lastname': row[3],
                    'name': row[4],
                    'surname': row[5],
                    'birthdate': row[6],
                    'login_qms': row[7],
                    'login_infoclinica': row[8]
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error finding patient by credentials: {e}")
            return None
    
    async def register_mobile_app_user(self, hisnumber_qms: Optional[str], 
                                     hisnumber_infoclinica: Optional[str]) -> Optional[str]:
        """Register a mobile app user and return UUID."""
        try:
            if not hisnumber_qms and not hisnumber_infoclinica:
                return None
            
            query = """
                INSERT INTO mobile_app_users (hisnumber_qms, hisnumber_infoclinica)
                VALUES ($1, $2)
                RETURNING uuid
            """
            params = (hisnumber_qms, hisnumber_infoclinica)
            
            results = await self.pool.execute_query(query, params)
            
            if results:
                return str(results[0][0])
            
            return None
            
        except Exception as e:
            logger.error(f"Error registering mobile app user: {e}")
            return None
    
    async def lock_patient_matching(self, patient_uuid: str, reason: str) -> bool:
        """Lock patient from further matching."""
        try:
            query = """
                UPDATE patients 
                SET matching_locked = TRUE,
                    matching_locked_at = CURRENT_TIMESTAMP,
                    matching_locked_reason = $2,
                    updated_at = CURRENT_TIMESTAMP
                WHERE uuid = $1
            """
            params = (patient_uuid, reason)
            
            result = await self.pool.execute_command(query, params)
            return "UPDATE 1" in result
            
        except Exception as e:
            logger.error(f"Error locking patient matching: {e}")
            return False
    
    async def unlock_patient_matching(self, patient_uuid: str) -> bool:
        """Unlock patient matching."""
        try:
            query = """
                UPDATE patients 
                SET matching_locked = FALSE,
                    matching_locked_at = NULL,
                    matching_locked_reason = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE uuid = $1
            """
            params = (patient_uuid,)
            
            result = await self.pool.execute_command(query, params)
            return "UPDATE 1" in result
            
        except Exception as e:
            logger.error(f"Error unlocking patient matching: {e}")
            return False
    
    async def get_mobile_app_stats(self) -> Dict[str, int]:
        """Get mobile app user statistics."""
        try:
            query = """
                SELECT 
                    COUNT(*) as total_mobile_users,
                    COUNT(CASE WHEN hisnumber_qms IS NOT NULL AND hisnumber_infoclinica IS NOT NULL THEN 1 END) as both_his_registered,
                    COUNT(CASE WHEN hisnumber_qms IS NOT NULL AND hisnumber_infoclinica IS NULL THEN 1 END) as qms_only,
                    COUNT(CASE WHEN hisnumber_qms IS NULL AND hisnumber_infoclinica IS NOT NULL THEN 1 END) as infoclinica_only
                FROM mobile_app_users
            """
            
            results = await self.pool.execute_query(query)
            
            if results:
                return {
                    "total_mobile_users": results[0][0],
                    "both_his_registered": results[0][1],
                    "qms_only": results[0][2],
                    "infoclinica_only": results[0][3]
                }
            
            return {"total_mobile_users": 0, "both_his_registered": 0, "qms_only": 0, "infoclinica_only": 0}
            
        except Exception as e:
            logger.error(f"Error getting mobile app stats: {e}")
            return {"total_mobile_users": 0, "both_his_registered": 0, "qms_only": 0, "infoclinica_only": 0}
    
    async def get_patient_matching_stats(self) -> List[Dict[str, Any]]:
        """Get patient matching statistics for the last 24 hours."""
        try:
            query = """
                SELECT 
                    match_type,
                    COUNT(*) as count,
                    COUNT(CASE WHEN created_uuid THEN 1 END) as new_patients_created,
                    COUNT(CASE WHEN mobile_app_uuid IS NOT NULL THEN 1 END) as mobile_app_matches
                FROM patient_matching_log
                WHERE match_time >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
                GROUP BY match_type
                ORDER BY count DESC
            """
            
            results = await self.pool.execute_query(query)
            
            stats = []
            for row in results:
                stats.append({
                    "match_type": row[0],
                    "count": row[1],
                    "new_patients_created": row[2],
                    "mobile_app_matches": row[3]
                })
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting patient matching stats: {e}")
            return []
    
    async def update_patient_credentials(self, patient_uuid: str, 
                                       qms_login: Optional[str] = None, 
                                       qms_password: Optional[str] = None,
                                       ic_login: Optional[str] = None, 
                                       ic_password: Optional[str] = None) -> bool:
        """Update patient login/password credentials in database."""
        try:
            query = """
                UPDATE patients 
                SET login_qms = COALESCE($2, login_qms),
                    password_qms = COALESCE($3, password_qms),
                    login_infoclinica = COALESCE($4, login_infoclinica),
                    password_infoclinica = COALESCE($5, password_infoclinica),
                    updated_at = CURRENT_TIMESTAMP
                WHERE uuid = $1
            """
            
            await self.pool.execute_query(
                query, 
                (patient_uuid, qms_login, qms_password, ic_login, ic_password)
            )
            
            logger.info(f"Updated credentials for patient: {patient_uuid}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating patient credentials: {e}")
            return False
        
    async def find_mobile_app_user_by_patient_uuid(self, patient_uuid: str) -> Optional[Dict]:
        """Find mobile app user by patient UUID."""
        try:
            query = """
                SELECT m.uuid, m.hisnumber_qms, m.hisnumber_infoclinica
                FROM mobile_app_users m
                WHERE m.uuid = $1
            """
            results = await self.pool.execute_query(query, (patient_uuid,))
            
            if results:
                return {
                    'uuid': str(results[0][0]),
                    'hisnumber_qms': results[0][1],
                    'hisnumber_infoclinica': results[0][2]
                }
            return None
            
        except Exception as e:
            logger.error(f"Error finding mobile app user by patient UUID: {e}")
            return None
    
    async def update_mobile_app_user_hisnumbers(self, mobile_uuid: str, 
                                               hisnumber_qms: Optional[str] = None,
                                               hisnumber_infoclinica: Optional[str] = None) -> Optional[str]:
        """Update mobile app user with HIS numbers."""
        try:
            query = """
                UPDATE mobile_app_users 
                SET hisnumber_qms = COALESCE($2, hisnumber_qms),
                    hisnumber_infoclinica = COALESCE($3, hisnumber_infoclinica),
                    updated_at = CURRENT_TIMESTAMP
                WHERE uuid = $1
                RETURNING uuid
            """
            results = await self.pool.execute_query(query, (mobile_uuid, hisnumber_qms, hisnumber_infoclinica))
            
            if results:
                return str(results[0][0])
            return None
            
        except Exception as e:
            logger.error(f"Error updating mobile app user HIS numbers: {e}")
            return None
    
    async def update_patient_credentials(self, patient_uuid: str, 
                                       qms_login: Optional[str] = None, 
                                       qms_password: Optional[str] = None,
                                       ic_login: Optional[str] = None, 
                                       ic_password: Optional[str] = None) -> bool:
        """Update patient login/password credentials in database."""
        try:
            query = """
                UPDATE patients 
                SET login_qms = COALESCE($2, login_qms),
                    password_qms = COALESCE($3, password_qms),
                    login_infoclinica = COALESCE($4, login_infoclinica),
                    password_infoclinica = COALESCE($5, password_infoclinica),
                    updated_at = CURRENT_TIMESTAMP
                WHERE uuid = $1
            """
            
            await self.pool.execute_query(
                query, 
                (patient_uuid, qms_login, qms_password, ic_login, ic_password)
            )
            
            logger.info(f"Updated credentials for patient: {patient_uuid}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating patient credentials: {e}")
            return False
    
    async def get_mobile_app_stats(self) -> Dict:
        """Get mobile app statistics."""
        try:
            query = """
                SELECT 
                    COUNT(*) as total_mobile_users,
                    COUNT(CASE WHEN hisnumber_qms IS NOT NULL AND hisnumber_infoclinica IS NOT NULL THEN 1 END) as both_his_registered,
                    COUNT(CASE WHEN hisnumber_qms IS NOT NULL AND hisnumber_infoclinica IS NULL THEN 1 END) as qms_only,
                    COUNT(CASE WHEN hisnumber_qms IS NULL AND hisnumber_infoclinica IS NOT NULL THEN 1 END) as infoclinica_only
                FROM mobile_app_users
            """
            results = await self.pool.execute_query(query)
            
            if results:
                row = results[0]
                return {
                    'total_mobile_users': row[0],
                    'both_his_registered': row[1],
                    'qms_only': row[2],
                    'infoclinica_only': row[3]
                }
            return {}
            
        except Exception as e:
            logger.error(f"Error getting mobile app stats: {e}")
            return {}

    async def get_patient_matching_stats(self) -> list:
        """Get patient matching statistics for last 24 hours."""
        try:
            query = """
                SELECT 
                    match_type,
                    COUNT(*) as count,
                    COUNT(CASE WHEN created_uuid THEN 1 END) as new_patients_created,
                    COUNT(CASE WHEN mobile_app_uuid IS NOT NULL THEN 1 END) as mobile_app_matches
                FROM patient_matching_log
                WHERE match_time > NOW() - INTERVAL '24 hours'
                GROUP BY match_type
                ORDER BY count DESC
            """
            results = await self.pool.execute_query(query)
            
            if results:
                return [
                    {
                        'match_type': row[0],
                        'count': row[1], 
                        'new_patients_created': row[2],
                        'mobile_app_matches': row[3]
                    }
                    for row in results
                ]
            return []
            
        except Exception as e:
            logger.error(f"Error getting patient matching stats: {e}")
            return []

    async def lock_patient_matching(self, patient_uuid: str, reason: str) -> bool:
        """Lock patient from further matching."""
        try:
            query = """
                UPDATE patients
                SET 
                    matching_locked = TRUE,
                    matching_locked_at = CURRENT_TIMESTAMP,
                    matching_locked_reason = $2,
                    updated_at = CURRENT_TIMESTAMP
                WHERE uuid = $1
            """
            await self.pool.execute_query(query, (patient_uuid, reason))
            return True
            
        except Exception as e:
            logger.error(f"Error locking patient matching: {e}")
            return False

    async def unlock_patient_matching(self, patient_uuid: str) -> bool:
        """Unlock patient matching."""
        try:
            query = """
                UPDATE patients
                SET 
                    matching_locked = FALSE,
                    matching_locked_at = NULL,
                    matching_locked_reason = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE uuid = $1
            """
            await self.pool.execute_query(query, (patient_uuid,))
            return True
            
        except Exception as e:
            logger.error(f"Error unlocking patient matching: {e}")
            return False
    
    async def find_patient_by_credentials(self, lastname: str, firstname: str, 
                                        midname: Optional[str], bdate: str, 
                                        cllogin: str, clpassword: str) -> Optional[Dict]:
        """Find patient by comprehensive credentials matching."""
        try:
            # First try to find by login/password
            query_login = """
                SELECT uuid, hisnumber_qms, hisnumber_infoclinica, 
                       lastname, name, surname, birthdate,
                       login_qms, login_infoclinica
                FROM patients 
                WHERE (login_qms = $1 AND password_qms = $2) 
                   OR (login_infoclinica = $1 AND password_infoclinica = $2)
                LIMIT 1
            """
            results = await self.pool.execute_query(query_login, (cllogin, clpassword))
            
            if results:
                row = results[0]
                return {
                    'uuid': str(row[0]),
                    'hisnumber_qms': row[1],
                    'hisnumber_infoclinica': row[2],
                    'lastname': row[3],
                    'name': row[4],
                    'surname': row[5],
                    'birthdate': row[6],
                    'login_qms': row[7],
                    'login_infoclinica': row[8]
                }
            
            # If not found by credentials, try by personal data
            from datetime import datetime
            bdate_obj = datetime.strptime(bdate, '%Y-%m-%d').date()
            
            if midname:
                query_personal = """
                    SELECT uuid, hisnumber_qms, hisnumber_infoclinica, 
                           lastname, name, surname, birthdate,
                           login_qms, login_infoclinica
                    FROM patients 
                    WHERE lastname = $1 AND name = $2 AND surname = $3 AND birthdate = $4
                    LIMIT 1
                """
                results = await self.pool.execute_query(query_personal, (lastname, firstname, midname, bdate_obj))
            else:
                query_personal = """
                    SELECT uuid, hisnumber_qms, hisnumber_infoclinica, 
                           lastname, name, surname, birthdate,
                           login_qms, login_infoclinica
                    FROM patients 
                    WHERE lastname = $1 AND name = $2 AND birthdate = $3 AND surname IS NULL
                    LIMIT 1
                """
                results = await self.pool.execute_query(query_personal, (lastname, firstname, bdate_obj))
            
            if results:
                row = results[0]
                return {
                    'uuid': str(row[0]),
                    'hisnumber_qms': row[1],
                    'hisnumber_infoclinica': row[2],
                    'lastname': row[3],
                    'name': row[4],
                    'surname': row[5],
                    'birthdate': row[6],
                    'login_qms': row[7],
                    'login_infoclinica': row[8]
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error finding patient by credentials: {e}")
            return None

def get_patient_repository() -> PatientRepository:
    """Get patient repository instance."""
    return PatientRepository()