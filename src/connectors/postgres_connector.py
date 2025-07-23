import logging
import psycopg2
from typing import Dict, Any, Optional, List, Tuple
from src.config.settings import setup_logger, get_decrypted_database_config

class PostgresConnector:
    """Connector for PostgreSQL database operations."""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize PostgreSQL connector.
        
        Args:
            config: Database configuration dictionary. If None, will use decrypted config from settings.
        """
        # Use decrypted config if no config provided
        if config is None:
            config = get_decrypted_database_config()["PostgreSQL"]
        
        self.config = config
        self.connection = None
        self.logger = setup_logger(__name__, "connectors")
        
        # Log connection details (without password)
        safe_config = {k: v if k.lower() != 'password' else '********' for k, v in self.config.items()}
        self.logger.debug(f"Initializing PostgreSQL connector with config: {safe_config}")
        
    def connect(self) -> bool:
        try:
            self.connection = psycopg2.connect(
                host=self.config.get('host', 'localhost'),
                database=self.config.get('database', 'medical_system'),
                user=self.config.get('user', 'postgres'),
                password=self.config.get('password', ''),
                port=self.config.get('port', 5432)
            )
            self.logger.info("Connected to PostgreSQL")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            # Don't log the actual password in error messages
            if 'password' in str(e).lower():
                self.logger.error("Connection failed - check username, password, and database settings")
            return False
            
    def disconnect(self) -> None:
        if self.connection:
            try:
                self.connection.close()
                self.logger.info("Disconnected from PostgreSQL")
            except Exception as e:
                self.logger.error(f"Error disconnecting from PostgreSQL: {e}")
    
    def execute_query(self, query: str, params: tuple = None) -> Tuple[List[Any], List[str]]:
        """
        Execute a query and return the results with column names.
        
        Args:
            query: SQL query to execute
            params: Parameters for the query
            
        Returns:
            For SELECT queries: Tuple of (rows, column_names)
            For non-SELECT queries: Tuple of (None, None)
        """
        if not self.connection:
            raise Exception("Not connected to database")
            
        with self.connection.cursor() as cursor:
            # Execute the query
            cursor.execute(query, params or ())
            
            # Determine if this is a SELECT query by checking if cursor.description exists
            # For non-SELECT queries (INSERT, UPDATE, DELETE), description will be None
            if cursor.description:
                rows = cursor.fetchall()
                column_names = [desc[0].lower() for desc in cursor.description]
                return rows, column_names
            else:
                # For non-SELECT queries, return None for both rows and column_names
                # This way we can tell it's a non-SELECT query by checking if rows is None
                self.connection.commit()
                return None, None
    
    def test_connection(self) -> bool:
        """
        Test the database connection.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            if self.connect():
                # Try a simple query
                rows, columns = self.execute_query("SELECT 1")
                self.disconnect()
                return True
            return False
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
    
    def normalize_document_number(self, document_number: Optional[str]) -> Optional[int]:
        """Normalize document number to ensure it contains only digits and convert to integer."""
        if not document_number:
            return None
            
        # Extract only digits
        import re
        digits = re.sub(r'\D', '', str(document_number))
        
        # Convert to integer if we have digits
        if digits:
            try:
                return int(digits)
            except ValueError:
                return None
        return None
            
    def insert_patient(self, patient: Dict[str, Any]) -> bool:
        """
        Insert a patient record into the PostgreSQL database.
        
        Args:
            patient: Dictionary containing patient data
            
        Returns:
            bool: True if insertion was successful, False otherwise
        """
        try:
            # Process document_number if present in patient data
            document_number = patient.get('document_number')
            if document_number is not None:
                # Only normalize if it's a string; if it's already an int, leave it
                if not isinstance(document_number, int):
                    document_number = self.normalize_document_number(document_number)
                patient['document_number'] = document_number
            
            # Process document type
            document_types = patient.get('documenttypes')
            if document_types is None and patient.get('document_number') is not None:
                # Default to passport (type 1) if document number exists but type is not specified
                patient['documenttypes'] = 1
                self.logger.info("Set default document type to Passport (1)")
            
            with self.connection.cursor() as cursor:
                cursor.execute("""
                INSERT INTO patientsdet (
                    hisnumber, source, businessunit, lastname, name, surname, birthdate,
                    documenttypes, document_number, email, telephone, his_password
                ) VALUES (
                    %(hisnumber)s, %(source)s, %(businessunit)s, %(lastname)s, 
                    %(name)s, %(surname)s, %(birthdate)s, %(documenttypes)s, %(document_number)s, 
                    %(email)s, %(telephone)s, %(his_password)s
                )
                RETURNING id
                """, patient)
                
                inserted_id = cursor.fetchone()[0]
                self.connection.commit()
                
                self.logger.info(f"Patient inserted with ID: {inserted_id}")
                return True
        except Exception as e:
            self.logger.error(f"Error inserting patient: {str(e)}")
            if self.connection:
                self.connection.rollback()
            return False
            
    def get_patient_by_hisnumber(self, hisnumber: str, source: int) -> Optional[Dict[str, Any]]:
        """
        Get a patient record by HIS number and source.
        
        Args:
            hisnumber: The HIS-specific patient identifier
            source: The source system ID (1=qMS, 2=Инфоклиника)
            
        Returns:
            Dict with patient data or None if not found
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                SELECT pd.*, p.documenttypes, p.document_number, p.uuid
                FROM patientsdet pd
                JOIN patients p ON pd.uuid = p.uuid
                WHERE pd.hisnumber = %s AND pd.source = %s
                """, (hisnumber, source))
                
                result = cursor.fetchone()
                
                if result:
                    # Convert to dictionary - update with your actual column names
                    columns = [
                        'id', 'hisnumber', 'source', 'businessunit', 'lastname', 'name', 'surname', 
                        'birthdate', 'documenttypes', 'document_number', 'email', 'telephone', 'his_password', 
                        'uuid', 'p_documenttypes', 'p_document_number', 'p_uuid'
                    ]
                    return dict(zip(columns, result))
                return None
        except Exception as e:
            self.logger.error(f"Error getting patient: {str(e)}")
            return None