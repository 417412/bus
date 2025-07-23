import logging
import psycopg2
from typing import Dict, Any, Optional, List

class PostgresConnector:
    """Connector for PostgreSQL destination system."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        self.logger = logging.getLogger(__name__)
        
    def connect(self) -> bool:
        try:
            self.connection = psycopg2.connect(
                host=self.config.get('host', 'localhost'),
                database=self.config.get('database', 'medical_system'),
                user=self.config.get('user', 'postgres'),
                password=self.config.get('password', '')
            )
            self.logger.info("Connected to PostgreSQL")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            return False
            
    def disconnect(self) -> None:
        if self.connection:
            self.connection.close()
            self.logger.info("Disconnected from PostgreSQL")
    
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
            
            cursor = self.connection.cursor()
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
            cursor.close()
            
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
            cursor = self.connection.cursor()
            cursor.execute("""
            SELECT pd.*, p.documenttypes, p.document_number, p.uuid
            FROM patientsdet pd
            JOIN patients p ON pd.uuid = p.uuid
            WHERE pd.hisnumber = %s AND pd.source = %s
            """, (hisnumber, source))
            
            result = cursor.fetchone()
            cursor.close()
            
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