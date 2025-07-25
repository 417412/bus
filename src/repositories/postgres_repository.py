import logging
from typing import Dict, Any, Optional, List
from src.connectors.postgres_connector import PostgresConnector

class PostgresRepository:

    def __init__(self, connector: PostgresConnector = None):
        """Initialize repository with optional connector."""
        if connector is None:
            # Create connector with default decrypted config
            connector = PostgresConnector()
            
        self.connector = connector
        self.logger = logging.getLogger(__name__)
    
    def insert_patient(self, patient_data: Dict[str, Any]) -> bool:
        """
        Insert a patient record into the patientsdet table.
        
        Args:
            patient_data: Dictionary containing patient data
            
        Returns:
            True if insertion was successful, False otherwise
        """
        try:
            # Ensure hisnumber is a string
            patient_data = patient_data.copy()  # Don't modify original
            patient_data['hisnumber'] = str(patient_data.get('hisnumber', ''))
            
            cursor = self.connector.connection.cursor()
            cursor.execute("""
                INSERT INTO patientsdet (
                    hisnumber, source, businessunit, lastname, name, surname, birthdate,
                    documenttypes, document_number, email, telephone, his_password, login_email
                ) VALUES (
                    %(hisnumber)s, %(source)s, %(businessunit)s, %(lastname)s, 
                    %(name)s, %(surname)s, %(birthdate)s, %(documenttypes)s, %(document_number)s, 
                    %(email)s, %(telephone)s, %(his_password)s, %(login_email)s
                )
            """, patient_data)
            
            self.connector.connection.commit()
            cursor.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Error inserting patient {patient_data.get('hisnumber')}: {e}")
            self.connector.connection.rollback()
            return False
    
    def get_patient_by_hisnumber(self, hisnumber: str, source: int) -> Optional[Dict[str, Any]]:
        """
        Get a patient by HIS number and source.
        
        Args:
            hisnumber: The HIS number
            source: Source system ID
            
        Returns:
            Patient data or None if not found
        """
        if not self.connector.connection:
            self.logger.error("Not connected to PostgreSQL")
            return None
            
        try:
            cursor = self.connector.connection.cursor()
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
                    'login_email', 'uuid', 'p_documenttypes', 'p_document_number', 'p_uuid'
                ]
                return dict(zip(columns, result))
            return None
        except Exception as e:
            self.logger.error(f"Error getting patient: {str(e)}")
            return None

    def upsert_patient(self, patient_data: Dict[str, Any]) -> bool:
        """
        Insert or update a patient record in the patientsdet table.
        
        Args:
            patient_data: Dictionary containing patient data
            
        Returns:
            True if operation was successful, False otherwise
        """
        try:
            # Ensure hisnumber is a string
            patient_data = patient_data.copy()  # Don't modify original
            patient_data['hisnumber'] = str(patient_data.get('hisnumber', ''))
            
            cursor = self.connector.connection.cursor()
            cursor.execute("""
                INSERT INTO patientsdet (
                    hisnumber, source, businessunit, lastname, name, surname, birthdate,
                    documenttypes, document_number, email, telephone, his_password, login_email
                ) VALUES (
                    %(hisnumber)s, %(source)s, %(businessunit)s, %(lastname)s, 
                    %(name)s, %(surname)s, %(birthdate)s, %(documenttypes)s, %(document_number)s, 
                    %(email)s, %(telephone)s, %(his_password)s, %(login_email)s
                )
                ON CONFLICT (hisnumber, source) 
                DO UPDATE SET
                    businessunit = EXCLUDED.businessunit,
                    lastname = EXCLUDED.lastname,
                    name = EXCLUDED.name,
                    surname = EXCLUDED.surname,
                    birthdate = EXCLUDED.birthdate,
                    documenttypes = EXCLUDED.documenttypes,
                    document_number = EXCLUDED.document_number,
                    email = EXCLUDED.email,
                    telephone = EXCLUDED.telephone,
                    his_password = EXCLUDED.his_password,
                    login_email = EXCLUDED.login_email
            """, patient_data)
            
            self.connector.connection.commit()
            cursor.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Error upserting patient {patient_data.get('hisnumber')}: {e}")
            self.connector.connection.rollback()
            return False

    def mark_patient_deleted(self, hisnumber: str, source: int) -> bool:
        """
        Mark a patient as deleted (for future implementation).
        
        Args:
            hisnumber: Patient's HIS number
            source: Source system ID
            
        Returns:
            True if operation was successful, False otherwise
        """
        try:
            # Ensure hisnumber is a string
            hisnumber_str = str(hisnumber) if hisnumber is not None else None
            
            cursor = self.connector.connection.cursor()
            # For now, we'll just log the deletion request
            # In the future, this could set a deleted flag or move to an archive table
            self.logger.info(f"Delete request for patient {hisnumber_str} from source {source}")
            cursor.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Error marking patient {hisnumber} as deleted: {e}")
            return False
        
    def get_patient_count_by_source(self, source_id: int) -> int:
        """
        Get the count of patients from a specific source.
        
        Args:
            source_id: Source system ID
            
        Returns:
            Total patient count for the source
        """
        try:
            if not self.connector.connection:
                self.logger.error("Not connected to PostgreSQL")
                return 0
                
            cursor = self.connector.connection.cursor()
            cursor.execute("""
                SELECT COUNT(*) 
                FROM patientsdet 
                WHERE source = %s
            """, (source_id,))
            
            count = cursor.fetchone()[0]
            cursor.close()
            
            self.logger.info(f"Total patients in PostgreSQL from source {source_id}: {count}")
            return count
        except Exception as e:
            self.logger.error(f"Error getting patient count: {str(e)}")
            return 0
        
    def patient_exists(self, hisnumber: str, source: int) -> bool:
        """
        Check if a patient already exists in the patientsdet table.
        
        Args:
            hisnumber: Patient's HIS number
            source: Source system ID
            
        Returns:
            True if patient exists, False otherwise
        """
        try:
            cursor = self.connector.connection.cursor()
            
            # Convert hisnumber to string to match VARCHAR column type
            hisnumber_str = str(hisnumber) if hisnumber is not None else None
            
            cursor.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM patientsdet 
                    WHERE hisnumber = %s AND source = %s
                )
            """, (hisnumber_str, source))
            
            result = cursor.fetchone()[0]
            cursor.close()
            return result
            
        except Exception as e:
            self.logger.error(f"Error checking if patient {hisnumber} exists: {e}")
            return False
    
    def get_total_patient_count(self, source: int = None) -> int:
        """
        Get the total number of patients in the patientsdet table.
        
        Args:
            source: Optional source ID to filter by (1=qMS, 2=Инфоклиника)
        
        Returns:
            Total patient count for the specified source, or all patients if source=None
        """
        try:
            cursor = self.connector.connection.cursor()
            
            if source is not None:
                cursor.execute("SELECT COUNT(*) FROM patientsdet WHERE source = %s", (source,))
                count = cursor.fetchone()[0]
                self.logger.info(f"Total patient count in PostgreSQL for source {source}: {count}")
            else:
                cursor.execute("SELECT COUNT(*) FROM patientsdet")
                count = cursor.fetchone()[0]
                self.logger.info(f"Total patient count in PostgreSQL (all sources): {count}")
            
            cursor.close()
            return count
            
        except Exception as e:
            self.logger.error(f"Error getting total patient count: {e}")
            return 0