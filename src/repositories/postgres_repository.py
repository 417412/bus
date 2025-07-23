import logging
from typing import Dict, Any, Optional, List
from src.connectors.postgres_connector import PostgresConnector

class PostgresRepository:
    """Repository for accessing PostgreSQL data."""
    
    def __init__(self, connector: PostgresConnector):
        self.connector = connector
        self.logger = logging.getLogger(__name__)
    
    def insert_patient(self, patient: Dict[str, Any]) -> bool:
        """
        Insert a patient record into PostgreSQL.
        
        Args:
            patient: Patient record to insert
            
        Returns:
            True if successful, False otherwise
        """
        if not self.connector.connection:
            self.logger.error("Not connected to PostgreSQL")
            return False
            
        try:
            cursor = self.connector.connection.cursor()
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
            self.connector.connection.commit()
            cursor.close()
            
            self.logger.info(f"Patient inserted with ID: {inserted_id}")
            return True
        except Exception as e:
            self.logger.error(f"Error inserting patient: {str(e)}")
            if self.connector.connection:
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
                    'uuid', 'p_documenttypes', 'p_document_number', 'p_uuid'
                ]
                return dict(zip(columns, result))
            return None
        except Exception as e:
            self.logger.error(f"Error getting patient: {str(e)}")
            return None

    def upsert_patient(self, patient: Dict[str, Any]) -> bool:
        """
        Update an existing patient or insert a new one if it doesn't exist.

        Args:
            patient: Patient data to upsert

        Returns:
            True if successful, False otherwise
        """
        try:
            hisnumber = patient.get('hisnumber')
            source = patient.get('source')

            if not hisnumber or not source:
                self.logger.error("Missing hisnumber or source in patient data")
                return False

            # Check if patient exists
            cursor = self.connector.connection.cursor()
            cursor.execute("""
                SELECT id FROM patientsdet
                WHERE hisnumber = '%s' AND source = %s
            """, (hisnumber, source))

            result = cursor.fetchone()

            if result:
                # Patient exists, update
                patient_id = result[0]

                # Build SET clause and parameters
                set_clauses = []
                params = {}

                for key, value in patient.items():
                    if key not in ('hisnumber', 'source'):  # Don't update primary keys
                        set_clauses.append(f"{key} = %({key})s")
                        params[key] = value

                params['id'] = patient_id

                # Execute update
                if set_clauses:
                    update_sql = f"""
                        UPDATE patientsdet
                        SET {', '.join(set_clauses)}
                        WHERE id = %(id)s
                    """
                    cursor.execute(update_sql, params)
                    self.connector.connection.commit()
                    self.logger.info(f"Updated patient: {hisnumber} (source: {source})")
                    return True
                else:
                    self.logger.warning(f"No fields to update for patient: {hisnumber}")
                    return False
            else:
                # Patient doesn't exist, insert
                return self.insert_patient(patient)

        except Exception as e:
            self.logger.error(f"Error upserting patient: {str(e)}")
            if self.connector.connection:
                self.connector.connection.rollback()
            return False

    def mark_patient_deleted(self, hisnumber: str, source: int) -> bool:
        """
        Mark a patient as deleted.

        Note: In medical systems, we typically don't delete patient records,
        but instead mark them as inactive or archived.

        Args:
            hisnumber: HIS number of the patient
            source: Source system ID

        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if we need to add a 'deleted' or 'active' column to patientsdet
            # For now, we'll log a message and return success
            self.logger.info(f"Patient deletion requested for {hisnumber} (source: {source})")
            self.logger.warning("Patient deletion not implemented - patients are retained for medical records")
            return True

            # If implementing actual deletion behavior in the future:
            # cursor = self.connector.connection.cursor()
            # cursor.execute("""
            #    UPDATE patientsdet SET active = FALSE WHERE hisnumber = %s AND source = %s
            # """, (hisnumber, source))
            # self.connector.connection.commit()
            # return cursor.rowcount > 0
        except Exception as e:
            self.logger.error(f"Error marking patient as deleted: {str(e)}")
            if self.connector.connection:
                self.connector.connection.rollback()
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
            cursor.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM patientsdet 
                    WHERE hisnumber = %s AND source = %s
                )
            """, (hisnumber, source))  # Use parameterized query instead of string formatting
            
            result = cursor.fetchone()[0]
            cursor.close()
            return result
            
        except Exception as e:
            self.logger.error(f"Error checking if patient {hisnumber} exists: {e}")
            return False