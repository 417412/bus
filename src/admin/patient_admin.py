import psycopg2
import psycopg2.extras
from typing import List, Dict, Any, Optional, Tuple
import logging

class PatientAdmin:
    """Administration interface for patient data."""
    
    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self.connection = None
        self.logger = logging.getLogger(__name__)
        
    def connect(self) -> bool:
        try:
            self.connection = psycopg2.connect(
                host=self.db_config.get("host", "localhost"),
                database=self.db_config.get("database"),
                user=self.db_config.get("user"),
                password=self.db_config.get("password")
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {str(e)}")
            return False
            
    def disconnect(self) -> None:
        if self.connection:
            self.connection.close()
            
    def find_potential_duplicates_by_passport(self) -> List[Dict[str, Any]]:
        """Find potential duplicate patients by passport."""
        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            query = """
                SELECT 
                    p.passport,
                    COUNT(p.uuid) as patient_count,
                    ARRAY_AGG(p.uuid) as uuids
                FROM 
                    patients p
                WHERE 
                    p.passport IS NOT NULL
                GROUP BY 
                    p.passport
                HAVING 
                    COUNT(p.uuid) > 1
                ORDER BY 
                    COUNT(p.uuid) DESC
            """
            cursor.execute(query)
            results = [dict(row) for row in cursor]
            cursor.close()
            return results
        except Exception as e:
            self.logger.error(f"Error finding duplicate passports: {str(e)}")
            return []
            
    def find_potential_duplicates_by_name(self) -> List[Dict[str, Any]]:
        """Find potential duplicate patients by name and birthdate."""
        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            query = """
                SELECT 
                    p.lastname,
                    p.name,
                    p.surname,
                    p.birthdate,
                    COUNT(p.uuid) as patient_count,
                    ARRAY_AGG(p.uuid) as uuids
                FROM 
                    patients p
                WHERE 
                    p.lastname IS NOT NULL AND
                    p.name IS NOT NULL AND
                    p.birthdate IS NOT NULL
                GROUP BY 
                    p.lastname, p.name, p.surname, p.birthdate
                HAVING 
                    COUNT(p.uuid) > 1
                ORDER BY 
                    COUNT(p.uuid) DESC
            """
            cursor.execute(query)
            results = [dict(row) for row in cursor]
            cursor.close()
            return results
        except Exception as e:
            self.logger.error(f"Error finding duplicate names: {str(e)}")
            return []
            
    def get_patient_details(self, uuid: str) -> Optional[Dict[str, Any]]:
        """Get consolidated patient details by UUID."""
        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            query = """
                SELECT
                    p.uuid,
                    p.passport,
                    p.lastname,
                    p.name,
                    p.surname,
                    p.birthdate,
                    p.hisnumber_qms,
                    p.hisnumber_infoclinica,
                    p.email_qms,
                    p.telephone_qms,
                    p.password_qms,
                    p.email_infoclinica,
                    p.telephone_infoclinica,
                    p.password_infoclinica,
                    p.primary_source,
                    hl.name as primary_source_name,
                    (SELECT COUNT(*) FROM protocols pr WHERE pr.uuid = p.uuid) as protocol_count
                FROM
                    patients p
                LEFT JOIN
                    hislist hl ON p.primary_source = hl.id
                WHERE
                    p.uuid = %s
            """
            cursor.execute(query, (uuid,))
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                return dict(result)
            return None
        except Exception as e:
            self.logger.error(f"Error getting patient details: {str(e)}")
            return None
    
    def get_patient_protocols(self, uuid: str) -> List[Dict[str, Any]]:
        """Get patient protocols."""
        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            query = """
                SELECT
                    pr.id,
                    pr.uuid,
                    pr.source,
                    hl.name as source_name,
                    pr.businessunit,
                    bu.name as businessunit_name,
                    pr.date,
                    pr.doctor,
                    pr.protocolname,
                    pr.servicename,
                    pr.servicecode
                FROM
                    protocols pr
                JOIN
                    hislist hl ON pr.source = hl.id
                JOIN
                    businessunits bu ON pr.businessunit = bu.id
                WHERE
                    pr.uuid = %s
                ORDER BY
                    pr.date DESC
            """
            cursor.execute(query, (uuid,))
            results = [dict(row) for row in cursor]
            cursor.close()
            return results
        except Exception as e:
            self.logger.error(f"Error getting patient protocols: {str(e)}")
            return []
    
    def get_patient_raw_records(self, uuid: str) -> List[Dict[str, Any]]:
        """Get all raw patient records from patientsdet table."""
        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            query = """
                SELECT
                    pd.id,
                    pd.hisnumber,
                    pd.source,
                    hl.name as source_name,
                    pd.businessunit,
                    bu.name as businessunit_name,
                    pd.lastname,
                    pd.name,
                    pd.surname,
                    pd.birthdate,
                    pd.passport,
                    pd.email,
                    pd.telephone
                FROM
                    patientsdet pd
                JOIN
                    hislist hl ON pd.source = hl.id
                JOIN
                    businessunits bu ON pd.businessunit = bu.id
                WHERE
                    pd.uuid = %s
                ORDER BY
                    pd.source, pd.hisnumber
            """
            cursor.execute(query, (uuid,))
            results = [dict(row) for row in cursor]
            cursor.close()
            return results
        except Exception as e:
            self.logger.error(f"Error getting patient raw records: {str(e)}")
            return []
            
    def merge_patients(self, source_uuid: str, target_uuid: str, admin_user: str) -> bool:
        """Merge two patient records."""
        try:
            # First verify both patients exist
            source_patient = self.get_patient_details(source_uuid)
            target_patient = self.get_patient_details(target_uuid)
            
            if not source_patient or not target_patient:
                self.logger.error("Source or target patient not found")
                return False
                
            cursor = self.connection.cursor()
            
            # Start a transaction
            self.connection.autocommit = False
            
            # 1. Update consolidated patient info in target record
            cursor.execute("""
                UPDATE patients
                SET 
                    passport = COALESCE(patients.passport, %s),
                    lastname = COALESCE(patients.lastname, %s),
                    name = COALESCE(patients.name, %s),
                    surname = COALESCE(patients.surname, %s),
                    birthdate = COALESCE(patients.birthdate, %s),
                    
                    hisnumber_qms = COALESCE(patients.hisnumber_qms, %s),
                    email_qms = COALESCE(patients.email_qms, %s),
                    telephone_qms = COALESCE(patients.telephone_qms, %s),
                    password_qms = COALESCE(patients.password_qms, %s),
                    
                    hisnumber_infoclinica = COALESCE(patients.hisnumber_infoclinica, %s),
                    email_infoclinica = COALESCE(patients.email_infoclinica, %s),
                    telephone_infoclinica = COALESCE(patients.telephone_infoclinica, %s),
                    password_infoclinica = COALESCE(patients.password_infoclinica, %s)
                WHERE uuid = %s
            """, (
                source_patient.get('passport'),
                source_patient.get('lastname'),
                source_patient.get('name'),
                source_patient.get('surname'),
                source_patient.get('birthdate'),
                
                source_patient.get('hisnumber_qms'),
                source_patient.get('email_qms'),
                source_patient.get('telephone_qms'),
                source_patient.get('password_qms'),
                
                source_patient.get('hisnumber_infoclinica'),
                source_patient.get('email_infoclinica'),
                source_patient.get('telephone_infoclinica'),
                source_patient.get('password_infoclinica'),
                
                target_uuid
            ))
            
            # 2. Update all patientsdet records pointing to source_uuid
            cursor.execute("""
                UPDATE patientsdet
                SET uuid = %s
                WHERE uuid = %s
            """, (target_uuid, source_uuid))
            
            # 3. Update all protocols pointing to source_uuid
            cursor.execute("""
                UPDATE protocols
                SET uuid = %s
                WHERE uuid = %s
            """, (target_uuid, source_uuid))
            
            # 4. Delete the source patient record
            cursor.execute("""
                DELETE FROM patients
                WHERE uuid = %s
            """, (source_uuid,))
            
            # 5. Log the merge action
            cursor.execute("""
                INSERT INTO patient_matching_log (
                    hisnumber, source, match_type, passport, created_uuid
                ) VALUES (
                    %s, 0, 'MANUAL_MERGE', %s, FALSE
                )
            """, (
                f"MERGE:{source_uuid}->{target_uuid}",
                source_patient.get('passport')
            ))
            
            # Commit the transaction
            self.connection.commit()
            cursor.close()
            return True
        except Exception as e:
            self.logger.error(f"Error merging patients: {str(e)}")
            if self.connection:
                self.connection.rollback()
            return False
            
    def get_matching_statistics(self) -> Dict[str, Any]:
        """Get statistics about patient matching."""
        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            # Get total patient counts
            cursor.execute("""
                SELECT COUNT(*) as total_patients FROM patients
            """)
            total_patients = cursor.fetchone()[0]
            
            # Get counts by match type
            cursor.execute("""
                SELECT 
                    match_type, 
                    COUNT(*) as count
                FROM 
                    patient_matching_log
                GROUP BY 
                    match_type
            """)
            match_types = {row['match_type']: row['count'] for row in cursor}
            
            # Get counts by source
            cursor.execute("""
                SELECT 
                    hl.name as source_name,
                    COUNT(DISTINCT pd.uuid) as patient_count,
                    COUNT(pd.id) as record_count
                FROM 
                    patientsdet pd
                JOIN
                    hislist hl ON pd.source = hl.id
                GROUP BY 
                    hl.name
            """)
            sources = {row['source_name']: {
                'patient_count': row['patient_count'],
                'record_count': row['record_count']
            } for row in cursor}
            
            # Get counts for patients with multiple sources
            cursor.execute("""
                WITH patient_sources AS (
                    SELECT 
                        pd.uuid, 
                        COUNT(DISTINCT pd.source) as source_count
                    FROM 
                        patientsdet pd
                    GROUP BY 
                        pd.uuid
                )
                SELECT 
                    source_count,
                    COUNT(*) as patient_count
                FROM 
                    patient_sources
                GROUP BY 
                    source_count
                ORDER BY 
                    source_count
            """)
            source_counts = {row['source_count']: row['patient_count'] for row in cursor}
            
            cursor.close()
            
            return {
                'total_patients': total_patients,
                'match_types': match_types,
                'sources': sources,
                'source_counts': source_counts
            }
        except Exception as e:
            self.logger.error(f"Error getting matching statistics: {str(e)}")
            return {}