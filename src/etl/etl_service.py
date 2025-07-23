import logging
from typing import List, Dict, Any, Optional
from src.repositories.firebird_repository import FirebirdRepository
from src.repositories.postgres_repository import PostgresRepository
from src.etl.transformers.firebird_transformer import FirebirdTransformer

class ETLService:
    """Service for orchestrating ETL processes."""
    
    def __init__(self, source_repo: FirebirdRepository, target_repo: PostgresRepository):
        self.source_repo = source_repo
        self.target_repo = target_repo
        self.transformer = FirebirdTransformer()
        self.logger = logging.getLogger(__name__)
        
    def process_batch(self, batch_size: int = 100, last_id: Optional[str] = None) -> int:
        """
        Process a batch of patient data from source to target.
        
        Args:
            batch_size: Maximum number of records to process
            last_id: ID of the last processed record for incremental loading
            
        Returns:
            Number of successfully processed records
        """
        # Extract
        self.logger.info(f"Extracting up to {batch_size} patients from source")
        raw_patients = self.source_repo.get_patients(batch_size=batch_size, last_id=last_id)
        
        if not raw_patients:
            self.logger.warning("No patients found to process")
            return 0
            
        # Transform
        self.logger.info(f"Transforming {len(raw_patients)} patients")
        transformed_patients = []
        for patient in raw_patients:
            try:
                transformed = self.transformer.transform_patient(patient)
                transformed_patients.append(transformed)
            except Exception as e:
                self.logger.error(f"Error transforming patient: {str(e)}")
        
        # Load
        self.logger.info(f"Loading {len(transformed_patients)} patients into target")
        success_count = 0
        max_id = None
        
        for patient in transformed_patients:
            try:
                hisnumber = patient.get('hisnumber')
                
                # Track the maximum hisnumber for incremental loading
                if hisnumber and (max_id is None or hisnumber > max_id):
                    max_id = hisnumber
                
                if self.target_repo.insert_patient(patient):
                    success_count += 1
            except Exception as e:
                self.logger.error(f"Error loading patient {patient.get('hisnumber')}: {str(e)}")
        
        # Save the last processed ID
        if max_id is not None:
            self.source_repo.save_last_processed_id(max_id)
        
        self.logger.info(f"Batch complete: {success_count}/{len(transformed_patients)} patients processed successfully")
        return success_count
    
    def process_delta(self, batch_size: int = 100) -> int:
        """
        Process delta records (changes) from source to target.
        
        Args:
            batch_size: Maximum number of records to process
            
        Returns:
            Number of successfully processed records
        """
        # Get delta records
        delta_records, processed_count = self.source_repo.get_patient_deltas(batch_size=batch_size)
        
        if not delta_records:
            self.logger.info("No delta records to process")
            return 0
        
        self.logger.info(f"Processing {len(delta_records)} delta records")
        
        # Group records by operation type
        inserts = []
        updates = []
        deletes = []
        
        for record in delta_records:
            operation = record.get('operation', '').upper()
            
            # Remove the operation and delta_id fields
            record.pop('operation', None)
            record.pop('delta_id', None)
            
            # Transform the record
            try:
                transformed = self.transformer.transform_patient(record)
                
                if operation == 'INSERT':
                    inserts.append(transformed)
                elif operation == 'UPDATE':
                    updates.append(transformed)
                elif operation == 'DELETE':
                    deletes.append(transformed)
            except Exception as e:
                self.logger.error(f"Error transforming delta record: {str(e)}")
        
        # Process inserts and updates
        success_count = 0
        
        # Process inserts
        for record in inserts:
            try:
                if self.target_repo.insert_patient(record):
                    success_count += 1
            except Exception as e:
                self.logger.error(f"Error inserting patient {record.get('hisnumber')}: {str(e)}")
        
        # Process updates
        for record in updates:
            try:
                if self.target_repo.upsert_patient(record):
                    success_count += 1
            except Exception as e:
                self.logger.error(f"Error updating patient {record.get('hisnumber')}: {str(e)}")
        
        # Process deletes (if implemented)
        for record in deletes:
            try:
                hisnumber = record.get('hisnumber')
                source = record.get('source')
                if hisnumber and source:
                    if self.target_repo.mark_patient_deleted(hisnumber, source):
                        success_count += 1
            except Exception as e:
                self.logger.error(f"Error deleting patient {record.get('hisnumber')}: {str(e)}")
        
        self.logger.info(f"Delta processing complete: {success_count} records processed successfully")
        return success_count