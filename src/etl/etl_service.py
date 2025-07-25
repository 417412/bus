import logging
from typing import List, Dict, Any, Optional, Union
from src.repositories.firebird_repository import FirebirdRepository
from src.repositories.yottadb_repository import YottaDBRepository
from src.repositories.postgres_repository import PostgresRepository
from src.etl.transformers.firebird_transformer import FirebirdTransformer
from src.etl.transformers.yottadb_transformer import YottaDBTransformer
from src.models.patient import Patient

class ETLService:
    """Service for orchestrating ETL processes."""
    
    def __init__(self, source_repo: Union[FirebirdRepository, YottaDBRepository], target_repo: PostgresRepository):
        self.source_repo = source_repo
        self.target_repo = target_repo
        
        # Select the appropriate transformer based on source repository type
        if isinstance(source_repo, FirebirdRepository):
            self.transformer = FirebirdTransformer()
            self.logger = logging.getLogger(__name__ + ".firebird")
        elif isinstance(source_repo, YottaDBRepository):
            self.transformer = YottaDBTransformer()
            self.logger = logging.getLogger(__name__ + ".yottadb")
        else:
            raise ValueError(f"Unsupported source repository type: {type(source_repo)}")
        
        self.logger.info(f"ETL Service initialized with {type(self.transformer).__name__}")
    
    def process_patient_record(self, raw_patient: Dict[str, Any]) -> Optional[Patient]:
        """
        Process a raw patient record into a standardized Patient model.
        
        This method:
        1. Uses the appropriate transformer to normalize the data
        2. Creates a Patient model from the transformed data
        
        Args:
            raw_patient: Raw patient data from source system
            
        Returns:
            Patient model instance or None if processing fails
        """
        try:
            source = raw_patient.get('source')
            hisnumber = raw_patient.get('hisnumber', 'unknown')
            
            self.logger.debug(f"Processing patient {hisnumber} from source {source}")
            
            # STEP 1: Transform the raw data using the appropriate transformer
            transformed_data = self.transformer.transform_patient(raw_patient)
            
            if not transformed_data:
                self.logger.error(f"Transformer returned None for patient {hisnumber}")
                return None
            
            self.logger.debug(f"Transformer mapped document type: {raw_patient.get('documenttypes')} -> {transformed_data.get('documenttypes')}")
            
            # STEP 2: Create Patient model from transformed data
            if source == 1:  # YottaDB/qMS
                patient = Patient.from_yottadb_raw(transformed_data)
            elif source == 2:  # Firebird/Infoclinica
                patient = Patient.from_firebird_raw(transformed_data)
            else:
                self.logger.error(f"Unknown source: {source}")
                return None
            
            self.logger.debug(f"Created Patient model for {patient.hisnumber} with final document type {patient.documenttypes}")
            return patient
                
        except Exception as e:
            self.logger.error(f"Error processing patient record: {e}")
            self.logger.error(f"Raw patient data: {raw_patient}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return None
        
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
        self.logger.info(f"Extracting up to {batch_size} patients from {type(self.source_repo).__name__}")
        raw_patients = self.source_repo.get_patients(batch_size=batch_size, last_id=last_id)
        
        if not raw_patients:
            self.logger.warning("No patients found to process")
            return 0
            
        # Transform and Load using Patient model
        self.logger.info(f"Processing {len(raw_patients)} patients using transformer + Patient model")
        success_count = 0
        max_id = None
        
        for raw_patient in raw_patients:
            try:
                # Use the new process_patient_record method (transformer + Patient model)
                patient = self.process_patient_record(raw_patient)
                if not patient:
                    continue
                
                # Track the maximum hisnumber for incremental loading
                hisnumber = patient.hisnumber
                if hisnumber and (max_id is None or hisnumber > max_id):
                    max_id = hisnumber
                
                # Convert to dict for database operations
                patient_dict = patient.to_patientsdet_dict()
                
                if self.target_repo.insert_patient(patient_dict):
                    success_count += 1
                    
            except Exception as e:
                self.logger.error(f"Error processing patient {raw_patient.get('hisnumber')}: {str(e)}")
        
        # Save the last processed ID (only for repositories that support it)
        if max_id is not None and hasattr(self.source_repo, 'save_last_processed_id'):
            self.source_repo.save_last_processed_id(max_id)
        
        self.logger.info(f"Batch complete: {success_count}/{len(raw_patients)} patients processed successfully")
        return success_count
    
    def process_delta(self, batch_size: int = 100) -> int:
        """
        Process delta records (changes) from source to target.
        Only available for repositories that support delta processing.
        
        Args:
            batch_size: Maximum number of records to process
            
        Returns:
            Number of successfully processed records
        """
        # Check if source repository supports delta processing
        if not hasattr(self.source_repo, 'get_patient_deltas'):
            self.logger.warning(f"{type(self.source_repo).__name__} does not support delta processing")
            return 0
        
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
            
            # Remove the operation field before processing
            record_copy = record.copy()
            record_copy.pop('operation', None)
            
            # Use transformer + Patient model
            try:
                patient = self.process_patient_record(record_copy)
                if not patient:
                    continue
                
                patient_dict = patient.to_patientsdet_dict()
                
                if operation == 'INSERT':
                    inserts.append(patient_dict)
                elif operation == 'UPDATE':
                    updates.append(patient_dict)
                elif operation == 'DELETE':
                    deletes.append(patient_dict)
                    
            except Exception as e:
                self.logger.error(f"Error transforming delta record: {str(e)}")
        
        # Process operations
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
        
        # Process deletes
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