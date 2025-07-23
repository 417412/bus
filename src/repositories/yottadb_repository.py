import logging
import os
from typing import List, Dict, Any, Optional, Tuple
from src.connectors.yottadb_connector import YottaDBConnector
from datetime import datetime
from src.config.settings import setup_logger, STATE_DIR

class YottaDBRepository:
    """Repository for accessing YottaDB data via HTTP API."""
    
    def __init__(self, connector: YottaDBConnector):
        self.connector = connector
        self.logger = setup_logger(__name__, "repositories")
        self.source_id = 1  # qMS
        self.state_dir = str(STATE_DIR)
        
        # Ensure state directory exists
        os.makedirs(self.state_dir, exist_ok=True)
        
        # Cache for storing all patients data
        self._all_patients_cache = None
        self._cache_timestamp = None
        self._cache_duration = 300  # 5 minutes cache
        
    def _get_all_patients_cached(self) -> List[Dict[str, Any]]:
        """
        Get all patients with caching to avoid repeated API calls.
        
        Returns:
            List of all patient records
        """
        current_time = datetime.now()
        
        # Check if cache is valid
        if (self._all_patients_cache is not None and 
            self._cache_timestamp is not None and
            (current_time - self._cache_timestamp).total_seconds() < self._cache_duration):
            
            self.logger.debug("Using cached patient data")
            return self._all_patients_cache
        
        # Cache is invalid or doesn't exist, fetch new data
        self.logger.debug("Fetching fresh patient data from API")
        self._all_patients_cache = self.connector.fetch_all_patients()
        self._cache_timestamp = current_time
        
        return self._all_patients_cache
    
    def get_patients(self, batch_size: int = 100, last_id: str = None) -> List[Dict[str, Any]]:
        """
        Fetch patient data from YottaDB API with batch processing simulation.
        
        Args:
            batch_size: Maximum number of patients to fetch
            last_id: ID of the last processed patient
            
        Returns:
            List of dictionaries with patient data
        """
        try:
            # Get all patients from API (cached)
            all_patients = self._get_all_patients_cached()
            
            if not all_patients:
                return []
            
            # Sort patients by hisnumber for consistent ordering
            all_patients.sort(key=lambda x: x.get('hisnumber', ''))
            
            # Find starting position based on last_id
            start_index = 0
            if last_id:
                for i, patient in enumerate(all_patients):
                    if str(patient.get('hisnumber', '')) > str(last_id):
                        start_index = i
                        break
                else:
                    # last_id is greater than all existing IDs
                    return []
            
            # Get the batch
            end_index = start_index + batch_size
            batch = all_patients[start_index:end_index]
            
            self.logger.debug(f"Retrieved batch of {len(batch)} patients from YottaDB (starting from index {start_index})")
            return batch
            
        except Exception as e:
            self.logger.error(f"Error fetching patients from YottaDB: {str(e)}")
            return []
    
    def get_patient_deltas(self, batch_size: int = 100) -> Tuple[List[Dict[str, Any]], int]:
        """
        For YottaDB HTTP API, we don't have delta functionality.
        This method returns empty results since we only do full loads.
        
        Args:
            batch_size: Maximum number of deltas to fetch (ignored)
            
        Returns:
            Tuple of (empty list, 0) since deltas are not supported
        """
        self.logger.info("Delta sync not supported for YottaDB HTTP API, returning empty results")
        return [], 0
    
    def get_last_processed_id(self) -> Optional[str]:
        """Get last processed patient ID from state file."""
        try:
            file_path = os.path.join(self.state_dir, "yottadb_last_id.txt")
            if not os.path.exists(file_path):
                return None
                
            with open(file_path, "r") as f:
                last_id = f.read().strip()
                return last_id if last_id else None
        except Exception as e:
            self.logger.error(f"Error reading last processed ID: {str(e)}")
            return None
            
    def save_last_processed_id(self, last_id: str) -> None:
        """Save last processed patient ID to state file."""
        try:
            file_path = os.path.join(self.state_dir, "yottadb_last_id.txt")
            with open(file_path, "w") as f:
                f.write(str(last_id))
            self.logger.debug(f"Saved last processed ID: {last_id}")
        except Exception as e:
            self.logger.error(f"Error saving last processed ID: {str(e)}")
    
    def save_last_sync_time(self) -> None:
        """Save the timestamp of the last successful sync."""
        try:
            file_path = os.path.join(self.state_dir, "yottadb_last_sync.txt")
            timestamp = datetime.now().isoformat()
            with open(file_path, "w") as f:
                f.write(timestamp)
            self.logger.debug(f"Saved last sync time: {timestamp}")
        except Exception as e:
            self.logger.error(f"Error saving last sync time: {str(e)}")
    
    def get_last_sync_time(self) -> Optional[datetime]:
        """Get the timestamp of the last successful sync."""
        try:
            file_path = os.path.join(self.state_dir, "yottadb_last_sync.txt")
            if not os.path.exists(file_path):
                return None
                
            with open(file_path, "r") as f:
                timestamp_str = f.read().strip()
                if timestamp_str:
                    return datetime.fromisoformat(timestamp_str)
                return None
        except Exception as e:
            self.logger.error(f"Error reading last sync time: {str(e)}")
            return None
        
    def get_total_patient_count(self, include_last_id: bool = False) -> int:
        """
        Get the total count of patients in the source system.
        
        Args:
            include_last_id: If True, returns the count and last ID as a tuple
        
        Returns:
            Total patient count or (count, last_id) tuple if include_last_id is True
        """
        try:
            # Get all patients to count them and find the last ID
            all_patients = self._get_all_patients_cached()
            total_count = len(all_patients)
            
            if total_count == 0:
                if include_last_id:
                    return 0, None
                return 0
            
            # Find the maximum hisnumber as last_id
            last_id = None
            if all_patients:
                # Sort by hisnumber to find the maximum
                sorted_patients = sorted(all_patients, key=lambda x: str(x.get('hisnumber', '')))
                last_id = sorted_patients[-1].get('hisnumber')
            
            self.logger.info(f"Total patient count in YottaDB: {total_count}, last ID: {last_id}")
            
            if include_last_id:
                return total_count, last_id
            return total_count
            
        except Exception as e:
            self.logger.error(f"Error getting patient count from YottaDB: {e}")
            return 0 if not include_last_id else (0, None)