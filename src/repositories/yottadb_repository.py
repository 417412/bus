import logging
import os
from typing import List, Dict, Any, Optional, Tuple, Set
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
    
    def _build_patient_record(self, fields: List[str]) -> Optional[Dict[str, Any]]:
        """
        Build a patient record from field list.
        
        Expected format: hisnumber|lastname|name|surname|birthdate|documenttypes|series|number|email|telephone|login_email
        
        Args:
            fields: List of field values
            
        Returns:
            Patient record dict or None if invalid
        """
        try:
            # Ensure we have enough fields
            if len(fields) < 10:
                self.logger.debug(f"Insufficient fields: {len(fields)} < 10")
                return None
            
            # Keep the full hisnumber as provided (e.g., "41449/A22")
            hisnumber_field = fields[0].strip()
            
            # Basic validation - ensure we have some meaningful ID
            if not hisnumber_field:
                self.logger.debug("Empty hisnumber field")
                return None
            
            # Build patient record with both emails separated
            patient = {
                "hisnumber": hisnumber_field,  # Keep full format like "41449/A22"
                "lastname": fields[1].strip() if len(fields) > 1 else "",
                "name": fields[2].strip() if len(fields) > 2 else "",
                "surname": fields[3].strip() if len(fields) > 3 else "",
                "birthdate": fields[4].strip() if len(fields) > 4 else "",
                "documenttypes": fields[5].strip() if len(fields) > 5 else "",
                "series": fields[6].strip() if len(fields) > 6 else "",
                "number": fields[7].strip() if len(fields) > 7 else "",
                "email": fields[8].strip() if len(fields) > 8 else "",        # Contact email (first)
                "telephone": fields[9].strip() if len(fields) > 9 else "",
                "login_email": fields[10].strip() if len(fields) > 10 else "",  # Login email (second)
                "source": self.source_id,  # Add source ID
                "businessunit": 1,  # Default businessunit for qMS
            }
            
            return patient
            
        except Exception as e:
            self.logger.debug(f"Error building patient record from fields {fields[:3]}...: {e}")
            return None
    
    def _parse_patient_data(self, lines: List[str]) -> List[Dict[str, Any]]:
        """
        Parse patient data from API response lines.
        Handles multi-line records and data validation.
        
        Args:
            lines: List of lines from API response
            
        Returns:
            List of parsed patient records
        """
        patients = []
        current_record = []
        skipped_lines = 0
        delimiter = self.connector.delimiter
        
        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if not line:
                continue
            
            # Split by delimiter
            fields = line.split(delimiter)
            
            # Check if this looks like the start of a new record (has patient ID pattern)
            is_new_record = False
            if fields and len(fields) > 0:
                first_field = fields[0].strip()
                if '/' in first_field and first_field.split('/')[0].isdigit():
                    is_new_record = True
            
            if is_new_record and current_record:
                # Process the previous record
                patient = self._build_patient_record(current_record)
                if patient:
                    patients.append(patient)
                current_record = []
            
            # Add current line fields to the record
            current_record.extend(fields)
            
            # If we have enough fields for a complete record, process it
            if len(current_record) >= 10:
                patient = self._build_patient_record(current_record)
                if patient:
                    patients.append(patient)
                current_record = []
        
        # Process any remaining record
        if current_record:
            patient = self._build_patient_record(current_record)
            if patient:
                patients.append(patient)
        
        if skipped_lines > 0:
            self.logger.info(f"Skipped {skipped_lines} lines due to insufficient data")
        
        return patients
        
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
        
        # Get raw response from connector
        all_patients_raw = self.connector.fetch_all_patients()
        
        # If connector already parsed the data, use it directly
        if all_patients_raw and isinstance(all_patients_raw[0], dict):
            self._all_patients_cache = all_patients_raw
        else:
            # If we get raw lines, parse them here
            self._all_patients_cache = self._parse_patient_data(all_patients_raw)
        
        self._cache_timestamp = current_time
        
        return self._all_patients_cache
    
    def get_all_patients_raw(self) -> List[Dict[str, Any]]:
        """
        Get ALL patients from the API without any filtering.
        This is used for debugging and admin purposes.
        
        Returns:
            List of all patient records (unfiltered)
        """
        return self._get_all_patients_cached()
    
    def get_processed_hisnumbers(self) -> Set[str]:
        """
        Get set of all hisnumbers that have been processed from state file.
        
        Returns:
            Set of processed hisnumbers
        """
        try:
            file_path = os.path.join(self.state_dir, "yottadb_processed_hisnumbers.txt")
            if not os.path.exists(file_path):
                return set()
                
            with open(file_path, "r") as f:
                hisnumbers = set()
                for line in f:
                    line = line.strip()
                    if line:
                        hisnumbers.add(line)
                return hisnumbers
        except Exception as e:
            self.logger.error(f"Error reading processed hisnumbers: {str(e)}")
            return set()
    
    def save_processed_hisnumbers(self, hisnumbers: Set[str]) -> None:
        """
        Save set of processed hisnumbers to state file.
        
        Args:
            hisnumbers: Set of hisnumbers to save
        """
        try:
            file_path = os.path.join(self.state_dir, "yottadb_processed_hisnumbers.txt")
            with open(file_path, "w") as f:
                for hisnumber in sorted(hisnumbers):
                    f.write(f"{hisnumber}\n")
            self.logger.debug(f"Saved {len(hisnumbers)} processed hisnumbers")
        except Exception as e:
            self.logger.error(f"Error saving processed hisnumbers: {str(e)}")
    
    def add_processed_hisnumber(self, hisnumber: str) -> None:
        """
        Add a single hisnumber to the processed set.
        
        Args:
            hisnumber: HIS number to mark as processed
        """
        try:
            processed = self.get_processed_hisnumbers()
            processed.add(hisnumber)
            self.save_processed_hisnumbers(processed)
        except Exception as e:
            self.logger.error(f"Error adding processed hisnumber {hisnumber}: {str(e)}")
    
    def get_patients(self, batch_size: int = 100, last_id: str = None) -> List[Dict[str, Any]]:
        """
        Fetch patient data from YottaDB API, returning only unprocessed patients.
        
        Args:
            batch_size: Maximum number of patients to fetch
            last_id: Ignored for this implementation
            
        Returns:
            List of dictionaries with unprocessed patient data
        """
        try:
            # Get all patients from API (cached)
            all_patients = self._get_all_patients_cached()
            
            if not all_patients:
                return []
            
            # Get set of already processed hisnumbers
            processed_hisnumbers = self.get_processed_hisnumbers()
            
            # Filter out already processed patients
            unprocessed_patients = [
                patient for patient in all_patients 
                if patient.get('hisnumber') not in processed_hisnumbers
            ]
            
            # Sort unprocessed patients by hisnumber for consistent ordering
            unprocessed_patients.sort(key=lambda x: x.get('hisnumber', ''))
            
            # Get the batch
            batch = unprocessed_patients[:batch_size]
            
            total_patients = len(all_patients)
            processed_count = len(processed_hisnumbers)
            unprocessed_count = len(unprocessed_patients)
            
            self.logger.info(f"YottaDB status: {total_patients} total, {processed_count} processed, {unprocessed_count} unprocessed")
            self.logger.debug(f"Retrieved batch of {len(batch)} unprocessed patients from YottaDB")
            
            return batch
            
        except Exception as e:
            self.logger.error(f"Error fetching patients from YottaDB: {str(e)}")
            return []
    
    def get_patient_deltas(self, batch_size: int = 100) -> Tuple[List[Dict[str, Any]], int]:
        """
        Get patients that need to be updated (new or changed).
        For YottaDB, this means comparing current API data with processed hisnumbers.
        
        Args:
            batch_size: Maximum number of deltas to fetch
            
        Returns:
            Tuple of (patient list, total_unprocessed_count)
        """
        try:
            # Get unprocessed patients
            unprocessed_patients = self.get_patients(batch_size)
            
            # Get total count of unprocessed
            all_patients = self._get_all_patients_cached()
            processed_hisnumbers = self.get_processed_hisnumbers()
            total_unprocessed = len([p for p in all_patients if p.get('hisnumber') not in processed_hisnumbers])
            
            return unprocessed_patients, total_unprocessed
            
        except Exception as e:
            self.logger.error(f"Error getting patient deltas from YottaDB: {str(e)}")
            return [], 0
    
    def get_last_processed_id(self) -> Optional[str]:
        """Get last processed patient ID from state file (legacy support)."""
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
        """
        Save last processed patient ID to state file and add to processed set.
        
        Args:
            last_id: Last processed hisnumber
        """
        try:
            # Save to legacy file for compatibility
            file_path = os.path.join(self.state_dir, "yottadb_last_id.txt")
            with open(file_path, "w") as f:
                f.write(str(last_id))
            
            # Add to processed hisnumbers set
            self.add_processed_hisnumber(last_id)
            
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
            
            # Also get unprocessed count for better reporting
            processed_hisnumbers = self.get_processed_hisnumbers()
            unprocessed_count = len([p for p in all_patients if p.get('hisnumber') not in processed_hisnumbers])
            
            self.logger.info(f"YottaDB patient counts - Total: {total_count}, Processed: {len(processed_hisnumbers)}, Unprocessed: {unprocessed_count}")
            
            if include_last_id:
                return total_count, last_id
            return total_count
            
        except Exception as e:
            self.logger.error(f"Error getting patient count from YottaDB: {e}")
            return 0 if not include_last_id else (0, None)
    
    def get_source_id(self) -> int:
        """Get the source ID for this repository."""
        return self.source_id
    
    def reset_processed_state(self) -> None:
        """
        Reset the processed state (clear all processed hisnumbers).
        Useful for full re-sync.
        """
        try:
            processed_file = os.path.join(self.state_dir, "yottadb_processed_hisnumbers.txt")
            if os.path.exists(processed_file):
                os.remove(processed_file)
            
            last_id_file = os.path.join(self.state_dir, "yottadb_last_id.txt")
            if os.path.exists(last_id_file):
                os.remove(last_id_file)
                
            self.logger.info("Reset YottaDB processed state - will re-process all patients")
        except Exception as e:
            self.logger.error(f"Error resetting processed state: {str(e)}")