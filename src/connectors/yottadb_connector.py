import logging
import re
import csv
import os
from typing import Dict, Any, List, Optional, Tuple

class YottaDBConnector:
    """Connector for YottaDB source system (qMS)."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        self.logger = logging.getLogger(__name__)
        self.source_id = 1  # qMS
        self.csv_file_path = config.get('csv_file_path', 'data/qms_patients.csv')
        self.delimiter = config.get('delimiter', '|')
        
    def connect(self) -> bool:
        """
        Verify CSV file exists and is readable.
        For YottaDB connector, this just checks if the CSV file exists.
        """
        try:
            if not os.path.exists(self.csv_file_path):
                self.logger.error(f"CSV file not found: {self.csv_file_path}")
                return False
                
            # Test if we can open and read the file
            with open(self.csv_file_path, 'r', encoding='utf-8') as f:
                # Try to read the first line
                first_line = f.readline()
                if not first_line:
                    self.logger.error(f"CSV file is empty: {self.csv_file_path}")
                    return False
            
            self.logger.info(f"Successfully connected to CSV file: {self.csv_file_path}")
            return True
        except Exception as e:
            self.logger.error(f"Error connecting to CSV file: {str(e)}")
            return False
            
    def disconnect(self) -> None:
        """
        For CSV files, there's no actual connection to close.
        """
        self.logger.debug("Disconnected from CSV file (no-op)")
            
    def fetch_patients(self, batch_size: int = 100, last_processed_row: int = 0) -> List[Dict[str, Any]]:
        """
        Fetch raw patient records from CSV file without transformation.
        
        Args:
            batch_size: Maximum number of records to fetch
            last_processed_row: Last processed row number to support incremental loading
            
        Returns:
            List of raw patient records
        """
        patients = []
        try:
            with open(self.csv_file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter=self.delimiter)
                
                # Skip header row if it exists
                header = next(reader, None)
                
                # Skip already processed rows
                for _ in range(last_processed_row):
                    next(reader, None)
                
                # Process batch_size records
                row_count = 0
                for row in reader:
                    if row_count >= batch_size:
                        break
                        
                    if len(row) < 10:  # Make sure we have all required fields
                        self.logger.warning(f"Skipping row with insufficient fields: {row}")
                        continue
                    
                    # Extract fields without transformation
                    # Format: hisnumber|lastname|name|surname|birthdate|documenttypes|series|number|email|telephone
                    patient = {
                        "hisnumber": row[0] if len(row) > 0 else "",
                        "lastname": row[1] if len(row) > 1 else "",
                        "name": row[2] if len(row) > 2 else "",
                        "surname": row[3] if len(row) > 3 else "",
                        "birthdate": row[4] if len(row) > 4 else "",
                        "documenttypes": row[5] if len(row) > 5 else "",
                        "series": row[6] if len(row) > 6 else "",
                        "number": row[7] if len(row) > 7 else "",
                        "email": row[8] if len(row) > 8 else "",
                        "telephone": row[9] if len(row) > 9 else "",
                        "source": self.source_id,  # Add source ID
                        "businessunit": 1,  # Default businessunit for qMS
                    }
                    
                    patients.append(patient)
                    row_count += 1
            
            self.logger.info(f"Fetched {len(patients)} raw patients from YottaDB (qMS)")
            return patients
        except Exception as e:
            self.logger.error(f"Error fetching patients from YottaDB (qMS): {str(e)}")
            return []
    
    def get_total_patient_count(self) -> int:
        """
        Get the total number of patients in the CSV file.
        
        Returns:
            Total patient count
        """
        try:
            with open(self.csv_file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter=self.delimiter)
                
                # Skip header row if it exists
                next(reader, None)
                
                # Count rows
                count = sum(1 for _ in reader)
                
            self.logger.info(f"Total patient count in YottaDB (qMS): {count}")
            return count
        except Exception as e:
            self.logger.error(f"Error getting patient count from YottaDB (qMS): {str(e)}")
            return 0
    
    def execute_query(self, query: str, params: tuple = None) -> Tuple[List[Any], List[str]]:
        """
        Mock implementation to maintain API compatibility with other connectors.
        This is a no-op for CSV-based connector.
        
        Returns:
            Empty tuple to indicate no results
        """
        self.logger.warning("execute_query called on YottaDBConnector, which doesn't support direct queries")
        return ([], [])