import logging
import requests
import csv
import io
from typing import Dict, Any, List, Optional, Tuple

class YottaDBConnector:
    """Connector for YottaDB source system (qMS) via HTTP API."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        self.logger = logging.getLogger(__name__)
        self.source_id = 1  # qMS
        self.api_url = config.get('api_url', 'http://192.168.156.43/cgi-bin/qms_export_pat')
        self.timeout = config.get('timeout', 30)
        self.delimiter = config.get('delimiter', '|')
        
    def connect(self) -> bool:
        """
        Test connection to YottaDB API endpoint.
        """
        try:
            self.logger.info(f"Testing connection to YottaDB API: {self.api_url}")
            
            # Test the API endpoint with a small timeout
            response = requests.get(self.api_url, timeout=5)
            
            if response.status_code == 200:
                # Check if we get some data
                content = response.text.strip()
                if content:
                    lines = content.split('\n')
                    self.logger.info(f"Successfully connected to YottaDB API. Got {len(lines)} records.")
                    return True
                else:
                    self.logger.warning("API responded but returned empty content")
                    return False
            else:
                self.logger.error(f"API returned status code: {response.status_code}")
                return False
                
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"Connection error to YottaDB API: {str(e)}")
            return False
        except requests.exceptions.Timeout as e:
            self.logger.error(f"Timeout connecting to YottaDB API: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Error connecting to YottaDB API: {str(e)}")
            return False
            
    def disconnect(self) -> None:
        """
        For HTTP API, there's no persistent connection to close.
        """
        self.logger.debug("Disconnected from YottaDB API (no-op)")
    
    def fetch_all_patients(self) -> List[Dict[str, Any]]:
        """
        Fetch all patient records from YottaDB API.
        
        Returns:
            List of raw patient records
        """
        patients = []
        try:
            self.logger.info(f"Fetching all patients from YottaDB API: {self.api_url}")
            
            response = requests.get(self.api_url, timeout=self.timeout)
            
            if response.status_code != 200:
                self.logger.error(f"API returned status code: {response.status_code}")
                return []
            
            content = response.text.strip()
            if not content:
                self.logger.warning("API returned empty content")
                return []
            
            # Parse the pipe-delimited data
            lines = content.split('\n')
            self.logger.info(f"Processing {len(lines)} lines from API response")
            
            for line_num, line in enumerate(lines, 1):
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    # Split by delimiter
                    fields = line.split(self.delimiter)
                    
                    # Ensure we have at least the minimum required fields
                    if len(fields) < 10:
                        self.logger.warning(f"Line {line_num}: Insufficient fields ({len(fields)}): {line}")
                        continue
                    
                    # Extract fields according to the specification:
                    # 1. hisnumber_qms, 2. lastname, 3. name, 4. surname, 5. birthdate,
                    # 6. document type, 7. document series, 8. document number, 
                    # 9. email_feedback, 10. telephone, 11. login_email (optional)
                    patient = {
                        "hisnumber": fields[0].strip() if len(fields) > 0 else "",
                        "lastname": fields[1].strip() if len(fields) > 1 else "",
                        "name": fields[2].strip() if len(fields) > 2 else "",
                        "surname": fields[3].strip() if len(fields) > 3 else "",
                        "birthdate": fields[4].strip() if len(fields) > 4 else "",
                        "documenttypes": fields[5].strip() if len(fields) > 5 else "",
                        "series": fields[6].strip() if len(fields) > 6 else "",
                        "number": fields[7].strip() if len(fields) > 7 else "",
                        "email": fields[8].strip() if len(fields) > 8 else "",
                        "telephone": fields[9].strip() if len(fields) > 9 else "",
                        "login_email": fields[10].strip() if len(fields) > 10 else "",
                        "source": self.source_id,  # Add source ID
                        "businessunit": 1,  # Default businessunit for qMS
                    }
                    
                    patients.append(patient)
                    
                except Exception as e:
                    self.logger.error(f"Error parsing line {line_num}: {line} - {str(e)}")
                    continue
            
            self.logger.info(f"Successfully fetched {len(patients)} patients from YottaDB API")
            return patients
            
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"Connection error fetching patients from YottaDB API: {str(e)}")
            return []
        except requests.exceptions.Timeout as e:
            self.logger.error(f"Timeout fetching patients from YottaDB API: {str(e)}")
            return []
        except Exception as e:
            self.logger.error(f"Error fetching patients from YottaDB API: {str(e)}")
            return []
    
    def get_total_patient_count(self) -> int:
        """
        Get the total number of patients by calling the API.
        
        Returns:
            Total patient count
        """
        try:
            # For HTTP API, we need to fetch all data to count
            # This could be optimized with a separate count endpoint if available
            patients = self.fetch_all_patients()
            count = len(patients)
            
            self.logger.info(f"Total patient count in YottaDB (qMS): {count}")
            return count
        except Exception as e:
            self.logger.error(f"Error getting patient count from YottaDB: {str(e)}")
            return 0
    
    def execute_query(self, query: str, params: tuple = None) -> Tuple[List[Any], List[str]]:
        """
        Mock implementation to maintain API compatibility with other connectors.
        This is a no-op for HTTP API-based connector.
        
        Returns:
            Empty tuple to indicate no results
        """
        self.logger.warning("execute_query called on YottaDBConnector, which doesn't support direct queries")
        return ([], [])