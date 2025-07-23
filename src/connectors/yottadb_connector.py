import logging
import requests
import csv
import io
import time
from typing import Dict, Any, List, Optional, Tuple
from src.config.settings import setup_logger

class YottaDBConnector:
    """Connector for YottaDB source system (qMS) via HTTP API."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        self.logger = setup_logger(__name__, "connectors")
        self.source_id = 1  # qMS
        self.api_url = config.get('api_url', 'http://192.168.156.43/cgi-bin/qms_export_pat')
        self.timeout = config.get('timeout', 300)  # 5 minutes default timeout
        self.connect_timeout = config.get('connect_timeout', 300)  # Connection timeout
        self.delimiter = config.get('delimiter', '|')
        self.max_retries = config.get('max_retries', 2)  # Fewer retries for long operations
        
    def connect(self) -> bool:
        """
        Test connection to YottaDB API endpoint.
        Since the API takes 2-3 minutes, we'll do a lightweight test first,
        then a full test if needed.
        """
        try:
            self.logger.info(f"Testing connection to YottaDB API: {self.api_url}")
            self.logger.info(f"Note: YottaDB API typically takes 2-3 minutes to respond")
            self.logger.info(f"Using timeouts: connect={self.connect_timeout}s, read={self.timeout}s")
            
            # First try a quick connectivity test with shorter timeout
            try:
                self.logger.info("Performing quick connectivity test...")
                response = requests.get(
                    self.api_url, 
                    timeout=(self.connect_timeout, 30),  # 30 second read timeout for quick test
                    headers={'User-Agent': 'Medical-ETL/1.0'}
                )
                # If we get a response (even if it times out on read), connection works
                self.logger.info("Quick connectivity test passed")
                return True
                
            except requests.exceptions.ReadTimeout:
                # This is expected for the quick test - it means connection works
                self.logger.info("Quick test timed out (expected) - connection is working")
                return True
                
            except requests.exceptions.ConnectTimeout as e:
                self.logger.error(f"Connection timeout to YottaDB API: {str(e)}")
                # Try one more time with full timeout settings
                self.logger.info("Retrying with full timeout settings...")
                try:
                    response = requests.get(
                        self.api_url,
                        timeout=(self.connect_timeout, self.timeout),
                        headers={'User-Agent': 'Medical-ETL/1.0'}
                    )
                    self.logger.info("Full timeout test successful")
                    return True
                except requests.exceptions.ConnectTimeout:
                    self.logger.error("Connection timeout on retry - API is not reachable")
                    return False
                except requests.exceptions.ReadTimeout:
                    self.logger.info("Full timeout test - read timeout but connection works")
                    return True
                except Exception as retry_e:
                    self.logger.error(f"Retry failed: {str(retry_e)}")
                    return False
                    
            except requests.exceptions.ConnectionError as e:
                self.logger.error(f"Connection error to YottaDB API: {str(e)}")
                return False
                
            except Exception as e:
                self.logger.error(f"Error testing YottaDB API connection: {str(e)}")
                # If we got a response (even error), connection might work
                if hasattr(e, 'response') and e.response is not None:
                    self.logger.info("Got response despite error - connection appears to work")
                    return True
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to test YottaDB API connection: {str(e)}")
            return False
            
    def disconnect(self) -> None:
        """
        For HTTP API, there's no persistent connection to close.
        """
        self.logger.debug("Disconnected from YottaDB API (no-op)")
    
    def fetch_all_patients(self) -> List[Dict[str, Any]]:
        """
        Fetch all patient records from YottaDB API.
        This operation takes 2-3 minutes.
        
        Returns:
            List of raw patient records
        """
        patients = []
        
        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Fetching all patients from YottaDB API (attempt {attempt + 1}/{self.max_retries})")
                self.logger.info(f"This operation typically takes 2-3 minutes, please wait...")
                self.logger.info(f"Using full timeout settings: connect={self.connect_timeout}s, read={self.timeout}s")
                
                start_time = time.time()
                
                response = requests.get(
                    self.api_url, 
                    timeout=(self.connect_timeout, self.timeout),  # Use full configured timeouts
                    headers={'User-Agent': 'Medical-ETL/1.0'}
                )
                
                end_time = time.time()
                duration = end_time - start_time
                self.logger.info(f"API call completed in {duration:.1f} seconds")
                
                if response.status_code != 200:
                    self.logger.error(f"API returned status code: {response.status_code}")
                    if attempt < self.max_retries - 1:
                        self.logger.info(f"Retrying in 30 seconds...")
                        time.sleep(30)
                        continue
                    return []
                
                content = response.text.strip()
                if not content:
                    self.logger.warning("API returned empty content")
                    if attempt < self.max_retries - 1:
                        self.logger.info(f"Retrying in 30 seconds...")
                        time.sleep(30)
                        continue
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
                        
                        # Extract fields according to the specification
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
                
            except requests.exceptions.ConnectTimeout as e:
                self.logger.error(f"Connection timeout to YottaDB API (attempt {attempt + 1}): {str(e)}")
                if attempt < self.max_retries - 1:
                    self.logger.info(f"Retrying in 60 seconds...")
                    time.sleep(60)
                    continue
            except requests.exceptions.ReadTimeout as e:
                self.logger.error(f"Read timeout from YottaDB API (attempt {attempt + 1}): {str(e)}")
                self.logger.error(f"API call exceeded {self.timeout} seconds. Current timeout setting: {self.timeout}s")
                if attempt < self.max_retries - 1:
                    self.logger.info(f"Retrying in 60 seconds...")
                    time.sleep(60)
                    continue
            except requests.exceptions.ConnectionError as e:
                self.logger.error(f"Connection error to YottaDB API (attempt {attempt + 1}): {str(e)}")
                if attempt < self.max_retries - 1:
                    self.logger.info(f"Retrying in 60 seconds...")
                    time.sleep(60)
                    continue
            except Exception as e:
                self.logger.error(f"Error fetching patients from YottaDB API (attempt {attempt + 1}): {str(e)}")
                if attempt < self.max_retries - 1:
                    self.logger.info(f"Retrying in 60 seconds...")
                    time.sleep(60)
                    continue
        
        self.logger.error(f"Failed to fetch patients from YottaDB API after {self.max_retries} attempts")
        return []
    
    def get_total_patient_count(self) -> int:
        """
        Get the total number of patients by calling the API.
        
        Returns:
            Total patient count
        """
        try:
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
        """
        self.logger.warning("execute_query called on YottaDBConnector, which doesn't support direct queries")
        return ([], [])