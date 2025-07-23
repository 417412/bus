import logging
import requests
import csv
import io
import time
import subprocess
import sys
import socket
from urllib.parse import urlparse
from typing import Dict, Any, List, Optional, Tuple
from src.config.settings import setup_logger, get_decrypted_database_config

class YottaDBConnector:
    """Connector for YottaDB API operations."""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize YottaDB connector.
        
        Args:
            config: API configuration dictionary. If None, will use decrypted config from settings.
        """
        # Use decrypted config if no config provided  
        if config is None:
            config = get_decrypted_database_config()["YottaDB"]
        
        self.config = config
        self.connection = None
        self.logger = setup_logger(__name__, "connectors")
        self.source_id = 1  # qMS
        self.api_url = config.get('api_url', 'http://192.168.156.43/cgi-bin/qms_export_pat')
        self.timeout = config.get('timeout', 300)  # 5 minutes default timeout
        self.connect_timeout = config.get('connect_timeout', 300)  # Connection timeout
        self.delimiter = config.get('delimiter', '|')
        self.max_retries = config.get('max_retries', 2)  # Fewer retries for long operations
        
    def _extract_host_from_url(self, url: str) -> Optional[str]:
        """Extract hostname/IP from URL."""
        try:
            parsed = urlparse(url)
            return parsed.hostname
        except Exception as e:
            self.logger.error(f"Error parsing URL {url}: {e}")
            return None
    
    def _ping_host(self, host: str) -> bool:
        """
        Ping the host to check basic network connectivity.
        
        Args:
            host: Hostname or IP address to ping
            
        Returns:
            True if ping successful, False otherwise
        """
        try:
            # Determine ping command based on OS
            if sys.platform.lower().startswith('win'):
                # Windows
                cmd = ['ping', '-n', '1', '-w', '5000', host]  # 5 second timeout
            else:
                # Linux/Unix
                cmd = ['ping', '-c', '1', '-W', '5', host]  # 5 second timeout
            
            self.logger.info(f"Pinging {host}...")
            
            # Execute ping command
            result = subprocess.run(
                cmd, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE, 
                timeout=10  # Overall timeout for the ping command
            )
            
            if result.returncode == 0:
                self.logger.info(f"Ping to {host} successful")
                return True
            else:
                self.logger.warning(f"Ping to {host} failed (return code: {result.returncode})")
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.warning(f"Ping to {host} timed out")
            return False
        except FileNotFoundError:
            self.logger.warning("Ping command not found, falling back to socket test")
            return self._socket_connect_test(host)
        except Exception as e:
            self.logger.warning(f"Ping failed with error: {e}, falling back to socket test")
            return self._socket_connect_test(host)
    
    def _socket_connect_test(self, host: str, port: int = 80) -> bool:
        """
        Test TCP connectivity to host:port using socket.
        
        Args:
            host: Hostname or IP address
            port: Port number (default 80 for HTTP)
            
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.logger.info(f"Testing TCP connection to {host}:{port}...")
            
            # Create socket and test connection
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)  # 5 second timeout
            
            result = sock.connect_ex((host, port))
            sock.close()
            
            if result == 0:
                self.logger.info(f"TCP connection to {host}:{port} successful")
                return True
            else:
                self.logger.warning(f"TCP connection to {host}:{port} failed (error code: {result})")
                return False
                
        except socket.gaierror as e:
            self.logger.error(f"DNS resolution failed for {host}: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Socket connection test failed: {e}")
            return False
        
    def connect(self) -> bool:
        """
        Test connection to YottaDB API endpoint using ping/socket test.
        This is a lightweight test that doesn't call the actual API.
        """
        try:
            self.logger.info(f"Testing connectivity to YottaDB API: {self.api_url}")
            self.logger.info(f"Configured timeouts: connect={self.connect_timeout}s, read={self.timeout}s")
            
            # Extract hostname from URL
            host = self._extract_host_from_url(self.api_url)
            if not host:
                self.logger.error("Could not extract hostname from API URL")
                return False
            
            self.logger.info(f"Extracted host: {host}")
            
            # First try ping
            if self._ping_host(host):
                self.logger.info("Network connectivity test passed (ping successful)")
                return True
            
            # If ping fails, try socket connection test
            self.logger.info("Ping failed, trying TCP connection test...")
            
            # Extract port from URL, default to 80
            try:
                parsed = urlparse(self.api_url)
                port = parsed.port if parsed.port else (443 if parsed.scheme == 'https' else 80)
            except:
                port = 80
            
            if self._socket_connect_test(host, port):
                self.logger.info("Network connectivity test passed (TCP connection successful)")
                return True
            
            # Both tests failed
            self.logger.error(f"Network connectivity test failed - {host} is not reachable")
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to test YottaDB API connectivity: {str(e)}")
            return False
            
    def disconnect(self) -> None:
        """
        For HTTP API, there's no persistent connection to close.
        """
        self.logger.debug("Disconnected from YottaDB API (no-op)")
    
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
        
        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if not line:
                continue
            
            # Split by delimiter
            fields = line.split(self.delimiter)
            
            # Check if this looks like the start of a new record (has patient ID pattern)
            # Patient IDs typically start with numbers followed by /
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
    
    def _build_patient_record(self, fields: List[str]) -> Optional[Dict[str, Any]]:
        """
        Build a patient record from field list.
        
        Args:
            fields: List of field values
            
        Returns:
            Patient record dict or None if invalid
        """
        try:
            # Ensure we have enough fields
            if len(fields) < 10:
                return None
            
            # Keep the full hisnumber as provided (e.g., "41449/A22")
            hisnumber_field = fields[0].strip()
            
            # Basic validation - ensure we have some meaningful ID
            if not hisnumber_field:
                return None
            
            # Build patient record with full hisnumber
            patient = {
                "hisnumber": hisnumber_field,  # Keep full format like "41449/A22"
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
            
            return patient
            
        except Exception as e:
            self.logger.debug(f"Error building patient record from fields {fields[:3]}...: {e}")
            return None
    
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
                
                # Use the new parser that handles multi-line records
                patients = self._parse_patient_data(lines)
                
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