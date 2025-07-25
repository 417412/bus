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
    
    def fetch_all_patients(self) -> List[str]:
        """
        Fetch all patient records from YottaDB API.
        This operation takes 2-3 minutes.
        
        Returns:
            List of raw response lines for the repository to parse
        """
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
                
                # Return raw lines for the repository to parse
                lines = content.split('\n')
                self.logger.info(f"Successfully fetched {len(lines)} lines from YottaDB API")
                return lines
                
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
            lines = self.fetch_all_patients()
            if not lines:
                return 0
            
            # Count non-empty lines as a rough estimate
            count = len([line for line in lines if line.strip()])
            
            self.logger.info(f"Total patient lines in YottaDB (qMS): {count}")
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