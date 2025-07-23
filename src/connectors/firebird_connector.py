import logging
import socket
import sys
from typing import Tuple, List, Dict, Any, Optional
from firebird.driver import connect, driver_config
from src.config.settings import setup_logger

class FirebirdConnector:
    """Basic connector for Firebird providing connection management functionality."""
    
    # Class-level flag to track if server was already registered
    _server_registered = False
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        self.logger = setup_logger(__name__, "connectors")
        self.server_name = "infoclinica_server"
        self.db_name = "infoclinica_db"
        
    def connect(self) -> bool:
        """Establish connection to Firebird database."""
        try:
            # Parse host and port from the config
            host_string = self.config.get('host', 'localhost')
            if ':' in host_string:
                host, port = host_string.split(':')
            else:
                host = host_string
                port = "3050"  # Default Firebird port
                
            # Try to diagnose connectivity before proceeding
            self._diagnose_connectivity(host, int(port))
            
            # Only register server and database if not already done
            if not FirebirdConnector._server_registered:
                try:
                    # Register Firebird server configuration
                    server_cfg = f"""[{self.server_name}]
host = {host}
port = {port}
user = {self.config.get('user', 'SYSDBA')}
password = {self.config.get('password', 'masterkey')}
"""
                    self.logger.debug(f"Server configuration: {server_cfg}")
                    driver_config.register_server(self.server_name, server_cfg)
                    
                    # Register database
                    db_path = self.config.get('database', '')
                        
                    db_cfg = f"""[{self.db_name}]
server = {self.server_name}
database = {db_path}
protocol = inet
charset = {self.config.get('charset', 'cp1251')}
"""
                    self.logger.debug(f"Database configuration: {db_cfg}")
                    driver_config.register_database(self.db_name, db_cfg)
                    
                    # Set class-level flag to indicate registration was successful
                    FirebirdConnector._server_registered = True
                    self.logger.info("Registered Firebird server and database configurations")
                    
                except Exception as reg_error:
                    # If error contains "already registered", we can proceed
                    if "already registered" in str(reg_error):
                        self.logger.info("Server already registered, using existing configuration")
                        FirebirdConnector._server_registered = True
                    else:
                        # It's a different error, re-raise it
                        raise reg_error
            else:
                self.logger.info("Using previously registered Firebird configuration")
            
            # Establish connection
            self.logger.info(f"Attempting to connect to Firebird at {host}:{port}, database: {self.config.get('database', '')}")
            self.connection = connect(self.db_name)
            
            self.logger.info("Connected to Firebird using firebird-driver")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to Firebird: {str(e)}")
            return False
    
    def _diagnose_connectivity(self, host: str, port: int) -> None:
        """Diagnose network connectivity to the Firebird server."""
        try:
            # Try to establish a TCP connection to diagnose network issues
            self.logger.info(f"Testing TCP connectivity to {host}:{port}")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)  # 5 second timeout
            result = s.connect_ex((host, port))
            s.close()
            
            if result == 0:
                self.logger.info(f"TCP connection to {host}:{port} successful")
            else:
                self.logger.warning(f"TCP connection to {host}:{port} failed with error code {result}")
                
                # Try ping to diagnose basic connectivity
                import subprocess
                try:
                    ping_param = "-n" if sys.platform.lower() == "win32" else "-c"
                    ping_cmd = ["ping", ping_param, "1", host]
                    self.logger.info(f"Attempting to ping {host}")
                    ping_output = subprocess.check_output(ping_cmd).decode()
                    self.logger.info(f"Ping result: {ping_output.splitlines()[-1]}")
                except Exception as ping_error:
                    self.logger.warning(f"Ping failed: {str(ping_error)}")
        except Exception as e:
            self.logger.warning(f"Connectivity diagnosis failed: {str(e)}")
            
    def disconnect(self) -> None:
        """Close connection to the database."""
        if self.connection:
            try:
                self.connection.close()
                self.logger.info("Disconnected from Firebird")
            except Exception as e:
                self.logger.error(f"Error disconnecting from Firebird: {str(e)}")
                
    def execute_query(self, query: str, params: tuple = None) -> Tuple[List[Any], List[str]]:
        """
        Execute a query and return the results with column names.
        
        Args:
            query: SQL query to execute
            params: Parameters for the query
            
        Returns:
            For SELECT queries: Tuple of (rows, column_names)
            For non-SELECT queries: Tuple of (None, None)
        """
        if not self.connection:
            raise Exception("Not connected to database")
            
        with self.connection.cursor() as cursor:
            # Execute the query
            cursor.execute(query, params or ())
            
            # Determine if this is a SELECT query by checking if cursor.description exists
            # For non-SELECT queries (INSERT, UPDATE, DELETE), description will be None
            if cursor.description:
                rows = cursor.fetchall()
                column_names = [desc[0].lower() for desc in cursor.description]
                return rows, column_names
            else:
                # For non-SELECT queries, return None for both rows and column_names
                # This way we can tell it's a non-SELECT query by checking if rows is None
                self.connection.commit()
                return None, None