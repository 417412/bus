import logging
import socket
import sys
from typing import Tuple, List, Dict, Any, Optional
from firebird.driver import connect, driver_config
from src.config.settings import setup_logger, get_decrypted_database_config

class FirebirdConnector:
    """Basic connector for Firebird providing connection management functionality."""
    
    # Class-level flag to track if server was already registered
    _server_registered = False
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize Firebird connector.
        
        Args:
            config: Database configuration dictionary. If None, will use decrypted config from settings.
        """
        # Use decrypted config if no config provided
        if config is None:
            config = get_decrypted_database_config()["Firebird"]
        
        self.config = config
        self.connection = None
        self.logger = setup_logger(__name__, "connectors")
        self.server_name = "infoclinica_server"
        self.db_name = "infoclinica_db"
        
        # Log connection details (without password)
        safe_config = {k: v if k.lower() != 'password' else '********' for k, v in self.config.items()}
        self.logger.debug(f"Initializing Firebird connector with config: {safe_config}")
        
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
            
            # Get decrypted password for connection
            password = self.config.get('password', 'masterkey')
            user = self.config.get('user', 'SYSDBA')
            
            # Only register server and database if not already done
            if not FirebirdConnector._server_registered:
                try:
                    # Register Firebird server configuration
                    server_cfg = f"""[{self.server_name}]
host = {host}
port = {port}
user = {user}
password = {password}
"""
                    # Log config without password
                    safe_server_cfg = server_cfg.replace(f"password = {password}", "password = ********")
                    self.logger.debug(f"Server configuration: {safe_server_cfg}")
                    
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
            self.logger.info(f"Attempting to connect to Firebird at {host}:{port}, "
                           f"database: {self.config.get('database', '')}, user: {user}")
            self.connection = connect(self.db_name)
            
            self.logger.info("Connected to Firebird using firebird-driver")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to Firebird: {str(e)}")
            # Don't log the actual password in error messages
            if 'password' in str(e).lower():
                self.logger.error("Connection failed - check username, password, and database path")
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
        """
        if not self.connection:
            raise Exception("Not connected to database")
        
        # For SELECT queries that might need fresh data (like delta queries),
        # commit any pending transaction to start fresh
        if query.strip().upper().startswith('SELECT') and 'delta' in query.lower():
            try:
                self.connection.commit()
                self.logger.debug("Committed transaction before delta query to ensure fresh read")
            except Exception as e:
                self.logger.debug(f"Transaction commit before delta query failed (may be normal): {e}")
            
        with self.connection.cursor() as cursor:
            # Execute the query
            cursor.execute(query, params or ())
            
            # Determine if this is a SELECT query by checking if cursor.description exists
            if cursor.description:
                rows = cursor.fetchall()
                column_names = [desc[0].lower() for desc in cursor.description]
                
                # Debug logging
                self.logger.debug(f"Query executed: {query[:100]}...")
                self.logger.debug(f"Rows returned: {len(rows) if rows else 0}")
                self.logger.debug(f"Column names: {column_names}")
                self.logger.debug(f"Type of rows: {type(rows)}")
                
                return rows, column_names
            else:
                # For non-SELECT queries, return None for both rows and column_names
                self.connection.commit()
                return None, None
    
    def test_connection(self) -> bool:
        """
        Test the database connection.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            if self.connect():
                # Try a simple query
                rows, columns = self.execute_query("SELECT 1 FROM RDB$DATABASE")
                self.disconnect()
                return True
            return False
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False