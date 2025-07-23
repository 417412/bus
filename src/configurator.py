#!/usr/bin/env python3
"""
Configuration utility for the medical system ETL application.
Allows updating settings.py with new configuration values.
Now supports encrypted password storage.
"""

import argparse
import json
import sys
import os
import getpass
from pathlib import Path
from typing import Dict, Any

# Add the parent directory to the path so Python can find the modules
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from src.utils.config_manager import ConfigManager
from src.utils.password_manager import get_password_manager
from src.config.settings import get_config_info, reload_config, setup_logger

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Configure ETL application settings with encrypted password support",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Set directories
  python configurator.py --logs-dir /var/log/etl --state-dir /var/lib/etl
  
  # Configure database (password will be prompted securely)
  python configurator.py --pg-host db.example.com --pg-port 5432 --pg-user etl_user --pg-password
  
  # Set password from environment variable
  PGPASSWORD=secret python configurator.py --pg-password-env PGPASSWORD
  
  # Set logging options
  python configurator.py --log-level DEBUG --log-retention 7 --log-max-size 50
  
  # Configure ETL parameters
  python configurator.py --batch-size 500 --sync-interval 600
  
  # Show current configuration
  python configurator.py --show-config
  
  # Load configuration from file (passwords will be encrypted automatically)
  python configurator.py --config-file config.json
  
  # Encrypt existing plaintext passwords in settings
  python configurator.py --encrypt-existing-passwords
        """
    )
    
    # Directory configuration
    parser.add_argument("--logs-dir", type=str, help="Directory for log files")
    parser.add_argument("--state-dir", type=str, help="Directory for state files")
    
    # PostgreSQL configuration
    parser.add_argument("--pg-host", type=str, help="PostgreSQL host")
    parser.add_argument("--pg-port", type=int, help="PostgreSQL port")
    parser.add_argument("--pg-database", type=str, help="PostgreSQL database name")
    parser.add_argument("--pg-user", type=str, help="PostgreSQL username")
    parser.add_argument("--pg-password", action="store_true", help="Prompt for PostgreSQL password")
    parser.add_argument("--pg-password-env", type=str, help="Environment variable containing PostgreSQL password")
    
    # Firebird configuration
    parser.add_argument("--fb-host", type=str, help="Firebird host:port")
    parser.add_argument("--fb-database", type=str, help="Firebird database path")
    parser.add_argument("--fb-user", type=str, help="Firebird username")
    parser.add_argument("--fb-password", action="store_true", help="Prompt for Firebird password")
    parser.add_argument("--fb-password-env", type=str, help="Environment variable containing Firebird password")
    parser.add_argument("--fb-charset", type=str, help="Firebird charset")
    
    # YottaDB configuration
    parser.add_argument("--yottadb-url", type=str, help="YottaDB API URL")
    parser.add_argument("--yottadb-timeout", type=int, help="YottaDB API timeout (seconds)")
    parser.add_argument("--yottadb-retries", type=int, help="YottaDB API max retries")
    
    # Logging configuration
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Logging level")
    parser.add_argument("--log-max-size", type=int, help="Maximum log file size (MB)")
    parser.add_argument("--log-backup-count", type=int, help="Number of backup log files")
    parser.add_argument("--log-retention", type=int, help="Log retention period (days)")
    
    # ETL configuration
    parser.add_argument("--batch-size", type=int, help="Default batch size for ETL operations")
    parser.add_argument("--max-retries", type=int, help="Maximum retries for failed operations")
    parser.add_argument("--sync-interval", type=int, help="Sync interval in seconds")
    parser.add_argument("--retry-delay", type=int, help="Retry delay in seconds")
    
    # System configuration
    parser.add_argument("--max-workers", type=int, help="Maximum number of worker threads")
    parser.add_argument("--monitoring-port", type=int, help="Port for monitoring interface")
    parser.add_argument("--status-file", type=str, help="Path to status file")
    
    # Password management
    parser.add_argument("--encrypt-existing-passwords", action="store_true", 
                        help="Encrypt any existing plaintext passwords in settings")
    parser.add_argument("--decrypt-passwords", action="store_true",
                        help="Show decrypted passwords (for debugging)")
    
    # Utility options
    parser.add_argument("--show-config", action="store_true", help="Show current configuration")
    parser.add_argument("--config-file", type=str, help="Load configuration from JSON file")
    parser.add_argument("--export-config", type=str, help="Export current configuration to JSON file")
    parser.add_argument("--validate", action="store_true", help="Validate configuration file")
    parser.add_argument("--backup", action="store_true", help="Create backup before making changes")
    
    return parser.parse_args()

def get_password_securely(prompt: str, env_var: str = None) -> str:
    """Get password from environment variable or prompt user."""
    if env_var and env_var in os.environ:
        return os.environ[env_var]
    
    return getpass.getpass(f"{prompt}: ")

def update_database_config(config_manager: ConfigManager, args: argparse.Namespace) -> bool:
    """Update database configurations with encrypted password support."""
    from src.config.settings import DATABASE_CONFIG
    
    updated = False
    new_config = DATABASE_CONFIG.copy()
    password_manager = get_password_manager()
    
    # First decrypt existing passwords for updating
    new_config = password_manager.decrypt_config_passwords(new_config)
    
    # PostgreSQL updates
    pg_updated = False
    if any([args.pg_host, args.pg_port, args.pg_database, args.pg_user, args.pg_password, args.pg_password_env]):
        pg_config = new_config["PostgreSQL"].copy()
        
        if args.pg_host:
            pg_config["host"] = args.pg_host
            print(f"Setting PostgreSQL host to: {args.pg_host}")
            pg_updated = True
        if args.pg_port:
            pg_config["port"] = args.pg_port
            print(f"Setting PostgreSQL port to: {args.pg_port}")
            pg_updated = True
        if args.pg_database:
            pg_config["database"] = args.pg_database
            print(f"Setting PostgreSQL database to: {args.pg_database}")
            pg_updated = True
        if args.pg_user:
            pg_config["user"] = args.pg_user
            print(f"Setting PostgreSQL user to: {args.pg_user}")
            pg_updated = True
        if args.pg_password or args.pg_password_env:
            password = get_password_securely("PostgreSQL password", args.pg_password_env)
            pg_config["password"] = password
            print("Setting PostgreSQL password (will be encrypted)")
            pg_updated = True
        
        if pg_updated:
            new_config["PostgreSQL"] = pg_config
            updated = True
    
    # Firebird updates
    fb_updated = False
    if any([args.fb_host, args.fb_database, args.fb_user, args.fb_password, args.fb_password_env, args.fb_charset]):
        fb_config = new_config["Firebird"].copy()
        
        if args.fb_host:
            fb_config["host"] = args.fb_host
            print(f"Setting Firebird host to: {args.fb_host}")
            fb_updated = True
        if args.fb_database:
            fb_config["database"] = args.fb_database
            print(f"Setting Firebird database to: {args.fb_database}")
            fb_updated = True
        if args.fb_user:
            fb_config["user"] = args.fb_user
            print(f"Setting Firebird user to: {args.fb_user}")
            fb_updated = True
        if args.fb_password or args.fb_password_env:
            password = get_password_securely("Firebird password", args.fb_password_env)
            fb_config["password"] = password
            print("Setting Firebird password (will be encrypted)")
            fb_updated = True
        if args.fb_charset:
            fb_config["charset"] = args.fb_charset
            print(f"Setting Firebird charset to: {args.fb_charset}")
            fb_updated = True
        
        if fb_updated:
            new_config["Firebird"] = fb_config
            updated = True
    
    # YottaDB updates (no passwords typically needed)
    if any([args.yottadb_url, args.yottadb_timeout, args.yottadb_retries]):
        ydb_config = new_config["YottaDB"].copy()
        
        if args.yottadb_url:
            ydb_config["api_url"] = args.yottadb_url
            print(f"Setting YottaDB URL to: {args.yottadb_url}")
        if args.yottadb_timeout:
            ydb_config["timeout"] = args.yottadb_timeout
            print(f"Setting YottaDB timeout to: {args.yottadb_timeout}")
        if args.yottadb_retries:
            ydb_config["max_retries"] = args.yottadb_retries
            print(f"Setting YottaDB max retries to: {args.yottadb_retries}")
        
        new_config["YottaDB"] = ydb_config
        updated = True
    
    if updated:
        # Encrypt passwords before saving
        encrypted_config = password_manager.encrypt_config_passwords(new_config)
        config_manager.update_dict_variable("DATABASE_CONFIG", encrypted_config)
    
    return updated

def encrypt_existing_passwords(config_manager: ConfigManager) -> bool:
    """Encrypt any existing plaintext passwords in the configuration."""
    from src.config.settings import DATABASE_CONFIG
    
    password_manager = get_password_manager()
    
    print("Checking for plaintext passwords to encrypt...")
    
    # Check if any passwords need encryption
    needs_encryption = False
    
    def check_needs_encryption(obj, path=""):
        nonlocal needs_encryption
        if isinstance(obj, dict):
            for key, value in obj.items():
                current_path = f"{path}.{key}" if path else key
                if key.lower() == 'password' and isinstance(value, str) and value:
                    if not password_manager.is_encrypted(value):
                        print(f"Found plaintext password at: {current_path}")
                        needs_encryption = True
                else:
                    check_needs_encryption(value, current_path)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                check_needs_encryption(item, f"{path}[{i}]")
    
    check_needs_encryption(DATABASE_CONFIG)
    
    if needs_encryption:
        print("Encrypting plaintext passwords...")
        encrypted_config = password_manager.encrypt_config_passwords(DATABASE_CONFIG)
        config_manager.update_dict_variable("DATABASE_CONFIG", encrypted_config)
        print("Passwords encrypted successfully!")
        return True
    else:
        print("All passwords are already encrypted or empty.")
        return False

def update_directories(config_manager: ConfigManager, args: argparse.Namespace) -> bool:
    """Update directory configurations."""
    updated = False
    
    if args.logs_dir:
        print(f"Setting logs directory to: {args.logs_dir}")
        config_manager.update_path_variable("LOGS_DIR", args.logs_dir)
        updated = True
    
    if args.state_dir:
        print(f"Setting state directory to: {args.state_dir}")
        config_manager.update_path_variable("STATE_DIR", args.state_dir)
        updated = True
    
    return updated

def update_database_config(config_manager: ConfigManager, args: argparse.Namespace) -> bool:
    """Update database configurations."""
    from src.config.settings import DATABASE_CONFIG
    
    updated = False
    new_config = DATABASE_CONFIG.copy()
    
    # PostgreSQL updates
    if any([args.pg_host, args.pg_port, args.pg_database, args.pg_user, args.pg_password]):
        pg_config = new_config["PostgreSQL"].copy()
        
        if args.pg_host:
            pg_config["host"] = args.pg_host
            print(f"Setting PostgreSQL host to: {args.pg_host}")
        if args.pg_port:
            pg_config["port"] = args.pg_port
            print(f"Setting PostgreSQL port to: {args.pg_port}")
        if args.pg_database:
            pg_config["database"] = args.pg_database
            print(f"Setting PostgreSQL database to: {args.pg_database}")
        if args.pg_user:
            pg_config["user"] = args.pg_user
            print(f"Setting PostgreSQL user to: {args.pg_user}")
        if args.pg_password:
            pg_config["password"] = args.pg_password
            print("Setting PostgreSQL password (hidden)")
        
        new_config["PostgreSQL"] = pg_config
        updated = True
    
    # Firebird updates
    if any([args.fb_host, args.fb_database, args.fb_user, args.fb_password, args.fb_charset]):
        fb_config = new_config["Firebird"].copy()
        
        if args.fb_host:
            fb_config["host"] = args.fb_host
            print(f"Setting Firebird host to: {args.fb_host}")
        if args.fb_database:
            fb_config["database"] = args.fb_database
            print(f"Setting Firebird database to: {args.fb_database}")
        if args.fb_user:
            fb_config["user"] = args.fb_user
            print(f"Setting Firebird user to: {args.fb_user}")
        if args.fb_password:
            fb_config["password"] = args.fb_password
            print("Setting Firebird password (hidden)")
        if args.fb_charset:
            fb_config["charset"] = args.fb_charset
            print(f"Setting Firebird charset to: {args.fb_charset}")
        
        new_config["Firebird"] = fb_config
        updated = True
    
    # YottaDB updates
    if any([args.yottadb_url, args.yottadb_timeout, args.yottadb_retries]):
        ydb_config = new_config["YottaDB"].copy()
        
        if args.yottadb_url:
            ydb_config["api_url"] = args.yottadb_url
            print(f"Setting YottaDB URL to: {args.yottadb_url}")
        if args.yottadb_timeout:
            ydb_config["timeout"] = args.yottadb_timeout
            print(f"Setting YottaDB timeout to: {args.yottadb_timeout}")
        if args.yottadb_retries:
            ydb_config["max_retries"] = args.yottadb_retries
            print(f"Setting YottaDB max retries to: {args.yottadb_retries}")
        
        new_config["YottaDB"] = ydb_config
        updated = True
    
    if updated:
        config_manager.update_dict_variable("DATABASE_CONFIG", new_config)
    
    return updated

def update_logging_config(config_manager: ConfigManager, args: argparse.Namespace) -> bool:
    """Update logging configuration."""
    from src.config.settings import LOGGING_CONFIG
    
    updated = False
    new_config = LOGGING_CONFIG.copy()
    
    if args.log_level:
        new_config["level"] = args.log_level
        print(f"Setting log level to: {args.log_level}")
        updated = True
    
    if args.log_max_size:
        new_config["max_file_size_mb"] = args.log_max_size
        print(f"Setting max log file size to: {args.log_max_size} MB")
        updated = True
    
    if args.log_backup_count:
        new_config["backup_count"] = args.log_backup_count
        print(f"Setting log backup count to: {args.log_backup_count}")
        updated = True
    
    if args.log_retention:
        new_config["log_retention_days"] = args.log_retention
        print(f"Setting log retention to: {args.log_retention} days")
        updated = True
    
    if updated:
        # Remove the files and base_dir keys as they are auto-generated
        config_to_write = {k: v for k, v in new_config.items() 
                          if k not in ['files', 'base_dir']}
        config_manager.update_dict_variable("LOGGING_CONFIG", config_to_write)
    
    return updated

def update_etl_config(config_manager: ConfigManager, args: argparse.Namespace) -> bool:
    """Update ETL configuration."""
    from src.config.settings import ETL_CONFIG
    
    updated = False
    new_config = ETL_CONFIG.copy()
    
    if args.batch_size:
        new_config["default_batch_size"] = args.batch_size
        print(f"Setting default batch size to: {args.batch_size}")
        updated = True
    
    if args.max_retries:
        new_config["max_retries"] = args.max_retries
        print(f"Setting max retries to: {args.max_retries}")
        updated = True
    
    if args.sync_interval:
        new_config["sync_interval_seconds"] = args.sync_interval
        print(f"Setting sync interval to: {args.sync_interval} seconds")
        updated = True
    
    if args.retry_delay:
        new_config["retry_delay_seconds"] = args.retry_delay
        print(f"Setting retry delay to: {args.retry_delay} seconds")
        updated = True
    
    if updated:
        config_manager.update_dict_variable("ETL_CONFIG", new_config)
    
    return updated

def update_system_config(config_manager: ConfigManager, args: argparse.Namespace) -> bool:
    """Update system configuration."""  
    from src.config.settings import SYSTEM_CONFIG
    
    updated = False
    new_config = SYSTEM_CONFIG.copy()
    
    if args.max_workers:
        new_config["max_workers"] = args.max_workers
        print(f"Setting max workers to: {args.max_workers}")
        updated = True
    
    if args.monitoring_port:
        new_config["monitoring_port"] = args.monitoring_port
        print(f"Setting monitoring port to: {args.monitoring_port}")
        updated = True
    
    if args.status_file:
        new_config["status_file"] = args.status_file
        print(f"Setting status file to: {args.status_file}")
        updated = True
    
    if updated:
        config_manager.update_dict_variable("SYSTEM_CONFIG", new_config)
    
    return updated

def load_config_from_file(config_manager: ConfigManager, config_file: str) -> bool:
    """Load configuration from JSON file."""
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        print(f"Loading configuration from: {config_file}")
        
        # Process each section
        updated = False
        
        if "directories" in config:
            dirs = config["directories"]
            if "logs_dir" in dirs:
                config_manager.update_path_variable("LOGS_DIR", dirs["logs_dir"])
                updated = True
            if "state_dir" in dirs:
                config_manager.update_path_variable("STATE_DIR", dirs["state_dir"])
                updated = True
        
        if "database_config" in config:
            config_manager.update_dict_variable("DATABASE_CONFIG", config["database_config"])
            updated = True
        
        if "logging_config" in config:
            # Remove auto-generated keys
            logging_config = {k: v for k, v in config["logging_config"].items() 
                            if k not in ['files', 'base_dir']}
            config_manager.update_dict_variable("LOGGING_CONFIG", logging_config)
            updated = True
        
        if "etl_config" in config:
            config_manager.update_dict_variable("ETL_CONFIG", config["etl_config"])
            updated = True
        
        if "system_config" in config:
            config_manager.update_dict_variable("SYSTEM_CONFIG", config["system_config"])
            updated = True
        
        return updated
        
    except Exception as e:
        print(f"Error loading configuration file: {e}")
        return False

def show_current_config():
    """Display current configuration."""
    config_info = get_config_info()  # This already masks passwords
    
    print("=== Current ETL Configuration ===\n")
    
    print("Directories:")
    for key, value in config_info["directories"].items():
        print(f"  {key}: {value}")
    
    print("\nDatabase Configuration:")
    for db_type, db_config in config_info["database_config"].items():
        print(f"  {db_type}:")
        for key, value in db_config.items():
            print(f"    {key}: {value}")
    
    print("\nLogging Configuration:")
    for key, value in config_info["logging_config"].items():
        if key != "files":  # Skip the files dict as it's long
            print(f"  {key}: {value}")
    
    print("\nETL Configuration:")
    for key, value in config_info["etl_config"].items():
        print(f"  {key}: {value}")
    
    print("\nSystem Configuration:")
    for key, value in config_info["system_config"].items():
        print(f"  {key}: {value}")

def export_config_to_file(filename: str):
    """Export current configuration to JSON file."""
    try:
        config_info = get_config_info()
        
        with open(filename, 'w') as f:
            json.dump(config_info, f, indent=2, default=str)
        
        print(f"Configuration exported to: {filename}")
        return True
    except Exception as e:
        print(f"Error exporting configuration: {e}")
        return False

def main():
    """Main configurator function."""
    args = parse_args()
    
    # Handle encryption of existing passwords
    if args.encrypt_existing_passwords:
        try:
            config_manager = ConfigManager()
            if encrypt_existing_passwords(config_manager):
                print("Configuration updated with encrypted passwords!")
                reload_config()
            return 0
        except Exception as e:
            print(f"Error encrypting passwords: {e}")
            return 1
    
    # Handle decrypt passwords (for debugging)
    if args.decrypt_passwords:
        try:
            from src.config.settings import DATABASE_CONFIG
            password_manager = get_password_manager()
            decrypted_config = password_manager.decrypt_config_passwords(DATABASE_CONFIG)
            
            print("=== Decrypted Database Configuration ===")
            for db_type, db_config in decrypted_config.items():
                print(f"\n{db_type}:")
                for key, value in db_config.items():
                    print(f"  {key}: {value}")
            return 0
        except Exception as e:
            print(f"Error decrypting passwords: {e}")
            return 1
    
    # Handle show config
    if args.show_config:
        show_current_config()
        return 0
    
    # Handle export config
    if args.export_config:
        if export_config_to_file(args.export_config):
            return 0
        else:
            return 1
    
    # Handle validation
    if args.validate:
        config_manager = ConfigManager()
        if config_manager.validate_settings():
            print("Configuration file is valid")
            return 0
        else:
            print("Configuration file has errors")
            return 1
    
    # Check if any updates are requested
    update_args = [
        args.logs_dir, args.state_dir,
        args.pg_host, args.pg_port, args.pg_database, args.pg_user, args.pg_password,
        args.fb_host, args.fb_database, args.fb_user, args.fb_password, args.fb_charset,
        args.yottadb_url, args.yottadb_timeout, args.yottadb_retries,
        args.log_level, args.log_max_size, args.log_backup_count, args.log_retention,
        args.batch_size, args.max_retries, args.sync_interval, args.retry_delay,
        args.max_workers, args.monitoring_port, args.status_file,
        args.config_file
    ]
    
    if not any(update_args):
        print("No configuration changes specified. Use --help for available options.")
        return 0
    
    try:
        config_manager = ConfigManager()
        
        # Create backup if requested
        if args.backup:
            backup_file = config_manager.settings_file.with_suffix('.py.backup')
            if config_manager.settings_file.exists():
                import shutil
                shutil.copy2(config_manager.settings_file, backup_file)
                print(f"Backup created: {backup_file}")
        
        updated = False
        
        # Load from file first if specified
        if args.config_file:
            updated |= load_config_from_file(config_manager, args.config_file)
        
        # Apply individual updates
        updated |= update_directories(config_manager, args)
        updated |= update_database_config(config_manager, args)
        updated |= update_logging_config(config_manager, args)
        updated |= update_etl_config(config_manager, args)
        updated |= update_system_config(config_manager, args)
        
        if updated:
            # Validate the updated configuration
            if config_manager.validate_settings():
                print("\nConfiguration updated successfully!")
                
                # Reload configuration in memory
                reload_config()
                
                print("Configuration reloaded. Changes will take effect on next application start.")
                return 0
            else:
                print("\nError: Updated configuration is invalid!")
                return 1
        else:
            print("No changes made to configuration.")
            return 0
            
    except Exception as e:
        print(f"Error updating configuration: {e}")
        return 1

if __name__ == "__main__":
    exit(main())