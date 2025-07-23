"""
Configuration settings for the medical system ETL application.
This file can be automatically updated by the configurator.
"""

import os
from pathlib import Path

# Base directory for the project
BASE_DIR = Path(__file__).parent.parent.parent

# =============================================================================
# CONFIGURABLE SECTION - These values can be updated by configurator.py
# =============================================================================

# Directory Configuration
LOGS_DIR = BASE_DIR / "logs"
STATE_DIR = BASE_DIR / "state"

# Database Configurations (passwords are encrypted)
DATABASE_CONFIG = {
    "PostgreSQL": {
        "host": "localhost",
        "port": 5432,
        "database": "medical_system",
        "user": "medapp_user",
        "password": "ENC:Z0FBQUFBQm9nV3ZScmVKZUhucjJHU2hYbE1jZ0s1LWNxZ0VMY1ZMWElQdFRVRG5lNGRVSzJaYWkxZ2l5QnJMTWtUcjlOTWlpVXFkUjJQbmZuNE5UeWF3MFJnSXJIak5Wb2c9PQ==",
},
    "Firebird": {
        "host": "192.168.160.168",
        "port": 3050,
        "database": "099-1",
        "user": "SYSDBA",
        "password": "ENC:Z0FBQUFBQm9nV3ZSQ3dMOW9EcGVJbnRWSmNtNlo3ZjRtMUdtYWY2Tl82blFLSEpUang1emJaRlhJbk9YX3dtR0FheFVpZWpwcWd3UEJyZjh0VmNzMWx4STIxMHd0Vl9fM1E9PQ==",
        "charset": "UTF-8",
        "debug": False,
},
    "YottaDB": {
        "api_url": "http://192.168.156.43/cgi-bin/qms_export_pat",
        "timeout": 300,
        "connect_timeout": 300,
        "max_retries": 2,
        "delimiter": "|",
},
}

# Logging Configuration
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "max_file_size_mb": 100,
    "backup_count": 5,
    "log_retention_days": 30
}

# ETL Configuration
ETL_CONFIG = {
    "default_batch_size": 1000,
    "max_retries": 3,
    "retry_delay_seconds": 60,
    "sync_interval_seconds": 300,
    "initial_load_check_interval": 5,  # batches
    "memory_cleanup_interval": 50000,  # records
    "analyze_interval": 100000  # records
}

# System Configuration
SYSTEM_CONFIG = {
    "max_workers": 4,
    "enable_monitoring": True,
    "monitoring_port": 8080,
    "status_file": "etl_status.json"
}

# =============================================================================
# END CONFIGURABLE SECTION
# =============================================================================

# Ensure directories exist
LOGS_DIR.mkdir(exist_ok=True, parents=True)
STATE_DIR.mkdir(exist_ok=True, parents=True)

# Document type mapping (used for display and data mapping)
DOCUMENT_TYPES = {
    1: 'Паспорт',
    2: 'Паспорт СССР',
    3: 'Заграничный паспорт РФ',
    4: 'Заграничный паспорт СССР', 
    5: 'Свидетельство о рождении',
    6: 'Удостоверение личности офицера',
    7: 'Справка об освобождении из места лишения свободы',
    8: 'Военный билет',
    9: 'Дипломатический паспорт РФ',
    10: 'Иностранный паспорт',
    11: 'Свидетельство беженца',
    12: 'Вид на жительство',
    13: 'Удостоверение беженца',
    14: 'Временное удостоверение',
    15: 'Паспорт моряка',
    16: 'Военный билет офицера запаса',
    17: 'Иные документы'
}

# Document type mapping for external systems
EXTERNAL_DOCUMENT_TYPE_MAPPING = {
    99: 17,  # Map Infoclinica's "Other" (99) to our "Other documents" (17)
}

# Build dynamic log file paths
_LOG_FILES = {
    "etl_daemon_firebird": LOGS_DIR / "etl_daemon_firebird.log",
    "etl_daemon_yottadb": LOGS_DIR / "etl_daemon_yottadb.log",
    "debug_yottadb": LOGS_DIR / "debug_yottadb.log",
    "test_etl": LOGS_DIR / "test_etl.log",
    "test_generator": LOGS_DIR / "test_generator.log",
    "connectors": LOGS_DIR / "connectors.log",
    "repositories": LOGS_DIR / "repositories.log",
    "transformers": LOGS_DIR / "transformers.log",
    "general": LOGS_DIR / "general.log"
}

# Add log files to logging config
LOGGING_CONFIG["files"] = {k: str(v) for k, v in _LOG_FILES.items()}
LOGGING_CONFIG["base_dir"] = str(LOGS_DIR)

def get_decrypted_database_config():
    """Get database configuration with decrypted passwords."""
    try:
        from src.utils.password_manager import get_password_manager
        
        password_manager = get_password_manager()
        return password_manager.decrypt_config_passwords(DATABASE_CONFIG)
    except Exception as e:
        # Fallback to original config if decryption fails
        print(f"Warning: Could not decrypt passwords, using original config: {e}")
        return DATABASE_CONFIG

def setup_logger(name: str, log_file_key: str = "general", level: str = None):
    """
    Set up a logger with file and console handlers.
    
    Args:
        name: Name of the logger
        log_file_key: Key from LOGGING_CONFIG["files"] to determine log file
        level: Log level (defaults to LOGGING_CONFIG["level"])
    
    Returns:
        Configured logger
    """
    import logging
    from logging.handlers import RotatingFileHandler
    
    # Get or create logger
    logger = logging.getLogger(name)
    
    # Don't add handlers if they already exist
    if logger.handlers:
        return logger
    
    # Set level
    log_level = level or LOGGING_CONFIG["level"]
    logger.setLevel(getattr(logging, log_level))
    
    # Create formatter
    formatter = logging.Formatter(LOGGING_CONFIG["format"])
    
    # File handler with rotation
    log_file = LOGGING_CONFIG["files"].get(log_file_key, LOGGING_CONFIG["files"]["general"])
    
    # Ensure the directory exists
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    
    max_bytes = LOGGING_CONFIG["max_file_size_mb"] * 1024 * 1024
    backup_count = LOGGING_CONFIG["backup_count"]
    
    file_handler = RotatingFileHandler(
        log_file, 
        maxBytes=max_bytes, 
        backupCount=backup_count
    )
    file_handler.setLevel(getattr(logging, log_level))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, log_level))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    return logger

def reload_config():
    """
    Reload configuration after it has been updated.
    This function updates the global variables when settings are changed.
    """
    global LOGS_DIR, STATE_DIR, _LOG_FILES
    
    # Ensure new directories exist
    LOGS_DIR.mkdir(exist_ok=True, parents=True)
    STATE_DIR.mkdir(exist_ok=True, parents=True)
    
    # Rebuild log file paths
    _LOG_FILES = {
        "etl_daemon_firebird": LOGS_DIR / "etl_daemon_firebird.log",
        "etl_daemon_yottadb": LOGS_DIR / "etl_daemon_yottadb.log",
        "debug_yottadb": LOGS_DIR / "debug_yottadb.log", 
        "test_etl": LOGS_DIR / "test_etl.log",
        "test_generator": LOGS_DIR / "test_generator.log",
        "connectors": LOGS_DIR / "connectors.log",
        "repositories": LOGS_DIR / "repositories.log",
        "transformers": LOGS_DIR / "transformers.log",
        "general": LOGS_DIR / "general.log"
    }
    
    # Update logging config
    LOGGING_CONFIG["files"] = {k: str(v) for k, v in _LOG_FILES.items()}
    LOGGING_CONFIG["base_dir"] = str(LOGS_DIR)

def get_config_info():
    """Get current configuration information with decrypted passwords for display."""
    config = {
        "directories": {
            "base_dir": str(BASE_DIR),
            "logs_dir": str(LOGS_DIR),
            "state_dir": str(STATE_DIR)
        },
        "database_config": get_decrypted_database_config(),
        "logging_config": LOGGING_CONFIG,
        "etl_config": ETL_CONFIG,
        "system_config": SYSTEM_CONFIG
    }
    
    # Mask passwords in the returned config for security
    import copy
    masked_config = copy.deepcopy(config)
    
    def mask_passwords(obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key.lower() == 'password' and isinstance(value, str) and value:
                    obj[key] = "********"
                elif isinstance(value, (dict, list)):
                    mask_passwords(value)
        elif isinstance(obj, list):
            for item in obj:
                mask_passwords(item)
    
    mask_passwords(masked_config)
    return masked_config