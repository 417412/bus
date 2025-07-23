"""
Configuration settings for the medical system ETL application.
"""

import os
from pathlib import Path

# Base directory for the project
BASE_DIR = Path(__file__).parent.parent.parent

# Logs directory - create if it doesn't exist
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)

# State directory - create if it doesn't exist  
STATE_DIR = BASE_DIR / "state"
STATE_DIR.mkdir(exist_ok=True)

# Database connection configurations
DATABASE_CONFIG = {
    "PostgreSQL": {
        "host": "localhost",
        "database": "medical_system",
        "user": "medapp_user",
        "password": "the2zG6tbewA3"
    },
    "Firebird": {
        "host": "192.168.160.168:3050",
        "database": "099-1",            
        "user": "SYSDBA",
        "password": "masterkey",
        "charset": "UTF-8",
        "debug": False
    },
    "YottaDB": {
        # YottaDB specific configuration for HTTP API
        "api_url": "http://192.168.156.43/cgi-bin/qms_export_pat",
        "timeout": 300,  # 5 minutes read timeout (allowing extra time)
        "connect_timeout": 300,  # 30 seconds connection timeout
        "max_retries": 2,  # Fewer retries since each call is long
        "delimiter": "|"
    }
}

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
    # Infoclinica document type -> Our system
    99: 17,  # Map Infoclinica's "Other" (99) to our "Other documents" (17)
}

# Logging configuration
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "base_dir": str(LOGS_DIR),
    "files": {
        "etl_daemon_firebird": str(LOGS_DIR / "etl_daemon_firebird.log"),
        "etl_daemon_yottadb": str(LOGS_DIR / "etl_daemon_yottadb.log"),
        "debug_yottadb": str(LOGS_DIR / "debug_yottadb.log"),
        "test_etl": str(LOGS_DIR / "test_etl.log"),
        "test_generator": str(LOGS_DIR / "test_generator.log"),
        "connectors": str(LOGS_DIR / "connectors.log"),
        "repositories": str(LOGS_DIR / "repositories.log"),
        "transformers": str(LOGS_DIR / "transformers.log"),
        "general": str(LOGS_DIR / "general.log")
    }
}

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
    
    # File handler
    log_file = LOGGING_CONFIG["files"].get(log_file_key, LOGGING_CONFIG["files"]["general"])
    file_handler = logging.FileHandler(log_file)
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