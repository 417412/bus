"""
API-specific configuration settings with PostgreSQL integration.
"""

import os
import sys
from pathlib import Path

# Add parent directories to path for imports
parent_dir = Path(__file__).parent.parent.parent
sys.path.append(str(parent_dir))

try:
    from src.config.settings import get_decrypted_database_config, setup_logger
    DATABASE_CONFIG_AVAILABLE = True
except ImportError:
    DATABASE_CONFIG_AVAILABLE = False

# API Configuration
API_CONFIG = {
    "title": "Patient Credential Management API",
    "description": "API for managing patient credentials across HIS systems with OAuth authentication and patient creation",
    "version": "1.1.0",
    "host": "0.0.0.0",
    "port": 8000,
    "debug": os.getenv("DEBUG", "false").lower() == "true"
}

# PostgreSQL Configuration for API (compatible with settings.py)
def get_postgresql_config():
    """
    Get PostgreSQL configuration from main settings or environment variables.
    This function ensures compatibility with the existing settings.py structure.
    """
    if DATABASE_CONFIG_AVAILABLE:
        try:
            db_config = get_decrypted_database_config()
            pg_config = db_config.get("PostgreSQL", {})
            
            return {
                "host": pg_config.get("host", "localhost"),
                "port": int(pg_config.get("port", 5432)),
                "database": pg_config.get("database", "medical_system"),
                "user": pg_config.get("user", "medapp_user"),
                "password": pg_config.get("password", ""),
                "connect_timeout": 30,
                "command_timeout": 60
            }
        except Exception as e:
            print(f"Warning: Could not load PostgreSQL config from settings: {e}")
    
    # Fallback to environment variables
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "database": os.getenv("POSTGRES_DB", "medical_system"),
        "user": os.getenv("POSTGRES_USER", "medapp_user"),
        "password": os.getenv("POSTGRES_PASSWORD", ""),
        "connect_timeout": int(os.getenv("POSTGRES_CONNECT_TIMEOUT", "30")),
        "command_timeout": int(os.getenv("POSTGRES_COMMAND_TIMEOUT", "60"))
    }

HIS_API_CONFIG = {
    "yottadb": {
        "base_url": os.getenv("YOTTADB_API_BASE", "http://192.168.156.43"),
        "credentials_endpoint": "/updatePatients/{hisnumber}/credentials",
        "create_endpoint": "/createPatients",
        "oauth": {
            "token_url": os.getenv("YOTTADB_TOKEN_URL", "http://192.168.156.43/token"),
            "client_id": os.getenv("YOTTADB_CLIENT_ID", ""),  # Empty string
            "client_secret": os.getenv("YOTTADB_CLIENT_SECRET", ""),  # Empty string
            "username": os.getenv("YOTTADB_USERNAME", "admin"),
            "password": os.getenv("YOTTADB_PASSWORD", "secret"),
            "scope": os.getenv("YOTTADB_SCOPE", "")  # Empty string
        }
    },
    "firebird": {
        "base_url": os.getenv("FIREBIRD_API_BASE", "http://192.168.160.141"),
        "credentials_endpoint": "/updatePatients/{hisnumber}/credentials",
        "create_endpoint": "/createPatients",
        "oauth": {
            "token_url": os.getenv("FIREBIRD_TOKEN_URL", "http://192.168.160.141/token"),
            "client_id": os.getenv("FIREBIRD_CLIENT_ID", ""),  # Empty string
            "client_secret": os.getenv("FIREBIRD_CLIENT_SECRET", ""),  # Empty string
            "username": os.getenv("FIREBIRD_USERNAME", "admin"),
            "password": os.getenv("FIREBIRD_PASSWORD", "secret"),
            "scope": os.getenv("FIREBIRD_SCOPE", "")  # Empty string
        }
    }
}

# Mobile App User Configuration
MOBILE_APP_CONFIG = {
    "registration_enabled": os.getenv("MOBILE_APP_REGISTRATION_ENABLED", "true").lower() == "true",
    "auto_register_on_create": os.getenv("MOBILE_APP_AUTO_REGISTER", "true").lower() == "true",
    "require_both_his": os.getenv("MOBILE_APP_REQUIRE_BOTH_HIS", "false").lower() == "true"
}

# Security Configuration
SECURITY_CONFIG = {
    "api_key_header": os.getenv("API_KEY_HEADER", "X-API-Key"),
    "api_key": os.getenv("API_KEY", ""),  # Empty means no API key required
    "cors_enabled": os.getenv("CORS_ENABLED", "true").lower() == "true",
    "cors_origins": os.getenv("CORS_ORIGINS", "*").split(","),
    "max_request_size": int(os.getenv("MAX_REQUEST_SIZE", "1048576"))  # 1MB
}

# HTTP Client Configuration
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))

# OAuth Configuration
OAUTH_TOKEN_CACHE_BUFFER = int(os.getenv("OAUTH_TOKEN_CACHE_BUFFER", "300"))  # 5 minutes

def setup_api_logger(name: str, level: str = None):
    """
    Set up a logger for API components using the main settings logger setup if available.
    Falls back to basic logging if main settings are not available.
    """
    if DATABASE_CONFIG_AVAILABLE:
        try:
            return setup_logger(name, "api", level)
        except Exception:
            pass
    
    # Fallback logger setup
    import logging
    from logging.handlers import RotatingFileHandler
    
    logger = logging.getLogger(name)
    
    if logger.handlers:
        return logger
    
    log_level = level or "INFO"
    logger.setLevel(getattr(logging, log_level))
    
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    # Ensure log directory exists
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True, parents=True)
    
    # File handler
    file_handler = RotatingFileHandler(
        log_dir / f"{name}.log",
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(getattr(logging, log_level))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, log_level))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    logger.propagate = False
    return logger

def validate_config():
    """Validate the configuration and return any issues."""
    issues = []
    
    # Check PostgreSQL configuration
    pg_config = get_postgresql_config()
    if not pg_config.get("password"):
        issues.append("PostgreSQL password is not configured")
    
    # Check HIS API configuration
    for his_name, his_config in HIS_API_CONFIG.items():
        oauth_config = his_config.get("oauth", {})
        if not oauth_config.get("client_secret") or oauth_config.get("client_secret") == "secret":
            issues.append(f"{his_name.upper()} OAuth client secret should be changed from default")
        if not oauth_config.get("password") or oauth_config.get("password") == "secret":
            issues.append(f"{his_name.upper()} OAuth password should be changed from default")
    
    # Check required directories
    log_dir = Path("logs")
    if not log_dir.exists():
        try:
            log_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            issues.append(f"Cannot create logs directory: {e}")
    
    return issues

def get_api_config():
    """Get the complete API configuration."""
    return {
        "api": API_CONFIG,
        "postgresql": get_postgresql_config(),
        "his_api": HIS_API_CONFIG,
        "mobile_app": MOBILE_APP_CONFIG,
        "security": SECURITY_CONFIG,
        "http_timeout": HTTP_TIMEOUT,
        "oauth_cache_buffer": OAUTH_TOKEN_CACHE_BUFFER,
        "environment": os.getenv("ENVIRONMENT", "development")
    }

def get_config_summary():
    """Get a summary of current configuration (with sensitive data masked)."""
    import copy
    
    config = get_api_config()
    
    # Mask sensitive information
    masked_config = copy.deepcopy(config)
    
    def mask_sensitive_data(obj, path=""):
        if isinstance(obj, dict):
            for key, value in obj.items():
                current_path = f"{path}.{key}" if path else key
                if any(sensitive in key.lower() for sensitive in ['password', 'secret', 'key', 'token']):
                    if isinstance(value, str) and value:
                        obj[key] = "********"
                elif isinstance(value, (dict, list)):
                    mask_sensitive_data(value, current_path)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                mask_sensitive_data(item, f"{path}[{i}]")
    
    mask_sensitive_data(masked_config)
    return masked_config