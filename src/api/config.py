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

# PostgreSQL Configuration for API
def get_postgresql_config():
    """
    Get PostgreSQL configuration from main settings or environment variables.
    """
    if DATABASE_CONFIG_AVAILABLE:
        try:
            db_config = get_decrypted_database_config()
            pg_config = db_config.get("PostgreSQL", {})
            
            return {
                "host": pg_config.get("host", "localhost"),
                "port": pg_config.get("port", 5432),
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

# HIS System API Endpoints with OAuth Configuration
HIS_API_CONFIG = {
    "yottadb": {
        "base_url": os.getenv("YOTTADB_API_BASE", "http://192.168.156.43"),
        "credentials_endpoint": "/updatePatients/{hisnumber}/credentials",
        "create_endpoint": "/createPatients",
        "oauth": {
            "token_url": os.getenv("YOTTADB_TOKEN_URL", "http://192.168.156.43/oauth/token"),
            "client_id": os.getenv("YOTTADB_CLIENT_ID", "yottadb_client"),
            "client_secret": os.getenv("YOTTADB_CLIENT_SECRET", "yottadb_secret"),
            "username": os.getenv("YOTTADB_USERNAME", "api_user"),
            "password": os.getenv("YOTTADB_PASSWORD", "api_password"),
            "scope": os.getenv("YOTTADB_SCOPE", "patient_update patient_create")
        }
    },
    "firebird": {
        "base_url": os.getenv("FIREBIRD_API_BASE", "http://firebird-server"),
        "credentials_endpoint": "/updatePatients/{hisnumber}/credentials",
        "create_endpoint": "/createPatients",
        "oauth": {
            "token_url": os.getenv("FIREBIRD_TOKEN_URL", "http://firebird-server/oauth/token"),
            "client_id": os.getenv("FIREBIRD_CLIENT_ID", "firebird_client"),
            "client_secret": os.getenv("FIREBIRD_CLIENT_SECRET", "firebird_secret"),
            "username": os.getenv("FIREBIRD_USERNAME", "api_user"),
            "password": os.getenv("FIREBIRD_PASSWORD", "api_password"),
            "scope": os.getenv("FIREBIRD_SCOPE", "patient_update patient_create")
        }
    }
}

# Mobile App User Configuration
MOBILE_APP_CONFIG = {
    "registration_enabled": os.getenv("MOBILE_APP_REGISTRATION_ENABLED", "true").lower() == "true",
    "auto_register_on_create": os.getenv("MOBILE_APP_AUTO_REGISTER", "true").lower() == "true",
    "require_both_his": os.getenv("MOBILE_APP_REQUIRE_BOTH_HIS", "false").lower() == "true"
}

# HTTP Client Configuration
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))

# OAuth Configuration
OAUTH_TOKEN_CACHE_BUFFER = int(os.getenv("OAUTH_TOKEN_CACHE_BUFFER", "300"))  # 5 minutes buffer before token expiry

# Patient Matching Configuration
PATIENT_MATCHING_CONFIG = {
    "enable_mobile_app_matching": True,
    "enable_document_matching": True,
    "enable_his_number_matching": True,
    "auto_lock_matched_patients": os.getenv("AUTO_LOCK_MATCHED_PATIENTS", "false").lower() == "true",
    "matching_timeout_seconds": int(os.getenv("MATCHING_TIMEOUT", "30"))
}

# API Rate Limiting Configuration
RATE_LIMITING_CONFIG = {
    "enabled": os.getenv("RATE_LIMITING_ENABLED", "false").lower() == "true",
    "requests_per_minute": int(os.getenv("RATE_LIMIT_RPM", "60")),
    "burst_size": int(os.getenv("RATE_LIMIT_BURST", "10"))
}

# Logging Configuration
def get_logging_config():
    """Get logging configuration for the API."""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    log_dir = Path(os.getenv("API_LOG_DIR", "logs"))
    log_dir.mkdir(exist_ok=True, parents=True)
    
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            },
            "detailed": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(funcName)s:%(lineno)d - %(message)s",
            },
        },
        "handlers": {
            "console": {
                "formatter": "default",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
                "level": log_level,
            },
            "file": {
                "formatter": "detailed",
                "class": "logging.handlers.RotatingFileHandler",
                "filename": str(log_dir / "api.log"),
                "maxBytes": 10485760,  # 10MB
                "backupCount": 5,
                "level": log_level,
            },
            "access_file": {
                "formatter": "default",
                "class": "logging.handlers.RotatingFileHandler",
                "filename": str(log_dir / "api_access.log"),
                "maxBytes": 10485760,  # 10MB
                "backupCount": 5,
                "level": "INFO",
            },
        },
        "loggers": {
            "patient_api": {
                "level": log_level,
                "handlers": ["console", "file"],
                "propagate": False,
            },
            "uvicorn.access": {
                "level": "INFO",
                "handlers": ["access_file"],
                "propagate": False,
            },
        },
        "root": {
            "level": log_level,
            "handlers": ["console", "file"],
        },
    }

LOGGING_CONFIG = get_logging_config()

# Health Check Configuration
HEALTH_CHECK_CONFIG = {
    "enabled": True,
    "check_database": True,
    "check_his_endpoints": os.getenv("HEALTH_CHECK_HIS", "false").lower() == "true",
    "timeout_seconds": int(os.getenv("HEALTH_CHECK_TIMEOUT", "5")),
    "cache_duration_seconds": int(os.getenv("HEALTH_CHECK_CACHE", "30"))
}

# Security Configuration
SECURITY_CONFIG = {
    "api_key_header": os.getenv("API_KEY_HEADER", "X-API-Key"),
    "api_key": os.getenv("API_KEY", ""),  # Empty means no API key required
    "cors_enabled": os.getenv("CORS_ENABLED", "true").lower() == "true",
    "cors_origins": os.getenv("CORS_ORIGINS", "*").split(","),
    "max_request_size": int(os.getenv("MAX_REQUEST_SIZE", "1048576"))  # 1MB
}

# Monitoring Configuration
MONITORING_CONFIG = {
    "metrics_enabled": os.getenv("METRICS_ENABLED", "false").lower() == "true",
    "metrics_endpoint": "/metrics",
    "prometheus_port": int(os.getenv("PROMETHEUS_PORT", "8001")),
    "collect_request_metrics": True,
    "collect_database_metrics": True,
    "collect_oauth_metrics": True
}

def get_config_summary():
    """Get a summary of current configuration (with sensitive data masked)."""
    import copy
    
    config = {
        "api": API_CONFIG,
        "postgresql": get_postgresql_config(),
        "his_systems": HIS_API_CONFIG,
        "mobile_app": MOBILE_APP_CONFIG,
        "patient_matching": PATIENT_MATCHING_CONFIG,
        "rate_limiting": RATE_LIMITING_CONFIG,
        "security": SECURITY_CONFIG,
        "monitoring": MONITORING_CONFIG,
        "health_check": HEALTH_CHECK_CONFIG
    }
    
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
        if not oauth_config.get("client_secret"):
            issues.append(f"{his_name.upper()} OAuth client secret is not configured")
        if not oauth_config.get("password"):
            issues.append(f"{his_name.upper()} OAuth password is not configured")
    
    # Check required directories
    log_dir = Path("logs")
    if not log_dir.exists():
        try:
            log_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            issues.append(f"Cannot create logs directory: {e}")
    
    return issues

def setup_api_logger(name: str, level: str = None):
    """
    Set up a logger for API components using the main settings logger setup if available.
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

# Environment-specific configurations
ENV = os.getenv("ENVIRONMENT", "development").lower()

if ENV == "production":
    API_CONFIG["debug"] = False
    LOGGING_CONFIG["handlers"]["console"]["level"] = "WARNING"
    RATE_LIMITING_CONFIG["enabled"] = True
    SECURITY_CONFIG["cors_origins"] = os.getenv("CORS_ORIGINS", "").split(",") if os.getenv("CORS_ORIGINS") else []
elif ENV == "testing":
    API_CONFIG["debug"] = True
    HTTP_TIMEOUT = 5
    OAUTH_TOKEN_CACHE_BUFFER = 60
    HEALTH_CHECK_CONFIG["timeout_seconds"] = 2
elif ENV == "development":
    API_CONFIG["debug"] = True
    LOGGING_CONFIG["handlers"]["console"]["level"] = "DEBUG"

# Export main configuration getter
def get_api_config():
    """Get the complete API configuration."""
    return {
        "api": API_CONFIG,
        "postgresql": get_postgresql_config(),
        "his_api": HIS_API_CONFIG,
        "mobile_app": MOBILE_APP_CONFIG,
        "http": {"timeout": HTTP_TIMEOUT},
        "oauth": {"cache_buffer": OAUTH_TOKEN_CACHE_BUFFER},
        "patient_matching": PATIENT_MATCHING_CONFIG,
        "rate_limiting": RATE_LIMITING_CONFIG,
        "logging": LOGGING_CONFIG,
        "health_check": HEALTH_CHECK_CONFIG,
        "security": SECURITY_CONFIG,
        "monitoring": MONITORING_CONFIG,
        "environment": ENV
    }