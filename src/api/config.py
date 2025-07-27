"""
API-specific configuration settings.
"""

import os

# API Configuration
API_CONFIG = {
    "title": "Patient Credential Management API",
    "description": "API for managing patient credentials across HIS systems with OAuth authentication and patient creation",
    "version": "1.1.0",
    "host": "0.0.0.0",
    "port": 8000,
    "debug": os.getenv("DEBUG", "false").lower() == "true"
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

# HTTP Client Configuration
HTTP_TIMEOUT = 30

# OAuth Configuration
OAUTH_TOKEN_CACHE_BUFFER = 300  # 5 minutes buffer before token expiry

# Logging Configuration
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        },
    },
    "handlers": {
        "default": {
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
        "file": {
            "formatter": "default",
            "class": "logging.FileHandler",
            "filename": "logs/api.log",
        },
    },
    "root": {
        "level": "INFO",
        "handlers": ["default", "file"],
    },
}