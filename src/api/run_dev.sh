#!/bin/bash

echo "Starting Patient Credential Management API in DEVELOPMENT mode..."

# =============================================================================
# Development Configuration
# =============================================================================

# HIS API Configuration (same as production but with debug)
export YOTTADB_API_BASE="http://192.168.156.43"
export YOTTADB_TOKEN_URL="http://192.168.156.43/token"
export YOTTADB_CLIENT_ID="admin"
export YOTTADB_CLIENT_SECRET="secret"
export YOTTADB_USERNAME="admin"
export YOTTADB_PASSWORD="secret"
export YOTTADB_SCOPE=""

export FIREBIRD_API_BASE="http://192.168.160.141"
export FIREBIRD_TOKEN_URL="http://192.168.160.141/token"
export FIREBIRD_CLIENT_ID="admin"
export FIREBIRD_CLIENT_SECRET="secret"
export FIREBIRD_USERNAME="admin"
export FIREBIRD_PASSWORD="secret"
export FIREBIRD_SCOPE=""

# Development-specific settings
export DEBUG="true"
export ENVIRONMENT="development"
export LOG_LEVEL="DEBUG"

# CORS for development (allow localhost)
export CORS_ENABLED="true"
export CORS_ORIGINS="http://localhost:3000,http://127.0.0.1:3000,http://localhost:8080"

# Mobile App Configuration
export MOBILE_APP_REGISTRATION_ENABLED="true"
export MOBILE_APP_AUTO_REGISTER="true"
export MOBILE_APP_REQUIRE_BOTH_HIS="false"

# Faster timeouts for development
export HTTP_TIMEOUT="10"
export OAUTH_TOKEN_CACHE_BUFFER="60"

# Enable health checks and metrics for development
export HEALTH_CHECK_HIS="true"
export METRICS_ENABLED="true"

# =============================================================================
# Setup
# =============================================================================

mkdir -p logs
mkdir -p state

# Check if we're in the right directory
if [ ! -f "main.py" ]; then
    echo "Error: main.py not found. Run this script from src/api directory"
    exit 1
fi

# Use uvicorn directly for development (with auto-reload)
if command -v uvicorn &> /dev/null; then
    echo "Running with uvicorn (auto-reload enabled)..."
    echo ""
    echo "Development server starting at:"
    echo "  - http://localhost:8000/docs (Swagger UI)"
    echo "  - http://localhost:8000/checkModifyPatient (Main endpoint)"
    echo ""
    
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload --log-level debug
else
    echo "uvicorn not found, using python directly..."
    python3 main.py
fi