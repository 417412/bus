#!/bin/bash

echo "Starting Patient Credential Management API..."

# =============================================================================
# HIS API Configuration
# =============================================================================

# YottaDB API Configuration (based on your actual setup)
export YOTTADB_API_BASE="http://192.168.156.43"
export YOTTADB_TOKEN_URL="http://192.168.156.43/token"  # Fixed: /token not /oauth/token
export YOTTADB_CLIENT_ID="admin"                        # Fixed: admin instead of placeholder
export YOTTADB_CLIENT_SECRET="secret"                   # Fixed: secret instead of placeholder
export YOTTADB_USERNAME="admin"                         # Fixed: admin instead of placeholder
export YOTTADB_PASSWORD="secret"                        # Fixed: secret instead of placeholder
export YOTTADB_SCOPE=""                                 # Fixed: empty scope as mentioned

# Firebird API Configuration (based on your actual setup)
export FIREBIRD_API_BASE="http://192.168.160.141"       # Fixed: actual IP from config
export FIREBIRD_TOKEN_URL="http://192.168.160.141/token" # Fixed: /token not /oauth/token
export FIREBIRD_CLIENT_ID="admin"                       # Fixed: admin instead of placeholder
export FIREBIRD_CLIENT_SECRET="secret"                  # Fixed: secret instead of placeholder
export FIREBIRD_USERNAME="admin"                        # Fixed: admin instead of placeholder
export FIREBIRD_PASSWORD="secret"                       # Fixed: secret instead of placeholder
export FIREBIRD_SCOPE=""                                # Fixed: empty scope as mentioned

# =============================================================================
# PostgreSQL Database Configuration (optional overrides)
# =============================================================================
# Note: The API will primarily use settings.py configuration, but you can override here if needed

# export POSTGRES_HOST="localhost"
# export POSTGRES_PORT="5432"
# export POSTGRES_DB="medical_system"
# export POSTGRES_USER="medapp_user"
# export POSTGRES_PASSWORD="your_password_here"

# =============================================================================
# Mobile App Configuration
# =============================================================================

export MOBILE_APP_REGISTRATION_ENABLED="true"
export MOBILE_APP_AUTO_REGISTER="true"
export MOBILE_APP_REQUIRE_BOTH_HIS="false"

# =============================================================================
# API Configuration
# =============================================================================

export DEBUG="false"
export ENVIRONMENT="production"
export LOG_LEVEL="INFO"

# CORS Configuration
export CORS_ENABLED="true"
export CORS_ORIGINS="*"  # Change to specific domains in production

# Security Configuration
export API_KEY=""  # Empty means no API key required

# HTTP Configuration
export HTTP_TIMEOUT="30"
export OAUTH_TOKEN_CACHE_BUFFER="300"

# =============================================================================
# Health Check and Monitoring
# =============================================================================

export HEALTH_CHECK_HIS="false"  # Set to true to check HIS endpoints in health check
export METRICS_ENABLED="false"

# =============================================================================
# Development Overrides (uncomment for development)
# =============================================================================

# export DEBUG="true"
# export ENVIRONMENT="development" 
# export LOG_LEVEL="DEBUG"
# export CORS_ORIGINS="http://localhost:3000,http://127.0.0.1:3000"

# =============================================================================
# Setup and Run
# =============================================================================

# Create necessary directories
echo "Creating required directories..."
mkdir -p logs
mkdir -p state

# Check if Python virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install/update dependencies
echo "Installing dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt

# Verify configuration
echo "Configuration Summary:"
echo "  YottaDB API: ${YOTTADB_API_BASE}"
echo "  Firebird API: ${FIREBIRD_API_BASE}"
echo "  Environment: ${ENVIRONMENT:-development}"
echo "  Debug Mode: ${DEBUG:-false}"
echo "  Mobile App Registration: ${MOBILE_APP_REGISTRATION_ENABLED:-true}"

# Check if main.py exists
if [ ! -f "main.py" ]; then
    echo "Error: main.py not found in current directory"
    echo "Make sure you're running this script from the src/api directory"
    exit 1
fi

# Optional: Check database connectivity (uncomment to enable)
# echo "Testing database connection..."
# python3 -c "
# import sys
# sys.path.append('../..')
# try:
#     from src.api.config import get_postgresql_config
#     config = get_postgresql_config()
#     print(f'Database: {config[\"host\"]}:{config[\"port\"]}/{config[\"database\"]}')
# except Exception as e:
#     print(f'Database config check failed: {e}')
# "

# Run the application
echo ""
echo "=========================================="
echo "Starting Patient Credential Management API"
echo "=========================================="
echo ""
echo "API will be available at:"
echo "  - Main endpoint: http://localhost:8000/checkModifyPatient"
echo "  - Health check: http://localhost:8000/health"
echo "  - API docs: http://localhost:8000/docs"
echo "  - Statistics: http://localhost:8000/stats"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

# Run with proper error handling
if python3 main.py; then
    echo "API server stopped normally"
else
    echo "API server stopped with error (exit code: $?)"
    echo "Check logs/patient_api.log for details"
    exit 1
fi

# Deactivate virtual environment
deactivate