#!/bin/bash

echo "Starting Patient Credential Management API..."

# =============================================================================
# HIS API Configuration - CORRECTED FOR YOUR OAUTH IMPLEMENTATION
# =============================================================================

# YottaDB API Configuration
export YOTTADB_API_BASE="http://192.168.156.43"
export YOTTADB_TOKEN_URL="http://192.168.156.43/token"
export YOTTADB_CLIENT_ID=""                             # CHANGED: Empty string (not "admin")
export YOTTADB_CLIENT_SECRET=""                         # CHANGED: Empty string (not "secret") 
export YOTTADB_USERNAME="admin"
export YOTTADB_PASSWORD="secret"
export YOTTADB_SCOPE=""

# Firebird API Configuration - CORRECTED BASED ON YOUR WORKING CURL
export FIREBIRD_API_BASE="http://192.168.160.141"
export FIREBIRD_TOKEN_URL="http://192.168.160.141/token"
export FIREBIRD_CLIENT_ID=""                            # CHANGED: Empty string (not "admin")
export FIREBIRD_CLIENT_SECRET=""                        # CHANGED: Empty string (not "secret")
export FIREBIRD_USERNAME="admin"
export FIREBIRD_PASSWORD="secret"
export FIREBIRD_SCOPE=""

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
echo "  OAuth Config: client_id='${YOTTADB_CLIENT_ID}', client_secret='${YOTTADB_CLIENT_SECRET}'"

# Check if main.py exists
if [ ! -f "main.py" ]; then
    echo "Error: main.py not found in current directory"
    echo "Make sure you're running this script from the src/api directory"
    exit 1
fi

# Test OAuth configuration before starting the server
echo ""
echo "Testing OAuth configuration..."
python3 -c "
import asyncio
import httpx

async def test_oauth():
    try:
        # Test Firebird OAuth (since we know this works)
        oauth_data = {
            'grant_type': '',
            'username': 'admin',
            'password': 'secret',
            'scope': '',
            'client_id': '',
            'client_secret': '',
        }
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                'http://192.168.160.141/token',
                data=oauth_data,
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            )
            
            if response.status_code == 200:
                token_data = response.json()
                print(f'✅ OAuth test successful - got token: {token_data[\"access_token\"][:20]}...')
                return True
            else:
                print(f'❌ OAuth test failed: {response.status_code} - {response.text}')
                return False
                
    except Exception as e:
        print(f'❌ OAuth test error: {e}')
        return False

# Run the test
if asyncio.run(test_oauth()):
    print('OAuth configuration looks good!')
else:
    print('⚠️  OAuth configuration might have issues, but starting anyway...')
"

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
echo "  - OAuth test: http://localhost:8000/test-oauth/firebird"
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