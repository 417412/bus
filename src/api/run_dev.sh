#!/bin/bash

echo "Starting Patient Credential Management API in DEVELOPMENT mode..."

# =============================================================================
# Development Configuration - CORRECTED FOR YOUR OAUTH IMPLEMENTATION
# =============================================================================

# HIS API Configuration
export YOTTADB_API_BASE="https://192.168.156.118:10443"
export YOTTADB_TOKEN_URL="https://192.168.156.118:10443/token"
export YOTTADB_CLIENT_ID=""                             # CHANGED: Empty string
export YOTTADB_CLIENT_SECRET=""                         # CHANGED: Empty string
export YOTTADB_USERNAME="admin"
export YOTTADB_PASSWORD="secret"
export YOTTADB_SCOPE=""

export FIREBIRD_API_BASE="http://192.168.160.141"
export FIREBIRD_TOKEN_URL="http://192.168.160.141/token"
export FIREBIRD_CLIENT_ID=""                            # CHANGED: Empty string
export FIREBIRD_CLIENT_SECRET=""                        # CHANGED: Empty string
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

# Install dependencies if needed
if [ ! -f "venv/bin/activate" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
else
    source venv/bin/activate
fi

# Quick OAuth test in development mode
echo "Quick OAuth test..."
python3 -c "
import asyncio, httpx
async def quick_test():
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.post('http://192.168.160.141/token', 
                data={'grant_type': '', 'username': 'admin', 'password': 'secret', 
                      'scope': '', 'client_id': '', 'client_secret': ''})
            print(f'OAuth quick test: {\"✅ OK\" if response.status_code == 200 else \"❌ FAIL\"}')
    except: print('OAuth quick test: ⚠️  Network issue')
asyncio.run(quick_test())
"

# Use uvicorn directly for development (with auto-reload)
if command -v uvicorn &> /dev/null; then
    echo "Running with uvicorn (auto-reload enabled)..."
    echo ""
    echo "Development server starting at:"
    echo "  - http://localhost:8000/docs (Swagger UI)"
    echo "  - http://localhost:8000/checkModifyPatient (Main endpoint)"
    echo "  - http://localhost:8000/test-oauth/firebird (Test OAuth)"
    echo ""
    
    # Run with uvicorn for auto-reload
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload --log-level debug
else
    echo "uvicorn not found, installing..."
    pip install uvicorn[standard]
    echo "Now running with uvicorn..."
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload --log-level debug
fi

# Deactivate virtual environment when done
deactivate