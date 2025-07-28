#!/bin/bash
"""
Script to run all API tests with updated architecture - FIXED VERSION.
"""

echo "Running API tests with updated architecture..."

# Create logs directory if it doesn't exist
mkdir -p logs

# Set test environment variables
export ENVIRONMENT=testing
export DEBUG=true
export LOG_LEVEL=WARNING  # Reduce log noise in tests

# Database test configuration
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=test_medical_system
export POSTGRES_USER=test_user
export POSTGRES_PASSWORD=test_password

# HIS API test configuration
export YOTTADB_API_BASE="http://test-yottadb.com"
export FIREBIRD_API_BASE="http://test-firebird.com"
export YOTTADB_TOKEN_URL="http://test-yottadb.com/oauth/token"
export FIREBIRD_TOKEN_URL="http://test-firebird.com/oauth/token"

# OAuth test credentials
export YOTTADB_CLIENT_ID="test_yottadb_client"
export YOTTADB_CLIENT_SECRET="test_yottadb_secret"
export YOTTADB_USERNAME="test_yottadb_user"
export YOTTADB_PASSWORD="test_yottadb_pass"
export FIREBIRD_CLIENT_ID="test_firebird_client"
export FIREBIRD_CLIENT_SECRET="test_firebird_secret"
export FIREBIRD_USERNAME="test_firebird_user"
export FIREBIRD_PASSWORD="test_firebird_pass"

# Mobile app configuration
export MOBILE_APP_REGISTRATION_ENABLED=true
export MOBILE_APP_AUTO_REGISTER=true
export MOBILE_APP_REQUIRE_BOTH_HIS=false

# Run basic tests
echo "Running basic tests..."
python3 -m pytest src/api/tests/test_main.py -v --tb=short -W ignore::DeprecationWarning

# Run patient creation tests
echo "Running patient creation tests..."
python3 -m pytest src/api/tests/test_patient_creation.py -v --tb=short -W ignore::DeprecationWarning

# Run integration tests if they exist
if [ -f "src/api/tests/test_integration.py" ]; then
    echo "Running integration tests..."
    python3 -m pytest src/api/tests/test_integration.py -v --tb=short -W ignore::DeprecationWarning
fi

# Run OAuth tests if they exist
if [ -f "src/api/tests/test_oauth.py" ]; then
    echo "Running OAuth tests..."
    python3 -m pytest src/api/tests/test_oauth.py -v --tb=short -W ignore::DeprecationWarning
fi

# Run performance tests (excluding slow tests by default)
if [ -f "src/api/tests/test_performance.py" ]; then
    echo "Running performance tests..."
    python3 -m pytest src/api/tests/test_performance.py -v -m "not slow" --tb=short -W ignore::DeprecationWarning
fi

# Run all tests with coverage
echo "Running all tests with coverage..."
python3 -m pytest src/api/tests/ \
    --cov=src.api.main \
    --cov=src.api.database \
    --cov=src.api.config \
    --cov-report=html \
    --cov-report=term \
    -v --tb=short \
    -W ignore::DeprecationWarning

# Run slow tests separately if requested
if [ "$1" = "--include-slow" ]; then
    echo "Running slow/stress tests..."
    if [ -f "src/api/tests/test_performance.py" ]; then
        python3 -m pytest src/api/tests/test_performance.py -v -m "slow" --tb=short -W ignore::DeprecationWarning
    fi
fi

echo "API tests completed!"
echo "Coverage report generated in htmlcov/index.html"