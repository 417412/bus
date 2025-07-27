#!/bin/bash
#Script to run all API tests with different configurations.

echo "Running API tests..."

# Create logs directory if it doesn't exist
mkdir -p logs

# Set test environment variables
export YOTTADB_API_BASE="http://test-yottadb.com"
export FIREBIRD_API_BASE="http://test-firebird.com"
export YOTTADB_TOKEN_URL="http://test-yottadb.com/oauth/token"
export FIREBIRD_TOKEN_URL="http://test-firebird.com/oauth/token"

# Run basic tests
echo "Running basic tests..."
python3 -m pytest src/api/tests/test_main.py -v

# Run patient creation tests
echo "Running patient creation tests..."
python3 -m pytest src/api/tests/test_patient_creation.py -v

# Run integration tests
echo "Running integration tests..."
python3 -m pytest src/api/tests/test_integration.py -v

# Run OAuth tests
echo "Running OAuth tests..."
python3 -m pytest src/api/tests/test_oauth.py -v

# Run performance tests (excluding slow tests by default)
echo "Running performance tests..."
python3 -m pytest src/api/tests/test_performance.py -v -m "not slow"

# Run all tests with coverage
echo "Running all tests with coverage..."
python3 -m pytest src/api/tests/ --cov=src.api.main --cov-report=html --cov-report=term -v

# Run slow tests separately if requested
if [ "$1" = "--include-slow" ]; then
    echo "Running slow/stress tests..."
    python3 -m pytest src/api/tests/test_performance.py -v -m "slow"
fi

echo "API tests completed!"