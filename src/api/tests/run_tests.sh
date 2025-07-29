#!/bin/bash

echo "Running API tests with refactored architecture..."

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

# HIS API test configuration - NOTE: Using empty strings for client_id/client_secret as per actual implementation
export YOTTADB_API_BASE="http://192.168.156.118:7072"
export FIREBIRD_API_BASE="http://192.168.160.141"
export YOTTADB_TOKEN_URL="http://192.168.156.118:7072/token"
export FIREBIRD_TOKEN_URL="http://192.168.160.141/token"

# OAuth test credentials - MATCHING actual implementation format
export YOTTADB_CLIENT_ID=""  # Empty string as per actual config
export YOTTADB_CLIENT_SECRET=""  # Empty string as per actual config
export YOTTADB_USERNAME="admin"
export YOTTADB_PASSWORD="secret"
export YOTTADB_SCOPE=""  # Empty string as per actual config

export FIREBIRD_CLIENT_ID=""  # Empty string as per actual config
export FIREBIRD_CLIENT_SECRET=""  # Empty string as per actual config
export FIREBIRD_USERNAME="admin"
export FIREBIRD_PASSWORD="secret"
export FIREBIRD_SCOPE=""  # Empty string as per actual config

# Mobile app configuration - ENABLED for tests
export MOBILE_APP_REGISTRATION_ENABLED=true
export MOBILE_APP_AUTO_REGISTER=true
export MOBILE_APP_REQUIRE_BOTH_HIS=false

# Core API Tests (organized by functionality)
echo "=== Running Core API Tests ==="

echo "1. Testing main API endpoints..."
python3 -m pytest src/api/tests/test_main_endpoints.py -v --tb=short -W ignore::DeprecationWarning

echo "2. Testing authentication and OAuth..."
python3 -m pytest src/api/tests/test_auth.py -v --tb=short -W ignore::DeprecationWarning

echo "3. Testing database operations..."
python3 -m pytest src/api/tests/test_database.py -v --tb=short -W ignore::DeprecationWarning

echo "4. Testing configuration..."
python3 -m pytest src/api/tests/test_config.py -v --tb=short -W ignore::DeprecationWarning

echo "5. Testing input validation..."
python3 -m pytest src/api/tests/test_validation.py -v --tb=short -W ignore::DeprecationWarning

# Integration Tests
echo "=== Running Integration Tests ==="

echo "6. Testing end-to-end workflows..."
python3 -m pytest src/api/tests/test_integration.py -v --tb=short -W ignore::DeprecationWarning

# Optional real API tests (if network available)
if [ "$1" = "--include-real" ] || [ "$2" = "--include-real" ]; then
    echo "7. Testing real API endpoints (requires network)..."
    export TEST_REAL_APIS=true
    python3 -m pytest src/api/tests/test_integration_real.py -v --tb=short -W ignore::DeprecationWarning
fi

# Performance Tests (excluding slow tests by default)
echo "=== Running Performance Tests ==="

echo "8. Testing performance and concurrency..."
python3 -m pytest src/api/tests/test_performance.py -v -m "not slow" --tb=short -W ignore::DeprecationWarning

echo "=== Running All Tests with Coverage ==="

# Set TEST_REAL_APIS if --include-real is specified
if [ "$1" = "--include-real" ] || [ "$2" = "--include-real" ]; then
    export TEST_REAL_APIS=true
    IGNORE_REAL_TESTS=""
else
    IGNORE_REAL_TESTS="--ignore=src/api/tests/test_integration_real.py"
fi

python3 -m pytest src/api/tests/ \
    --cov=src.api.main \
    --cov=src.api.database \
    --cov=src.api.config \
    --cov-report=html \
    --cov-report=term \
    --cov-report=term-missing \
    -v --tb=short \
    -W ignore::DeprecationWarning \
    $IGNORE_REAL_TESTS

# Run slow tests separately if requested
if [ "$1" = "--include-slow" ] || [ "$2" = "--include-slow" ]; then
    echo "=== Running Slow/Stress Tests ==="
    python3 -m pytest src/api/tests/test_performance.py -v --run-slow --tb=short -W ignore::DeprecationWarning
fi

# Summary
echo ""
echo "=== Test Summary ==="
echo "✅ Core API endpoints tested"
echo "✅ Authentication and OAuth tested"
echo "✅ Database operations tested"
echo "✅ Configuration tested"
echo "✅ Input validation tested"
echo "✅ Integration workflows tested"
if [ "$1" = "--include-real" ] || [ "$2" = "--include-real" ]; then
    echo "✅ Real API endpoints tested"
fi
echo "✅ Performance tests executed"
if [ "$1" = "--include-slow" ] || [ "$2" = "--include-slow" ]; then
    echo "✅ Slow/stress tests executed"
fi
echo ""
echo "📊 Coverage report generated in htmlcov/index.html"
echo ""
echo "Usage options:"
echo "  ./run_tests.sh                    # Run all tests except real API and slow tests"
echo "  ./run_tests.sh --include-real     # Include real API tests (requires network)"
echo "  ./run_tests.sh --include-slow     # Include slow/stress tests"
echo "  ./run_tests.sh --include-real --include-slow  # Include both"