#!/bin/bash

echo "Patient Credential Management API - Configuration Checker"
echo "========================================================="

# Check Python and dependencies
echo "1. Checking Python environment..."
python3 --version
echo "   Dependencies:"
pip list | grep -E "(fastapi|uvicorn|pydantic|httpx|asyncpg)" || echo "   Some dependencies missing - run: pip install -r requirements.txt"

echo ""

# Check directory structure
echo "2. Checking directory structure..."
[ -f "main.py" ] && echo "   ✓ main.py found" || echo "   ✗ main.py missing"
[ -f "config.py" ] && echo "   ✓ config.py found" || echo "   ✗ config.py missing"
[ -f "database.py" ] && echo "   ✓ database.py found" || echo "   ✗ database.py missing"
[ -f "requirements.txt" ] && echo "   ✓ requirements.txt found" || echo "   ✗ requirements.txt missing"
[ -d "logs" ] && echo "   ✓ logs directory exists" || echo "   ! logs directory will be created"

echo ""

# Check environment variables
echo "3. Checking environment configuration..."
echo "   YottaDB API: ${YOTTADB_API_BASE:-NOT SET}"
echo "   Firebird API: ${FIREBIRD_API_BASE:-NOT SET}"
echo "   Debug Mode: ${DEBUG:-false}"
echo "   Environment: ${ENVIRONMENT:-development}"

echo ""

# Test configuration loading
echo "4. Testing configuration loading..."
python3 -c "
import sys
sys.path.append('../..')
try:
    from src.api.config import get_api_config, validate_config
    config = get_api_config()
    issues = validate_config()
    
    print('   ✓ Configuration loaded successfully')
    print(f'   Database: {config[\"postgresql\"][\"host\"]}:{config[\"postgresql\"][\"port\"]}')
    
    if issues:
        print('   Configuration issues:')
        for issue in issues:
            print(f'     ! {issue}')
    else:
        print('   ✓ No configuration issues found')
        
except Exception as e:
    print(f'   ✗ Configuration error: {e}')
"

echo ""

# Test database connectivity
echo "5. Testing database connectivity..."
python3 -c "
import sys, asyncio
sys.path.append('../..')
try:
    from src.api.database import initialize_database, close_database, get_database_health
    
    async def test_db():
        if await initialize_database():
            health = await get_database_health()
            print(f'   ✓ Database connection successful')
            print(f'   Status: {health[\"status\"]}')
            if 'patients_count' in health:
                print(f'   Patients: {health[\"patients_count\"]}')
                print(f'   Mobile users: {health[\"mobile_users_count\"]}')
            await close_database()
        else:
            print('   ✗ Database connection failed')
    
    asyncio.run(test_db())
    
except Exception as e:
    print(f'   ✗ Database test error: {e}')
"

echo ""
echo "Configuration check complete!"
echo ""
echo "To start the API:"
echo "  Development: ./run_dev.sh"
echo "  Production:  ./run.sh"