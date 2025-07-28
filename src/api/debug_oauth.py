#!/usr/bin/env python3
"""
Debug script to test the actual OAuth/Auth implementation with your HIS systems.
"""

import asyncio
import httpx
import base64
from datetime import datetime

# Your actual endpoints
YOTTADB_BASE = "http://192.168.156.43"
FIREBIRD_BASE = "http://192.168.160.141"

async def test_oauth_flow(base_url: str, system_name: str):
    """Test OAuth flow for a HIS system."""
    print(f"\n=== Testing {system_name} OAuth ===")
    
    # Test 1: Try OAuth2 flow
    print("1. Testing OAuth2 flow...")
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            oauth_data = {
                "grant_type": "password",
                "username": "admin",
                "password": "secret",
                "client_id": "admin",
                "client_secret": "secret",
            }
            
            response = await client.post(
                f"{base_url}/token",
                data=oauth_data,
                headers={"Content-Type": "application/x-www-form-urlencoded"}
            )
            
            print(f"   Status: {response.status_code}")
            print(f"   Response: {response.text[:200]}...")
            
            if response.status_code == 200:
                token_data = response.json()
                if "access_token" in token_data:
                    print(f"   ✅ OAuth2 SUCCESS - Token: {token_data['access_token'][:20]}...")
                    return "oauth2", token_data["access_token"]
                    
    except Exception as e:
        print(f"   ❌ OAuth2 failed: {e}")
    
    # Test 2: Try Basic Auth
    print("2. Testing Basic Auth...")
    try:
        # Create basic auth header
        credentials = base64.b64encode(b"admin:secret").decode('ascii')
        basic_auth_header = f"Basic {credentials}"
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{base_url}/",  # Try a simple endpoint
                headers={"Authorization": basic_auth_header}
            )
            
            print(f"   Status: {response.status_code}")
            print(f"   Response: {response.text[:200]}...")
            
            if response.status_code in [200, 404]:  # 404 is OK, means auth worked but endpoint doesn't exist
                print(f"   ✅ Basic Auth might work")
                return "basic", basic_auth_header
                
    except Exception as e:
        print(f"   ❌ Basic Auth failed: {e}")
    
    # Test 3: Try no auth
    print("3. Testing no auth...")
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(f"{base_url}/")
            print(f"   Status: {response.status_code}")
            print(f"   Response: {response.text[:200]}...")
            
            if response.status_code == 200:
                print(f"   ✅ No auth needed")
                return "none", None
                
    except Exception as e:
        print(f"   ❌ No auth failed: {e}")
    
    return None, None

async def test_patient_endpoints(base_url: str, system_name: str, auth_type: str, auth_token: str):
    """Test the actual patient endpoints."""
    print(f"\n=== Testing {system_name} Patient Endpoints ===")
    
    headers = {}
    if auth_type == "oauth2":
        headers["Authorization"] = f"Bearer {auth_token}"
    elif auth_type == "basic":
        headers["Authorization"] = auth_token
    
    headers["Content-Type"] = "application/json"
    
    # Test createPatients endpoint
    print("1. Testing /createPatients endpoint...")
    try:
        test_patient = {
            "lastname": "TestPatient",
            "firstname": "Debug",
            "midname": "API",
            "bdate": "1990-01-01",
            "cllogin": "debug_login",
            "clpassword": "debug_password"
        }
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{base_url}/createPatients",
                json=test_patient,
                headers=headers
            )
            
            print(f"   Status: {response.status_code}")
            print(f"   Response: {response.text}")
            
            if response.status_code == 201:
                try:
                    data = response.json()
                    print(f"   ✅ Patient created - HIS#: {data.get('pcode')}")
                    return data.get('pcode')
                except:
                    print(f"   ✅ Patient created (couldn't parse JSON)")
                    return "CREATED"
            else:
                print(f"   ❌ Creation failed")
                
    except Exception as e:
        print(f"   ❌ Create test failed: {e}")
    
    return None

async def main():
    """Main debug function."""
    print("🔍 Debug OAuth/Auth Implementation")
    print("=" * 50)
    print(f"Testing at: {datetime.now()}")
    
    # Test both systems
    systems = [
        (YOTTADB_BASE, "YottaDB"),
        (FIREBIRD_BASE, "Firebird")
    ]
    
    results = {}
    
    for base_url, system_name in systems:
        print(f"\n🔍 Testing {system_name}: {base_url}")
        
        # Test auth methods
        auth_type, auth_token = await test_oauth_flow(base_url, system_name)
        results[system_name] = {
            "auth_type": auth_type,
            "auth_token": auth_token,
            "available": auth_type is not None
        }
        
        # If auth works, test patient endpoints
        if auth_type:
            patient_id = await test_patient_endpoints(base_url, system_name, auth_type, auth_token)
            results[system_name]["test_patient_id"] = patient_id
    
    # Summary
    print(f"\n📋 SUMMARY")
    print("=" * 50)
    for system_name, result in results.items():
        status = "✅ WORKING" if result["available"] else "❌ FAILED"
        auth_info = f"Auth: {result['auth_type']}" if result["auth_type"] else "Auth: FAILED"
        print(f"{system_name:12} | {status:12} | {auth_info}")
    
    # Recommendations
    print(f"\n💡 RECOMMENDATIONS")
    print("=" * 50)
    
    working_systems = [name for name, result in results.items() if result["available"]]
    
    if len(working_systems) == 2:
        print("✅ Both systems are reachable!")
        
        auth_types = [results[name]["auth_type"] for name in working_systems]
        if all(auth == "oauth2" for auth in auth_types):
            print("🔧 Use OAuth2 flow in your API code")
        elif all(auth == "basic" for auth in auth_types):
            print("🔧 Consider switching to Basic Auth instead of OAuth2")
        else:
            print("🔧 Mixed auth types - you'll need to handle both")
    else:
        print("❌ Some systems are not reachable. Check network/configuration.")
    
    print(f"\n🛠️  Next steps:")
    print("1. Update your config.py based on working auth methods")
    print("2. Modify the OAuth implementation if needed")
    print("3. Re-run your tests")

if __name__ == "__main__":
    asyncio.run(main())