#!/usr/bin/env python3
"""
Debug script to find the correct patient endpoints.
"""

import asyncio
import httpx
from datetime import datetime

async def test_firebird_endpoints():
    """Test different endpoint variations for Firebird."""
    base_url = "http://192.168.160.141"
    
    # First get OAuth token
    print("🔑 Getting OAuth token...")
    oauth_data = {
        "grant_type": "",
        "username": "admin",
        "password": "secret",
        "scope": "",
        "client_id": "",
        "client_secret": "",
    }
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        token_response = await client.post(
            f"{base_url}/token",
            data=oauth_data,
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        
        if token_response.status_code != 200:
            print(f"❌ Failed to get token: {token_response.status_code}")
            return
        
        token_data = token_response.json()
        access_token = token_data["access_token"]
        print(f"✅ Got token: {access_token[:20]}...")
        
        # Test different endpoint variations
        endpoints_to_test = [
            "/createPatients",
            "/createpatients", 
            "/create_patients",
            "/api/createPatients",
            "/api/v1/createPatients",
            "/patients/create",
            "/patients",
            "/patient/create",
            "/patient",
        ]
        
        test_patient = {
            "lastname": "TestPatient",
            "firstname": "Debug",
            "midname": "API",
            "bdate": "1990-01-01",
            "cllogin": "debug_login",
            "clpassword": "debug_password"
        }
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        print(f"\n🔍 Testing endpoints...")
        
        for endpoint in endpoints_to_test:
            url = f"{base_url}{endpoint}"
            print(f"\nTesting: {url}")
            
            try:
                # Try POST first (for creation)
                response = await client.post(url, json=test_patient, headers=headers, follow_redirects=True)
                print(f"  POST {response.status_code}: {response.text[:100]}...")
                
                if response.status_code in [200, 201]:
                    print(f"  ✅ POST SUCCESS at {endpoint}")
                    try:
                        data = response.json()
                        print(f"  Response data: {data}")
                        if 'pcode' in data:
                            print(f"  📋 Created patient with ID: {data['pcode']}")
                    except:
                        pass
                
                # Also try GET (to see if endpoint exists)
                get_response = await client.get(url, headers=headers, follow_redirects=True)
                print(f"  GET {get_response.status_code}: {get_response.text[:100]}...")
                
                if get_response.status_code in [200, 405]:  # 405 = Method Not Allowed is OK
                    print(f"  ✅ Endpoint exists: {endpoint}")
                
            except Exception as e:
                print(f"  ❌ Error: {e}")
        
        # Test with different HTTP methods
        print(f"\n🔧 Testing /createPatients with different methods...")
        create_url = f"{base_url}/createPatients"
        
        methods = ["POST", "PUT", "PATCH"]
        for method in methods:
            try:
                response = await client.request(
                    method, create_url, 
                    json=test_patient, 
                    headers=headers, 
                    follow_redirects=True
                )
                print(f"  {method} {response.status_code}: {response.text[:100]}...")
                
                if response.status_code in [200, 201]:
                    print(f"  ✅ {method} SUCCESS!")
                    
            except Exception as e:
                print(f"  ❌ {method} Error: {e}")

async def main():
    """Main debug function."""
    print("🔍 Debug Firebird Endpoints")
    print("=" * 50)
    
    await test_firebird_endpoints()
    
    print(f"\n💡 If you found a working endpoint, update your config.py:")
    print(f"   'create_endpoint': '/working_endpoint_here'")

if __name__ == "__main__":
    asyncio.run(main())