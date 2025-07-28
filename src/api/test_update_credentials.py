#!/usr/bin/env python3
"""
Test script for update credentials functionality.
This tests the scenario where a patient already exists in the database.
"""

import asyncio
import httpx
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path
parent_dir = Path(__file__).parent.parent.parent
sys.path.append(str(parent_dir))

from src.api.config import HIS_API_CONFIG

async def test_update_credentials_endpoints():
    """Test the updatePatients endpoints for both HIS systems."""
    
    print("🔄 Testing Update Credentials Endpoints")
    print("=" * 50)
    
    # Test data - this simulates updating credentials for an existing patient
    test_updates = [
        {
            "his_type": "yottadb",
            "hisnumber": "TEST123456",  # Replace with a real patient number from your system
            "new_credentials": {
                "cllogin": "updated_login_qms",
                "clpassword": "updated_password_qms"
            }
        },
        {
            "his_type": "firebird", 
            "hisnumber": "990652630",  # Use the patient ID we created earlier in debug
            "new_credentials": {
                "cllogin": "updated_login_ic",
                "clpassword": "updated_password_ic"
            }
        }
    ]
    
    for test_case in test_updates:
        his_type = test_case["his_type"]
        hisnumber = test_case["hisnumber"]
        credentials = test_case["new_credentials"]
        
        print(f"\n🔍 Testing {his_type.upper()} credential update")
        print(f"   Patient HIS#: {hisnumber}")
        print(f"   New login: {credentials['cllogin']}")
        print("-" * 40)
        
        config = HIS_API_CONFIG[his_type]
        
        # Step 1: Get OAuth token
        print("1. Getting OAuth token...")
        try:
            oauth_data = {
                "grant_type": "",
                "username": config["oauth"]["username"],
                "password": config["oauth"]["password"],
                "scope": "",
                "client_id": "",
                "client_secret": "",
            }
            
            async with httpx.AsyncClient(timeout=10.0) as client:
                token_response = await client.post(
                    config["oauth"]["token_url"],
                    data=oauth_data,
                    headers={"Content-Type": "application/x-www-form-urlencoded"}
                )
                
                if token_response.status_code != 200:
                    print(f"   ❌ OAuth failed: {token_response.status_code}")
                    continue
                
                token_data = token_response.json()
                access_token = token_data["access_token"]
                print(f"   ✅ OAuth successful: {access_token[:20]}...")
                
                # Step 2: Test the update endpoint
                print("2. Testing credential update...")
                update_url = config["base_url"] + config["credentials_endpoint"].format(hisnumber=hisnumber)
                print(f"   URL: {update_url}")
                
                headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json"
                }
                
                try:
                    update_response = await client.post(
                        update_url,
                        json=credentials,
                        headers=headers,
                        follow_redirects=True
                    )
                    
                    print(f"   Status: {update_response.status_code}")
                    print(f"   Response: {update_response.text[:200]}...")
                    
                    if update_response.status_code == 201:
                        print(f"   ✅ Update successful!")
                        try:
                            response_data = update_response.json()
                            print(f"   Response data: {response_data}")
                        except:
                            print(f"   (Response not JSON format)")
                    elif update_response.status_code == 404:
                        print(f"   ⚠️  Patient {hisnumber} not found - you may need to use a valid patient ID")
                    elif update_response.status_code == 405:
                        print(f"   ⚠️  Method not allowed - endpoint might not exist or need different HTTP method")
                    else:
                        print(f"   ❌ Update failed")
                        
                except Exception as e:
                    print(f"   ❌ Update request error: {e}")
                    
        except Exception as e:
            print(f"   ❌ OAuth error: {e}")

async def test_full_api_update_flow():
    """Test the full API flow for updating patient credentials."""
    
    print(f"\n🧪 Testing Full API Update Flow")
    print("=" * 50)
    
    # First, we need to create a patient in the database and HIS systems
    # Then test updating their credentials
    
    # Test patient data
    test_patient = {
        "lastname": "UpdateTest",
        "firstname": "Patient", 
        "midname": "API",
        "bdate": "1985-05-15",
        "cllogin": "original_login",
        "clpassword": "original_password"
    }
    
    updated_credentials = {
        "lastname": "UpdateTest",
        "firstname": "Patient",
        "midname": "API", 
        "bdate": "1985-05-15",
        "cllogin": "updated_login",  # Changed login
        "clpassword": "updated_password"  # Changed password
    }
    
    api_base = "http://localhost:8000"
    
    print("1. Creating initial patient...")
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Create patient
            create_response = await client.post(
                f"{api_base}/checkModifyPatient",
                json=test_patient
            )
            
            print(f"   Create status: {create_response.status_code}")
            if create_response.status_code == 200:
                create_data = create_response.json()
                print(f"   Create result: {create_data}")
                
                if create_data.get("action") == "create":
                    print("   ✅ Patient created successfully")
                    
                    # Wait a moment for data to be processed
                    print("2. Waiting 2 seconds for data processing...")
                    await asyncio.sleep(2)
                    
                    # Now test updating the same patient
                    print("3. Testing credential update...")
                    update_response = await client.post(
                        f"{api_base}/checkModifyPatient",
                        json=updated_credentials
                    )
                    
                    print(f"   Update status: {update_response.status_code}")
                    if update_response.status_code == 200:
                        update_data = update_response.json()
                        print(f"   Update result: {update_data}")
                        
                        if update_data.get("action") == "update":
                            print("   ✅ Patient credentials updated successfully!")
                        elif update_data.get("action") == "create":
                            print("   ⚠️  API created new patient instead of updating (patient not found in DB)")
                        else:
                            print(f"   ❓ Unexpected action: {update_data.get('action')}")
                    else:
                        print(f"   ❌ Update failed: {update_response.text}")
                else:
                    print(f"   ⚠️  Initial creation might have failed or patient was updated: {create_data}")
            else:
                print(f"   ❌ Create failed: {create_response.text}")
                
    except Exception as e:
        print(f"   ❌ API test error: {e}")

async def main():
    """Run all update credential tests."""
    
    print("🧪 Patient Credential Update Testing")
    print("=" * 60)
    print("This script tests both:")
    print("1. Direct HIS API credential update endpoints")
    print("2. Full API flow for patient credential updates")
    print("")
    
    # Test 1: Direct HIS endpoint testing
    await test_update_credentials_endpoints()
    
    # Test 2: Full API flow testing
    await test_full_api_update_flow()
    
    print(f"\n📋 Summary:")
    print(f"- Test 1 checks if HIS systems support credential updates")
    print(f"- Test 2 checks if our API correctly finds existing patients and updates them")
    print(f"- Check the logs for detailed information about each step")

if __name__ == "__main__":
    asyncio.run(main())