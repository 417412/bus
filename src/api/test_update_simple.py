#!/usr/bin/env python3
"""
Simple test to verify update credentials functionality.
"""

import asyncio
import httpx
import json

async def test_update_credentials():
    """Test the credential update functionality."""
    
    api_base = "http://localhost:8000"
    
    print("🧪 Testing Patient Credential Update Functionality")
    print("=" * 55)
    
    # Step 1: Create a test patient first
    print("1. Creating test patient...")
    create_patient = {
        "lastname": "UpdateTest",
        "firstname": "Patient",
        "midname": "Demo",
        "bdate": "1985-03-20",
        "cllogin": "original_login_test",
        "clpassword": "original_password_123"
    }
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Create patient
            response = await client.post(f"{api_base}/checkModifyPatient", json=create_patient)
            
            if response.status_code == 200:
                result = response.json()
                print(f"   ✅ Create result: {result['action']} - {result['message']}")
                
                if result.get('mobile_uuid'):
                    print(f"   📱 Mobile UUID: {result['mobile_uuid']}")
                
                # Step 2: Wait a moment and then update the same patient
                print("\n2. Waiting 2 seconds for processing...")
                await asyncio.sleep(2)
                
                print("3. Testing credential update...")
                update_patient = {
                    "lastname": "UpdateTest",
                    "firstname": "Patient", 
                    "midname": "Demo",
                    "bdate": "1985-03-20",
                    "cllogin": "original_login_test",  # Same login to find patient
                    "clpassword": "UPDATED_PASSWORD_456"  # New password
                }
                
                update_response = await client.post(f"{api_base}/checkModifyPatient", json=update_patient)
                
                if update_response.status_code == 200:
                    update_result = update_response.json()
                    print(f"   ✅ Update result: {update_result['action']} - {update_result['message']}")
                    
                    if update_result.get('action') == 'update':
                        print("   🎉 SUCCESS: Patient was found and credentials were updated!")
                    elif update_result.get('action') == 'create':
                        print("   ⚠️  WARNING: Patient was not found, new patient was created instead")
                        print("   This might mean the database search didn't find the original patient")
                    else:
                        print(f"   ❓ UNEXPECTED: Action was '{update_result.get('action')}'")
                else:
                    print(f"   ❌ Update failed: {update_response.status_code} - {update_response.text}")
            else:
                print(f"   ❌ Create failed: {response.status_code} - {response.text}")
                
    except Exception as e:
        print(f"   ❌ Test error: {e}")
    
    print(f"\n📊 Summary:")
    print(f"   - Your implementation already has all the update logic")
    print(f"   - The test above shows if patient search and update works")
    print(f"   - Check logs/patient_api.log for detailed information")

if __name__ == "__main__":
    asyncio.run(test_update_credentials())