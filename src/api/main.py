#!/usr/bin/env python3
"""
FastAPI application for patient credential management.
Provides endpoints for checking and modifying patient credentials across HIS systems.
"""

import os
import sys
from datetime import datetime, timedelta, date
from typing import Optional, Dict, Any
import logging
import uuid as uuid_module
from contextlib import asynccontextmanager

# Add the parent directory to the path
parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent_dir)
from src.api.config import HIS_API_CONFIG, MOBILE_APP_CONFIG, get_postgresql_config, setup_api_logger

from fastapi import FastAPI, HTTPException, status, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
import httpx
import asyncio

# Import configuration and database
from src.api.config import (
    API_CONFIG, HIS_API_CONFIG, MOBILE_APP_CONFIG, SECURITY_CONFIG,
    get_api_config, validate_config, setup_api_logger
)
from src.api.database import (
    initialize_database, close_database, get_database_health,
    get_patient_repository, PatientRepository
)

# Configure logging
logger = setup_api_logger("patient_api")

# Validate configuration on startup
config_issues = validate_config()
if config_issues:
    logger.warning(f"Configuration issues detected: {config_issues}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan events for FastAPI application."""
    # Startup
    try:
        logger.info("Initializing Patient Credential Management API...")
        
        # Initialize database
        if not await initialize_database():
            raise Exception("Database initialization failed")
        
        logger.info("API initialization completed successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize API: {e}")
        raise e
    
    yield
    
    # Shutdown
    logger.info("Shutting down Patient Credential Management API...")
    await close_database()
    logger.info("API shutdown completed")

# Create FastAPI app with lifespan
app = FastAPI(
    title=API_CONFIG["title"],
    description=API_CONFIG["description"],
    version=API_CONFIG["version"],
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Add CORS middleware
if SECURITY_CONFIG["cors_enabled"]:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=SECURITY_CONFIG["cors_origins"],
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE"],
        allow_headers=["*"],
    )

# Pydantic models with V2 validators
class PatientCredentialRequest(BaseModel):
    """Request model for patient credential operations."""
    lastname: str = Field(..., description="Patient's last name")
    firstname: str = Field(..., description="Patient's first name") 
    midname: Optional[str] = Field(None, description="Patient's middle name")
    bdate: str = Field(..., description="Patient's birth date (YYYY-MM-DD)")
    cllogin: str = Field(..., description="Patient's login")
    clpassword: str = Field(..., description="Patient's password")
    
    @field_validator('bdate')
    @classmethod
    def validate_bdate(cls, v: str) -> str:
        """Validate birth date format."""
        try:
            datetime.strptime(v, '%Y-%m-%d')
            return v
        except ValueError:
            raise ValueError('Birth date must be in YYYY-MM-DD format')
    
    def get_bdate_as_date(self) -> date:
        """Convert bdate string to date object for database operations."""
        return datetime.strptime(self.bdate, '%Y-%m-%d').date()

class PatientResponse(BaseModel):
    """Response model for patient operations."""
    success: str = Field(..., description="Operation success status")
    message: Optional[str] = Field(None, description="Additional message")
    action: Optional[str] = Field(None, description="Action performed (update/create)")
    mobile_uuid: Optional[str] = Field(None, description="Mobile app user UUID if created")

# Global OAuth token cache and locks
oauth_tokens = {}
oauth_locks = {}  # Per-system locks

# Dependency to get patient repository
def get_patient_repo() -> PatientRepository:
    return get_patient_repository()

async def get_oauth_token(his_type: str) -> Optional[str]:
    """
    Get OAuth token for specified HIS system with proper concurrency control.
    Uses token caching to avoid repeated authentication.
    """
    try:
        # Get or create a lock for this HIS system
        if his_type not in oauth_locks:
            oauth_locks[his_type] = asyncio.Lock()
        
        async with oauth_locks[his_type]:
            # Check if we have a valid cached token (inside the lock)
            cache_key = f"{his_type}_token"
            cache_expiry_key = f"{his_type}_token_expiry"
            
            if (cache_key in oauth_tokens and 
                cache_expiry_key in oauth_tokens and 
                datetime.now() < oauth_tokens[cache_expiry_key]):
                logger.debug(f"Using cached OAuth token for {his_type.upper()}")
                return oauth_tokens[cache_key]
            
            # Get new token (still inside the lock)
            config = HIS_API_CONFIG[his_type]["oauth"]
            token_url = config["token_url"]
            
            # Prepare OAuth request data - FIXED to match your working curl
            oauth_data = {
                "grant_type": "",  # Empty as in your curl
                "username": config["username"],
                "password": config["password"], 
                "scope": "",  # Empty as in your curl
                "client_id": "",  # Empty as in your curl
                "client_secret": "",  # Empty as in your curl
            }
            
            logger.info(f"Requesting OAuth token from {his_type.upper()}: {token_url}")
            
            # ENHANCED: Better error handling and network configuration
            try:
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(30.0, connect=10.0),  # Separate connect timeout
                    follow_redirects=True,  # Follow redirects automatically
                    limits=httpx.Limits(max_connections=10, max_keepalive_connections=5)
                ) as client:
                    response = await client.post(
                        token_url,
                        data=oauth_data,
                        headers={"Content-Type": "application/x-www-form-urlencoded"}
                    )
                    
                    if response.status_code == 200:
                        token_response = response.json()
                        access_token = token_response.get("access_token")
                        expires_in = token_response.get("expires_in", 3600)  # Default 1 hour
                        
                        if access_token:
                            # Cache the token with expiry buffer (subtract 5 minutes for safety)
                            expiry_time = datetime.now() + timedelta(seconds=expires_in - 300)
                            oauth_tokens[cache_key] = access_token
                            oauth_tokens[cache_expiry_key] = expiry_time
                            
                            logger.info(f"Successfully obtained OAuth token for {his_type.upper()}, expires at {expiry_time}")
                            return access_token
                        else:
                            logger.error(f"OAuth response missing access_token for {his_type.upper()}")
                            return None
                    else:
                        logger.error(f"OAuth authentication failed for {his_type.upper()}: {response.status_code} - {response.text}")
                        return None
                        
            except httpx.ConnectError as e:
                logger.error(f"Connection failed to {his_type.upper()} OAuth endpoint {token_url}: {e}")
                return None
            except httpx.TimeoutException as e:
                logger.error(f"Timeout connecting to {his_type.upper()} OAuth endpoint {token_url}: {e}")
                return None
            except Exception as e:
                logger.error(f"Network error getting OAuth token for {his_type.upper()}: {e}")
                return None
                    
    except Exception as e:
        logger.error(f"Error getting OAuth token for {his_type.upper()}: {e}")
        return None

async def update_his_credentials(his_type: str, hisnumber: str, cllogin: str, clpassword: str) -> bool:
    """
    Update patient credentials in specified HIS system via authenticated API call.
    
    Args:
        his_type: 'yottadb' or 'firebird'
        hisnumber: Patient's HIS number
        cllogin: New login
        clpassword: New password
        
    Returns:
        True if successful (HTTP 201), False otherwise
    """
    try:
        # Step 1: Get OAuth token
        access_token = await get_oauth_token(his_type)
        if not access_token:
            logger.error(f"Failed to obtain OAuth token for {his_type.upper()}")
            return False
        
        # Step 2: Prepare API call
        config = HIS_API_CONFIG[his_type]
        url = config["base_url"] + config["credentials_endpoint"].format(hisnumber=hisnumber)
        
        payload = {
            "cllogin": cllogin,
            "clpassword": clpassword
        }
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        logger.info(f"Updating {his_type.upper()} credentials for patient {hisnumber}")
        logger.debug(f"{his_type.upper()} update URL: {url}")
        
        # Step 3: Make authenticated API call with enhanced error handling
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, connect=10.0),
                follow_redirects=True
            ) as client:
                response = await client.post(url, json=payload, headers=headers)
                
                if response.status_code == 201:
                    logger.info(f"Successfully updated {his_type.upper()} credentials for patient {hisnumber}")
                    return True
                elif response.status_code == 401:
                    # Token might be expired, clear cache and retry once
                    cache_key = f"{his_type}_token"
                    cache_expiry_key = f"{his_type}_token_expiry"
                    if cache_key in oauth_tokens:
                        del oauth_tokens[cache_key]
                    if cache_expiry_key in oauth_tokens:
                        del oauth_tokens[cache_expiry_key]
                    
                    logger.warning(f"OAuth token expired for {his_type.upper()}, retrying with new token")
                    
                    # Get new token and retry
                    new_token = await get_oauth_token(his_type)
                    if new_token:
                        headers["Authorization"] = f"Bearer {new_token}"
                        retry_response = await client.post(url, json=payload, headers=headers)
                        
                        if retry_response.status_code == 201:
                            logger.info(f"Successfully updated {his_type.upper()} credentials for patient {hisnumber} (retry)")
                            return True
                        else:
                            logger.error(f"{his_type.upper()} credential update failed on retry: {retry_response.status_code} - {retry_response.text}")
                            return False
                    else:
                        logger.error(f"Failed to get new OAuth token for {his_type.upper()} retry")
                        return False
                else:
                    logger.error(f"{his_type.upper()} credential update failed: {response.status_code} - {response.text}")
                    return False
                    
        except httpx.ConnectError as e:
            logger.error(f"Connection failed to {his_type.upper()} update endpoint: {e}")
            return False
        except httpx.TimeoutException as e:
            logger.error(f"Timeout updating {his_type.upper()} credentials: {e}")
            return False
        except Exception as e:
            logger.error(f"Network error updating {his_type.upper()} credentials: {e}")
            return False
                
    except Exception as e:
        logger.error(f"Error updating {his_type.upper()} credentials for patient {hisnumber}: {e}")
        return False

async def create_his_patient(his_type: str, patient_data: PatientCredentialRequest) -> dict:
    """
    Create patient in specified HIS system via authenticated API call.
    
    Args:
        his_type: 'yottadb' or 'firebird'
        patient_data: Patient data for creation
        
    Returns:
        Dict with success status and hisnumber if successful
    """
    try:
        # Step 1: Get OAuth token
        access_token = await get_oauth_token(his_type)
        if not access_token:
            logger.error(f"Failed to obtain OAuth token for {his_type.upper()} patient creation")
            return {"success": False, "error": "OAuth authentication failed"}
        
        # Step 2: Prepare API call
        config = HIS_API_CONFIG[his_type]
        url = config["base_url"] + config["create_endpoint"]
        
        # Prepare payload - same structure as input request
        payload = {
            "lastname": patient_data.lastname,
            "firstname": patient_data.firstname,
            "midname": patient_data.midname,
            "bdate": patient_data.bdate,
            "cllogin": patient_data.cllogin,
            "clpassword": patient_data.clpassword
        }
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        logger.info(f"Creating patient in {his_type.upper()}: {patient_data.lastname}, {patient_data.firstname}")
        logger.debug(f"{his_type.upper()} create URL: {url}")
        
        # Step 3: Make authenticated API call with enhanced error handling
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, connect=10.0),
                follow_redirects=True  # FIXED: This should handle the 307 redirect
            ) as client:
                response = await client.post(url, json=payload, headers=headers)
                
                if response.status_code == 201:
                    try:
                        response_data = response.json()
                        hisnumber = response_data.get("pcode")
                        fullname = response_data.get("fullname", "")
                        message = response_data.get("message", "Patient created successfully")
                        
                        logger.info(f"Successfully created patient in {his_type.upper()}: {fullname} (HIS#{hisnumber})")
                        return {
                            "success": True,
                            "hisnumber": hisnumber,
                            "fullname": fullname,
                            "message": message
                        }
                    except Exception as json_error:
                        logger.warning(f"Failed to parse {his_type.upper()} response JSON, but creation was successful: {json_error}")
                        return {"success": True, "message": "Patient created successfully"}
                        
                elif response.status_code == 401:
                    # Token might be expired, clear cache and retry once
                    cache_key = f"{his_type}_token"
                    cache_expiry_key = f"{his_type}_token_expiry"
                    if cache_key in oauth_tokens:
                        del oauth_tokens[cache_key]
                    if cache_expiry_key in oauth_tokens:
                        del oauth_tokens[cache_expiry_key]
                    
                    logger.warning(f"OAuth token expired for {his_type.upper()}, retrying patient creation with new token")
                    
                    # Get new token and retry
                    new_token = await get_oauth_token(his_type)
                    if new_token:
                        headers["Authorization"] = f"Bearer {new_token}"
                        retry_response = await client.post(url, json=payload, headers=headers)
                        
                        if retry_response.status_code == 201:
                            try:
                                retry_data = retry_response.json()
                                hisnumber = retry_data.get("pcode")
                                fullname = retry_data.get("fullname", "")
                                message = retry_data.get("message", "Patient created successfully")
                                
                                logger.info(f"Successfully created patient in {his_type.upper()} (retry): {fullname} (HIS#{hisnumber})")
                                return {
                                    "success": True,
                                    "hisnumber": hisnumber,
                                    "fullname": fullname,
                                    "message": message
                                }
                            except Exception as json_error:
                                logger.warning(f"Failed to parse {his_type.upper()} retry response JSON, but creation was successful: {json_error}")
                                return {"success": True, "message": "Patient created successfully (retry)"}
                        else:
                            logger.error(f"{his_type.upper()} patient creation failed on retry: {retry_response.status_code} - {retry_response.text}")
                            return {"success": False, "error": f"Creation failed on retry: {retry_response.status_code}"}
                    else:
                        logger.error(f"Failed to get new OAuth token for {his_type.upper()} patient creation retry")
                        return {"success": False, "error": "Failed to refresh OAuth token"}
                elif response.status_code == 307:
                    # This shouldn't happen with follow_redirects=True, but just in case
                    logger.error(f"{his_type.upper()} returned redirect (307) - this should be handled automatically")
                    return {"success": False, "error": "Redirect not followed properly"}
                else:
                    logger.error(f"{his_type.upper()} patient creation failed: {response.status_code} - {response.text}")
                    return {"success": False, "error": f"Creation failed: {response.status_code} - {response.text}"}
                    
        except httpx.ConnectError as e:
            logger.error(f"Connection failed to {his_type.upper()} create endpoint: {e}")
            return {"success": False, "error": f"Connection failed: {str(e)}"}
        except httpx.TimeoutException as e:
            logger.error(f"Timeout creating patient in {his_type.upper()}: {e}")
            return {"success": False, "error": f"Timeout: {str(e)}"}
        except Exception as e:
            logger.error(f"Network error creating patient in {his_type.upper()}: {e}")
            return {"success": False, "error": f"Network error: {str(e)}"}
                
    except Exception as e:
        logger.error(f"Error creating patient in {his_type.upper()}: {e}")
        return {"success": False, "error": str(e)}

async def register_mobile_app_user_api(hisnumber_qms: Optional[str] = None, 
                                      hisnumber_infoclinica: Optional[str] = None,
                                      patient_repo: PatientRepository = None) -> Optional[str]:
    """
    Register a mobile app user and return the UUID.
    This should be called when both HIS patient creation calls succeed.
    """
    try:
        if not MOBILE_APP_CONFIG["registration_enabled"]:
            logger.info("Mobile app registration is disabled")
            return None
        
        if not hisnumber_qms and not hisnumber_infoclinica:
            logger.error("Cannot register mobile app user without at least one HIS number")
            return None
        
        if MOBILE_APP_CONFIG["require_both_his"] and (not hisnumber_qms or not hisnumber_infoclinica):
            logger.error("Mobile app configuration requires both HIS numbers")
            return None
        
        if not patient_repo:
            patient_repo = get_patient_repository()
        
        mobile_uuid = await patient_repo.register_mobile_app_user(hisnumber_qms, hisnumber_infoclinica)
        
        if mobile_uuid:
            logger.info(f"Successfully registered mobile app user: {mobile_uuid}")
        else:
            logger.error("Failed to register mobile app user")
        
        return mobile_uuid
        
    except Exception as e:
        logger.error(f"Error registering mobile app user: {e}")
        return None

@app.post("/checkModifyPatient")
async def check_modify_patient(request: PatientCredentialRequest):
    """
    Enhanced endpoint with mobile app registration and login/password updates.
    
    Flow:
    1. Check if patient exists with this login
    2. If not, create in both HIS systems and register mobile app user
    3. If exists, check which HIS numbers are missing
    4. If both HIS numbers exist, update login/password in both systems
    5. If missing HIS numbers, create patient in missing HIS
    6. Register/update mobile app user for proper matching
    """
    try:
        patient_repo = get_patient_repository()
        
        # Step 1: Check if patient exists using all available data
        existing_patient = await patient_repo.find_patient_by_credentials(
            request.lastname,
            request.firstname, 
            request.midname,
            request.bdate,
            request.cllogin,
            request.clpassword
        )
        
        qms_result = None
        ic_result = None
        mobile_uuid = None
        
        if not existing_patient:
            # Step 2: Patient doesn't exist - create in both HIS systems
            logger.info(f"Patient not found, creating in both HIS systems for login: {request.cllogin}")
            
            # Create in both HIS systems
            qms_result = await create_his_patient('yottadb', request)
            ic_result = await create_his_patient('infoclinica', request)
            
            # Get HIS numbers from results
            qms_hisnumber = qms_result.get("hisnumber") if qms_result and qms_result.get("success") else None
            ic_hisnumber = ic_result.get("hisnumber") if ic_result and ic_result.get("success") else None
            
            # Register mobile app user with both HIS numbers for proper matching
            if qms_hisnumber or ic_hisnumber:
                mobile_uuid = await register_mobile_app_user_api(
                    hisnumber_qms=qms_hisnumber,
                    hisnumber_infoclinica=ic_hisnumber,
                    patient_repo=patient_repo
                )
                logger.info(f"Registered mobile app user: {mobile_uuid} with QMS: {qms_hisnumber}, IC: {ic_hisnumber}")
            
        else:
            # Step 3: Patient exists - check which HIS numbers are missing
            logger.info(f"Patient found: {existing_patient.get('uuid')}")
            
            has_qms = bool(existing_patient.get('hisnumber_qms'))
            has_ic = bool(existing_patient.get('hisnumber_infoclinica'))
            
            current_qms = existing_patient.get('hisnumber_qms')
            current_ic = existing_patient.get('hisnumber_infoclinica')
            
            if has_qms and has_ic:
                # Step 4: Both HIS numbers exist - update login/password in both systems
                logger.info("Both HIS numbers exist, updating login/password in both systems")
                
                # Update QMS login/password
                qms_result = await update_his_patient_credentials('yottadb', current_qms, request)
                
                # Update Infoclinica login/password  
                ic_result = await update_his_patient_credentials('infoclinica', current_ic, request)
                
                # Update the database with new credentials
                await patient_repo.update_patient_credentials(
                    existing_patient.get('uuid'),
                    qms_login=request.cllogin,
                    qms_password=request.clpassword,
                    ic_login=request.cllogin,
                    ic_password=request.clpassword
                )
                
                mobile_uuid = existing_patient.get('uuid')  # Use existing patient UUID
                
            else:
                # Step 5: Create patient in missing HIS systems
                if not has_qms:
                    logger.info("Creating patient in QMS (missing)")
                    qms_result = await create_his_patient('yottadb', request)
                    if qms_result and qms_result.get("success"):
                        current_qms = qms_result.get("hisnumber")
                
                if not has_ic:
                    logger.info("Creating patient in Infoclinica (missing)")
                    ic_result = await create_his_patient('infoclinica', request)
                    if ic_result and ic_result.get("success"):
                        current_ic = ic_result.get("hisnumber")
                
                # Step 6: Update/create mobile app user for proper matching
                existing_mobile_user = await patient_repo.find_mobile_app_user_by_patient_uuid(
                    existing_patient.get('uuid')
                )
                
                if existing_mobile_user:
                    # Update existing mobile app user
                    mobile_uuid = await patient_repo.update_mobile_app_user_hisnumbers(
                        existing_mobile_user.get('uuid'),
                        hisnumber_qms=current_qms,
                        hisnumber_infoclinica=current_ic
                    )
                    logger.info(f"Updated mobile app user: {mobile_uuid}")
                else:
                    # Create new mobile app user entry for matching
                    mobile_uuid = await register_mobile_app_user_api(
                        hisnumber_qms=current_qms,
                        hisnumber_infoclinica=current_ic,
                        patient_repo=patient_repo
                    )
                    logger.info(f"Created mobile app user for existing patient: {mobile_uuid}")
        
        # Prepare response
        response_data = {
            "success": True,
            "patient_uuid": existing_patient.get('uuid') if existing_patient else None,
            "mobile_uuid": mobile_uuid,
            "qms_result": qms_result,
            "infoclinica_result": ic_result,
            "message": "Patient processing completed"
        }
        
        # Add HIS numbers to response
        if existing_patient:
            response_data.update({
                "hisnumber_qms": existing_patient.get('hisnumber_qms'),
                "hisnumber_infoclinica": existing_patient.get('hisnumber_infoclinica'),
                "operation": "both_exist_updated" if (has_qms and has_ic) else "missing_created"
            })
        else:
            response_data["operation"] = "new_patient_created"
        
        return response_data
        
    except Exception as e:
        logger.error(f"Error in check_modify_patient: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": "Failed to process patient"
        }


async def update_his_patient_credentials(system_name: str, hisnumber: str, patient_data: PatientCredentialRequest):
    """Update patient login/password credentials in HIS system."""
    try:
        system_config = HIS_API_CONFIG.get(system_name.lower())
        if not system_config:
            return {"success": False, "error": f"Unknown HIS system: {system_name}"}
        
        # Map system names to match your config
        # Since your config uses 'yottadb' and 'firebird' but you might be calling with different names
        system_map = {
            'yottadb': 'yottadb',
            'qms': 'yottadb',
            'firebird': 'firebird', 
            'infoclinica': 'firebird',
            'ic': 'firebird'
        }
        
        actual_system = system_map.get(system_name.lower())
        if not actual_system:
            return {"success": False, "error": f"Unknown HIS system: {system_name}"}
            
        system_config = HIS_API_CONFIG.get(actual_system)
        if not system_config:
            return {"success": False, "error": f"No configuration for HIS system: {actual_system}"}
        
        # Get authentication token
        auth_token = await get_oauth_token(actual_system)
        if not auth_token:
            return {"success": False, "error": f"Failed to get auth token for {actual_system}"}
        
        # Prepare headers
        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json"
        }
        
        # Use the credentials_endpoint from config, replacing the placeholder
        credentials_endpoint = system_config.get("credentials_endpoint", "/updatePatients/{hisnumber}/credentials")
        update_url = f"{system_config['base_url']}{credentials_endpoint}".format(hisnumber=hisnumber)
        
        # Prepare update data
        update_data = {
            "pcode": hisnumber,  # Patient code/number in HIS
            "cllogin": patient_data.cllogin,
            "clpassword": patient_data.clpassword
        }
        
        # Make API call to update credentials
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Use PUT for credential updates
            response = await client.put(
                update_url,
                json=update_data,
                headers=headers
            )
            
            if response.status_code in [200, 201, 204]:  # Added 204 for successful updates with no content
                try:
                    data = response.json() if response.content else {}
                    logger.info(f"Successfully updated credentials in {actual_system} for patient {hisnumber}")
                    return {
                        "success": True,
                        "hisnumber": hisnumber,
                        "system": actual_system,
                        "message": f"Credentials updated in {actual_system}",
                        "response_data": data
                    }
                except Exception as json_error:
                    logger.warning(f"Could not parse JSON response from {actual_system}, but status was success")
                    return {
                        "success": True,
                        "hisnumber": hisnumber,
                        "system": actual_system,
                        "message": f"Credentials updated in {actual_system}"
                    }
            else:
                logger.error(f"Failed to update credentials in {actual_system}: {response.status_code} - {response.text}")
                return {
                    "success": False,
                    "error": f"HIS API error: {response.status_code}",
                    "system": actual_system,
                    "details": response.text
                }
    
    except Exception as e:
        logger.error(f"Error updating patient credentials in {system_name}: {e}")
        return {
            "success": False,
            "error": str(e)
        }

# ALL OTHER ENDPOINTS ARE SUPPORTING/UTILITY ENDPOINTS

@app.get("/health")
async def health_check():
    """Enhanced health check endpoint."""
    try:
        # Get database health
        db_health = await get_database_health()
        
        # Get OAuth token status
        oauth_status = {}
        for his_type in ["yottadb", "firebird"]:
            cache_key = f"{his_type}_token"
            if cache_key in oauth_tokens:
                oauth_status[his_type] = "token_cached"
            else:
                oauth_status[his_type] = "no_token"
        
        # Get configuration summary
        config = get_api_config()
        
        health_data = {
            "status": "healthy" if db_health["status"] == "healthy" else "degraded",
            "timestamp": datetime.now().isoformat(),
            "version": API_CONFIG["version"],
            "environment": config["environment"],
            "database": db_health,
            "oauth_tokens": oauth_status,
            "mobile_app": {
                "registration_enabled": MOBILE_APP_CONFIG["registration_enabled"],
                "auto_register": MOBILE_APP_CONFIG["auto_register_on_create"],
                "require_both_his": MOBILE_APP_CONFIG["require_both_his"]
            },
            "his_endpoints": {
                "yottadb": {
                    "base_url": HIS_API_CONFIG["yottadb"]["base_url"],
                    "endpoints": ["credentials_update", "patient_create"]
                },
                "firebird": {
                    "base_url": HIS_API_CONFIG["firebird"]["base_url"],
                    "endpoints": ["credentials_update", "patient_create"]
                }
            }
        }
        
        status_code = 200 if health_data["status"] == "healthy" else 503
        return JSONResponse(content=health_data, status_code=status_code)
        
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "unhealthy",
                "timestamp": datetime.now().isoformat(),
                "error": str(e)
            }
        )

@app.get("/stats")
async def get_api_stats(patient_repo: PatientRepository = Depends(get_patient_repo)):
    """Get API and database statistics."""
    try:
        mobile_stats = await patient_repo.get_mobile_app_stats()
        matching_stats = await patient_repo.get_patient_matching_stats()
        
        return {
            "mobile_app_users": mobile_stats,
            "patient_matching_24h": matching_stats,
            "oauth_tokens_cached": len([k for k in oauth_tokens.keys() if not k.endswith('_expiry')]),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting API stats: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving statistics: {str(e)}"
        )

@app.get("/config")
async def get_configuration():
    """Get current API configuration (sensitive data masked)."""
    try:
        from src.api.config import get_config_summary
        return get_config_summary()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving configuration: {str(e)}"
        )

@app.post("/test-oauth/{his_type}")
async def test_oauth(his_type: str):
    """Test OAuth authentication for a specific HIS system."""
    if his_type not in ["yottadb", "firebird"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid HIS type. Must be 'yottadb' or 'firebird'"
        )
    
    try:
        token = await get_oauth_token(his_type)
        if token:
            return {
                "success": True,
                "message": f"OAuth authentication successful for {his_type.upper()}",
                "token_preview": token[:10] + "..." if len(token) > 10 else token
            }
        else:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={
                    "success": False,
                    "message": f"OAuth authentication failed for {his_type.upper()}"
                }
            )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": f"Error testing OAuth for {his_type.upper()}: {str(e)}"
            }
        )

@app.post("/test-create/{his_type}")
async def test_patient_creation(his_type: str, patient_data: PatientCredentialRequest):
    """Test patient creation for a specific HIS system."""
    if his_type not in ["yottadb", "firebird"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid HIS type. Must be 'yottadb' or 'firebird'"
        )
    
    try:
        result = await create_his_patient(his_type, patient_data)
        if result.get("success"):
            return {
                "success": True,
                "message": f"Patient creation successful in {his_type.upper()}",
                "patient": f"{patient_data.lastname}, {patient_data.firstname}",
                "hisnumber": result.get("hisnumber"),
                "details": result
            }
        else:
            return JSONResponse(
                status_code=status.HTTP_502_BAD_GATEWAY,
                content={
                    "success": False,
                    "message": f"Patient creation failed in {his_type.upper()}",
                    "error": result.get("error", "Unknown error")
                }
            )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": f"Error testing patient creation for {his_type.upper()}: {str(e)}"
            }
        )

@app.post("/mobile-user/register")
async def register_mobile_user(hisnumber_qms: Optional[str] = None,
                              hisnumber_infoclinica: Optional[str] = None,
                              patient_repo: PatientRepository = Depends(get_patient_repo)):
    """Register a mobile app user."""
    try:
        mobile_uuid = await register_mobile_app_user_api(
            hisnumber_qms=hisnumber_qms,
            hisnumber_infoclinica=hisnumber_infoclinica,
            patient_repo=patient_repo
        )
        
        if mobile_uuid:
            return {"success": True, "mobile_uuid": mobile_uuid, "message": "Mobile user registered successfully"}
        else:
            return {"success": False, "message": "Failed to register mobile user"}
    except Exception as e:
        logger.error(f"Error registering mobile user: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error registering mobile user: {str(e)}"
        )

@app.post("/patient/{patient_uuid}/lock")
async def lock_patient_matching(patient_uuid: str, reason: str = "Manual lock",
                               patient_repo: PatientRepository = Depends(get_patient_repo)):
    """Lock patient from further matching."""
    try:
        success = await patient_repo.lock_patient_matching(patient_uuid, reason)
        if success:
            return {"success": True, "message": f"Patient {patient_uuid} locked successfully"}
        else:
            return {"success": False, "message": f"Failed to lock patient {patient_uuid}"}
    except Exception as e:
        logger.error(f"Error locking patient matching: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error locking patient matching: {str(e)}"
        )

@app.post("/patient/{patient_uuid}/unlock")
async def unlock_patient_matching(patient_uuid: str,
                                 patient_repo: PatientRepository = Depends(get_patient_repo)):
    """Unlock patient matching."""
    try:
        success = await patient_repo.unlock_patient_matching(patient_uuid)
        if success:
            return {"success": True, "message": f"Patient {patient_uuid} unlocked successfully"}
        else:
            return {"success": False, "message": f"Failed to unlock patient {patient_uuid}"}
    except Exception as e:
        logger.error(f"Error unlocking patient matching: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error unlocking patient matching: {str(e)}"
        )

@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "name": API_CONFIG["title"],
        "version": API_CONFIG["version"],
        "description": API_CONFIG["description"],
        "docs_url": "/docs",
        "health_url": "/health",
        "main_endpoint": "/checkModifyPatient"
    }

if __name__ == "__main__":
    import uvicorn
    import traceback
    
    # Add console logging for startup debugging
    import logging
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    
    startup_logger = logging.getLogger("startup_debug")
    startup_logger.addHandler(console_handler)
    startup_logger.setLevel(logging.INFO)
    
    try:
        startup_logger.info("=== Starting Patient API ===")
        startup_logger.info("Loading configuration...")
        
        config = get_api_config()
        startup_logger.info(f"Config loaded successfully")
        startup_logger.info(f"API will start on {config['api']['host']}:{config['api']['port']}")
        
        # Test database connection
        startup_logger.info("Testing database configuration...")
        pg_config = get_postgresql_config()
        startup_logger.info(f"Database: {pg_config['host']}:{pg_config['port']}/{pg_config['database']}")
        
        # Test HIS API configuration
        startup_logger.info("Checking HIS API configuration...")
        for his_name, his_config in HIS_API_CONFIG.items():
            startup_logger.info(f"{his_name.upper()}: {his_config['base_url']}")
        
        startup_logger.info("Starting uvicorn server...")
        uvicorn.run(
            app, 
            host=config["api"]["host"], 
            port=config["api"]["port"],
            log_level="info"
        )
        
    except Exception as e:
        startup_logger.error(f"Failed to start API: {e}")
        startup_logger.error(f"Full traceback: {traceback.format_exc()}")
        print(f"\n=== STARTUP ERROR ===")
        print(f"Error: {e}")
        print(f"Full traceback:\n{traceback.format_exc()}")
        exit(1)