#!/usr/bin/env python3
"""
FastAPI application for patient credential management.
Provides endpoints for checking and modifying patient credentials across HIS systems.
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import logging
import uuid as uuid_module
from contextlib import asynccontextmanager

# Add the parent directory to the path
parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent_dir)

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

class PatientResponse(BaseModel):
    """Response model for patient operations."""
    success: str = Field(..., description="Operation success status")
    message: Optional[str] = Field(None, description="Additional message")
    action: Optional[str] = Field(None, description="Action performed (update/create)")
    mobile_uuid: Optional[str] = Field(None, description="Mobile app user UUID if created")

# Global OAuth token cache
oauth_tokens = {}

# Dependency to get patient repository
def get_patient_repo() -> PatientRepository:
    return get_patient_repository()

@app.on_event("startup")
async def startup_event():
    """Initialize database connection on startup."""
    try:
        logger.info("Initializing Patient Credential Management API...")
        
        # Initialize database
        if not await initialize_database():
            raise Exception("Database initialization failed")
        
        logger.info("API initialization completed successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize API: {e}")
        raise e

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on shutdown."""
    logger.info("Shutting down Patient Credential Management API...")
    await close_database()
    logger.info("API shutdown completed")


async def get_oauth_token(his_type: str) -> Optional[str]:
    """
    Get OAuth token for specified HIS system.
    Uses token caching to avoid repeated authentication.
    
    Args:
        his_type: 'yottadb' or 'firebird'
        
    Returns:
        Access token string or None if authentication failed
    """
    try:
        # Check if we have a valid cached token
        cache_key = f"{his_type}_token"
        cache_expiry_key = f"{his_type}_token_expiry"
        
        if (cache_key in oauth_tokens and 
            cache_expiry_key in oauth_tokens and 
            datetime.now() < oauth_tokens[cache_expiry_key]):
            logger.debug(f"Using cached OAuth token for {his_type.upper()}")
            return oauth_tokens[cache_key]
        
        # Get new token
        config = HIS_API_CONFIG[his_type]["oauth"]
        token_url = config["token_url"]
        
        # Prepare OAuth request data
        oauth_data = {
            "grant_type": "password",
            "username": config["username"],
            "password": config["password"],
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
        }
        
        # Add scope if specified
        if config.get("scope"):
            oauth_data["scope"] = config["scope"]
        
        logger.info(f"Requesting OAuth token from {his_type.upper()}: {token_url}")
        
        async with httpx.AsyncClient(timeout=30.0) as client:
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
        
        # Step 3: Make authenticated API call
        async with httpx.AsyncClient(timeout=30.0) as client:
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
        
        # Step 3: Make authenticated API call
        async with httpx.AsyncClient(timeout=30.0) as client:
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
            else:
                logger.error(f"{his_type.upper()} patient creation failed: {response.status_code} - {response.text}")
                return {"success": False, "error": f"Creation failed: {response.status_code} - {response.text}"}
                
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

@app.post("/checkModifyPatient", response_model=PatientResponse)
async def check_modify_patient(request: PatientCredentialRequest,
                             patient_repo: PatientRepository = Depends(get_patient_repo)):
    """
    Check and modify patient credentials across HIS systems.
    If patient not found, creates patient in both HIS systems and registers mobile app user.
    """
    logger.info(f"Processing credential request for patient: {request.lastname}, {request.firstname}")
    
    try:
        # Step 1: Find patient in PostgreSQL database
        patient = await patient_repo.find_patient_by_credentials(
            lastname=request.lastname,
            firstname=request.firstname,
            midname=request.midname,
            bdate=request.bdate,
            cllogin=request.cllogin
        )
        
        if patient:
            # Patient found - update credentials
            logger.info(f"Patient found, updating credentials: {patient['uuid']}")
            
            # Prepare authenticated API calls for both HIS systems
            update_tasks = []
            systems_to_update = []
            
            # Update YottaDB if patient has qMS number
            if patient['hisnumber_qms']:
                logger.info(f"Scheduling YottaDB credential update for patient {patient['hisnumber_qms']}")
                update_tasks.append(
                    update_his_credentials('yottadb', patient['hisnumber_qms'], request.cllogin, request.clpassword)
                )
                systems_to_update.append("YottaDB")
            else:
                logger.info("Patient has no qMS number, skipping YottaDB update")
            
            # Update Firebird if patient has Infoclinica number  
            if patient['hisnumber_infoclinica']:
                logger.info(f"Scheduling Firebird credential update for patient {patient['hisnumber_infoclinica']}")
                update_tasks.append(
                    update_his_credentials('firebird', patient['hisnumber_infoclinica'], request.cllogin, request.clpassword)
                )
                systems_to_update.append("Firebird")
            else:
                logger.info("Patient has no Infoclinica number, skipping Firebird update")
            
            if not update_tasks:
                logger.error("Patient has no HIS numbers, cannot update credentials")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Patient has no associated HIS numbers"
                )
            
            # Execute all authenticated API calls concurrently
            logger.info(f"Executing {len(update_tasks)} authenticated credential update operations")
            results = await asyncio.gather(*update_tasks, return_exceptions=True)
            
            # Check results
            success_count = 0
            failed_systems = []
            
            for i, result in enumerate(results):
                system_name = systems_to_update[i]
                
                if isinstance(result, Exception):
                    logger.error(f"{system_name} update failed with exception: {result}")
                    failed_systems.append(system_name)
                elif result:
                    success_count += 1
                    logger.info(f"{system_name} update completed successfully")
                else:
                    logger.error(f"{system_name} update failed")
                    failed_systems.append(system_name)
            
            # Return appropriate response for updates
            if success_count == len(update_tasks):
                logger.info(f"All {success_count} credential updates completed successfully")
                return PatientResponse(
                    success="true",
                    message=f"Credentials updated successfully in {success_count} system(s): {', '.join(systems_to_update)}",
                    action="update"
                )
            elif success_count > 0:
                logger.warning(f"Partial success: {success_count}/{len(update_tasks)} updates completed")
                successful_systems = [systems_to_update[i] for i, result in enumerate(results) if result and not isinstance(result, Exception)]
                failed_list = ", ".join(failed_systems)
                return PatientResponse(
                    success="partial",
                    message=f"Credentials updated in: {', '.join(successful_systems)}. Failed: {failed_list}",
                    action="update"
                )
            else:
                logger.error("All credential update operations failed")
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail="Failed to update credentials in any HIS system"
                )
        
        else:
            # Patient not found - create in both HIS systems
            logger.info(f"Patient not found, creating in both HIS systems: {request.lastname}, {request.firstname}")
            
            # Prepare patient creation calls for both HIS systems
            create_tasks = [
                create_his_patient('yottadb', request),
                create_his_patient('firebird', request)
            ]
            systems_to_create = ["YottaDB", "Firebird"]
            
            # Execute all authenticated API calls concurrently
            logger.info(f"Executing {len(create_tasks)} authenticated patient creation operations")
            results = await asyncio.gather(*create_tasks, return_exceptions=True)
            
            # Check results and extract HIS numbers for mobile app registration
            success_count = 0
            failed_systems = []
            created_hisnumbers = {}
            successful_systems = []
            
            for i, result in enumerate(results):
                system_name = systems_to_create[i]
                his_type = 'yottadb' if i == 0 else 'firebird'
                
                if isinstance(result, Exception):
                    logger.error(f"{system_name} creation failed with exception: {result}")
                    failed_systems.append(system_name)
                elif isinstance(result, dict):
                    if result.get('success'):
                        success_count += 1
                        successful_systems.append(system_name)
                        hisnumber = result.get('hisnumber')
                        if hisnumber:
                            created_hisnumbers[his_type] = hisnumber
                            logger.info(f"{system_name} creation completed successfully, HIS number: {hisnumber}")
                        else:
                            logger.info(f"{system_name} creation completed successfully")
                    else:
                        logger.error(f"{system_name} creation failed: {result.get('error')}")
                        failed_systems.append(system_name)
                else:
                    # Legacy boolean response
                    if result:
                        success_count += 1
                        successful_systems.append(system_name)
                        logger.info(f"{system_name} creation completed successfully")
                    else:
                        logger.error(f"{system_name} creation failed")
                        failed_systems.append(system_name)
            
            # Register mobile app user if at least one creation succeeded
            mobile_uuid = None
            if (MOBILE_APP_CONFIG["auto_register_on_create"] and 
                success_count > 0 and 
                (created_hisnumbers or not MOBILE_APP_CONFIG["require_both_his"])):
                
                mobile_uuid = await register_mobile_app_user_api(
                    hisnumber_qms=created_hisnumbers.get('yottadb'),
                    hisnumber_infoclinica=created_hisnumbers.get('firebird'),
                    patient_repo=patient_repo
                )
            
            # Return appropriate response for creation
            if success_count == len(create_tasks):
                logger.info(f"All {success_count} patient creations completed successfully")
                response_message = f"Patient created successfully in {success_count} system(s): {', '.join(successful_systems)}"
                
                return PatientResponse(
                    success="true",
                    message=response_message,
                    action="create",
                    mobile_uuid=mobile_uuid
                )
            elif success_count > 0:
                logger.warning(f"Partial success: {success_count}/{len(create_tasks)} creations completed")
                failed_list = ", ".join(failed_systems)
                response_message = f"Patient created in: {', '.join(successful_systems)}. Failed: {failed_list}"
                
                return PatientResponse(
                    success="partial",
                    message=response_message,
                    action="create",
                    mobile_uuid=mobile_uuid
                )
            else:
                logger.error("All patient creation operations failed")
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail="Failed to create patient in any HIS system"
                )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error processing request: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )

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
        "health_url": "/health"
    }

if __name__ == "__main__":
    import uvicorn
    
    config = get_api_config()
    uvicorn.run(
        app, 
        host=config["api"]["host"], 
        port=config["api"]["port"],
        log_level="info"
    )