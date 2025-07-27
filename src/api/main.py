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

# Add the parent directory to the path
parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent_dir)

from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator
import httpx
import asyncio

# Import only PostgreSQL components
from src.config.settings import setup_logger, get_decrypted_database_config
from src.connectors.postgres_connector import PostgresConnector

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = setup_logger("patient_api", "api")

# Create FastAPI app
app = FastAPI(
    title="Patient Credential Management API",
    description="API for managing patient credentials across HIS systems",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Pydantic models
class PatientCredentialRequest(BaseModel):
    """Request model for patient credential operations."""
    lastname: str = Field(..., description="Patient's last name")
    firstname: str = Field(..., description="Patient's first name") 
    midname: Optional[str] = Field(None, description="Patient's middle name")
    bdate: str = Field(..., description="Patient's birth date (YYYY-MM-DD)")
    cllogin: str = Field(..., description="Patient's login")
    clpassword: str = Field(..., description="Patient's password")
    
    @validator('bdate')
    def validate_bdate(cls, v):
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

# Configuration for HIS API endpoints with OAuth
HIS_API_CONFIG = {
    "yottadb": {
        "base_url": os.getenv("YOTTADB_API_BASE", "http://192.168.156.43"),
        "credentials_endpoint": "/updatePatients/{hisnumber}/credentials",
        "create_endpoint": "/createPatients",
        "oauth": {
            "token_url": os.getenv("YOTTADB_TOKEN_URL", "http://192.168.156.43/oauth/token"),
            "client_id": os.getenv("YOTTADB_CLIENT_ID", "yottadb_client"),
            "client_secret": os.getenv("YOTTADB_CLIENT_SECRET", "yottadb_secret"),
            "username": os.getenv("YOTTADB_USERNAME", "api_user"),
            "password": os.getenv("YOTTADB_PASSWORD", "api_password"),
            "scope": os.getenv("YOTTADB_SCOPE", "patient_update")
        }
    },
    "firebird": {
        "base_url": os.getenv("FIREBIRD_API_BASE", "http://firebird-server"),
        "credentials_endpoint": "/updatePatients/{hisnumber}/credentials",
        "create_endpoint": "/createPatients",
        "oauth": {
            "token_url": os.getenv("FIREBIRD_TOKEN_URL", "http://firebird-server/oauth/token"),
            "client_id": os.getenv("FIREBIRD_CLIENT_ID", "firebird_client"),
            "client_secret": os.getenv("FIREBIRD_CLIENT_SECRET", "firebird_secret"),
            "username": os.getenv("FIREBIRD_USERNAME", "api_user"),
            "password": os.getenv("FIREBIRD_PASSWORD", "api_password"),
            "scope": os.getenv("FIREBIRD_SCOPE", "patient_update")
        }
    }
}

# Global database connection and token cache
pg_connector = None
oauth_tokens = {}  # Cache for OAuth tokens

@app.on_event("startup")
async def startup_event():
    """Initialize database connection on startup."""
    global pg_connector
    
    try:
        logger.info("Initializing PostgreSQL connection...")
        pg_connector = PostgresConnector()
        
        if not pg_connector.connect():
            logger.error("Failed to connect to PostgreSQL database")
            raise Exception("PostgreSQL connection failed")
        
        logger.info("PostgreSQL connection initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize database connection: {e}")
        raise e

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up database connection on shutdown."""
    global pg_connector
    
    if pg_connector:
        pg_connector.disconnect()
        logger.info("PostgreSQL connection closed")

def find_patient_by_credentials(lastname: str, firstname: str, midname: Optional[str], 
                               bdate: str, cllogin: str) -> Optional[Dict[str, Any]]:
    """
    Find patient in PostgreSQL database by demographic data and login.
    
    Args:
        lastname: Patient's last name
        firstname: Patient's first name (maps to 'name' column)
        midname: Patient's middle name (maps to 'surname' column)
        bdate: Patient's birth date  
        cllogin: Patient's login (check both login_qms and login_infoclinica)
        
    Returns:
        Patient record dict or None if not found
    """
    try:
        # Build the query to find patient by demographics and login
        if midname:
            query = """
                SELECT 
                    uuid, lastname, name, surname, birthdate,
                    hisnumber_qms, hisnumber_infoclinica,
                    login_qms, login_infoclinica
                FROM patients 
                WHERE lastname = %s 
                AND name = %s 
                AND surname = %s
                AND birthdate = %s
                AND (login_qms = %s OR login_infoclinica = %s)
            """
            params = (lastname, firstname, midname, bdate, cllogin, cllogin)
        else:
            query = """
                SELECT 
                    uuid, lastname, name, surname, birthdate,
                    hisnumber_qms, hisnumber_infoclinica,
                    login_qms, login_infoclinica
                FROM patients 
                WHERE lastname = %s 
                AND name = %s 
                AND (surname IS NULL OR surname = '')
                AND birthdate = %s
                AND (login_qms = %s OR login_infoclinica = %s)
            """
            params = (lastname, firstname, bdate, cllogin, cllogin)
        
        logger.debug(f"Executing patient search query with params: {params}")
        
        rows, columns = pg_connector.execute_query(query, params)
        
        if not rows:
            logger.info(f"No patient found with credentials: {lastname}, {firstname}, {midname}, {bdate}, {cllogin}")
            return None
        
        if len(rows) > 1:
            logger.warning(f"Multiple patients found with same credentials, using first one")
        
        # Convert to dict
        patient = dict(zip(columns, rows[0]))
        logger.info(f"Found patient: UUID={patient['uuid']}, QMS={patient['hisnumber_qms']}, IC={patient['hisnumber_infoclinica']}")
        
        return patient
        
    except Exception as e:
        logger.error(f"Error searching for patient: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )

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

async def create_his_patient(his_type: str, patient_data: PatientCredentialRequest) -> bool:
    """
    Create patient in specified HIS system via authenticated API call.
    
    Args:
        his_type: 'yottadb' or 'firebird'
        patient_data: Patient data for creation
        
    Returns:
        True if successful (HTTP 201), False otherwise
    """
    try:
        # Step 1: Get OAuth token
        access_token = await get_oauth_token(his_type)
        if not access_token:
            logger.error(f"Failed to obtain OAuth token for {his_type.upper()} patient creation")
            return False
        
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
                logger.info(f"Successfully created patient in {his_type.upper()}: {patient_data.lastname}, {patient_data.firstname}")
                return True
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
                        logger.info(f"Successfully created patient in {his_type.upper()} (retry): {patient_data.lastname}, {patient_data.firstname}")
                        return True
                    else:
                        logger.error(f"{his_type.upper()} patient creation failed on retry: {retry_response.status_code} - {retry_response.text}")
                        return False
                else:
                    logger.error(f"Failed to get new OAuth token for {his_type.upper()} patient creation retry")
                    return False
            else:
                logger.error(f"{his_type.upper()} patient creation failed: {response.status_code} - {response.text}")
                return False
                
    except Exception as e:
        logger.error(f"Error creating patient in {his_type.upper()}: {e}")
        return False

@app.post("/checkModifyPatient", response_model=PatientResponse)
async def check_modify_patient(request: PatientCredentialRequest):
    """
    Check and modify patient credentials across HIS systems.
    If patient not found, creates patient in both HIS systems.
    
    This endpoint:
    1. Searches for the patient in PostgreSQL using demographic data and login
    2. If found: Updates credentials in both YottaDB and Firebird via authenticated API calls
    3. If not found: Creates patient in both YottaDB and Firebird via authenticated API calls
    4. Returns success status with action performed
    
    Args:
        request: Patient credential request containing demographic data and credentials
        
    Returns:
        PatientResponse with success status and action performed
        
    Raises:
        HTTPException: If both update/create operations fail
    """
    logger.info(f"Processing credential request for patient: {request.lastname}, {request.firstname}")
    
    try:
        # Step 1: Find patient in PostgreSQL database
        patient = find_patient_by_credentials(
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
            
            # Check results
            success_count = 0
            failed_systems = []
            
            for i, result in enumerate(results):
                system_name = systems_to_create[i]
                
                if isinstance(result, Exception):
                    logger.error(f"{system_name} creation failed with exception: {result}")
                    failed_systems.append(system_name)
                elif result:
                    success_count += 1
                    logger.info(f"{system_name} creation completed successfully")
                else:
                    logger.error(f"{system_name} creation failed")
                    failed_systems.append(system_name)
            
            # Return appropriate response for creation
            if success_count == len(create_tasks):
                logger.info(f"All {success_count} patient creations completed successfully")
                return PatientResponse(
                    success="true",
                    message=f"Patient created successfully in {success_count} system(s): {', '.join(systems_to_create)}",
                    action="create"
                )
            elif success_count > 0:
                logger.warning(f"Partial success: {success_count}/{len(create_tasks)} creations completed")
                successful_systems = [systems_to_create[i] for i, result in enumerate(results) if result and not isinstance(result, Exception)]
                failed_list = ", ".join(failed_systems)
                return PatientResponse(
                    success="partial",
                    message=f"Patient created in: {', '.join(successful_systems)}. Failed: {failed_list}",
                    action="create"
                )
            else:
                logger.error("All patient creation operations failed")
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail="Failed to create patient in any HIS system"
                )
            
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Unexpected error processing request: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        # Test database connection
        if pg_connector and pg_connector.connection:
            rows, columns = pg_connector.execute_query("SELECT 1")
            db_status = "connected" if rows else "error"
        else:
            db_status = "disconnected"
        
        # Test OAuth token availability (without exposing sensitive data)
        oauth_status = {}
        for his_type in ["yottadb", "firebird"]:
            cache_key = f"{his_type}_token"
            if cache_key in oauth_tokens:
                oauth_status[his_type] = "token_cached"
            else:
                oauth_status[his_type] = "no_token"
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "database": db_status,
            "oauth_tokens": oauth_status,
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
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "unhealthy",
                "timestamp": datetime.now().isoformat(),
                "error": str(e)
            }
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
        if result:
            return {
                "success": True,
                "message": f"Patient creation successful in {his_type.upper()}",
                "patient": f"{patient_data.lastname}, {patient_data.firstname}"
            }
        else:
            return JSONResponse(
                status_code=status.HTTP_502_BAD_GATEWAY,
                content={
                    "success": False,
                    "message": f"Patient creation failed in {his_type.upper()}"
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)