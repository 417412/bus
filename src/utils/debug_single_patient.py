#!/usr/bin/env python3
"""
Debug utility to load and process a single patient by hisnumber and source.
This helps debug ETL issues by following the complete pipeline for one specific patient.
"""

import os
import sys
import argparse
import logging

# Add the parent directory to the path so Python can find the modules
parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, parent_dir)

# Import configuration
from src.config.settings import setup_logger, get_decrypted_database_config

# Import connectors and repositories
from src.connectors.postgres_connector import PostgresConnector
from src.connectors.firebird_connector import FirebirdConnector
from src.connectors.yottadb_connector import YottaDBConnector
from src.repositories.postgres_repository import PostgresRepository
from src.repositories.firebird_repository import FirebirdRepository
from src.repositories.yottadb_repository import YottaDBRepository

# Import ETL components
from src.etl.etl_service import ETLService
from src.models.patient import Patient

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Debug single patient ETL processing"
    )
    parser.add_argument(
        "hisnumber",
        help="Patient HIS number to debug"
    )
    parser.add_argument(
        "--source",
        choices=["firebird", "yottadb", "qms", "infoclinica"],
        required=True,
        help="Source system (firebird/infoclinica or yottadb/qms)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose debug logging"
    )
    parser.add_argument(
        "--no-insert",
        action="store_true",
        help="Don't actually insert into PostgreSQL (dry run)"
    )
    return parser.parse_args()

def debug_firebird_patient(hisnumber: str, logger, dry_run: bool = False):
    """Debug a single patient from Firebird/Infoclinica."""
    
    logger.info("=" * 80)
    logger.info(f"DEBUGGING FIREBIRD PATIENT: {hisnumber}")
    logger.info("=" * 80)
    
    # Connect to Firebird
    fb_connector = FirebirdConnector()
    if not fb_connector.connect():
        logger.error("Failed to connect to Firebird")
        return False
    
    # Connect to PostgreSQL
    pg_connector = PostgresConnector()
    if not pg_connector.connect():
        logger.error("Failed to connect to PostgreSQL")
        fb_connector.disconnect()
        return False
    
    try:
        # Create repositories
        logger.info("STEP 1: Creating repositories and ETL service")
        fb_repo = FirebirdRepository(fb_connector)
        pg_repo = PostgresRepository(pg_connector)
        etl_service = ETLService(fb_repo, pg_repo)
        
        logger.info(f"ETL service transformer: {type(etl_service.transformer).__name__}")
        
        # Query specific patient from Firebird
        logger.info(f"STEP 2: Querying patient {hisnumber} from Firebird")
        
        query = """
            SELECT
                c.pcode AS hisnumber,
                2 AS source,
                CASE
                    WHEN (c.filial = 7) THEN 3
                    ELSE 2
                END AS businessunit,
                c.lastname,
                c.firstname AS name,
                c.midname AS surname,
                c.bdate AS birthdate,
                c.pasptype as documenttypes,
                REPLACE(REPLACE(c.paspser, '-', ''), ' ', '') || REPLACE(REPLACE(c.paspnum, '-', ''), ' ', '') AS document_number,
                c.clmail AS email,
                CASE
                    WHEN (c.phone3 IS NOT NULL AND c.phone3 <> '') THEN RemoveNonNumeric(c.phone3)
                    WHEN (c.phone2 IS NOT NULL AND c.phone2 <> '') THEN RemoveNonNumeric(c.phone2)
                    ELSE RemoveNonNumeric(c.phone1)
                END AS telephone,
                c.clpassword AS his_password,
                c.cllogin AS login_email
            FROM
                clients c
            WHERE
                c.pcode = ?
        """
        
        rows, columns = fb_connector.execute_query(query, (hisnumber,))
        
        if not rows:
            logger.error(f"Patient {hisnumber} not found in Firebird")
            return False
        
        if len(rows) > 1:
            logger.warning(f"Multiple patients found with hisnumber {hisnumber}, using first one")
        
        # Convert to dict
        raw_patient = dict(zip(columns, rows[0]))
        
        # Ensure documenttypes is integer
        doc_type = raw_patient.get('documenttypes')
        if doc_type is not None:
            try:
                raw_patient['documenttypes'] = int(doc_type)
            except (ValueError, TypeError):
                logger.warning(f"Invalid document type for patient {hisnumber}: {doc_type}")
                raw_patient['documenttypes'] = None
        
        logger.info("STEP 2 SUCCESS: Retrieved raw patient data from Firebird")
        logger.info("Raw patient data:")
        for key, value in raw_patient.items():
            logger.info(f"  {key}: {repr(value)} (type: {type(value).__name__})")
        
        # Transform using ETL service
        logger.info(f"\nSTEP 3: Processing through ETL pipeline")
        logger.info(f"Input document type: {raw_patient.get('documenttypes')} (type: {type(raw_patient.get('documenttypes'))})")
        
        patient = etl_service.process_patient_record(raw_patient)
        
        if not patient:
            logger.error("STEP 3 FAILED: ETL service returned None")
            return False
        
        logger.info("STEP 3 SUCCESS: ETL processing completed")
        logger.info(f"Patient object created:")
        logger.info(f"  hisnumber: {patient.hisnumber}")
        logger.info(f"  source: {patient.source}")
        logger.info(f"  businessunit: {patient.businessunit}")
        logger.info(f"  documenttypes: {patient.documenttypes} (was {raw_patient.get('documenttypes')})")
        logger.info(f"  document_number: {patient.document_number}")
        logger.info(f"  email: {patient.email}")
        logger.info(f"  login_email: {patient.login_email}")
        logger.info(f"  full_name: {patient.lastname} {patient.name} {patient.surname}")
        
        # Convert to dict for database
        logger.info(f"\nSTEP 4: Converting to database format")
        patient_dict = patient.to_patientsdet_dict()
        
        logger.info("Final patient dict for database:")
        for key, value in patient_dict.items():
            logger.info(f"  {key}: {repr(value)} (type: {type(value).__name__})")
        
        # Check if patient exists in PostgreSQL
        logger.info(f"\nSTEP 5: Checking if patient exists in PostgreSQL")
        exists = pg_repo.patient_exists(patient.hisnumber, patient.source)
        logger.info(f"Patient exists in PostgreSQL: {exists}")
        
        if exists:
            logger.info("Patient already exists, would use upsert")
            if not dry_run:
                logger.info("STEP 6: Performing upsert...")
                if pg_repo.upsert_patient(patient_dict):
                    logger.info("✅ UPSERT SUCCESS")
                    return True
                else:
                    logger.error("❌ UPSERT FAILED")
                    return False
            else:
                logger.info("STEP 6: SKIPPED (dry run)")
                return True
        else:
            logger.info("Patient does not exist, would insert")
            if not dry_run:
                logger.info("STEP 6: Performing insert...")
                if pg_repo.insert_patient(patient_dict):
                    logger.info("✅ INSERT SUCCESS")
                    return True
                else:
                    logger.error("❌ INSERT FAILED")
                    return False
            else:
                logger.info("STEP 6: SKIPPED (dry run)")
                return True
        
    except Exception as e:
        logger.error(f"Error during Firebird patient debug: {e}")
        import traceback
        logger.error(f"Traceback:\n{traceback.format_exc()}")
        return False
    finally:
        fb_connector.disconnect()
        pg_connector.disconnect()

def debug_yottadb_patient(hisnumber: str, logger, dry_run: bool = False):
    """Debug a single patient from YottaDB/qMS."""
    
    logger.info("=" * 80)
    logger.info(f"DEBUGGING YOTTADB PATIENT: {hisnumber}")
    logger.info("=" * 80)
    
    # Connect to YottaDB
    yottadb_connector = YottaDBConnector()
    if not yottadb_connector.connect():
        logger.error("Failed to connect to YottaDB")
        return False
    
    # Connect to PostgreSQL
    pg_connector = PostgresConnector()
    if not pg_connector.connect():
        logger.error("Failed to connect to PostgreSQL")
        yottadb_connector.disconnect()
        return False
    
    try:
        # Create repositories
        logger.info("STEP 1: Creating repositories and ETL service")
        yottadb_repo = YottaDBRepository(yottadb_connector)
        pg_repo = PostgresRepository(pg_connector)
        etl_service = ETLService(yottadb_repo, pg_repo)
        
        logger.info(f"ETL service transformer: {type(etl_service.transformer).__name__}")
        
        # Get ALL patients from YottaDB API (this will be cached)
        logger.info(f"STEP 2: Fetching ALL patients from YottaDB to find {hisnumber}")
        logger.info("This may take a few minutes on first run (cached afterwards)...")
        
        # Use the new method to get all patients without filtering
        all_patients = yottadb_repo.get_all_patients_raw()
        
        if not all_patients:
            logger.error("No patients retrieved from YottaDB")
            return False
        
        logger.info(f"Total patients retrieved: {len(all_patients)}")
        
        # Find the specific patient
        target_patient = None
        patient_index = None
        
        logger.info(f"Searching for patient {hisnumber} in {len(all_patients)} patients...")
        
        for i, patient in enumerate(all_patients):
            patient_hisnumber = str(patient.get('hisnumber', ''))
            if patient_hisnumber == str(hisnumber):
                target_patient = patient
                patient_index = i
                logger.info(f"Found patient at index {i}")
                break
            
            # Progress indicator for large datasets
            if i > 0 and i % 50000 == 0:
                logger.info(f"Searched {i} patients so far...")
        
        if not target_patient:
            logger.error(f"Patient {hisnumber} not found in {len(all_patients)} patients")
            
            # Show some samples from different parts of the data
            logger.info("Sample hisnumbers from different parts of the dataset:")
            
            # First 10
            logger.info("First 10:")
            for i, patient in enumerate(all_patients[:10]):
                logger.info(f"  [{i}] {patient.get('hisnumber')}")
            
            # Middle 10
            if len(all_patients) > 20:
                middle_start = len(all_patients) // 2 - 5
                logger.info(f"Middle 10 (around index {middle_start}):")
                for i, patient in enumerate(all_patients[middle_start:middle_start+10]):
                    logger.info(f"  [{middle_start + i}] {patient.get('hisnumber')}")
            
            # Last 10
            if len(all_patients) > 10:
                logger.info("Last 10:")
                for i, patient in enumerate(all_patients[-10:]):
                    logger.info(f"  [{len(all_patients) - 10 + i}] {patient.get('hisnumber')}")
            
            # Search for similar patterns
            similar_patients = []
            search_pattern = hisnumber.split('/')[0] if '/' in hisnumber else hisnumber
            for patient in all_patients:
                patient_hisnumber = str(patient.get('hisnumber', ''))
                if search_pattern in patient_hisnumber:
                    similar_patients.append(patient_hisnumber)
                    if len(similar_patients) >= 20:  # Show first 20 matches
                        break
            
            if similar_patients:
                logger.info(f"Hisnumbers containing '{search_pattern}' (first 20):")
                for similar in similar_patients:
                    logger.info(f"  {similar}")
            
            return False
        
        logger.info(f"STEP 2 SUCCESS: Found patient at index {patient_index} out of {len(all_patients)}")
        raw_patient = target_patient
        
        logger.info("Raw patient data:")
        for key, value in raw_patient.items():
            logger.info(f"  {key}: {repr(value)} (type: {type(value).__name__})")
        
        # Transform using ETL service
        logger.info(f"\nSTEP 3: Processing through ETL pipeline")
        logger.info(f"Input document type: {raw_patient.get('documenttypes')} (type: {type(raw_patient.get('documenttypes'))})")
        
        patient = etl_service.process_patient_record(raw_patient)
        
        if not patient:
            logger.error("STEP 3 FAILED: ETL service returned None")
            return False
        
        logger.info("STEP 3 SUCCESS: ETL processing completed")
        logger.info(f"Patient object created:")
        logger.info(f"  hisnumber: {patient.hisnumber}")
        logger.info(f"  source: {patient.source}")
        logger.info(f"  businessunit: {patient.businessunit}")
        logger.info(f"  documenttypes: {patient.documenttypes} (was {raw_patient.get('documenttypes')})")
        logger.info(f"  document_number: {patient.document_number}")
        logger.info(f"  email: {patient.email}")
        logger.info(f"  login_email: {patient.login_email}")
        logger.info(f"  full_name: {patient.lastname} {patient.name} {patient.surname}")
        
        # Convert to dict for database
        logger.info(f"\nSTEP 4: Converting to database format")
        patient_dict = patient.to_patientsdet_dict()
        
        logger.info("Final patient dict for database:")
        for key, value in patient_dict.items():
            logger.info(f"  {key}: {repr(value)} (type: {type(value).__name__})")
        
        # Check if patient exists in PostgreSQL
        logger.info(f"\nSTEP 5: Checking if patient exists in PostgreSQL")
        exists = pg_repo.patient_exists(patient.hisnumber, patient.source)
        logger.info(f"Patient exists in PostgreSQL: {exists}")
        
        if exists:
            logger.info("Patient already exists, would use upsert")
            if not dry_run:
                logger.info("STEP 6: Performing upsert...")
                if pg_repo.upsert_patient(patient_dict):
                    logger.info("✅ UPSERT SUCCESS")
                    return True
                else:
                    logger.error("❌ UPSERT FAILED")
                    return False
            else:
                logger.info("STEP 6: SKIPPED (dry run)")
                return True
        else:
            logger.info("Patient does not exist, would insert")
            if not dry_run:
                logger.info("STEP 6: Performing insert...")
                if pg_repo.insert_patient(patient_dict):
                    logger.info("✅ INSERT SUCCESS")
                    return True
                else:
                    logger.error("❌ INSERT FAILED")
                    return False
            else:
                logger.info("STEP 6: SKIPPED (dry run)")
                return True
        
    except Exception as e:
        logger.error(f"Error during YottaDB patient debug: {e}")
        import traceback
        logger.error(f"Traceback:\n{traceback.format_exc()}")
        return False
    finally:
        yottadb_connector.disconnect()
        pg_connector.disconnect()

def main():
    """Main function."""
    args = parse_args()
    
    # Set up logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Enable debug logging for specific modules
    if args.verbose:
        logging.getLogger('src.etl.etl_service').setLevel(logging.DEBUG)
        logging.getLogger('src.models.patient').setLevel(logging.DEBUG)
        logging.getLogger('src.etl.transformers').setLevel(logging.DEBUG)
    
    logger = setup_logger("debug_single_patient", "debug")
    
    logger.info(f"Starting single patient debug for hisnumber: {args.hisnumber}")
    logger.info(f"Source: {args.source}")
    logger.info(f"Dry run: {args.no_insert}")
    
    # Normalize source argument
    if args.source.lower() in ['firebird', 'infoclinica']:
        source_type = 'firebird'
    elif args.source.lower() in ['yottadb', 'qms']:
        source_type = 'yottadb'
    else:
        logger.error(f"Invalid source: {args.source}")
        return 1
    
    # Run the appropriate debug function
    try:
        if source_type == 'firebird':
            success = debug_firebird_patient(args.hisnumber, logger, args.no_insert)
        else:
            success = debug_yottadb_patient(args.hisnumber, logger, args.no_insert)
        
        if success:
            logger.info("\n🎉 DEBUG COMPLETED SUCCESSFULLY")
            return 0
        else:
            logger.error("\n❌ DEBUG FAILED")
            return 1
            
    except KeyboardInterrupt:
        logger.info("\nDebug interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        logger.error(f"Traceback:\n{traceback.format_exc()}")
        return 1

if __name__ == "__main__":
    exit(main())