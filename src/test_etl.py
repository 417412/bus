#!/usr/bin/env python3
"""
Main application for the medical system ETL testing.

This script tests the ETL process and database connectivity
using the new architecture with separate responsibilities.
"""

import os
import sys
import logging
import random
from datetime import datetime

# Add the parent directory to the path so Python can find the modules
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

# Import configuration
from src.config.settings import DATABASE_CONFIG, DOCUMENT_TYPES, setup_logger

# Import connectors
from src.connectors.postgres_connector import PostgresConnector
from src.connectors.firebird_connector import FirebirdConnector
from src.connectors.yottadb_connector import YottaDBConnector

# Import repositories
from src.repositories.postgres_repository import PostgresRepository
from src.repositories.firebird_repository import FirebirdRepository
from src.repositories.yottadb_repository import YottaDBRepository

# Import ETL components
from src.etl.etl_service import ETLService

# Set up logging
logger = setup_logger("test_etl", "test_etl")


# Test Functions
def test_postgres_connection():
    """Test PostgreSQL connection."""
    logger.info("Testing PostgreSQL connection...")
    pg_connector = PostgresConnector(DATABASE_CONFIG["PostgreSQL"])
    
    if pg_connector.connect():
        logger.info("PostgreSQL connection successful")
        
        # Test a simple query
        try:
            cursor = pg_connector.connection.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            logger.info(f"PostgreSQL version: {version}")
            cursor.close()
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
        
        pg_connector.disconnect()
        return True
    else:
        logger.error("PostgreSQL connection failed")
        return False


def test_firebird_connection():
    """Test Firebird connection."""
    logger.info("Testing Firebird connection...")
    logger.info(f"Firebird config: {DATABASE_CONFIG['Firebird']}")
    
    fb_connector = FirebirdConnector(DATABASE_CONFIG["Firebird"])
    
    if fb_connector.connect():
        logger.info("Firebird connection successful")
        
        # Test a simple query
        try:
            with fb_connector.connection.cursor() as cursor:
                cursor.execute("SELECT CURRENT_TIMESTAMP FROM RDB$DATABASE")
                timestamp = cursor.fetchone()[0]
                logger.info(f"Firebird current time: {timestamp}")
        except Exception as e:
            logger.error(f"Error executing Firebird query: {str(e)}")
        
        # Test connection through the repository
        try:
            fb_repo = FirebirdRepository(fb_connector)
            patients = fb_repo.get_patients(batch_size=3)
            logger.info(f"Retrieved {len(patients)} patients from Firebird through repository")
            
            # Display sample data
            for i, patient in enumerate(patients[:2]):
                logger.info(f"Patient {i+1}: {patient.get('lastname', 'Unknown')} {patient.get('name', 'Unknown')}")
        except Exception as e:
            logger.error(f"Error testing Firebird repository: {str(e)}")
        
        fb_connector.disconnect()
        return True
    else:
        logger.error("Firebird connection failed")
        return False


def test_yottadb_connection():
    """Test YottaDB connection using ping/socket test."""
    logger.info("Testing YottaDB connection...")
    
    yottadb_connector = YottaDBConnector(DATABASE_CONFIG["YottaDB"])
    
    # Test basic connectivity (quick ping/socket test)
    if yottadb_connector.connect():
        logger.info("YottaDB connectivity test successful")
        yottadb_connector.disconnect()
        return True
    else:
        logger.error("YottaDB connectivity test failed")
        return False


def test_yottadb_etl():
    """Test YottaDB ETL process with full API fetch and sample processing."""
    logger.info("Testing YottaDB ETL process...")
    logger.info("WARNING: This will fetch all data from YottaDB API (takes 2-3 minutes)")
    
    # Ask user confirmation
    response = input("Do you want to proceed with YottaDB ETL test? (y/N): ")
    if response.lower() not in ['y', 'yes']:
        logger.info("YottaDB ETL test skipped by user")
        return None
    
    yottadb_connector = YottaDBConnector(DATABASE_CONFIG["YottaDB"])
    pg_connector = PostgresConnector(DATABASE_CONFIG["PostgreSQL"])
    
    if not yottadb_connector.connect():
        logger.error("Failed to connect to YottaDB")
        return False
        
    if not pg_connector.connect():
        logger.error("Failed to connect to PostgreSQL")
        return False
    
    try:
        # Create repositories
        yottadb_repo = YottaDBRepository(yottadb_connector)
        pg_repo = PostgresRepository(pg_connector)
        
        # Create ETL service
        etl_service = ETLService(yottadb_repo, pg_repo)
        
        logger.info("Fetching all patients from YottaDB API...")
        
        # Get all patients from YottaDB (this triggers the full API call)
        all_patients = yottadb_repo._get_all_patients_cached()
        
        if not all_patients:
            logger.error("No patients retrieved from YottaDB")
            return False
        
        logger.info(f"Retrieved {len(all_patients)} patients from YottaDB")
        
        # Select 10 random patients for testing ETL
        test_batch_size = min(10, len(all_patients))
        test_patients = random.sample(all_patients, test_batch_size)
        
        logger.info(f"Testing ETL process with {test_batch_size} random patients...")
        
        # Process the test batch through ETL
        success_count = 0
        error_count = 0
        
        for i, raw_patient in enumerate(test_patients, 1):
            try:
                # Transform the patient data
                transformed_patient = etl_service.transformer.transform_patient(raw_patient)
                
                logger.info(f"Processing patient {i}/{test_batch_size}: {transformed_patient.get('lastname', 'Unknown')} {transformed_patient.get('name', 'Unknown')} (ID: {transformed_patient.get('hisnumber', 'Unknown')})")
                
                # Check if patient already exists
                hisnumber = transformed_patient.get('hisnumber')
                source = transformed_patient.get('source')
                
                if pg_repo.patient_exists(hisnumber, source):
                    logger.info(f"  Patient {hisnumber} already exists, using upsert")
                    if pg_repo.upsert_patient(transformed_patient):
                        success_count += 1
                        logger.info(f"  ✓ Patient {hisnumber} updated successfully")
                    else:
                        error_count += 1
                        logger.error(f"  ✗ Failed to update patient {hisnumber}")
                else:
                    if pg_repo.insert_patient(transformed_patient):
                        success_count += 1
                        logger.info(f"  ✓ Patient {hisnumber} inserted successfully")
                    else:
                        error_count += 1
                        logger.error(f"  ✗ Failed to insert patient {hisnumber}")
                        
            except Exception as e:
                error_count += 1
                logger.error(f"  ✗ Error processing patient {i}: {str(e)}")
        
        logger.info(f"YottaDB ETL test completed: {success_count} successful, {error_count} errors")
        
        # Show some statistics
        logger.info(f"Total patients in YottaDB: {len(all_patients)}")
        logger.info(f"Test batch processed: {test_batch_size}")
        logger.info(f"Success rate: {success_count/test_batch_size*100:.1f}%")
        
        return success_count > 0
        
    except Exception as e:
        logger.error(f"Error during YottaDB ETL test: {str(e)}")
        return False
    finally:
        # Disconnect
        yottadb_connector.disconnect()
        pg_connector.disconnect()


def test_postgres_schema():
    """Test if the PostgreSQL schema is set up correctly."""
    logger.info("Testing PostgreSQL schema...")
    pg_connector = PostgresConnector(DATABASE_CONFIG["PostgreSQL"])
    
    if pg_connector.connect():
        try:
            cursor = pg_connector.connection.cursor()
            
            # Check if essential tables exist
            essential_tables = ["hislist", "businessunits", "documenttypes", "patients", "patientsdet", "protocols"]
            missing_tables = []
            
            for table in essential_tables:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = %s
                    );
                """, (table,))
                if not cursor.fetchone()[0]:
                    missing_tables.append(table)
            
            if missing_tables:
                logger.error(f"Missing tables: {', '.join(missing_tables)}")
                logger.info("The database schema is not set up correctly")
            else:
                logger.info("All essential tables exist")
                
                # Check if we have reference data in the tables
                tables_to_check = {
                    "hislist": "HIS systems",
                    "businessunits": "business units", 
                    "documenttypes": "document types"
                }
                
                for table, description in tables_to_check.items():
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    logger.info(f"Found {count} {description} in {table} table")
                    
                    if count > 0:
                        cursor.execute(f"SELECT * FROM {table} LIMIT 5")
                        rows = cursor.fetchall()
                        for row in rows:
                            logger.info(f"  {table} entry: {row}")
            
            cursor.close()
            pg_connector.disconnect()
            return len(missing_tables) == 0
        except Exception as e:
            logger.error(f"Error checking schema: {str(e)}")
            pg_connector.disconnect()
            return False
    else:
        return False


def test_document_type_handling():
    """Test document type handling in the database through the repository."""
    logger.info("Testing document type handling...")
    
    pg_connector = PostgresConnector(DATABASE_CONFIG["PostgreSQL"])
    if not pg_connector.connect():
        logger.error("PostgreSQL connection failed")
        return False
    
    try:
        # Create repository
        pg_repo = PostgresRepository(pg_connector)
        
        # Generate test patients with different document types for BOTH sources
        test_patients = []
        
        # Test with qMS patients (source=1)
        for doc_type in [1, 3, 5, 10, 14, 17]:  # Sample of document types
            # Generate appropriate document number based on type
            if doc_type == 1:  # Russian passport - 10 digits
                doc_number = random.randint(1000000000, 9999999999)
            elif doc_type in (3, 10):  # Foreign passports - 9 digits
                doc_number = random.randint(100000000, 999999999)
            elif doc_type == 5:  # Birth certificate - 12 digits
                doc_number = random.randint(100000000000, 999999999999)
            else:  # Other documents - 8 digits
                doc_number = random.randint(10000000, 99999999)
            
            # Create a qMS patient
            qms_patient = {
                "hisnumber": f"QMS_DOCTEST_{doc_type}_{random.randint(1000, 9999)}",
                "source": 1,  # qMS
                "businessunit": 1,
                "lastname": f"ТестQMS{doc_type}",
                "name": "Тест", 
                "surname": "qMSович",
                "birthdate": "1990-01-01",
                "documenttypes": doc_type,
                "document_number": doc_number,
                "email": f"qms_doc{doc_type}@example.com",
                "telephone": "79991234567",
                "his_password": None  # qMS doesn't have passwords via API
            }
            test_patients.append(qms_patient)
            
            # Create an Infoclinica patient with different document number
            if doc_type == 1:  # Russian passport - 10 digits
                doc_number2 = random.randint(1000000000, 9999999999)
            elif doc_type in (3, 10):  # Foreign passports - 9 digits
                doc_number2 = random.randint(100000000, 999999999)
            elif doc_type == 5:  # Birth certificate - 12 digits
                doc_number2 = random.randint(100000000000, 999999999999)
            else:  # Other documents - 8 digits
                doc_number2 = random.randint(10000000, 99999999)
            
            infoclinica_patient = {
                "hisnumber": f"IC_DOCTEST_{doc_type}_{random.randint(1000, 9999)}",
                "source": 2,  # Инфоклиника
                "businessunit": 2,  # Медскан
                "lastname": f"ТестIC{doc_type}",
                "name": "Тест", 
                "surname": "Инфоклиникович",
                "birthdate": "1990-01-01",
                "documenttypes": doc_type,
                "document_number": doc_number2,
                "email": f"ic_doc{doc_type}@example.com",
                "telephone": "79997654321",
                "his_password": "testpass123"  # Infoclinica has passwords
            }
            test_patients.append(infoclinica_patient)
        
        # Insert the test patients
        inserted_count = 0
        for patient in test_patients:
            if pg_repo.insert_patient(patient):
                inserted_count += 1
                doc_type_name = DOCUMENT_TYPES.get(patient["documenttypes"], "Unknown")
                source_name = "qMS" if patient["source"] == 1 else "Инфоклиника"
                logger.info(f"Inserted {source_name} patient with {doc_type_name} (ID: {patient['documenttypes']}), number: {patient['document_number']}")
        
        logger.info(f"Successfully inserted {inserted_count} out of {len(test_patients)} test patients with different document types")
        
        pg_connector.disconnect()
        return inserted_count > 0
        
    except Exception as e:
        logger.error(f"Error testing document type handling: {str(e)}")
        pg_connector.disconnect()
        return False


def test_firebird_etl():
    """Test the Firebird ETL process using the new architecture."""
    logger.info("Testing Firebird ETL process...")
    
    # Create connectors
    fb_connector = FirebirdConnector(DATABASE_CONFIG["Firebird"])
    pg_connector = PostgresConnector(DATABASE_CONFIG["PostgreSQL"])
    
    if not fb_connector.connect():
        logger.error("Failed to connect to Firebird")
        return False
        
    if not pg_connector.connect():
        logger.error("Failed to connect to PostgreSQL")
        fb_connector.disconnect()
        return False
    
    try:
        # Create repositories
        fb_repo = FirebirdRepository(fb_connector)
        pg_repo = PostgresRepository(pg_connector)
        
        # Create ETL service
        etl_service = ETLService(fb_repo, pg_repo)
        
        # Process a batch
        batch_size = 10  # Small batch for testing
        logger.info(f"Processing {batch_size} patients through Firebird ETL")
        success_count = etl_service.process_batch(batch_size=batch_size)
        
        logger.info(f"Firebird ETL processed {success_count} patients successfully")
        
        # Check results
        if success_count > 0:
            logger.info("Firebird ETL process test successful")
            return True
        else:
            logger.warning("Firebird ETL process test completed but no records were processed")
            return False
            
    except Exception as e:
        logger.error(f"Error during Firebird ETL test: {str(e)}")
        return False
    finally:
        # Disconnect
        fb_connector.disconnect()
        pg_connector.disconnect()


def main():
    """Main test function."""
    logger.info("Starting tests...")
    
    # Test connections
    pg_result = test_postgres_connection()
    fb_result = test_firebird_connection()
    ydb_result = test_yottadb_connection()
    
    # Test schema
    schema_result = test_postgres_schema()
    
    # Test document type handling with new architecture
    if pg_result and schema_result:
        doc_type_result = test_document_type_handling()
    else:
        logger.warning("Skipping document type handling test due to connection or schema issues")
        doc_type_result = False
    
    # Test Firebird ETL process
    if pg_result and fb_result and schema_result:
        fb_etl_result = test_firebird_etl()
    else:
        logger.warning("Skipping Firebird ETL test due to connection or schema issues")
        fb_etl_result = False
    
    # Test YottaDB ETL process (optional - user choice due to time)
    if pg_result and ydb_result and schema_result:
        ydb_etl_result = test_yottadb_etl()
    else:
        logger.warning("Skipping YottaDB ETL test due to connection or schema issues")
        ydb_etl_result = False
    
    # Print summary
    logger.info("\nTest Results Summary:")
    logger.info(f"PostgreSQL Connection: {'✓' if pg_result else '✗'}")
    logger.info(f"Firebird Connection: {'✓' if fb_result else '✗'}")
    logger.info(f"YottaDB Connection: {'✓' if ydb_result else '✗'}")
    logger.info(f"PostgreSQL Schema: {'✓' if schema_result else '✗'}")
    logger.info(f"Document Type Handling: {'✓' if doc_type_result else '✗'}")
    logger.info(f"Firebird ETL Process: {'✓' if fb_etl_result else '✗'}")
    
    if ydb_etl_result is not None:
        logger.info(f"YottaDB ETL Process: {'✓' if ydb_etl_result else '✗'}")
    else:
        logger.info("YottaDB ETL Process: Skipped")
    
    logger.info("\nTests completed.")


if __name__ == "__main__":
    main()