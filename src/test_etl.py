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
parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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
    """Test YottaDB connection."""
    logger.info("Testing YottaDB connection...")
    logger.info("WARNING: This test will take 2-3 minutes due to YottaDB API response time")
    
    yottadb_connector = YottaDBConnector(DATABASE_CONFIG["YottaDB"])
    
    # First test basic connectivity (quick test)
    if yottadb_connector.connect():
        logger.info("YottaDB basic connection test successful")
        
        # Now test through repository with a limited fetch
        try:
            yottadb_repo = YottaDBRepository(yottadb_connector)
            
            # Get just a small batch to test functionality without full API call
            logger.info("Testing YottaDB repository with small batch...")
            patients = yottadb_repo.get_patients(batch_size=2)
            
            if patients:
                logger.info(f"Retrieved {len(patients)} patients from YottaDB")
                
                # Display sample data
                for i, patient in enumerate(patients):
                    logger.info(f"Patient {i+1}: {patient.get('lastname', 'Unknown')} {patient.get('name', 'Unknown')} (ID: {patient.get('hisnumber', 'Unknown')})")
            else:
                logger.warning("No patients retrieved from YottaDB (this might be normal if no data or API issues)")
        
        except Exception as e:
            logger.error(f"Error testing YottaDB repository: {str(e)}")
        
        yottadb_connector.disconnect()
        return True
    else:
        logger.error("YottaDB connection failed")
        return False


def test_yottadb_full_fetch():
    """Test YottaDB full data fetch (separate test due to time)."""
    logger.info("Testing YottaDB full data fetch...")
    logger.info("WARNING: This will take 2-3 minutes!")
    
    yottadb_connector = YottaDBConnector(DATABASE_CONFIG["YottaDB"])
    
    if yottadb_connector.connect():
        try:
            # Test full fetch
            patients = yottadb_connector.fetch_all_patients()
            logger.info(f"YottaDB full fetch returned {len(patients)} patients")
            
            if patients:
                # Show some sample data
                for i, patient in enumerate(patients[:3]):
                    logger.info(f"Sample patient {i+1}: {patient.get('lastname', 'Unknown')} {patient.get('name', 'Unknown')} (ID: {patient.get('hisnumber', 'Unknown')})")
                
                return True
            else:
                logger.warning("No patients returned from full fetch")
                return False
                
        except Exception as e:
            logger.error(f"Error during full fetch test: {str(e)}")
            return False
        finally:
            yottadb_connector.disconnect()
    else:
        logger.error("YottaDB connection failed")
        return False


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
        
        # Generate test patients with different document types
        test_patients = []
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
            
            # Create a patient with this document
            patient = {
                "hisnumber": f"DOCTEST_{doc_type}_{random.randint(1000, 9999)}",
                "source": 1,  # qMS
                "businessunit": 1,
                "lastname": f"Документов{doc_type}",
                "name": "Тест", 
                "surname": "Документович",
                "birthdate": "1990-01-01",
                "documenttypes": doc_type,
                "document_number": doc_number,
                "email": f"doc{doc_type}@example.com",
                "telephone": "+7 (999) 999-99-99",
                "his_password": "testpass"
            }
            test_patients.append(patient)
        
        # Insert the test patients
        inserted_count = 0
        for patient in test_patients:
            if pg_repo.insert_patient(patient):
                inserted_count += 1
                doc_type_name = DOCUMENT_TYPES.get(patient["documenttypes"], "Unknown")
                logger.info(f"Inserted patient with {doc_type_name} (ID: {patient['documenttypes']}), number: {patient['document_number']}")
        
        logger.info(f"Successfully inserted {inserted_count} out of {len(test_patients)} test patients with different document types")
        
        pg_connector.disconnect()
        return inserted_count > 0
        
    except Exception as e:
        logger.error(f"Error testing document type handling: {str(e)}")
        pg_connector.disconnect()
        return False


def test_etl_process():
    """Test the ETL process using the new architecture."""
    logger.info("Testing ETL process with the repository-transformer architecture...")
    
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
        logger.info(f"Processing {batch_size} patients through ETL")
        success_count = etl_service.process_batch(batch_size=batch_size)
        
        logger.info(f"ETL processed {success_count} patients successfully")
        
        # Check results
        if success_count > 0:
            logger.info("ETL process test successful")
            return True
        else:
            logger.warning("ETL process test completed but no records were processed")
            return False
            
    except Exception as e:
        logger.error(f"Error during ETL test: {str(e)}")
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
    
    # Test ETL process with new architecture
    if pg_result and fb_result and schema_result:
        etl_result = test_etl_process()
    else:
        logger.warning("Skipping ETL process test due to connection or schema issues")
        etl_result = False
    
    # Ask if user wants to test YottaDB full fetch (time-consuming)
    if ydb_result:
        logger.info("\nYottaDB basic connectivity test passed.")
        response = input("Do you want to test YottaDB full data fetch? This takes 2-3 minutes (y/N): ")
        if response.lower() in ['y', 'yes']:
            ydb_full_result = test_yottadb_full_fetch()
        else:
            ydb_full_result = None
            logger.info("Skipping YottaDB full fetch test")
    else:
        ydb_full_result = False
    
    # Print summary
    logger.info("\nTest Results Summary:")
    logger.info(f"PostgreSQL Connection: {'✓' if pg_result else '✗'}")
    logger.info(f"Firebird Connection: {'✓' if fb_result else '✗'}")
    logger.info(f"YottaDB Basic Connection: {'✓' if ydb_result else '✗'}")
    if ydb_full_result is not None:
        logger.info(f"YottaDB Full Fetch: {'✓' if ydb_full_result else '✗'}")
    logger.info(f"PostgreSQL Schema: {'✓' if schema_result else '✗'}")
    logger.info(f"Document Type Handling: {'✓' if doc_type_result else '✗'}")
    logger.info(f"ETL Process: {'✓' if etl_result else '✗'}")
    
    logger.info("\nTests completed.")


if __name__ == "__main__":
    main()