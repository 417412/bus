#!/usr/bin/env python3
"""
Test script to debug delta query issues.
"""

import os
import sys
from pathlib import Path

# Add the parent directory to the path
parent_dir = Path(__file__).parent.parent.parent
sys.path.append(str(parent_dir))

from src.connectors.firebird_connector import FirebirdConnector
from src.config.settings import get_decrypted_database_config, setup_logger

def test_delta_query():
    """Test the delta query directly."""
    logger = setup_logger("test_delta", "test_etl")
    
    # Get decrypted database config
    db_config = get_decrypted_database_config()
    
    # Create connector
    fb_connector = FirebirdConnector(db_config["Firebird"])
    
    if not fb_connector.connect():
        logger.error("Failed to connect to Firebird")
        return
    
    try:
        # Test 1: Check if the table exists and has data
        logger.info("=== Test 1: Check table existence and data ===")
        rows, columns = fb_connector.execute_query("SELECT COUNT(*) FROM Medscan_delta_clients")
        logger.info(f"Total records in Medscan_delta_clients: {rows[0][0]}")
        
        # Test 2: Check unprocessed records
        logger.info("=== Test 2: Check unprocessed records ===")
        rows, columns = fb_connector.execute_query("SELECT COUNT(*) FROM Medscan_delta_clients WHERE processed = 'N'")
        logger.info(f"Unprocessed records: {rows[0][0]}")
        
        # Test 3: Show some unprocessed records
        logger.info("=== Test 3: Show sample unprocessed records ===")
        rows, columns = fb_connector.execute_query("""
            SELECT FIRST 5 pcode, operation, processed, change_time 
            FROM Medscan_delta_clients 
            WHERE processed = 'N' 
            ORDER BY change_time DESC
        """)
        
        for row in rows:
            record = dict(zip(columns, row))
            logger.info(f"Sample record: {record}")
        
        # Test 4: Test the exact query used in the repository
        logger.info("=== Test 4: Test repository query ===")
        query = """
            SELECT
                d.pcode AS hisnumber,
                2 AS source,
                CASE
                    WHEN (d.filial = 7) THEN 3
                    ELSE 2
                END AS businessunit,
                d.lastname,
                d.firstname AS name,
                d.midname AS surname,
                d.bdate AS birthdate,
                d.pasptype as documenttypes,
                REPLACE(REPLACE(d.paspser, '-', ''), ' ', '') || REPLACE(REPLACE(d.paspnum, '-', ''), ' ', '') AS document_number,
                d.clmail AS email,
                CASE
                    WHEN (d.phone3 IS NOT NULL AND d.phone3 <> '') THEN RemoveNonNumeric(d.phone3)
                    WHEN (d.phone2 IS NOT NULL AND d.phone2 <> '') THEN RemoveNonNumeric(d.phone2)
                    ELSE RemoveNonNumeric(d.phone1)
                END AS telephone,
                d.clpassword AS his_password,
                d.operation
            FROM
                Medscan_delta_clients d
            WHERE
                d.processed = 'N'
            ORDER BY d.pcode
            ROWS 10
        """
        
        logger.info(f"Executing query: {query}")
        rows, columns = fb_connector.execute_query(query)
        logger.info(f"Repository query returned {len(rows)} records")
        
        for i, row in enumerate(rows[:3]):
            record = dict(zip(columns, row))
            logger.info(f"Record {i}: hisnumber={record.get('hisnumber')}, operation={record.get('operation')}")
        
    except Exception as e:
        logger.error(f"Error during testing: {e}")
    finally:
        fb_connector.disconnect()

if __name__ == "__main__":
    test_delta_query()