#!/usr/bin/env python3
"""
ETL Administration utilities.
Provides commands for managing ETL processes, checking status, and maintenance.
"""

import os
import sys
import argparse
import json
from datetime import datetime
from typing import Dict, Any, Optional

# Add the parent directory to the path
parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent_dir)

from src.config.settings import setup_logger, get_decrypted_database_config
from src.connectors.postgres_connector import PostgresConnector
from src.connectors.firebird_connector import FirebirdConnector
from src.connectors.yottadb_connector import YottaDBConnector
from src.repositories.postgres_repository import PostgresRepository
from src.repositories.firebird_repository import FirebirdRepository
from src.repositories.yottadb_repository import YottaDBRepository

def show_yottadb_status(args):
    """Show YottaDB processing status with new hisnumber tracking."""
    logger = setup_logger("etl_admin", "admin")
    
    try:
        # Connect to YottaDB
        yottadb_connector = YottaDBConnector()
        if not yottadb_connector.connect():
            logger.error("Failed to connect to YottaDB")
            return False
        
        yottadb_repo = YottaDBRepository(yottadb_connector)
        
        # Get status information
        logger.info("=== YottaDB Processing Status ===")
        
        # Total patients in source
        total_patients = yottadb_repo.get_total_patient_count()
        logger.info(f"Total patients in YottaDB: {total_patients}")
        
        # Processed hisnumbers
        processed_hisnumbers = yottadb_repo.get_processed_hisnumbers()
        processed_count = len(processed_hisnumbers)
        logger.info(f"Processed patients: {processed_count}")
        
        # Unprocessed count
        unprocessed_count = total_patients - processed_count
        logger.info(f"Unprocessed patients: {unprocessed_count}")
        
        # Completion percentage
        if total_patients > 0:
            completion_percent = (processed_count / total_patients) * 100
            logger.info(f"Completion: {completion_percent:.1f}%")
        
        # Last sync time
        last_sync = yottadb_repo.get_last_sync_time()
        if last_sync:
            logger.info(f"Last sync: {last_sync.isoformat()}")
        else:
            logger.info("Last sync: Never")
        
        # Show sample processed hisnumbers
        if processed_hisnumbers:
            sample_size = min(10, len(processed_hisnumbers))
            sample_hisnumbers = sorted(list(processed_hisnumbers))[:sample_size]
            logger.info(f"Sample processed hisnumbers (first {sample_size}):")
            for hisnumber in sample_hisnumbers:
                logger.info(f"  {hisnumber}")
        
        # Get sample of unprocessed patients
        if unprocessed_count > 0:
            logger.info("Sample unprocessed patients:")
            unprocessed_patients = yottadb_repo.get_patients(batch_size=5)
            for patient in unprocessed_patients:
                logger.info(f"  {patient.get('hisnumber')} - {patient.get('lastname', 'Unknown')} {patient.get('name', 'Unknown')}")
        
        yottadb_connector.disconnect()
        return True
        
    except Exception as e:
        logger.error(f"Error getting YottaDB status: {e}")
        return False

def reset_yottadb_state(args):
    """Reset YottaDB processed state."""
    logger = setup_logger("etl_admin", "admin")
    
    if not args.confirm:
        logger.error("This will reset all YottaDB processing state. Use --confirm to proceed.")
        return False
    
    try:
        # Connect to YottaDB
        yottadb_connector = YottaDBConnector()
        if not yottadb_connector.connect():
            logger.error("Failed to connect to YottaDB")
            return False
        
        yottadb_repo = YottaDBRepository(yottadb_connector)
        
        # Get current state before reset
        processed_before = len(yottadb_repo.get_processed_hisnumbers())
        logger.info(f"Current processed hisnumbers: {processed_before}")
        
        # Reset state
        logger.info("Resetting YottaDB processed state...")
        yottadb_repo.reset_processed_state()
        
        # Verify reset
        processed_after = len(yottadb_repo.get_processed_hisnumbers())
        logger.info(f"Processed hisnumbers after reset: {processed_after}")
        
        if processed_after == 0:
            logger.info("✅ YottaDB state reset successfully")
        else:
            logger.error("❌ State reset may have failed")
        
        yottadb_connector.disconnect()
        return True
        
    except Exception as e:
        logger.error(f"Error resetting YottaDB state: {e}")
        return False

def show_postgres_stats(args):
    """Show PostgreSQL statistics."""
    logger = setup_logger("etl_admin", "admin")
    
    try:
        # Connect to PostgreSQL
        pg_connector = PostgresConnector()
        if not pg_connector.connect():
            logger.error("Failed to connect to PostgreSQL")
            return False
        
        pg_repo = PostgresRepository(pg_connector)
        
        logger.info("=== PostgreSQL Statistics ===")
        
        # Total patients by source
        for source_id in [1, 2]:  # qMS and Infoclinica
            count = pg_repo.get_total_patient_count(source=source_id)
            source_name = "qMS" if source_id == 1 else "Infoclinica"
            logger.info(f"Total {source_name} patients: {count}")
        
        # Total patients
        total_count = pg_repo.get_total_patient_count()
        logger.info(f"Total patients (all sources): {total_count}")
        
        # Patient matching statistics
        rows, columns = pg_connector.execute_query("""
            SELECT match_type, COUNT(*) as count 
            FROM patient_matching_log 
            GROUP BY match_type 
            ORDER BY count DESC
        """)
        
        if rows:
            logger.info("Patient matching statistics:")
            for match_type, count in rows:
                logger.info(f"  {match_type}: {count}")
        
        # Recent activity
        rows, columns = pg_connector.execute_query("""
            SELECT match_type, COUNT(*) as count
            FROM patient_matching_log 
            WHERE match_time > NOW() - INTERVAL '24 hours'
            GROUP BY match_type 
            ORDER BY count DESC
        """)
        
        if rows:
            logger.info("Activity in last 24 hours:")
            for match_type, count in rows:
                logger.info(f"  {match_type}: {count}")
        
        pg_connector.disconnect()
        return True
        
    except Exception as e:
        logger.error(f"Error getting PostgreSQL stats: {e}")
        return False

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="ETL Administration Utilities")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # YottaDB status command
    status_parser = subparsers.add_parser('yottadb-status', help='Show YottaDB processing status')
    status_parser.set_defaults(func=show_yottadb_status)
    
    # YottaDB reset command
    reset_parser = subparsers.add_parser('yottadb-reset', help='Reset YottaDB processing state')
    reset_parser.add_argument('--confirm', action='store_true', 
                             help='Confirm the reset operation')
    reset_parser.set_defaults(func=reset_yottadb_state)
    
    # PostgreSQL stats command
    pg_parser = subparsers.add_parser('pg-stats', help='Show PostgreSQL statistics')
    pg_parser.set_defaults(func=show_postgres_stats)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Execute the command
    try:
        success = args.func(args)
        return 0 if success else 1
    except Exception as e:
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    exit(main())