#!/usr/bin/env python3
"""
ETL Daemon for medical system data integration.

This script runs as a daemon process, periodically syncing data from
source systems (Firebird/Infoclinica, YottaDB/qMS) to the target system (PostgreSQL).
It handles both initial data loads and incremental delta syncs.
"""

import os
import sys
import time
import logging
import argparse
import signal
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple

# Add the parent directory to the path so Python can find the modules
parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent_dir)

# Import configuration
from src.config.settings import DATABASE_CONFIG, LOGGING_CONFIG, setup_logger

# Import connectors and repositories
from src.connectors.postgres_connector import PostgresConnector
from src.connectors.firebird_connector import FirebirdConnector
from src.connectors.yottadb_connector import YottaDBConnector
from src.repositories.postgres_repository import PostgresRepository
from src.repositories.firebird_repository import FirebirdRepository
from src.repositories.yottadb_repository import YottaDBRepository

# Import ETL components
from src.etl.etl_service import ETLService

# Global flag for graceful shutdown
SHOULD_RUN = True

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="ETL daemon for medical system data integration"
    )
    parser.add_argument(
        "--source",
        choices=["firebird", "yottadb"],
        default="firebird",
        help="Source system to sync from (default: firebird)"
    )
    parser.add_argument(
        "--initial-load",
        action="store_true",
        help="Perform initial data load"
    )
    parser.add_argument(
        "--delta-sync",
        action="store_true",
        help="Perform delta synchronization only"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=300,  # 5 minutes
        help="Sync interval in seconds (default: 300)"
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=1000,
        help="Maximum number of records to process per batch (default: 1000)"
    )
    parser.add_argument(
        "--no-daemon",
        action="store_true",
        help="Run once and exit (don't run as daemon)"
    )
    parser.add_argument(
        "--max-duration",
        type=int,
        default=0,
        help="Maximum run duration in minutes (0 = no limit)"
    )
    parser.add_argument(
        "--status-file",
        type=str,
        default="etl_status.json",
        help="File to write ETL status information (default: etl_status.json)"
    )
    parser.add_argument(
        "--force-completion-check",
        action="store_true",
        help="Force checking if initial load is complete by comparing record counts"
    )
    return parser.parse_args()


def setup_signal_handlers():
    """Set up signal handlers for graceful shutdown."""
    def handle_signal(signum, frame):
        global SHOULD_RUN
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        SHOULD_RUN = False
    
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)


def write_status(status_file: str, status: Dict[str, Any]) -> None:
    """Write ETL status information to a file."""
    try:
        with open(status_file, 'w') as f:
            # Convert datetime objects to strings
            status_copy = status.copy()
            for key, value in status.items():
                if isinstance(value, datetime):
                    status_copy[key] = value.isoformat()
            
            json.dump(status_copy, f, indent=2)
        logger.debug(f"Status written to {status_file}")
    except Exception as e:
        logger.error(f"Error writing status file: {e}")


def create_etl_service(source_type: str) -> Optional[ETLService]:
    """Create and initialize ETL service with repositories and connectors."""
    try:
        # Create PostgreSQL connector (always needed as target)
        pg_connector = PostgresConnector(DATABASE_CONFIG["PostgreSQL"])
        
        if not pg_connector.connect():
            logger.error("Failed to connect to PostgreSQL database")
            return None
        
        # Create source connector based on type
        if source_type == "firebird":
            logger.info("Creating Firebird ETL service")
            source_connector = FirebirdConnector(DATABASE_CONFIG["Firebird"])
            
            if not source_connector.connect():
                logger.error("Failed to connect to Firebird database")
                pg_connector.disconnect()
                return None
            
            # Create repositories
            source_repo = FirebirdRepository(source_connector)
            pg_repo = PostgresRepository(pg_connector)
            
        elif source_type == "yottadb":
            logger.info("Creating YottaDB ETL service")
            source_connector = YottaDBConnector(DATABASE_CONFIG["YottaDB"])
            
            if not source_connector.connect():
                logger.error("Failed to connect to YottaDB API")
                pg_connector.disconnect()
                return None
            
            # Create repositories
            source_repo = YottaDBRepository(source_connector)
            pg_repo = PostgresRepository(pg_connector)
            
        else:
            logger.error(f"Unknown source type: {source_type}")
            pg_connector.disconnect()
            return None
        
        # Create and return ETL service
        etl_service = ETLService(source_repo, pg_repo)
        
        return etl_service
    except Exception as e:
        logger.error(f"Error creating ETL service: {e}")
        return None


def check_initial_load_complete(etl_service: ETLService) -> Tuple[bool, int, int]:
    """
    Check if the initial load is complete by comparing record counts.
    
    Returns:
        Tuple of (is_complete, source_count, destination_count)
    """
    try:
        # Get count from source
        source_count = etl_service.source_repo.get_total_patient_count()
        
        # Get count from destination (PostgreSQL)
        destination_count = etl_service.target_repo.get_patient_count_by_source(etl_service.source_repo.source_id)
        
        # Consider complete if we have at least 95% of records
        # (some might be skipped due to data quality issues)
        completion_threshold = 0.95
        is_complete = (destination_count >= source_count * completion_threshold)
        
        logger.info(f"Source ({etl_service.source_repo.__class__.__name__}) count: {source_count}, "
                   f"Destination count: {destination_count}, "
                   f"Completion rate: {destination_count/source_count:.2%}")
        
        return is_complete, source_count, destination_count
    except Exception as e:
        logger.error(f"Error checking initial load completion: {e}")
        return False, 0, 0


def perform_initial_load(etl_service: ETLService, max_records: int, force_check: bool = False) -> Dict[str, Any]:
    """
    Perform initial data load from source to PostgreSQL.
    
    Args:
        etl_service: The ETL service to use
        max_records: Maximum number of records to process per batch
        force_check: If True, will check if initial load is complete before starting
        
    Returns:
        Status dictionary
    """
    source_name = etl_service.source_repo.__class__.__name__
    
    # First check if initial load is already complete
    if force_check:
        is_complete, source_count, destination_count = check_initial_load_complete(etl_service)
        if is_complete:
            logger.info(f"Initial load already complete: {destination_count}/{source_count} records loaded")
            
            status = {
                "operation": "initial_load",
                "source": source_name,
                "start_time": datetime.now(),
                "end_time": datetime.now(),
                "duration": 0,
                "processed_records": destination_count,
                "success_count": destination_count,
                "error_count": 0,
                "source_count": source_count,
                "destination_count": destination_count,
                "completion_rate": destination_count/source_count if source_count > 0 else 0,
                "status": "completed",
                "message": "Initial load was already complete"
            }
            
            # Save last sync time
            etl_service.source_repo.save_last_sync_time()
            
            return status
    
    logger.info(f"Starting initial data load from {source_name}")
    start_time = datetime.now()
    status = {
        "operation": "initial_load",
        "source": source_name,
        "start_time": start_time,
        "processed_records": 0,
        "success_count": 0,
        "error_count": 0,
        "last_id": None,
        "status": "running"
    }
    
    try:
        total_processed = 0
        last_id = etl_service.source_repo.get_last_processed_id()
        
        if last_id:
            logger.info(f"Resuming from last processed ID: {last_id}")
            status["last_id"] = last_id
        
        # Get initial count for progress tracking
        source_count, _ = etl_service.source_repo.get_total_patient_count(include_last_id=True)
        
        consecutive_empty_batches = 0
        max_empty_batches = 3  # Stop after 3 consecutive empty batches
        batch_counter = 0  # Counter for batches processed
        
        while SHOULD_RUN and consecutive_empty_batches < max_empty_batches:
            batch_counter += 1
            batch_start = datetime.now()
            
            # Get a batch of patients from source
            patients = etl_service.source_repo.get_patients(batch_size=max_records, last_id=last_id)
            
            if not patients:
                consecutive_empty_batches += 1
                logger.info(f"No patients found beyond ID {last_id}. Empty batch {consecutive_empty_batches}/{max_empty_batches}")
                
                if consecutive_empty_batches >= max_empty_batches:
                    logger.info(f"Reached {max_empty_batches} consecutive empty batches, finishing initial load")
                    break
                
                # Try with a new last_id (increment to avoid getting stuck)
                try:
                    # If it's numeric, add 1
                    new_last_id = str(int(last_id) + 1) if last_id else "0"
                    last_id = new_last_id
                    etl_service.source_repo.save_last_processed_id(last_id)
                    logger.info(f"Incrementing last_id to {last_id} to continue search")
                except (ValueError, TypeError):
                    # If not numeric, we can't easily increment - might need to reset
                    logger.warning(f"Cannot increment non-numeric last_id: {last_id}")
                    break
                    
                continue
            
            # Reset counter since we found records
            consecutive_empty_batches = 0
            
            # Process the batch of patients
            batch_success_count = 0
            for raw_patient in patients:
                try:
                    # Transform the patient data first
                    patient = etl_service.transformer.transform_patient(raw_patient)
                    
                    # Check if the patient already exists to avoid duplicate key violations
                    hisnumber = patient.get('hisnumber')
                    source = patient.get('source')
                    
                    if etl_service.target_repo.patient_exists(hisnumber, source):
                        logger.debug(f"Patient {hisnumber} already exists, skipping")
                        batch_success_count += 1  # Count as success since it's already there
                        continue
                        
                    # Insert the patient
                    if etl_service.target_repo.insert_patient(patient):
                        batch_success_count += 1
                except Exception as e:
                    logger.error(f"Error processing patient {raw_patient.get('hisnumber')}: {e}")
                    status["error_count"] += 1
            
            # Find the maximum hisnumber in this batch to use as the next last_id
            if patients:
                try:
                    # Get the highest hisnumber from this batch
                    max_hisnumber = max(p.get('hisnumber', '0') for p in patients)
                    if max_hisnumber:
                        last_id = max_hisnumber
                        etl_service.source_repo.save_last_processed_id(last_id)
                        logger.debug(f"Updated last_id to {last_id}")
                except Exception as e:
                    logger.error(f"Error determining last processed ID: {e}")
            
            # Update status
            status["processed_records"] += len(patients)
            status["success_count"] += batch_success_count
            status["last_id"] = last_id
            
            batch_end = datetime.now()
            batch_duration = (batch_end - batch_start).total_seconds()
            
            total_processed += batch_success_count
            
            # Get current progress
            _, current_last_id = etl_service.source_repo.get_total_patient_count(include_last_id=True)
            progress = "unknown"
            if current_last_id and last_id:
                try:
                    progress = f"{last_id}/{current_last_id}"
                except (ValueError, TypeError):
                    progress = f"{last_id}/{current_last_id}"
            
            logger.debug(f"Processed batch in {batch_duration:.2f}s, "
                        f"total processed: {total_processed}, "
                        f"progress: {progress}")
            
            # Check if we've reached the end of data
            if len(patients) < max_records:
                logger.info(f"Retrieved fewer records ({len(patients)}) than requested ({max_records}), "
                          f"might be near end of data")
            
            # Periodically check completion (every 5 batches)
            if batch_counter % 5 == 0:
                is_complete, final_source_count, final_dest_count = check_initial_load_complete(etl_service)
                status["source_count"] = final_source_count
                status["destination_count"] = final_dest_count
                
                if is_complete:
                    logger.info(f"Initial load complete: {final_dest_count}/{final_source_count} records loaded")
                    break
        
        # Final completion check
        is_complete, final_source_count, final_dest_count = check_initial_load_complete(etl_service)
        status["source_count"] = final_source_count
        status["destination_count"] = final_dest_count
        status["completion_rate"] = final_dest_count/final_source_count if final_source_count > 0 else 0
        
        # Update final status
        status["end_time"] = datetime.now()
        status["duration"] = (status["end_time"] - start_time).total_seconds()
        status["status"] = "completed" if SHOULD_RUN else "interrupted"
        
        if is_complete:
            # Save last sync time only if we actually completed the initial load
            etl_service.source_repo.save_last_sync_time()
            logger.info(f"Initial load {status['status']}, "
                      f"processed {total_processed} records in "
                      f"{status['duration']:.2f} seconds")
        else:
            logger.warning(f"Initial load not complete: {final_dest_count}/{final_source_count} records loaded. "
                         f"Will continue on next run.")
        
        return status
    except Exception as e:
        logger.error(f"Error during initial load: {e}")
        status["end_time"] = datetime.now()
        status["duration"] = (status["end_time"] - start_time).total_seconds()
        status["status"] = "failed"
        status["error"] = str(e)
        return status


def perform_yottadb_sync(etl_service: ETLService, max_records: int) -> Dict[str, Any]:
    """
    Perform YottaDB full synchronization (upsert all records).
    Since YottaDB provides current state, we upsert everything.
    """
    source_name = etl_service.source_repo.__class__.__name__
    
    logger.info(f"Starting YottaDB full sync from {source_name}")
    start_time = datetime.now()
    status = {
        "operation": "yottadb_full_sync",
        "source": source_name,
        "start_time": start_time,
        "processed_records": 0,
        "success_count": 0,
        "error_count": 0,
        "new_records": 0,
        "updated_records": 0,
        "last_id": None,
        "status": "running"
    }
    
    try:
        total_processed = 0
        last_id = etl_service.source_repo.get_last_processed_id()
        
        if last_id:
            logger.info(f"Resuming YottaDB sync from last processed ID: {last_id}")
        
        consecutive_empty_batches = 0
        max_empty_batches = 3
        batch_counter = 0
        
        while SHOULD_RUN and consecutive_empty_batches < max_empty_batches:
            batch_counter += 1
            batch_start = datetime.now()
            
            # Get a batch of patients from source
            patients = etl_service.source_repo.get_patients(batch_size=max_records, last_id=last_id)
            
            if not patients:
                consecutive_empty_batches += 1
                logger.info(f"No more patients found beyond ID {last_id}. Empty batch {consecutive_empty_batches}/{max_empty_batches}")
                if consecutive_empty_batches >= max_empty_batches:
                    logger.info("YottaDB sync completed - no more records")
                    break
                
                # Try incrementing last_id for YottaDB
                try:
                    new_last_id = str(int(last_id) + 1) if last_id else "0"
                    last_id = new_last_id
                    etl_service.source_repo.save_last_processed_id(last_id)
                    logger.info(f"Incrementing last_id to {last_id} to continue search")
                except (ValueError, TypeError):
                    logger.warning(f"Cannot increment non-numeric last_id: {last_id}")
                    break
                continue
            
            consecutive_empty_batches = 0
            
            # Process the batch of patients with UPSERT logic
            batch_success_count = 0
            batch_new_count = 0
            batch_updated_count = 0
            
            for raw_patient in patients:
                try:
                    # Transform the patient data
                    patient = etl_service.transformer.transform_patient(raw_patient)
                    
                    hisnumber = patient.get('hisnumber')
                    source = patient.get('source')
                    
                    # Check if patient exists to track new vs updated
                    patient_exists = etl_service.target_repo.patient_exists(hisnumber, source)
                    
                    # Always upsert (insert or update)
                    if etl_service.target_repo.upsert_patient(patient):
                        batch_success_count += 1
                        if patient_exists:
                            batch_updated_count += 1
                        else:
                            batch_new_count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing patient {raw_patient.get('hisnumber')}: {e}")
                    status["error_count"] += 1
            
            # Update tracking
            if patients:
                try:
                    max_hisnumber = max(p.get('hisnumber', '0') for p in patients)
                    if max_hisnumber:
                        last_id = max_hisnumber
                        etl_service.source_repo.save_last_processed_id(last_id)
                        logger.debug(f"Updated last_id to {last_id}")
                except Exception as e:
                    logger.error(f"Error determining last processed ID: {e}")
            
            # Update status
            status["processed_records"] += len(patients)
            status["success_count"] += batch_success_count
            status["new_records"] += batch_new_count
            status["updated_records"] += batch_updated_count
            status["last_id"] = last_id
            
            batch_end = datetime.now()
            batch_duration = (batch_end - batch_start).total_seconds()
            total_processed += batch_success_count
            
            logger.info(f"Processed YottaDB batch {batch_counter} in {batch_duration:.2f}s: "
                       f"{len(patients)} records, {batch_new_count} new, {batch_updated_count} updated, "
                       f"total processed: {total_processed}")
            
            # Check if we've reached the end of data
            if len(patients) < max_records:
                logger.info(f"Retrieved fewer records ({len(patients)}) than requested ({max_records}), "
                          f"might be near end of data")
        
        # Save last sync time
        etl_service.source_repo.save_last_sync_time()
        
        # Update final status
        status["end_time"] = datetime.now()
        status["duration"] = (status["end_time"] - start_time).total_seconds()
        status["status"] = "completed" if SHOULD_RUN else "interrupted"
        
        logger.info(f"YottaDB sync {status['status']}: "
                   f"processed {total_processed} records "
                   f"({status['new_records']} new, {status['updated_records']} updated) "
                   f"in {status['duration']:.2f} seconds")
        
        return status
        
    except Exception as e:
        logger.error(f"Error during YottaDB sync: {e}")
        status["end_time"] = datetime.now()
        status["duration"] = (status["end_time"] - start_time).total_seconds()
        status["status"] = "failed"
        status["error"] = str(e)
        return status


def perform_delta_sync(etl_service: ETLService, max_records: int) -> Dict[str, Any]:
    """Perform delta synchronization from source to PostgreSQL."""
    source_name = etl_service.source_repo.__class__.__name__
    
    logger.info(f"Starting delta synchronization from {source_name}")
    start_time = datetime.now()
    status = {
        "operation": "delta_sync",
        "source": source_name,
        "start_time": start_time,
        "total_delta_records": 0,
        "unique_patients": 0,
        "processed_records": 0,
        "success_count": 0,
        "error_count": 0,
        "operations": {
            "INSERT": 0,
            "UPDATE": 0,
            "DELETE": 0
        },
        "status": "running"
    }
    
    try:
        # Get delta records
        delta_records, processed_count = etl_service.source_repo.get_patient_deltas(batch_size=max_records)
        
        status["total_delta_records"] = processed_count
        status["unique_patients"] = len(delta_records)
        
        if not delta_records:
            logger.info("No delta records to process")
            status["status"] = "completed"
            status["end_time"] = datetime.now()
            status["duration"] = (status["end_time"] - start_time).total_seconds()
            return status
        
        # Group records by operation type
        inserts = []
        updates = []
        deletes = []
        
        for raw_record in delta_records:
            try:
                operation = raw_record.get('operation', '').upper()
                
                # Remove the operation field (no delta_id in our schema)
                record_copy = raw_record.copy()
                record_copy.pop('operation', None)
                
                # Transform the record
                transformed_record = etl_service.transformer.transform_patient(record_copy)
                
                if operation == 'INSERT':
                    inserts.append(transformed_record)
                    status["operations"]["INSERT"] += 1
                elif operation == 'UPDATE':
                    updates.append(transformed_record)
                    status["operations"]["UPDATE"] += 1
                elif operation == 'DELETE':
                    deletes.append(transformed_record)
                    status["operations"]["DELETE"] += 1
            except Exception as e:
                logger.error(f"Error transforming delta record: {e}")
                status["error_count"] += 1
        
        # Process inserts and updates
        success_count = 0
        
        # Process inserts
        for record in inserts:
            try:
                hisnumber = record.get('hisnumber')
                source = record.get('source')
                
                # Check if record already exists before trying to insert
                if etl_service.target_repo.patient_exists(hisnumber, source):
                    logger.info(f"Patient {hisnumber} already exists, treating as UPDATE instead of INSERT")
                    if etl_service.target_repo.upsert_patient(record):
                        success_count += 1
                else:
                    if etl_service.target_repo.insert_patient(record):
                        success_count += 1
            except Exception as e:
                logger.error(f"Error inserting patient {record.get('hisnumber')}: {e}")
                status["error_count"] += 1
        
        # Process updates
        for record in updates:
            try:
                if etl_service.target_repo.upsert_patient(record):
                    success_count += 1
            except Exception as e:
                logger.error(f"Error updating patient {record.get('hisnumber')}: {e}")
                status["error_count"] += 1
        
        # Process deletes
        for record in deletes:
            try:
                hisnumber = record.get('hisnumber')
                source = record.get('source')
                if hisnumber and source:
                    if etl_service.target_repo.mark_patient_deleted(hisnumber, source):
                        success_count += 1
            except Exception as e:
                logger.error(f"Error deleting patient {record.get('hisnumber')}: {e}")
                status["error_count"] += 1
        
        status["processed_records"] = len(inserts) + len(updates) + len(deletes)
        status["success_count"] = success_count
        
        # Save last sync time
        etl_service.source_repo.save_last_sync_time()
        
        # Update final status
        status["end_time"] = datetime.now()
        status["duration"] = (status["end_time"] - start_time).total_seconds()
        status["status"] = "completed"
        
        logger.info(f"Delta sync completed, processed {success_count}/{status['processed_records']} "
                    f"records in {status['duration']:.2f} seconds")
        
        return status
    except Exception as e:
        logger.error(f"Error during delta sync: {e}")
        status["end_time"] = datetime.now()
        status["duration"] = (status["end_time"] - start_time).total_seconds()
        status["status"] = "failed"
        status["error"] = str(e)
        return status


def main():
    """Main function."""
    args = parse_args()
    
    # Set up source-specific logging
    log_file_key = f"etl_daemon_{args.source}"
    global logger
    logger = setup_logger("etl_daemon", log_file_key)
    
    setup_signal_handlers()
    
    logger.info(f"ETL Daemon starting with source: {args.source}")
    
    start_time = datetime.now()
    max_end_time = None
    
    if args.max_duration > 0:
        max_end_time = start_time + timedelta(minutes=args.max_duration)
        logger.info(f"Maximum run duration set to {args.max_duration} minutes")
    
    # Run once or as daemon
    run_once = args.no_daemon or args.initial_load
    
    if args.initial_load and args.delta_sync:
        logger.error("Cannot specify both --initial-load and --delta-sync")
        return 1
    
    # Delta sync is not supported for YottaDB
    if args.source == "yottadb" and args.delta_sync:
        logger.error("Delta sync is not supported for YottaDB source")
        return 1
    
    # Set up ETL service
    etl_service = create_etl_service(args.source)
    if not etl_service:
        logger.error("Failed to create ETL service")
        return 1
    
    try:
        iteration = 0
        
        while SHOULD_RUN:
            iteration += 1
            cycle_start = datetime.now()
            
            logger.info(f"Starting ETL cycle {iteration}")
            
            # Check if we've exceeded the maximum run duration
            if max_end_time and datetime.now() >= max_end_time:
                logger.info(f"Maximum run duration of {args.max_duration} minutes reached")
                break
            
            # Perform ETL operations
            if args.initial_load:
                if args.source == "yottadb":
                    status = perform_yottadb_sync(etl_service, args.max_records)
                else:
                    status = perform_initial_load(etl_service, args.max_records, args.force_completion_check)
                write_status(args.status_file, status)
                break  # Exit after initial load
            elif args.delta_sync:
                status = perform_delta_sync(etl_service, args.max_records)
                write_status(args.status_file, status)
                if run_once:
                    break
            else:
                # Regular operation
                if args.source == "yottadb":
                    # For YottaDB, always do full sync
                    logger.info("YottaDB source: performing periodic full sync")
                    status = perform_yottadb_sync(etl_service, args.max_records)
                else:
                    # For Firebird, check initial load completion and do delta sync
                    is_complete, source_count, destination_count = check_initial_load_complete(etl_service)
                    last_sync_time = etl_service.source_repo.get_last_sync_time()
                    
                    if not is_complete:
                        # Initial load is not complete, continue with it
                        logger.info(f"Initial load not complete ({destination_count}/{source_count}). Continuing...")
                        status = perform_initial_load(etl_service, args.max_records)
                        
                        # If the load is still not complete, we'll continue in the next cycle
                        if status.get("status") == "completed" and status.get("completion_rate", 0) < 0.95:
                            logger.warning("Initial load cycle completed but overall load is not complete. "
                                          "Will continue in next cycle.")
                    elif last_sync_time is None:
                        # Initial load is complete but no sync time recorded
                        # This can happen if we're switching from an old version
                        logger.info("Initial load complete but no sync time recorded. Setting now.")
                        etl_service.source_repo.save_last_sync_time()
                        status = {"operation": "sync_time_update", "status": "completed"}
                    else:
                        # Initial load is complete and we have a sync time, do delta sync
                        logger.info(f"Last sync: {last_sync_time.isoformat()}, performing delta sync")
                        status = perform_delta_sync(etl_service, args.max_records)
                
                write_status(args.status_file, status)
                
                if run_once:
                    break
            
            # If running as daemon, sleep until next cycle
            if not run_once:
                cycle_end = datetime.now()
                cycle_duration = (cycle_end - cycle_start).total_seconds()
                
                # Calculate sleep time
                sleep_time = max(1, args.interval - cycle_duration)
                
                if sleep_time > 0:
                    logger.info(f"Waiting {sleep_time:.2f} seconds until next sync cycle")
                    # Sleep in small increments to allow for clean shutdown
                    sleep_increment = 1
                    for _ in range(int(sleep_time // sleep_increment)):
                        if not SHOULD_RUN:
                            break
                        time.sleep(sleep_increment)
                    # Sleep any remaining time
                    remaining = sleep_time % sleep_increment
                    if remaining > 0 and SHOULD_RUN:
                        time.sleep(remaining)
        
        logger.info("ETL Daemon shutting down")
        
    except Exception as e:
        logger.error(f"Unhandled exception in ETL daemon: {e}")
        return 1
    finally:
        # Clean up resources
        if etl_service:
            if hasattr(etl_service.source_repo.connector, 'disconnect'):
                etl_service.source_repo.connector.disconnect()
            if hasattr(etl_service.target_repo.connector, 'disconnect'):
                etl_service.target_repo.connector.disconnect()
    
    return 0


if __name__ == "__main__":
    exit(main())