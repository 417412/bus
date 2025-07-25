import logging
import os
from typing import List, Dict, Any, Optional, Tuple
from src.connectors.firebird_connector import FirebirdConnector
from datetime import datetime
import itertools
from src.config.settings import setup_logger, STATE_DIR

class FirebirdRepository:
    
    def __init__(self, connector: FirebirdConnector = None):
        """Initialize repository with optional connector."""
        if connector is None:
            # Create connector with default decrypted config
            connector = FirebirdConnector()
            
        self.connector = connector
        self.logger = setup_logger(__name__, "repositories")
        self.source_id = 2  # Инфоклиника
        self.state_dir = str(STATE_DIR)
        
        # Ensure state directory exists
        os.makedirs(self.state_dir, exist_ok=True)
        
    def get_patients(self, batch_size: int = 100, last_id: str = None) -> List[Dict[str, Any]]:
        """
        Fetch raw patient data from Firebird.
        
        Args:
            batch_size: Maximum number of patients to fetch
            last_id: ID of the last processed patient
            
        Returns:
            List of dictionaries with patient data
        """
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
                c.clpassword AS his_password
            FROM
                clients c
            WHERE
                c.pcode IS NOT NULL
                AND c.pcode > 0
        """
        
        # Add condition for incremental loading if last_id is provided
        if last_id:
            try:
                # Try to convert last_id to integer for comparison
                int_last_id = int(last_id)
                query += f" AND c.pcode > {int_last_id}"
            except (ValueError, TypeError):
                # Fall back to string comparison if conversion fails
                query += f" AND c.pcode > '{last_id}'"
            
        # Add order by
        query += " ORDER BY c.pcode"
            
        # Add limit if needed
        if batch_size:
            query += f" ROWS {batch_size}"
            
        # Execute query
        try:
            rows, columns = self.connector.execute_query(query)
            
            # Convert to list of dictionaries
            patients = [dict(zip(columns, row)) for row in rows]
            
            # Enhanced logging with batch range information
            if patients:
                min_hisnumber = min(p.get('hisnumber', 0) for p in patients)
                max_hisnumber = max(p.get('hisnumber', 0) for p in patients)
                self.logger.info(f"Retrieved batch of {len(patients)} patients from Firebird "
                               f"(range: {min_hisnumber} - {max_hisnumber})")
            else:
                self.logger.info(f"Retrieved empty batch from Firebird (last_id: {last_id})")
                
            return patients
        except Exception as e:
            self.logger.error(f"Error fetching patients from Firebird: {str(e)}")
            return []
    
    def get_patient_deltas(self, batch_size: int = 100) -> Tuple[List[Dict[str, Any]], int]:
        """
        Fetch patient delta records from Firebird.
        """
        self.logger.info(f"=== Starting get_patient_deltas with batch_size={batch_size} ===")
        
        # Force a fresh transaction by committing any pending work
        try:
            if hasattr(self.connector, 'connection') and self.connector.connection:
                self.connector.connection.commit()
                self.logger.debug("Committed pending transaction before delta query")
        except Exception as e:
            self.logger.debug(f"Transaction commit failed (may be normal): {e}")
        
        # Query to get deltas that haven't been processed yet
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
        """
            
        # Add limit if needed
        if batch_size:
            query += f" ROWS {batch_size}"
        
        self.logger.info(f"Executing delta query with batch_size={batch_size}")
        self.logger.debug(f"Query: {query}")
            
        # Execute query
        try:
            rows, columns = self.connector.execute_query(query)
            
            # Debug the rows object
            self.logger.info(f"✓ Delta query executed successfully")
            self.logger.info(f"Type of rows: {type(rows)}")
            self.logger.info(f"len(rows): {len(rows)}")
            self.logger.info(f"bool(rows): {bool(rows)}")
            
            # Additional debugging for the systemd issue
            if len(rows) == 0:
                self.logger.warning("⚠ Zero rows returned - checking if records exist in delta table")
                
                # Check if there are any unprocessed records at all
                check_query = "SELECT COUNT(*) FROM Medscan_delta_clients WHERE processed = 'N'"
                try:
                    check_rows, _ = self.connector.execute_query(check_query)
                    if check_rows and len(check_rows) > 0:
                        unprocessed_count = check_rows[0][0]
                        self.logger.warning(f"Database shows {unprocessed_count} unprocessed records exist")
                        if unprocessed_count > 0:
                            self.logger.error("🔥 TRANSACTION ISOLATION ISSUE: Records exist but query returns 0 rows")
                            
                            # Try to force a new transaction
                            self.logger.info("Attempting to force fresh transaction...")
                            if hasattr(self.connector, 'connection') and self.connector.connection:
                                try:
                                    self.connector.connection.rollback()
                                    self.logger.info("Rolled back transaction")
                                    
                                    # Retry the query
                                    self.logger.info("Retrying delta query after rollback...")
                                    rows, columns = self.connector.execute_query(query)
                                    self.logger.info(f"After rollback retry: len(rows) = {len(rows)}")
                                    
                                except Exception as rollback_error:
                                    self.logger.error(f"Rollback failed: {rollback_error}")
                    else:
                        self.logger.info("No unprocessed records in delta table (this is normal)")
                except Exception as check_error:
                    self.logger.error(f"Error checking unprocessed record count: {check_error}")
            
            # Original check and processing continues...
            if not rows:
                self.logger.info("No delta records found, returning empty results")
                return [], 0
            
            # At this point, let's see what we actually have
            self.logger.info(f"After 'if not rows' check - proceeding with processing")
            
            # Log first few raw records
            for i, row in enumerate(rows[:3]):
                row_dict = dict(zip(columns, row))
                self.logger.info(f"Raw record {i}: hisnumber={row_dict.get('hisnumber')}, operation={row_dict.get('operation')}")
            
            # Get unique records (handle duplicates by using the latest delta record)
            patient_deltas = {}
            processed_pcodes = []
            
            self.logger.info("Processing records to get unique deltas...")
            
            # Process rows in reverse to ensure we get the latest delta for each patient
            for i, row in enumerate(reversed(rows)):
                row_dict = dict(zip(columns, row))
                hisnumber = row_dict.get('hisnumber')
                operation = row_dict.get('operation')
                
                self.logger.debug(f"Processing row {i}: hisnumber={hisnumber}, operation={operation}")
                
                # Only add this record if we haven't seen this hisnumber yet
                if hisnumber and hisnumber not in patient_deltas:
                    patient_deltas[hisnumber] = row_dict
                    self.logger.debug(f"✓ Added unique delta for hisnumber {hisnumber} with operation {operation}")
                else:
                    self.logger.debug(f"⚠ Skipped duplicate delta for hisnumber {hisnumber} with operation {operation}")
                
                # Keep track of all pcodes for marking as processed
                if hisnumber and hisnumber not in processed_pcodes:
                    processed_pcodes.append(hisnumber)
            
            # Convert dictionary to list
            unique_deltas = list(patient_deltas.values())
            
            self.logger.info(f"✓ Processed {len(rows)} delta records into {len(unique_deltas)} unique patients")
            self.logger.info(f"PCodes to mark as processed: {len(processed_pcodes)} - {processed_pcodes[:5]}")
            
            # Log some unique deltas for verification
            for i, delta in enumerate(unique_deltas[:3]):
                self.logger.info(f"Unique delta {i}: hisnumber={delta.get('hisnumber')}, operation={delta.get('operation')}, lastname={delta.get('lastname')}")
            
            # Mark these records as processed only if we have any
            processed_count = 0
            if processed_pcodes:
                self.logger.info(f"Marking {len(processed_pcodes)} pcodes as processed...")
                processed_count = self._mark_deltas_as_processed(processed_pcodes)
                
                # Verify the results
                if processed_count > 0:
                    verified_processed, verified_total = self._verify_processed_records(processed_pcodes)
                    self.logger.info(f"✓ Marked {processed_count} records as processed")
                    self.logger.info(f"✓ Verification: {verified_processed}/{verified_total} records confirmed processed")
                    
                    if verified_processed < len(processed_pcodes):
                        self.logger.warning(f"⚠ Some records may not have been marked as processed: "
                                        f"{verified_processed}/{len(processed_pcodes)}")
                else:
                    self.logger.error("✗ Failed to mark any records as processed due to deadlock/error")
            else:
                self.logger.warning("No pcodes to mark as processed (this shouldn't happen if we have deltas)")
            
            self.logger.info(f"=== get_patient_deltas completed: returning {len(unique_deltas)} deltas, {processed_count} processed ===")
            return unique_deltas, processed_count
            
        except Exception as e:
            self.logger.error(f"✗ Error fetching patient deltas from Firebird: {str(e)}")
            self.logger.error(f"Failed query: {query}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return [], 0
        
    def _mark_deltas_as_processed(self, pcodes: List[str]) -> int:
        """
        Mark delta records as processed in the Firebird database with deadlock handling.
        
        Args:
            pcodes: List of pcodes (hisnumbers) to mark as processed
            
        Returns:
            Number of records successfully marked as processed
        """
        # Defensive check - make sure pcodes is not None
        if pcodes is None:
            self.logger.warning("pcodes is None, cannot mark records as processed")
            return 0
            
        # Ensure it's a list and not some other object
        if not isinstance(pcodes, list):
            self.logger.warning(f"pcodes is not a list but {type(pcodes)}, converting")
            try:
                pcodes = list(pcodes)  # Try to convert to list
            except:
                self.logger.error(f"Cannot convert pcodes to list: {pcodes}")
                return 0
                
        # Empty list check
        if not pcodes:
            self.logger.warning("Empty pcodes list, nothing to mark as processed")
            return 0
        
        # Deadlock retry logic
        max_retries = 3
        retry_delay = 1  # Start with 1 second delay
        
        for attempt in range(max_retries):
            try:
                self.logger.debug(f"Attempt {attempt + 1}/{max_retries} to mark {len(pcodes)} records as processed")
                
                # Execute update in smaller batches to reduce deadlock chance
                batch_size = 100  # Smaller batch size to reduce lock contention
                total_processed = 0
                
                for i in range(0, len(pcodes), batch_size):
                    batch = pcodes[i:i + batch_size]
                    if not batch:
                        continue
                    
                    # Process this batch
                    batch_processed = self._update_batch_processed_status(batch, attempt + 1)
                    total_processed += batch_processed
                    
                    # Small delay between batches to reduce contention
                    if i + batch_size < len(pcodes):
                        import time
                        time.sleep(0.1)
                
                self.logger.info(f"Successfully marked {total_processed} delta records as processed on attempt {attempt + 1}")
                return total_processed
                
            except Exception as e:
                error_msg = str(e).lower()
                
                if 'deadlock' in error_msg or 'concurrent' in error_msg or 'lock' in error_msg:
                    self.logger.warning(f"Deadlock/lock conflict on attempt {attempt + 1}/{max_retries}: {e}")
                    
                    if attempt < max_retries - 1:
                        # Exponential backoff with jitter
                        import time
                        import random
                        delay = retry_delay * (2 ** attempt) + random.uniform(0, 1)
                        self.logger.info(f"Retrying in {delay:.2f} seconds...")
                        time.sleep(delay)
                        continue
                    else:
                        self.logger.error(f"Failed to mark deltas as processed after {max_retries} attempts due to deadlock")
                        return 0
                else:
                    # Non-deadlock error, don't retry
                    self.logger.error(f"Non-deadlock error marking deltas as processed: {e}")
                    return 0
        
        return 0
    
    def _update_batch_processed_status(self, batch: List[str], attempt: int) -> int:
        """
        Update a batch of records with processed status.
        
        Args:
            batch: List of pcodes for this batch
            attempt: Current attempt number (for logging)
            
        Returns:
            Number of records successfully updated
        """
        if not batch:
            return 0
        
        try:
            # For Firebird, construct the IN clause directly with values
            value_list = []
            for pcode in batch:
                if pcode is None:
                    continue
                    
                try:
                    if isinstance(pcode, (int, float)):
                        # For numeric values, don't use quotes
                        value_list.append(str(int(pcode)))
                    else:
                        # For string values, use quotes and escape single quotes
                        safe_value = str(pcode).replace("'", "''")
                        value_list.append(f"'{safe_value}'")
                except Exception as e:
                    self.logger.warning(f"Failed to process pcode value: {pcode}, error: {e}")
                    continue
            
            if not value_list:
                self.logger.warning(f"No valid values in batch, skipping")
                return 0
            
            # Join the values with commas
            values_string = ", ".join(value_list)
            
            # Use a more specific update with ORDER BY to reduce deadlock chance
            # Also add a row limit to process records in a predictable order
            query = f"""
                UPDATE Medscan_delta_clients
                SET processed = 'Y'
                WHERE pcode IN ({values_string})
                AND processed = 'N'
            """
            
            self.logger.debug(f"Attempt {attempt}: Updating batch of {len(value_list)} records")
            
            # Execute the query
            rows, columns = self.connector.execute_query(query)
            
            # For Firebird, we can't easily get the number of affected rows from UPDATE
            # So we'll return the number of pcodes we attempted to update
            return len(value_list)
            
        except Exception as e:
            # Re-raise the exception so the retry logic can handle it
            raise e
    
    def _verify_processed_records(self, pcodes: List[str]) -> Tuple[int, int]:
        """
        Verify how many records were actually marked as processed.
        
        Args:
            pcodes: List of pcodes to check
            
        Returns:
            Tuple of (processed_count, total_count)
        """
        if not pcodes:
            return 0, 0
        
        try:
            # Build the IN clause for verification
            value_list = []
            for pcode in pcodes:
                if pcode is None:
                    continue
                try:
                    if isinstance(pcode, (int, float)):
                        value_list.append(str(int(pcode)))
                    else:
                        safe_value = str(pcode).replace("'", "''")
                        value_list.append(f"'{safe_value}'")
                except:
                    continue
            
            if not value_list:
                return 0, 0
            
            values_string = ", ".join(value_list)
            
            # Count processed vs total
            query = f"""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN processed = 'Y' THEN 1 ELSE 0 END) as processed
                FROM Medscan_delta_clients 
                WHERE pcode IN ({values_string})
            """
            
            rows, columns = self.connector.execute_query(query)
            
            if rows and len(rows[0]) >= 2:
                total = rows[0][0]
                processed = rows[0][1] or 0
                self.logger.debug(f"Verification: {processed}/{total} records marked as processed")
                return processed, total
            
            return 0, 0
            
        except Exception as e:
            self.logger.error(f"Error verifying processed records: {e}")
            return 0, 0

    def get_last_processed_id(self) -> Optional[str]:
        """Get last processed patient ID from state file."""
        try:
            file_path = os.path.join(self.state_dir, "infoclinica_last_id.txt")
            if not os.path.exists(file_path):
                return None
                
            with open(file_path, "r") as f:
                return f.read().strip() or None
        except Exception as e:
            self.logger.error(f"Error reading last processed ID: {str(e)}")
            return None
            
    def save_last_processed_id(self, last_id: str) -> None:
        """Save last processed patient ID to state file."""
        try:
            file_path = os.path.join(self.state_dir, "infoclinica_last_id.txt")
            with open(file_path, "w") as f:
                f.write(str(last_id))
        except Exception as e:
            self.logger.error(f"Error saving last processed ID: {str(e)}")
    
    def save_last_sync_time(self) -> None:
        """Save the timestamp of the last successful sync."""
        try:
            file_path = os.path.join(self.state_dir, "infoclinica_last_sync.txt")
            timestamp = datetime.now().isoformat()
            with open(file_path, "w") as f:
                f.write(timestamp)
        except Exception as e:
            self.logger.error(f"Error saving last sync time: {str(e)}")
    
    def get_last_sync_time(self) -> Optional[datetime]:
        """Get the timestamp of the last successful sync."""
        try:
            file_path = os.path.join(self.state_dir, "infoclinica_last_sync.txt")
            if not os.path.exists(file_path):
                return None
                
            with open(file_path, "r") as f:
                timestamp_str = f.read().strip()
                if timestamp_str:
                    return datetime.fromisoformat(timestamp_str)
                return None
        except Exception as e:
            self.logger.error(f"Error reading last sync time: {str(e)}")
            return None
        
    def get_total_patient_count(self, include_last_id: bool = False) -> int:
        """
        Get the total count of patients in the source system.
        
        Args:
            include_last_id: If True, returns the count and last ID as a tuple
        
        Returns:
            Total patient count or (count, last_id) tuple if include_last_id is True
        """
        try:
            # Get the total count
            count_query = """
                SELECT COUNT(*) AS total, MAX(c.pcode) AS max_id
                FROM clients c
                WHERE c.pcode IS NOT NULL AND c.pcode > 0
            """
            
            rows, columns = self.connector.execute_query(count_query)
            
            if rows and len(rows[0]) >= 2:
                total_count = rows[0][0]
                last_id = rows[0][1]
                
                # Only log this once per ETL run, not every batch
                self.logger.debug(f"Total patient count in Firebird: {total_count}, last ID: {last_id}")
                
                if include_last_id:
                    return total_count, last_id
                return total_count
            else:
                self.logger.error("Failed to get patient count from Firebird")
                return 0 if not include_last_id else (0, None)
        except Exception as e:
            self.logger.error(f"Error getting patient count from Firebird: {e}")
            return 0 if not include_last_id else (0, None)
        
    def get_source_id(self) -> int:
        """Get the source ID for this repository."""
        return self.source_id