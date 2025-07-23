import logging
import os
from typing import List, Dict, Any, Optional, Tuple
from src.connectors.firebird_connector import FirebirdConnector
from datetime import datetime
import itertools
from src.config.settings import setup_logger

class FirebirdRepository:
    """Repository for accessing Firebird data."""
    
    def __init__(self, connector: FirebirdConnector):
        self.connector = connector
        self.logger = setup_logger(__name__, "repositories")
        self.source_id = 2  # Инфоклиника
        self.state_dir = "state"
        
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
            self.logger.debug(f"Retrieved {len(patients)} patients from Firebird")
            return patients
        except Exception as e:
            self.logger.error(f"Error fetching patients from Firebird: {str(e)}")
            return []
    
    def get_patient_deltas(self, batch_size: int = 100) -> Tuple[List[Dict[str, Any]], int]:
        """
        Fetch patient delta records from Firebird.
        
        Args:
            batch_size: Maximum number of deltas to fetch
            
        Returns:
            Tuple of (list of dictionaries with patient delta data, count of processed records)
        """
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
            
        # Execute query
        try:
            rows, columns = self.connector.execute_query(query)
            
            # Get unique records (handle duplicates by using the latest delta record)
            patient_deltas = {}
            processed_pcodes = []
            
            # Process rows in reverse to ensure we get the latest delta for each patient
            for row in reversed(rows):
                row_dict = dict(zip(columns, row))
                hisnumber = row_dict.get('hisnumber')
                
                # Only add this record if we haven't seen this hisnumber yet
                if hisnumber and hisnumber not in patient_deltas:
                    patient_deltas[hisnumber] = row_dict
                
                # Keep track of all pcodes for marking as processed
                if hisnumber and hisnumber not in processed_pcodes:
                    processed_pcodes.append(hisnumber)
            
            # Convert dictionary to list
            unique_deltas = list(patient_deltas.values())
            self.logger.info(f"Retrieved {len(rows)} delta records, {len(unique_deltas)} unique patients")
            
            # Mark these records as processed only if we have any
            if processed_pcodes:
                processed_count = self._mark_deltas_as_processed(processed_pcodes)
            else:
                processed_count = 0
                self.logger.info("No pcodes to mark as processed")
            
            return unique_deltas, processed_count
        except Exception as e:
            self.logger.error(f"Error fetching patient deltas from Firebird: {str(e)}")
            return [], 0

    def _mark_deltas_as_processed(self, pcodes: List[str]) -> int:
        """
        Mark delta records as processed in the Firebird database.
        
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
            
        try:
            # Execute update in batches to avoid query size limits
            batch_size = 500
            total_processed = 0
            
            # Debug log the input
            self.logger.debug(f"Starting to process {len(pcodes)} pcodes: {pcodes[:5]}...")
            
            for i in range(0, len(pcodes), batch_size):
                batch = pcodes[i:i + batch_size]
                if not batch:
                    continue
                    
                # For Firebird, construct the IN clause directly with values rather than placeholders
                # Convert each value to a string representation suitable for SQL
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
                            # Replace single quotes with two single quotes (SQL escaping)
                            safe_value = str(pcode).replace("'", "''")
                            value_list.append(f"'{safe_value}'")
                    except Exception as e:
                        self.logger.warning(f"Failed to process pcode value: {pcode}, error: {e}")
                        continue
                
                # Defensive check - make sure value_list is not empty
                if not value_list:
                    self.logger.warning(f"No valid values in batch, skipping")
                    continue
                    
                # Join the values with commas
                values_string = ", ".join(value_list)
                    
                # Construct the query with the values directly in the IN clause
                query = f"""
                    UPDATE Medscan_delta_clients
                    SET processed = 'Y'
                    WHERE pcode IN ({values_string});
                """
                
                # Debug log the query (truncated for large queries)
                query_log = query
                if len(query_log) > 1000:
                    query_log = query_log[:500] + "..." + query_log[-500:]
                self.logger.debug(f"Executing query: {query_log}")
                
                # Execute the query without parameters since we've already included them directly
                # For UPDATE queries, execute_query returns (None, None)
                # We don't need to check the return values here
                self.connector.execute_query(query)
                total_processed += len(value_list)
            
            self.logger.info(f"Marked {total_processed} delta records as processed")
            return total_processed
            
        except Exception as e:
            self.logger.error(f"Error marking deltas as processed: {str(e)}")
            return 0
    
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
                
                self.logger.info(f"Total patient count in Firebird: {total_count}, last ID: {last_id}")
                
                if include_last_id:
                    return total_count, last_id
                return total_count
            else:
                self.logger.error("Failed to get patient count from Firebird")
                return 0 if not include_last_id else (0, None)
        except Exception as e:
            self.logger.error(f"Error getting patient count from Firebird: {e}")
            return 0 if not include_last_id else (0, None)