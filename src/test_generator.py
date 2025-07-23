#!/usr/bin/env python3
"""
Test data generator for the patient matching system.

This script generates test records with varying degrees of overlap
to test the patient matching trigger mechanism in the database.
It can test both inserts and updates.
"""

import argparse
import random
import string
import psycopg2
from datetime import datetime, timedelta
import time
import gc
from typing import Dict, List, Tuple, Optional, Set


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Generate test patient data for database testing')
    
    # Main mode selection
    parser.add_argument('--mode', type=str, choices=['insert', 'update'], default='insert',
                        help='Test mode: insert new records or update existing ones')
    
    # Database connection parameters
    parser.add_argument('-d', '--db', type=str, default='medical_system',
                        help='Database name (default: medical_system)')
    parser.add_argument('-u', '--user', type=str, default='medapp_user',
                        help='Database user (default: medapp_user)')
    parser.add_argument('-p', '--password', type=str, required=True,
                        help='Database password')
    parser.add_argument('-H', '--host', type=str, default='localhost',
                        help='Database host (default: localhost)')
    parser.add_argument('--port', type=int, default=5432,
                        help='Database port (default: 5432)')
    
    # Insert mode parameters
    parser.add_argument('-q', '--quantity', type=int, default=100000,
                        help='Number of records to process (default: 100000)')
    parser.add_argument('--duplicate-rate', type=float, default=0.15,
                        help='Rate of document duplications in insert mode (0-1, default: 0.15)')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Batch size for operations (default: 1000)')
    
    # Update mode parameters
    parser.add_argument('--update-null-document-rate', type=float, default=0.7,
                        help='Rate of NULL documents to update with values (0-1, default: 0.7)')
    parser.add_argument('--update-fields-rate', type=float, default=0.3,
                        help='Rate of records to update with new field values (0-1, default: 0.3)')
    parser.add_argument('--match-existing-document-rate', type=float, default=0.5,
                        help='Rate of NULL->value updates that should match existing documents (0-1, default: 0.5)')
    
    # Performance and memory options
    parser.add_argument('--memory-efficient', action='store_true',
                        help='Use memory-efficient mode (slower but uses less memory)')
    parser.add_argument('--clean-memory-every', type=int, default=50000,
                        help='Clean memory every N records (default: 50000)')
    parser.add_argument('--analyze-every', type=int, default=100000,
                        help='Run ANALYZE every N records (default: 100000)')
    
    return parser.parse_args()


class TestDataGenerator:
    """Generator for test patient data."""
    
    def __init__(self, conn_params: Dict[str, str], duplicate_rate: float = 0.15, 
                 memory_efficient: bool = False, max_memory_items: int = 10000):
        """
        Initialize the data generator.
        
        Args:
            conn_params: Database connection parameters
            duplicate_rate: Rate of document duplications (0-1)
            memory_efficient: If True, use less memory but potentially slower generation
            max_memory_items: Maximum number of items to keep in memory for duplicates
        """
        self.conn_params = conn_params
        self.duplicate_rate = duplicate_rate
        self.memory_efficient = memory_efficient
        self.max_memory_items = max_memory_items
        
        self.russian_first_names = ["Александр", "Мария", "Дмитрий", "Анна", "Иван", "Елена", 
                                   "Сергей", "Ольга", "Андрей", "Наталья", "Алексей", "Татьяна"]
        self.russian_last_names = ["Иванов", "Смирнов", "Кузнецов", "Попов", "Васильев", 
                                  "Петров", "Соколов", "Михайлов", "Новиков", "Федоров"]
        self.russian_patronymics = ["Александрович", "Дмитриевич", "Иванович", "Сергеевич", 
                                   "Андреевич", "Алексеевич", "Николаевич", "Владимирович"]
        
        # Female versions of patronymics and last names
        self.russian_patronymics_f = [p.replace("вич", "вна") for p in self.russian_patronymics]
        self.russian_last_names_f = [l + "а" if not l.endswith("ий") else 
                                   l.replace("ий", "ая") for l in self.russian_last_names]
        
        # Combined list for easier random selection
        self.all_last_names = self.russian_last_names + self.russian_last_names_f
        
        # Document type weights (1 is most common - passport)
        self.document_type_weights = {
            1: 0.7,    # Passport (most common)
            3: 0.1,    # Foreign passport
            5: 0.05,   # Birth certificate
            10: 0.05,  # Foreign passport
            14: 0.05,  # Temporary ID
            17: 0.05   # Other documents
        }
        
        # Used document pairs to track duplicates - use a fixed-size list for memory efficiency
        self.used_documents: List[Tuple[int, int]] = []  # List of (doc_type, doc_number) pairs
        
        # Existing document pairs from the patients table
        self.existing_documents: List[Tuple[int, int]] = []
        
        # Instead of tracking all used HIS numbers, use high starting counters
        # This assumes we're starting with a fresh database or know the highest values
        self.qms_counter = random.randint(1000000, 9000000)
        self.infoclinica_counter = random.randint(100000, 900000)
        
        self.total_generated = 0
        
    def connect_db(self) -> psycopg2.extensions.connection:
        """Create database connection."""
        return psycopg2.connect(**self.conn_params)
    
    def load_existing_data(self):
        """Load existing document numbers and other data from the database."""
        conn = self.connect_db()
        cursor = conn.cursor()
        
        # Load existing document numbers
        cursor.execute("SELECT documenttypes, document_number FROM patients WHERE document_number IS NOT NULL")
        self.existing_documents = [(row[0], row[1]) for row in cursor.fetchall() if row[0] is not None]
        
        # Get the highest HIS numbers
        cursor.execute("SELECT MAX(hisnumber) FROM patientsdet WHERE source = 1")
        max_qms = cursor.fetchone()[0]
        if max_qms and max_qms.startswith('QMS'):
            try:
                self.qms_counter = int(max_qms[3:]) + 1
            except ValueError:
                pass
                
        cursor.execute("SELECT MAX(hisnumber) FROM patientsdet WHERE source = 2")
        max_infoclinica = cursor.fetchone()[0]
        if max_infoclinica and max_infoclinica.startswith('IC'):
            try:
                self.infoclinica_counter = int(max_infoclinica[2:]) + 1
            except ValueError:
                pass
        
        cursor.close()
        conn.close()
        
        print(f"Loaded {len(self.existing_documents)} existing document numbers")
        print(f"Starting HIS counters: qMS={self.qms_counter}, Infoclinica={self.infoclinica_counter}")
        
    def _trim_memory_lists(self):
        """Trim memory lists to prevent excessive memory usage."""
        if self.memory_efficient:
            # Keep only a subset of document pairs for duplicates
            if len(self.used_documents) > self.max_memory_items:
                # Keep some random ones for duplication
                keep_indices = random.sample(range(len(self.used_documents)), 
                                           int(self.max_memory_items * 0.8))
                self.used_documents = [self.used_documents[i] for i in keep_indices]
    
    def generate_document_type(self) -> int:
        """Generate a random document type based on realistic distribution."""
        # Choose document type based on weights
        r = random.random()
        cumulative = 0
        for doc_type, weight in self.document_type_weights.items():
            cumulative += weight
            if r <= cumulative:
                return doc_type
        return 1  # Default to passport if something goes wrong
    
    def generate_document_number(self, doc_type: int) -> int:
        """Generate a document number appropriate for the document type."""
        if doc_type == 1:  # Russian passport - 10 digits
            return random.randint(1000000000, 9999999999)
        elif doc_type in (3, 10):  # Foreign passports - 9 digits
            return random.randint(100000000, 999999999)
        elif doc_type == 5:  # Birth certificate - 12 digits
            return random.randint(100000000000, 999999999999)
        else:  # Other documents - 8 digits
            return random.randint(10000000, 99999999)
    
    def generate_document(self, force_duplicate: bool = False) -> Tuple[Optional[int], Optional[int]]:
        """
        Generate a document type and number, with a chance to reuse an existing one.
        
        Args:
            force_duplicate: If True, will return a duplicate if possible
            
        Returns:
            Tuple of (document_type, document_number) or (None, None) for missing document
        """
        # Sometimes generate None to simulate missing documents
        if random.random() < 0.1 and not force_duplicate:
            return (None, None)
            
        # Decide whether to use an existing document (duplicate)
        if (force_duplicate or random.random() < self.duplicate_rate) and self.used_documents:
            return random.choice(self.used_documents)
        
        # Generate a new document type and number
        doc_type = self.generate_document_type()
        doc_number = self.generate_document_number(doc_type)
        
        # Save for potential future duplicates
        self.used_documents.append((doc_type, doc_number))
        
        return (doc_type, doc_number)
    
    def get_existing_document(self) -> Tuple[Optional[int], Optional[int]]:
        """Return a randomly selected existing document from the database."""
        if not self.existing_documents:
            return (None, None)
        return random.choice(self.existing_documents)
    
    def generate_phone(self) -> str:
        """Generate a random Russian phone number."""
        return f"+7{random.randint(900, 999)}{random.randint(1000000, 9999999)}"
    
    def generate_email(self, first_name: str, last_name: str) -> str:
        """Generate an email based on name."""
        domains = ["gmail.com", "yandex.ru", "mail.ru", "rambler.ru", "outlook.com"]
        transliteration = {
            'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'e',
            'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
            'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
            'ф': 'f', 'х': 'kh', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
            'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya'
        }
        
        first = ''.join(transliteration.get(c.lower(), c.lower()) for c in first_name)
        last = ''.join(transliteration.get(c.lower(), c.lower()) for c in last_name)
        
        random_num = random.randint(1, 999)
        email_type = random.choice([
            f"{first[0]}.{last}",
            f"{first}.{last[0]}",
            f"{first}_{last}",
            f"{last}{first[0]}",
            f"{first}{random_num}"
        ])
        
        return f"{email_type}@{random.choice(domains)}"
    
    def generate_his_number(self, his_id: int) -> str:
        """
        Generate a unique HIS-specific patient number using a counter approach.
        
        Args:
            his_id: ID of the HIS system (1=qMS, 2=Инфоклиника)
        
        Returns:
            A unique HIS number
        """
        if his_id == 1:  # qMS
            self.qms_counter += 1
            return f"QMS{self.qms_counter}"
        else:  # Инфоклиника
            self.infoclinica_counter += 1
            return f"IC{self.infoclinica_counter}"
    
    def generate_name(self) -> Tuple[str, str, str, bool]:
        """
        Generate a random Russian name.
        
        Returns:
            Tuple of (last_name, first_name, patronymic, is_female)
        """
        is_female = random.choice([True, False])
        
        if is_female:
            first_name = random.choice(self.russian_first_names[1::2])  # Female names
            patronymic = random.choice(self.russian_patronymics_f)
            last_name = random.choice(self.russian_last_names_f)
        else:
            first_name = random.choice(self.russian_first_names[0::2])  # Male names
            patronymic = random.choice(self.russian_patronymics)
            last_name = random.choice(self.russian_last_names)
            
        return last_name, first_name, patronymic, is_female
    
    def generate_birthdate(self) -> datetime.date:
        """Generate a random birthdate between 18 and 90 years ago."""
        days_ago = random.randint(18*365, 90*365)
        return (datetime.now() - timedelta(days=days_ago)).date()
    
    def generate_password(self) -> str:
        """Generate a random password."""
        chars = string.ascii_letters + string.digits
        return ''.join(random.choice(chars) for _ in range(random.randint(8, 14)))
    
    def generate_patient_record(self, 
                               patient_index: int = 0, 
                               force_duplicate: bool = False) -> Dict:
        """
        Generate a patient record for insertion.
        
        Args:
            patient_index: Used to control duplication
            force_duplicate: If True, will try to create a duplicate
            
        Returns:
            Dictionary with patient data
        """
        his_id = random.choice([1, 2])  # 1 = qMS, 2 = Инфоклиника
        business_unit_id = random.randint(1, 3)
        
        # For some records, force a duplicate to test matching
        doc_type, doc_number = self.generate_document(force_duplicate)
        
        # Generate name and demographic data
        last_name, first_name, patronymic, _ = self.generate_name()
        birthdate = self.generate_birthdate()
        
        # Generate other details
        his_number = self.generate_his_number(his_id)
        phone = self.generate_phone()
        email = self.generate_email(first_name, last_name)
        password = self.generate_password()
        
        return {
            'hisnumber': his_number,
            'source': his_id,
            'businessunit': business_unit_id,
            'lastname': last_name,
            'name': first_name,
            'surname': patronymic,
            'birthdate': birthdate,
            'documenttypes': doc_type,
            'document_number': doc_number,
            'email': email,
            'telephone': phone,
            'his_password': password
        }
    
    def generate_batch(self, batch_size: int) -> List[Dict]:
        """Generate a batch of patient records."""
        batch = []
        
        # Ensure some duplicates within each batch
        force_duplicates = random.sample(range(batch_size), 
                                        int(batch_size * self.duplicate_rate * 0.5))
        
        for i in range(batch_size):
            force_dup = i in force_duplicates and self.used_documents
            record = self.generate_patient_record(self.total_generated + i, force_dup)
            batch.append(record)
            
        self.total_generated += batch_size
        
        # Periodically trim memory usage
        if self.memory_efficient and self.total_generated % (self.max_memory_items * 10) == 0:
            self._trim_memory_lists()
            # Force garbage collection
            gc.collect()
            
        return batch
    
    def insert_batch(self, conn: psycopg2.extensions.connection, batch: List[Dict]) -> int:
        """
        Insert a batch of records into the database.
        
        Returns:
            Number of successfully inserted records
        """
        cursor = conn.cursor()
        success_count = 0
        
        # Regular individual inserts with error handling
        for record in batch:
            try:
                cursor.execute("""
                INSERT INTO patientsdet (
                    hisnumber, source, businessunit, lastname, name, surname, birthdate,
                    documenttypes, document_number, email, telephone, his_password
                ) VALUES (
                    %(hisnumber)s, %(source)s, %(businessunit)s, %(lastname)s, 
                    %(name)s, %(surname)s, %(birthdate)s, %(documenttypes)s, %(document_number)s, 
                    %(email)s, %(telephone)s, %(his_password)s
                )
                """, record)
                success_count += 1
            except Exception as e:
                conn.rollback()  # Rollback the transaction
                print(f"Error inserting record: {e}")
                # Continue with next record
            
            # Commit every 100 records to avoid large transactions
            if success_count % 100 == 0:
                conn.commit()
            
        # Final commit for remaining records
        conn.commit()
        cursor.close()
        
        return success_count

    def run_analyze(self, conn: psycopg2.extensions.connection) -> None:
        """Run ANALYZE on tables to update statistics."""
        try:
            cursor = conn.cursor()
            cursor.execute("ANALYZE patientsdet")
            cursor.execute("ANALYZE patients")
            cursor.execute("ANALYZE patient_matching_log")
            conn.commit()
            cursor.close()
            print("Database statistics updated with ANALYZE")
        except Exception as e:
            print(f"Warning: Could not update statistics: {e}")
    
    def fetch_rows_for_update(self, conn: psycopg2.extensions.connection, 
                            limit: int, null_document_only: bool = False) -> List[Dict]:
        """
        Fetch rows from patientsdet for updating.
        
        Args:
            conn: Database connection
            limit: Maximum number of rows to fetch
            null_document_only: If True, only fetch rows with NULL document
            
        Returns:
            List of dictionaries with row data
        """
        cursor = conn.cursor()
        
        where_clause = "WHERE (documenttypes IS NULL OR document_number IS NULL)" if null_document_only else ""
        
        query = f"""
        SELECT id, hisnumber, source, businessunit, lastname, name, surname, 
               birthdate, documenttypes, document_number, email, telephone, his_password, uuid
        FROM patientsdet
        {where_clause}
        ORDER BY RANDOM()
        LIMIT {limit}
        """
        
        cursor.execute(query)
        
        rows = []
        for row in cursor.fetchall():
            rows.append({
                'id': row[0],
                'hisnumber': row[1],
                'source': row[2],
                'businessunit': row[3],
                'lastname': row[4],
                'name': row[5],
                'surname': row[6],
                'birthdate': row[7],
                'documenttypes': row[8],
                'document_number': row[9],
                'email': row[10],
                'telephone': row[11],
                'his_password': row[12],
                'uuid': row[13]
            })
        
        cursor.close()
        return rows
    
    def generate_updates_for_null_documents(self, rows: List[Dict], 
                                          match_existing_rate: float) -> List[Dict]:
        """
        Generate document updates for rows with NULL documents.
        
        Args:
            rows: List of rows from patientsdet with NULL documents
            match_existing_rate: Rate of updates that should match existing documents
            
        Returns:
            List of update dictionaries
        """
        updates = []
        
        for row in rows:
            update = row.copy()
            
            # Decide whether to use an existing document (to test merging)
            if random.random() < match_existing_rate and self.existing_documents:
                doc_type, doc_number = self.get_existing_document()
                update['documenttypes'] = doc_type
                update['document_number'] = doc_number
            else:
                # Generate a new document that doesn't exist yet
                doc_type = self.generate_document_type()
                doc_number = self.generate_document_number(doc_type)
                update['documenttypes'] = doc_type
                update['document_number'] = doc_number
                # Add to used list to avoid duplicates
                self.used_documents.append((doc_type, doc_number))
            
            updates.append(update)
        
        return updates
    
    def generate_field_updates(self, rows: List[Dict]) -> List[Dict]:
        """
        Generate updates for fields other than document.
        
        Args:
            rows: List of rows from patientsdet
            
        Returns:
            List of update dictionaries
        """
        updates = []
        
        for row in rows:
            update = row.copy()
            
            # Decide which fields to update
            fields_to_update = random.sample([
                'lastname', 'name', 'surname', 'birthdate', 
                'email', 'telephone', 'his_password'
            ], random.randint(1, 3))
            
            # Generate new values for selected fields
            if 'lastname' in fields_to_update or 'name' in fields_to_update or 'surname' in fields_to_update:
                last_name, first_name, patronymic, _ = self.generate_name()
                
                if 'lastname' in fields_to_update:
                    update['lastname'] = last_name
                if 'name' in fields_to_update:
                    update['name'] = first_name
                if 'surname' in fields_to_update:
                    update['surname'] = patronymic
            
            if 'birthdate' in fields_to_update:
                update['birthdate'] = self.generate_birthdate()
            
            if 'email' in fields_to_update:
                update['email'] = self.generate_email(update['name'], update['lastname'])
            
            if 'telephone' in fields_to_update:
                update['telephone'] = self.generate_phone()
            
            if 'his_password' in fields_to_update:
                update['his_password'] = self.generate_password()
            
            updates.append(update)
        
        return updates
    
    def apply_updates(self, conn: psycopg2.extensions.connection, updates: List[Dict]) -> int:
        """
        Apply updates to the patientsdet table.
        
        Args:
            conn: Database connection
            updates: List of update dictionaries
            
        Returns:
            Number of successful updates
        """
        cursor = conn.cursor()
        success_count = 0
        update_types = {'document_added': 0, 'fields_updated': 0}
        
        for update in updates:
            original_doc_type = update.get('original_doc_type')
            original_doc_number = update.get('original_doc_number')
            
            # Generate update SQL based on what's changed
            set_clauses = []
            params = {}
            
            for key, value in update.items():
                # Skip ID and administrative fields
                if key in ['id', 'uuid', 'original_doc_type', 'original_doc_number', 'update_type']:
                    continue
                
                set_clauses.append(f"{key} = %({key})s")
                params[key] = value
            
            # Add ID parameter
            params['id'] = update['id']
            
            # Execute update if we have set clauses
            if set_clauses:
                try:
                    sql = f"""
                    UPDATE patientsdet 
                    SET {', '.join(set_clauses)}
                    WHERE id = %(id)s
                    """
                    cursor.execute(sql, params)
                    
                    # Track update type
                    if (original_doc_type is None or original_doc_number is None) and \
                       update.get('documenttypes') is not None and update.get('document_number') is not None:
                        update_types['document_added'] += 1
                    else:
                        update_types['fields_updated'] += 1
                    
                    success_count += 1
                except Exception as e:
                    conn.rollback()
                    print(f"Error updating record: {e}")
                    continue
            
            # Commit every 100 records
            if success_count % 100 == 0:
                conn.commit()
        
        # Final commit
        conn.commit()
        
        print(f"Update summary: {update_types['document_added']} documents added, "
              f"{update_types['fields_updated']} records had fields updated")
        
        cursor.close()
        return success_count


def run_insert_test(args: argparse.Namespace):
    """Run the insert test mode."""
    conn_params = {
        'dbname': args.db,
        'user': args.user,
        'password': args.password,
        'host': args.host,
        'port': args.port
    }
    
    generator = TestDataGenerator(conn_params, args.duplicate_rate, args.memory_efficient)
    
    try:
        conn = generator.connect_db()
        print(f"Connected to database {args.db}")
        
        total_records = args.quantity
        batch_size = args.batch_size
        num_batches = (total_records + batch_size - 1) // batch_size  # Ceiling division
        
        start_time = time.time()
        mem_cleanup_counter = 0
        analyze_counter = 0
        total_inserted = 0
        
        for i in range(num_batches):
            current_batch_size = min(batch_size, total_records - i * batch_size)
            if current_batch_size <= 0:
                break
                
            batch_start = time.time()
            print(f"Generating batch {i+1}/{num_batches} ({current_batch_size} records)...")
            batch = generator.generate_batch(current_batch_size)
            
            print(f"Inserting batch {i+1}...")
            inserted = generator.insert_batch(conn, batch)
            total_inserted += inserted
            
            elapsed = time.time() - start_time
            batch_time = time.time() - batch_start
            records_per_sec = total_inserted / elapsed if elapsed > 0 else 0
            batch_per_sec = inserted / batch_time if batch_time > 0 else 0
            
            print(f"Progress: {total_inserted}/{total_records} records "
                  f"({total_inserted/total_records*100:.1f}%), "
                  f"Avg: {records_per_sec:.1f} records/sec, "
                  f"Current: {batch_per_sec:.1f} records/sec")
            
            # Periodic memory cleanup
            mem_cleanup_counter += current_batch_size
            if mem_cleanup_counter >= args.clean_memory_every:
                print("Cleaning memory...")
                # Clear references and force garbage collection
                batch = None
                gc.collect()
                mem_cleanup_counter = 0
            
            # Periodically run ANALYZE to update statistics
            analyze_counter += current_batch_size
            if analyze_counter >= args.analyze_every:
                print("Updating database statistics...")
                generator.run_analyze(conn)
                analyze_counter = 0
        
        end_time = time.time()
        total_time = end_time - start_time
        print(f"\nCompleted generating {total_inserted} records in {total_time:.2f} seconds")
        print(f"Average speed: {total_inserted/total_time:.1f} records/second")
        
        # Run final ANALYZE to ensure statistics are up to date
        generator.run_analyze(conn)
        
        # Print database statistics
        print_database_stats(conn)
        
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0


def run_update_test(args: argparse.Namespace):
    """Run the update test mode."""
    conn_params = {
        'dbname': args.db,
        'user': args.user,
        'password': args.password,
        'host': args.host,
        'port': args.port
    }
    
    generator = TestDataGenerator(conn_params, args.duplicate_rate, args.memory_efficient)
    
    try:
        conn = generator.connect_db()
        print(f"Connected to database {args.db}")
        
        # Load existing data for better updates
        generator.load_existing_data()
        
        total_records = args.quantity
        batch_size = args.batch_size
        
        # Track update results
        null_document_updates = 0
        field_updates = 0
        
        # Get stats before updates
        print("\nDatabase statistics before updates:")
        original_stats = get_database_stats(conn)
        print_stats_dict(original_stats)
        
        # 1. Update NULL documents
        print("\n--- Testing NULL Document Updates ---")
        null_document_count = min(
            int(total_records * args.update_null_document_rate),
            original_stats['null_document_count']
        )
        
        if null_document_count > 0:
            print(f"Will update {null_document_count} records with NULL documents")
            
            # Process in batches
            remaining = null_document_count
            while remaining > 0:
                current_batch = min(batch_size, remaining)
                
                # Fetch records with NULL documents
                print(f"Fetching {current_batch} records with NULL documents...")
                null_document_rows = generator.fetch_rows_for_update(conn, current_batch, True)
                
                if not null_document_rows:
                    print("No more NULL document records found")
                    break
                
                # Generate document updates
                print(f"Generating document updates...")
                updates = generator.generate_updates_for_null_documents(
                    null_document_rows, 
                    args.match_existing_document_rate
                )
                
                # Mark original state
                for update in updates:
                    update['original_doc_type'] = update.get('documenttypes')
                    update['original_doc_number'] = update.get('document_number')
                    update['update_type'] = 'document_added'
                
                # Apply updates
                print(f"Applying {len(updates)} document updates...")
                updated = generator.apply_updates(conn, updates)
                null_document_updates += updated
                
                print(f"Updated {updated} records with new document values")
                remaining -= updated
                
                # Print progress
                if null_document_count > 0:
                    print(f"NULL document updates progress: {null_document_updates}/{null_document_count} "
                          f"({null_document_updates/null_document_count*100:.1f}%)")
        else:
            print("No NULL document records to update")
        
        # 2. Update fields on random records
        print("\n--- Testing Field Updates ---")
        field_update_count = int(total_records * args.update_fields_rate)
        
        if field_update_count > 0:
            print(f"Will update {field_update_count} records with new field values")
            
            # Process in batches
            remaining = field_update_count
            while remaining > 0:
                current_batch = min(batch_size, remaining)
                
                # Fetch random records
                print(f"Fetching {current_batch} random records...")
                rows = generator.fetch_rows_for_update(conn, current_batch, False)
                
                if not rows:
                    print("No more records found")
                    break

                # Generate field updates
                print(f"Generating field value updates...")
                updates = generator.generate_field_updates(rows)

                # Mark original state
                for update, original in zip(updates, rows):
                    update['original_doc_type'] = original.get('documenttypes')
                    update['original_doc_number'] = original.get('document_number')
                    update['update_type'] = 'fields_updated'
                
                # Apply updates
                print(f"Applying {len(updates)} field updates...")
                updated = generator.apply_updates(conn, updates)
                field_updates += updated
                
                print(f"Updated {updated} records with new field values")
                remaining -= updated
                
                # Print progress
                if field_update_count > 0:
                    print(f"Field updates progress: {field_updates}/{field_update_count} "
                          f"({field_updates/field_update_count*100:.1f}%)")
        else:
            print("No field updates requested")
        
        # Run final ANALYZE to ensure statistics are up to date
        generator.run_analyze(conn)
        
        # Get stats after updates
        print("\nDatabase statistics after updates:")
        final_stats = get_database_stats(conn)
        print_stats_dict(final_stats)
        
        # Print update statistics
        print("\nUpdate Test Results:")
        print(f"Total records with NULL documents updated: {null_document_updates}")
        print(f"Total records with field values updated: {field_updates}")
        print(f"Total merged patients: {original_stats['patients_count'] - final_stats['patients_count']}")
        
        # Analyze log changes
        print("\nMatching Log Changes:")
        changes = analyze_log_changes(conn, original_stats['log_stats'], final_stats['log_stats'])
        for match_type, count in changes.items():
            print(f"  - {match_type}: +{count}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error in update test: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


def get_database_stats(conn: psycopg2.extensions.connection) -> Dict:
    """Get current database statistics."""
    cursor = conn.cursor()
    stats = {}
    
    # Count patients in consolidated table
    cursor.execute("SELECT COUNT(*) FROM patients")
    stats['patients_count'] = cursor.fetchone()[0]
    
    # Count unique records in patientsdet
    cursor.execute("SELECT COUNT(*) FROM patientsdet")
    stats['patientsdet_count'] = cursor.fetchone()[0]
    
    # Count null document records
    cursor.execute("SELECT COUNT(*) FROM patientsdet WHERE documenttypes IS NULL OR document_number IS NULL")
    stats['null_document_count'] = cursor.fetchone()[0]
    
    # Count document types distribution
    cursor.execute("""
        SELECT documenttypes, COUNT(*) 
        FROM patientsdet
        WHERE documenttypes IS NOT NULL
        GROUP BY documenttypes 
        ORDER BY COUNT(*) DESC
    """)
    stats['document_types'] = {doc_type: count for doc_type, count in cursor.fetchall()}
    
    # Count match types
    cursor.execute("""
        SELECT match_type, COUNT(*) 
        FROM patient_matching_log 
        GROUP BY match_type 
        ORDER BY COUNT(*) DESC
    """)
    stats['log_stats'] = {match_type: count for match_type, count in cursor.fetchall()}
    
    cursor.close()
    return stats


def analyze_log_changes(conn: psycopg2.extensions.connection, 
                       before: Dict[str, int], 
                       after: Dict[str, int]) -> Dict[str, int]:
    """Analyze changes in the matching log."""
    changes = {}
    
    # Compare counts for each match type
    all_types = set(before.keys()) | set(after.keys())
    
    for match_type in all_types:
        before_count = before.get(match_type, 0)
        after_count = after.get(match_type, 0)
        
        if after_count > before_count:
            changes[match_type] = after_count - before_count
    
    return changes


def print_stats_dict(stats: Dict):
    """Print database statistics in a readable format."""
    print(f"Total patients in consolidated table: {stats['patients_count']}")
    print(f"Total records in patientsdet: {stats['patientsdet_count']}")
    print(f"Records with NULL documents: {stats['null_document_count']} "
          f"({stats['null_document_count']/stats['patientsdet_count']*100:.1f}%)")
    
    if 'document_types' in stats and stats['document_types']:
        print("Document type distribution:")
        for doc_type, count in stats['document_types'].items():
            doc_name = get_document_type_name(doc_type)
            print(f"  - {doc_name} (ID: {doc_type}): {count} "
                  f"({count/(stats['patientsdet_count']-stats['null_document_count'])*100:.1f}%)")
    
    print("Matching log entries:")
    for match_type, count in stats['log_stats'].items():
        print(f"  - {match_type}: {count} ({count/stats['patientsdet_count']*100:.1f}%)")


def get_document_type_name(doc_type: int) -> str:
    """Get a human-readable name for a document type ID."""
    document_types = {
        1: 'Паспорт',
        2: 'Паспорт СССР',
        3: 'Заграничный паспорт РФ',
        4: 'Заграничный паспорт СССР',
        5: 'Свидетельство о рождении',
        6: 'Удостоверение личности офицера',
        7: 'Справка об освобождении из места лишения свободы',
        8: 'Военный билет',
        9: 'Дипломатический паспорт РФ',
        10: 'Иностранный паспорт',
        11: 'Свидетельство беженца',
        12: 'Вид на жительство',
        13: 'Удостоверение беженца',
        14: 'Временное удостоверение',
        15: 'Паспорт моряка',
        16: 'Военный билет офицера запаса',
        17: 'Иные документы'
    }
    return document_types.get(doc_type, f'Неизвестный тип ({doc_type})')


def print_database_stats(conn: psycopg2.extensions.connection):
    """Print current database statistics."""
    stats = get_database_stats(conn)
    print_stats_dict(stats)


def main():
    """Main function to run the test data generator."""
    args = parse_args()
    
    if args.mode == 'insert':
        print("Running in INSERT test mode")
        return run_insert_test(args)
    else:  # update mode
        print("Running in UPDATE test mode")
        return run_update_test(args)


if __name__ == "__main__":
    exit(main())