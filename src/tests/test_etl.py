#!/usr/bin/env python3
"""
ETL integration tests for the medical system.

This module tests the complete ETL process and database connectivity
using the new architecture with encrypted password support and Patient model.
"""

import os
import sys
import pytest
import logging
import random
from datetime import datetime
from unittest.mock import Mock, patch
from pathlib import Path

# Add the parent directory to the path so Python can find the modules
parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent_dir)

# Import configuration
from src.config.settings import DOCUMENT_TYPES, get_decrypted_database_config, setup_logger

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

# Import Patient model
from src.models.patient import Patient

class TestETLIntegration:
    """Integration tests for the ETL system."""
    
    def setup_method(self):
        """Setup for each test method."""
        self.logger = setup_logger("test_etl", "test_etl")
        
        # Use decrypted database config
        self.db_config = get_decrypted_database_config()
        
        # Test data for document type testing
        self.test_document_types = [1, 3, 5, 10, 14, 17]  # Sample of document types
    
    def test_postgres_connection(self):
        """Test PostgreSQL connection."""
        self.logger.info("Testing PostgreSQL connection...")
        
        # Use default decrypted config
        pg_connector = PostgresConnector()
        
        assert pg_connector.connect(), "PostgreSQL connection should succeed"
        
        # Test a simple query
        rows, columns = pg_connector.execute_query("SELECT version()")
        assert rows is not None, "Should return version information"
        assert len(rows) > 0, "Should have at least one row"
        
        version = rows[0][0]
        self.logger.info(f"PostgreSQL version: {version}")
        
        pg_connector.disconnect()

    def test_firebird_connection(self):
        """Test Firebird connection."""
        self.logger.info("Testing Firebird connection...")
        
        # Use default decrypted config
        fb_connector = FirebirdConnector()
        
        if not fb_connector.connect():
            pytest.skip("Firebird connection not available")
        
        # Test a simple query
        rows, columns = fb_connector.execute_query("SELECT CURRENT_TIMESTAMP FROM RDB$DATABASE")
        assert rows is not None, "Should return timestamp"
        assert len(rows) > 0, "Should have at least one row"
        
        timestamp = rows[0][0]
        self.logger.info(f"Firebird current time: {timestamp}")
        
        # Test connection through the repository
        fb_repo = FirebirdRepository(fb_connector)
        patients = fb_repo.get_patients(batch_size=3)
        self.logger.info(f"Retrieved {len(patients)} patients from Firebird through repository")
        
        # Display sample data
        for i, patient in enumerate(patients[:2]):
            self.logger.info(f"Patient {i+1}: {patient.get('lastname', 'Unknown')} {patient.get('name', 'Unknown')}")
        
        fb_connector.disconnect()

    def test_yottadb_connection(self):
        """Test YottaDB connection using ping/socket test."""
        self.logger.info("Testing YottaDB connection...")
        
        # Use default decrypted config
        yottadb_connector = YottaDBConnector()
        
        # Test basic connectivity (quick ping/socket test)
        if yottadb_connector.connect():
            self.logger.info("YottaDB connectivity test successful")
            yottadb_connector.disconnect()
        else:
            pytest.skip("YottaDB connectivity not available")

    def test_postgres_schema(self):
        """Test if the PostgreSQL schema is set up correctly."""
        self.logger.info("Testing PostgreSQL schema...")
        
        pg_connector = PostgresConnector()
        assert pg_connector.connect(), "PostgreSQL connection should succeed"
        
        try:
            # Check if essential tables exist
            essential_tables = ["hislist", "businessunits", "documenttypes", "patients", "patientsdet", "protocols"]
            missing_tables = []
            
            for table in essential_tables:
                rows, columns = pg_connector.execute_query("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = %s
                    )
                """, (table,))
                
                if not rows[0][0]:  # Table doesn't exist
                    missing_tables.append(table)
            
            if missing_tables:
                self.logger.error(f"Missing tables: {', '.join(missing_tables)}")
                pytest.fail(f"Database schema is not set up correctly. Missing tables: {', '.join(missing_tables)}")
            
            self.logger.info("All essential tables exist")
            
            # Check if we have reference data in the tables
            tables_to_check = {
                "hislist": "HIS systems",
                "businessunits": "business units", 
                "documenttypes": "document types"
            }
            
            for table, description in tables_to_check.items():
                rows, columns = pg_connector.execute_query(f"SELECT COUNT(*) FROM {table}")
                count = rows[0][0]
                self.logger.info(f"Found {count} {description} in {table} table")
                
                if count > 0:
                    rows, columns = pg_connector.execute_query(f"SELECT * FROM {table} LIMIT 5")
                    for row in rows:
                        self.logger.info(f"  {table} entry: {row}")
        
        finally:
            pg_connector.disconnect()

    def test_patient_model(self):
        """Test the Patient model functionality."""
        self.logger.info("Testing Patient model...")
        
        # Test Firebird patient creation
        firebird_raw = {
            'hisnumber': '12345',
            'source': 2,
            'businessunit': 2,
            'lastname': 'Тестов',
            'name': 'Тест',
            'surname': 'Тестович',
            'birthdate': '1990-01-01',
            'documenttypes': 1,
            'document_number': 1234567890,
            'email': 'test@example.com',
            'telephone': '79991234567',
            'his_password': 'password123',
            'login_email': 'login@example.com'
        }
        
        patient = Patient.from_firebird_raw(firebird_raw)
        assert patient.hisnumber == '12345'
        assert patient.source == 2
        assert patient.get_source_name() == 'Инфоклиника'
        assert patient.has_document()
        assert patient.has_contact_info()
        assert patient.has_login_credentials()
        
        # Test YottaDB patient creation
        yottadb_raw = {
            'hisnumber': '67890/A22',
            'source': 1,
            'businessunit': 1,
            'lastname': 'Петров',
            'name': 'Петр',
            'surname': 'Петрович',
            'birthdate': '1985-05-15',
            'documenttypes': 1,
            'document_number': 9876543210,
            'email': 'contact@example.com',
            'telephone': '79997654321',
            'his_password': None,
            'login_email': 'api_login@example.com'
        }
        
        patient = Patient.from_yottadb_raw(yottadb_raw)
        assert patient.hisnumber == '67890/A22'
        assert patient.source == 1
        assert patient.get_source_name() == 'qMS'
        assert patient.has_document()
        assert patient.has_contact_info()
        assert patient.login_email == 'api_login@example.com'
        
        # Test conversion to dict
        patient_dict = patient.to_patientsdet_dict()
        assert 'uuid' not in patient_dict  # Should be removed for database insert
        assert patient_dict['login_email'] == 'api_login@example.com'
        
        self.logger.info("Patient model tests passed")

    def test_document_type_handling(self):
        """Test document type handling in the database through the repository."""
        self.logger.info("Testing document type handling...")
        
        pg_connector = PostgresConnector()
        assert pg_connector.connect(), "PostgreSQL connection should succeed"
        
        try:
            # Create repository
            pg_repo = PostgresRepository(pg_connector)
            
            # Generate test patients with different document types for BOTH sources
            test_patients = []
            
            # Test with qMS patients (source=1)
            for doc_type in self.test_document_types:
                # Generate appropriate document number based on type
                doc_number = self._generate_document_number(doc_type)
                
                # Create a qMS patient using Patient model
                qms_raw = {
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
                    "his_password": None,  # qMS doesn't have passwords via API
                    "login_email": f"qms_login{doc_type}@api.com"
                }
                
                patient = Patient.from_yottadb_raw(qms_raw)
                test_patients.append(patient.to_patientsdet_dict())
                
                # Create an Infoclinica patient with different document number
                doc_number2 = self._generate_document_number(doc_type)
                
                infoclinica_raw = {
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
                    "his_password": "testpass123",  # Infoclinica has passwords
                    "login_email": f"ic_login{doc_type}@system.com"
                }
                
                patient = Patient.from_firebird_raw(infoclinica_raw)
                test_patients.append(patient.to_patientsdet_dict())
            
            # Insert the test patients
            inserted_count = 0
            for patient_dict in test_patients:
                if pg_repo.insert_patient(patient_dict):
                    inserted_count += 1
                    doc_type_name = DOCUMENT_TYPES.get(patient_dict["documenttypes"], "Unknown")
                    source_name = "qMS" if patient_dict["source"] == 1 else "Инфоклиника"
                    self.logger.info(f"Inserted {source_name} patient with {doc_type_name} (ID: {patient_dict['documenttypes']}), "
                                   f"number: {patient_dict['document_number']}, login: {patient_dict.get('login_email', 'N/A')}")
            
            self.logger.info(f"Successfully inserted {inserted_count} out of {len(test_patients)} test patients with different document types")
            
            # Verify at least some patients were inserted
            assert inserted_count > 0, "Should insert at least some test patients"
            
        finally:
            pg_connector.disconnect()

    def test_firebird_etl_process(self):
        """Test the Firebird ETL process using the Patient model."""
        self.logger.info("Testing Firebird ETL process with Patient model...")
        
        # Create connectors with default decrypted config
        fb_connector = FirebirdConnector()
        pg_connector = PostgresConnector()
        
        if not fb_connector.connect():
            pytest.skip("Firebird connection not available")
            
        assert pg_connector.connect(), "PostgreSQL connection should succeed"
        
        try:
            # Create repositories
            fb_repo = FirebirdRepository(fb_connector)
            pg_repo = PostgresRepository(pg_connector)
            
            # Create ETL service
            etl_service = ETLService(fb_repo, pg_repo)
            
            # Process a small batch
            batch_size = 5  # Small batch for testing
            self.logger.info(f"Processing {batch_size} patients through Firebird ETL with Patient model")
            
            # Get a batch of patients
            patients = fb_repo.get_patients(batch_size=batch_size)
            
            if not patients:
                pytest.skip("No patients available in Firebird for testing")
            
            # Process each patient using the NEW Patient model approach
            success_count = 0
            for raw_patient in patients:
                try:
                    # CHANGE: Use ETL service's process_patient_record method instead of direct transformer
                    patient = etl_service.process_patient_record(raw_patient)
                    
                    if not patient:
                        self.logger.warning(f"Failed to process patient: {raw_patient.get('hisnumber')}")
                        continue
                    
                    # Convert to dict for database operations
                    patient_dict = patient.to_patientsdet_dict()
                    
                    # Log document type handling
                    if raw_patient.get('documenttypes') and patient.documenttypes:
                        if raw_patient.get('documenttypes') != patient.documenttypes:
                            self.logger.info(f"Patient {patient.hisnumber}: mapped document type "
                                           f"{raw_patient.get('documenttypes')} -> {patient.documenttypes}")
                    
                    # Check if the patient already exists
                    if pg_repo.patient_exists(patient.hisnumber, patient.source):
                        self.logger.debug(f"Patient {patient.hisnumber} already exists, skipping")
                        success_count += 1  # Count as success since it's already there
                        continue
                        
                    # Insert the patient
                    if pg_repo.insert_patient(patient_dict):
                        success_count += 1
                        self.logger.debug(f"Successfully processed patient {patient.hisnumber}")
                        
                        # Verify login_email was properly stored
                        if patient.login_email:
                            self.logger.debug(f"  Login email: {patient.login_email}")
                    
                except Exception as e:
                    self.logger.error(f"Error processing patient: {e}")
                    # Log the raw patient data for debugging
                    self.logger.error(f"Raw patient data: {raw_patient}")
            
            self.logger.info(f"Firebird ETL processed {success_count}/{len(patients)} patients successfully")
            
            # Verify some processing occurred
            assert success_count > 0, "Should process at least some patients successfully"
            
        finally:
            # Disconnect
            fb_connector.disconnect()
            pg_connector.disconnect()

    @pytest.mark.slow
    def test_yottadb_etl_process(self):
        """Test YottaDB ETL process with full API fetch and sample processing using Patient model."""
        self.logger.info("Testing YottaDB ETL process with Patient model...")
        self.logger.info("WARNING: This will fetch all data from YottaDB API (takes 2-3 minutes)")
        
        # Create connectors with default decrypted config
        yottadb_connector = YottaDBConnector()
        pg_connector = PostgresConnector()
        
        if not yottadb_connector.connect():
            pytest.skip("YottaDB connection not available")
            
        assert pg_connector.connect(), "PostgreSQL connection should succeed"
        
        try:
            # Create repositories
            yottadb_repo = YottaDBRepository(yottadb_connector)
            pg_repo = PostgresRepository(pg_connector)
            
            # Create ETL service
            etl_service = ETLService(yottadb_repo, pg_repo)
            
            self.logger.info("Fetching all patients from YottaDB API...")
            
            # Get all patients from YottaDB (this triggers the full API call)
            all_patients = yottadb_repo.get_patients()
            
            if not all_patients:
                pytest.skip("No patients retrieved from YottaDB")
            
            self.logger.info(f"Retrieved {len(all_patients)} patients from YottaDB")
            
            # Select 10 random patients for testing ETL
            test_batch_size = min(10, len(all_patients))
            test_patients = random.sample(all_patients, test_batch_size)
            
            self.logger.info(f"Testing ETL process with {test_batch_size} random patients using Patient model...")
            
            # Process the test batch through ETL
            success_count = 0
            error_count = 0
            
            for i, raw_patient in enumerate(test_patients, 1):
                try:
                    # CHANGE: Use ETL service's process_patient_record method instead of direct transformer
                    patient = etl_service.process_patient_record(raw_patient)
                    
                    if not patient:
                        self.logger.warning(f"Failed to process patient: {raw_patient.get('hisnumber')}")
                        error_count += 1
                        continue
                    
                    self.logger.info(f"Processing patient {i}/{test_batch_size}: "
                                   f"{patient.lastname} {patient.name} (ID: {patient.hisnumber})")
                    
                    # Log document type mapping
                    if raw_patient.get('documenttypes') and patient.documenttypes:
                        self.logger.info(f"  Document type: {raw_patient.get('documenttypes')} -> {patient.documenttypes}")
                    
                    self.logger.info(f"  Contact email: {patient.email}, Login email: {patient.login_email}")
                    
                    # Convert to dict for database operations
                    patient_dict = patient.to_patientsdet_dict()
                    
                    # Check if patient already exists
                    if pg_repo.patient_exists(patient.hisnumber, patient.source):
                        self.logger.info(f"  Patient {patient.hisnumber} already exists, using upsert")
                        if pg_repo.upsert_patient(patient_dict):
                            success_count += 1
                            self.logger.info(f"  ✓ Patient {patient.hisnumber} updated successfully")
                        else:
                            error_count += 1
                            self.logger.error(f"  ✗ Failed to update patient {patient.hisnumber}")
                    else:
                        if pg_repo.insert_patient(patient_dict):
                            success_count += 1
                            self.logger.info(f"  ✓ Patient {patient.hisnumber} inserted successfully")
                        else:
                            error_count += 1
                            self.logger.error(f"  ✗ Failed to insert patient {patient.hisnumber}")
                            
                except Exception as e:
                    error_count += 1
                    self.logger.error(f"  ✗ Error processing patient {i}: {str(e)}")
                    # Log the raw patient data for debugging
                    self.logger.error(f"Raw patient data: {raw_patient}")
            
            self.logger.info(f"YottaDB ETL test completed: {success_count} successful, {error_count} errors")
            
            # Show some statistics
            self.logger.info(f"Total patients in YottaDB: {len(all_patients)}")
            self.logger.info(f"Test batch processed: {test_batch_size}")
            self.logger.info(f"Success rate: {success_count/test_batch_size*100:.1f}%")
            
            # Verify some processing occurred
            assert success_count > 0, "Should process at least some patients successfully"
            
        finally:
            # Disconnect
            yottadb_connector.disconnect()
            pg_connector.disconnect()

    def test_password_encryption_integration(self):
        """Test that encrypted passwords work in real database connections."""
        self.logger.info("Testing password encryption integration...")
        
        # Test that we can create connectors with default (decrypted) config
        pg_connector = PostgresConnector()  # Should use decrypted passwords
        fb_connector = FirebirdConnector()  # Should use decrypted passwords
        yottadb_connector = YottaDBConnector()  # Should use decrypted config
        
        # Test PostgreSQL connection with decrypted password
        assert pg_connector.connect(), "PostgreSQL should connect with decrypted password"
        pg_connector.disconnect()
        
        # Test Firebird connection with decrypted password (if available)
        if fb_connector.connect():
            self.logger.info("Firebird connection successful with decrypted password")
            fb_connector.disconnect()
        else:
            self.logger.info("Firebird connection not available (expected in some environments)")
        
        # Test YottaDB connectivity
        if yottadb_connector.connect():
            self.logger.info("YottaDB connection successful with decrypted config")
            yottadb_connector.disconnect()
        else:
            self.logger.info("YottaDB connection not available (expected in some environments)")

    def _generate_document_number(self, doc_type: int) -> int:
        """Generate appropriate document number based on document type."""
        if doc_type == 1:  # Russian passport - 10 digits
            return random.randint(1000000000, 9999999999)
        elif doc_type in (3, 10):  # Foreign passports - 9 digits
            return random.randint(100000000, 999999999)
        elif doc_type == 5:  # Birth certificate - 12 digits
            return random.randint(100000000000, 999999999999)
        else:  # Other documents - 8 digits
            return random.randint(10000000, 99999999)


class TestETLComponents:
    """Unit tests for individual ETL components."""
    
    def setup_method(self):
        """Setup for each test method."""
        self.logger = setup_logger("test_etl_components", "test_etl")
    
    def test_etl_service_initialization_with_firebird(self):
        """Test ETL service initialization with Firebird repository."""
        # Create mock connectors
        mock_fb_connector = Mock()
        mock_pg_connector = Mock()
        
        # Create real repositories (but with mock connectors)
        fb_repo = FirebirdRepository(mock_fb_connector)
        pg_repo = PostgresRepository(mock_pg_connector)

        # Create ETL service
        etl_service = ETLService(fb_repo, pg_repo)
        
        assert etl_service.source_repo == fb_repo
        assert etl_service.target_repo == pg_repo
        assert etl_service.transformer is not None
    
        # Check that it created the correct transformer type
        from src.etl.transformers.firebird_transformer import FirebirdTransformer
        assert isinstance(etl_service.transformer, FirebirdTransformer)
        
        # Check that process_patient_record method exists
        assert hasattr(etl_service, 'process_patient_record')

    def test_etl_service_initialization_with_yottadb(self):
        """Test ETL service initialization with YottaDB repository."""
        # Create mock connectors
        mock_ydb_connector = Mock()
        mock_pg_connector = Mock()

        # Create real repositories (but with mock connectors)
        ydb_repo = YottaDBRepository(mock_ydb_connector)
        pg_repo = PostgresRepository(mock_pg_connector)

        # Create ETL service
        etl_service = ETLService(ydb_repo, pg_repo)

        assert etl_service.source_repo == ydb_repo
        assert etl_service.target_repo == pg_repo
        assert etl_service.transformer is not None

        # Check that it created the correct transformer type
        from src.etl.transformers.yottadb_transformer import YottaDBTransformer
        assert isinstance(etl_service.transformer, YottaDBTransformer)
        
        # Check that process_patient_record method exists
        assert hasattr(etl_service, 'process_patient_record')

    def test_etl_service_initialization_with_unsupported_repository(self):
        """Test ETL service initialization with unsupported repository type."""
        # Create mock repositories that aren't recognized types
        mock_source_repo = Mock()
        mock_target_repo = Mock()

        # This should raise a ValueError
        with pytest.raises(ValueError, match="Unsupported source repository type"):
            ETLService(mock_source_repo, mock_target_repo)

    def test_patient_model_integration_with_etl_service(self):
        """Test Patient model integration with ETL service."""
        # Create mock connectors
        mock_fb_connector = Mock()
        mock_pg_connector = Mock()
        
        # Create real repositories
        fb_repo = FirebirdRepository(mock_fb_connector)
        pg_repo = PostgresRepository(mock_pg_connector)
        
        # Create ETL service
        etl_service = ETLService(fb_repo, pg_repo)
        
        # Test processing a Firebird patient
        firebird_raw = {
            'hisnumber': '12345',
            'source': 2,
            'businessunit': 2,
            'lastname': 'Тестов',
            'name': 'Тест',
            'surname': 'Тестович',
            'birthdate': '1990-01-01',
            'documenttypes': 1,
            'document_number': 1234567890,
            'email': 'test@example.com',
            'telephone': '79991234567',
            'his_password': 'password123',
            'login_email': 'login@example.com'
        }
        
        patient = etl_service.process_patient_record(firebird_raw)
        assert patient is not None
        assert isinstance(patient, Patient)
        assert patient.hisnumber == '12345'
        assert patient.source == 2
        assert patient.login_email == 'login@example.com'
        
        # Test YottaDB patient
        yottadb_raw = {
            'hisnumber': '67890/A22',
            'source': 1,
            'businessunit': 1,
            'lastname': 'Петров',
            'name': 'Петр',
            'surname': 'Петрович',
            'email': 'contact@example.com',
            'login_email': 'api_login@example.com'
        }
        
        # Switch to YottaDB repository
        ydb_repo = YottaDBRepository(mock_fb_connector)  # Reuse mock
        etl_service = ETLService(ydb_repo, pg_repo)
        
        patient = etl_service.process_patient_record(yottadb_raw)
        assert patient is not None
        assert isinstance(patient, Patient)
        assert patient.hisnumber == '67890/A22'
        assert patient.source == 1
        assert patient.login_email == 'api_login@example.com'

    def test_document_type_mapping(self):
        """Test document type mapping from configuration."""
        # Test that we have the expected document types
        assert 1 in DOCUMENT_TYPES  # Паспорт
        assert 17 in DOCUMENT_TYPES  # Иные документы
        
        # Test specific mappings
        assert DOCUMENT_TYPES[1] == 'Паспорт'
        assert DOCUMENT_TYPES[17] == 'Иные документы'
        
        self.logger.info(f"Found {len(DOCUMENT_TYPES)} document types in configuration")

    @patch('src.etl.etl_service.FirebirdTransformer')
    def test_etl_service_with_mocked_transformer(self, mock_transformer_class):
        """Test ETL service initialization with mocked transformer."""
        # Create mock transformer instance
        mock_transformer = Mock()
        mock_transformer_class.return_value = mock_transformer

        # Create mock connectors
        mock_fb_connector = Mock()
        mock_pg_connector = Mock()

        # Create real repositories
        fb_repo = FirebirdRepository(mock_fb_connector)
        pg_repo = PostgresRepository(mock_pg_connector)

        # Create ETL service
        etl_service = ETLService(fb_repo, pg_repo)

        # Verify the transformer was created correctly
        mock_transformer_class.assert_called_once()
        assert etl_service.transformer == mock_transformer


# Standalone test runner for manual execution
def run_integration_tests():
    """Run integration tests manually."""
    logger = setup_logger("test_etl_runner", "test_etl")
    
    logger.info("Starting ETL integration tests with Patient model...")
    
    test_instance = TestETLIntegration()
    test_instance.setup_method()
    
    tests = [
        ("PostgreSQL Connection", test_instance.test_postgres_connection),
        ("PostgreSQL Schema", test_instance.test_postgres_schema),
        ("Patient Model", test_instance.test_patient_model),
        ("Firebird Connection", test_instance.test_firebird_connection),
        ("YottaDB Connection", test_instance.test_yottadb_connection),
        ("Document Type Handling", test_instance.test_document_type_handling),
        ("Password Encryption Integration", test_instance.test_password_encryption_integration),
        ("Firebird ETL Process", test_instance.test_firebird_etl_process),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        logger.info(f"\n--- Running {test_name} ---")
        try:
            test_func()
            results[test_name] = "✓ PASSED"
            logger.info(f"{test_name}: PASSED")
        except Exception as e:
            results[test_name] = f"✗ FAILED: {str(e)}"
            logger.error(f"{test_name}: FAILED - {str(e)}")
    
    # Print summary
    logger.info("\n" + "="*50)
    logger.info("ETL INTEGRATION TEST RESULTS")
    logger.info("="*50)
    
    for test_name, result in results.items():
        logger.info(f"{test_name}: {result}")
    
    # Ask about YottaDB test
    try:
        response = input("\nRun YottaDB ETL test? (takes 2-3 minutes) [y/N]: ")
        if response.lower() in ['y', 'yes']:
            logger.info("\n--- Running YottaDB ETL Process ---")
            try:
                test_instance.test_yottadb_etl_process()
                results["YottaDB ETL Process"] = "✓ PASSED"
                logger.info("YottaDB ETL Process: PASSED")
            except Exception as e:
                results["YottaDB ETL Process"] = f"✗ FAILED: {str(e)}"
                logger.error(f"YottaDB ETL Process: FAILED - {str(e)}")
    except KeyboardInterrupt:
        logger.info("\nYottaDB test skipped by user")
    
    passed = sum(1 for result in results.values() if result.startswith("✓"))
    total = len(results)
    
    logger.info(f"\nFinal Results: {passed}/{total} tests passed")
    
    return passed == total


if __name__ == "__main__":
    # Run tests manually if executed directly
    success = run_integration_tests()
    sys.exit(0 if success else 1)