#!/usr/bin/env python3
"""
Database cleanup utility for the medical system.

This script truncates all tables in the correct order to avoid foreign key constraint violations.
It's useful for cleaning up the database before running tests.
"""

import os
import sys
import logging
import argparse
import psycopg2
from typing import List, Dict, Any, Optional

# Add the parent directory to the path so Python can find the modules
parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent_dir)

# Import configuration with encrypted password support
from src.config.settings import LOGGING_CONFIG, setup_logger, get_decrypted_database_config

# Set up logging
logger = setup_logger("clear_database", "general")


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    # Get decrypted database config for defaults
    db_config = get_decrypted_database_config()["PostgreSQL"]
    
    parser = argparse.ArgumentParser(
        description="Clear the database for testing purposes"
    )
    parser.add_argument(
        "--keep-reference-data", 
        action="store_true",
        help="Keep reference data in tables like hislist, businessunits, and documenttypes"
    )
    parser.add_argument(
        "--reinitialize",
        action="store_true",
        help="Drop all tables and recreate the entire database schema with triggers"
    )
    parser.add_argument(
        "--database", 
        type=str, 
        default=db_config["database"],
        help=f"Database name (default: {db_config['database']})"
    )
    parser.add_argument(
        "--user", 
        type=str, 
        default=db_config["user"],
        help=f"Database user (default: {db_config['user']})"
    )
    parser.add_argument(
        "--password", 
        type=str, 
        default=None,  # Don't show the password in help
        help="Database password (default: from configuration)"
    )
    parser.add_argument(
        "--host", 
        type=str, 
        default=db_config["host"],
        help=f"Database host (default: {db_config['host']})"
    )
    parser.add_argument(
        "--port", 
        type=int, 
        default=db_config.get("port", 5432),
        help=f"Database port (default: {db_config.get('port', 5432)})"
    )
    parser.add_argument(
        "--dry-run", 
        action="store_true",
        help="Show what would be done without actually doing it"
    )
    parser.add_argument(
        "--reset-sequences", 
        action="store_true",
        help="Reset sequences to 1 after truncating tables"
    )
    return parser.parse_args()


def get_connection(args: argparse.Namespace) -> psycopg2.extensions.connection:
    """Create a database connection."""
    # If password not provided via command line, use decrypted password from config
    password = args.password
    if password is None:
        db_config = get_decrypted_database_config()["PostgreSQL"]
        password = db_config["password"]
    
    # Log connection attempt (without password)
    logger.info(f"Connecting to database: {args.user}@{args.host}:{args.port}/{args.database}")
    
    try:
        conn = psycopg2.connect(
            dbname=args.database,
            user=args.user,
            password=password,
            host=args.host,
            port=args.port
        )
        logger.info("Successfully connected to PostgreSQL database")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
        # Don't log the actual password in error messages
        if 'password' in str(e).lower():
            logger.error("Connection failed - check username, password, and database settings")
        raise


def get_tables_with_dependencies(conn: psycopg2.extensions.connection) -> Dict[str, List[str]]:
    """
    Get all tables and their dependencies.
    
    Returns a dictionary where keys are table names and values are lists of
    tables that depend on the key table.
    """
    cursor = conn.cursor()
    
    # Get all foreign key constraints
    cursor.execute("""
        SELECT
            tc.table_name,
            ccu.table_name AS referenced_table
        FROM
            information_schema.table_constraints AS tc
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
    """)
    
    dependencies = {}
    for table, referenced_table in cursor.fetchall():
        if referenced_table not in dependencies:
            dependencies[referenced_table] = []
        dependencies[referenced_table].append(table)
    
    # Get all tables
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE'
    """)
    
    all_tables = [row[0] for row in cursor.fetchall()]
    
    # Add tables without dependencies
    for table in all_tables:
        if table not in dependencies:
            dependencies[table] = []
    
    cursor.close()
    return dependencies


def get_truncation_order(dependencies: Dict[str, List[str]]) -> List[str]:
    """
    Determine the order in which tables should be truncated.
    
    This is essentially a topological sort of the dependency graph in reverse:
    tables with no dependents get truncated first, followed by tables whose
    dependents have already been truncated.
    """
    # Create a copy of dependencies to work with
    remaining_deps = {table: list(deps) for table, deps in dependencies.items()}
    
    # Tables to be processed
    truncation_order = []
    
    # Process tables until all have been added to the truncation order
    while remaining_deps:
        # Find tables without remaining dependents
        tables_to_truncate = [
            table for table, deps in remaining_deps.items() if not deps
        ]
        
        if not tables_to_truncate:
            # If there are no tables without dependents but dependencies remain,
            # we have a circular dependency
            logger.warning("Circular dependency detected, remaining tables: %s", remaining_deps)
            # Add all remaining tables to the truncation order
            truncation_order.extend(remaining_deps.keys())
            break
        
        # Add tables to truncation order
        truncation_order.extend(tables_to_truncate)
        
        # Remove these tables from dependencies
        for table in tables_to_truncate:
            remaining_deps.pop(table)
        
        # Remove these tables from the dependents lists
        for deps in remaining_deps.values():
            for table in tables_to_truncate:
                if table in deps:
                    deps.remove(table)
    
    # Reverse the order so that tables with no dependencies are truncated first
    return truncation_order[::-1]


def drop_all_tables(conn: psycopg2.extensions.connection, args: argparse.Namespace) -> None:
    """Drop all tables and our custom functions in the database."""
    cursor = conn.cursor()
    
    if args.dry_run:
        logger.info("Would drop all tables and custom functions")
        cursor.close()
        return
    
    logger.info("Dropping all tables and custom functions...")
    
    # First, drop our custom triggers
    custom_triggers = [
        ('trg_process_new_patient', 'patientsdet'),
        ('trg_update_patient', 'patientsdet')
    ]
    
    for trigger_name, table_name in custom_triggers:
        try:
            logger.info(f"Dropping trigger: {trigger_name}")
            cursor.execute(f"DROP TRIGGER IF EXISTS {trigger_name} ON {table_name}")
        except Exception as e:
            logger.warning(f"Could not drop trigger {trigger_name}: {str(e)}")
    
    # Then drop our custom functions
    custom_functions = [
        'process_new_patient',
        'update_patient_from_patientsdet'
    ]
    
    for func in custom_functions:
        try:
            logger.info(f"Dropping function: {func}")
            cursor.execute(f"DROP FUNCTION IF EXISTS {func}() CASCADE")
        except Exception as e:
            logger.warning(f"Could not drop function {func}: {str(e)}")
    
    # Get all user tables (excluding system tables)
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE'
        AND table_name NOT LIKE 'pg_%'
        AND table_name NOT LIKE 'sql_%'
    """)
    
    tables = [row[0] for row in cursor.fetchall()]
    
    # Drop all tables with CASCADE to handle dependencies
    logger.info(f"Dropping {len(tables)} tables...")
    for table in tables:
        try:
            logger.info(f"Dropping table: {table}")
            cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        except Exception as e:
            logger.error(f"Error dropping table {table}: {str(e)}")
            conn.rollback()
            continue
    
    conn.commit()
    cursor.close()


def execute_sql_file(conn: psycopg2.extensions.connection, file_path: str, args: argparse.Namespace) -> bool:
    """Execute SQL commands from a file."""
    if not os.path.exists(file_path):
        logger.error(f"SQL file not found: {file_path}")
        return False
    
    if args.dry_run:
        logger.info(f"Would execute SQL file: {file_path}")
        return True
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        cursor = conn.cursor()
        
        # For SQL files with complex functions, execute the entire content at once
        # This handles dollar-quoted strings and complex function definitions properly
        try:
            logger.info(f"Executing SQL file: {file_path}")
            cursor.execute(sql_content)
            conn.commit()
            cursor.close()
            logger.info(f"Successfully executed SQL file: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error executing SQL file {file_path}: {str(e)}")
            conn.rollback()
            cursor.close()
            return False
        
    except Exception as e:
        logger.error(f"Error reading SQL file {file_path}: {str(e)}")
        return False


def reinitialize_database(conn: psycopg2.extensions.connection, args: argparse.Namespace) -> bool:
    """Reinitialize the entire database schema."""
    logger.info("Reinitializing database schema...")
    
    # Get the directory containing this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    # Paths to SQL files
    schema_file = os.path.join(project_root, 'schema', 'schema.sql')
    trigger_file = os.path.join(project_root, 'triggers', 'matching_trigger.sql')
    indexes_file = os.path.join(project_root, 'triggers', 'create_indexes.sql')
    
    # Execute schema file
    logger.info("Creating database schema...")
    if not execute_sql_file(conn, schema_file, args):
        logger.error("Failed to create database schema")
        return False
    
    # Execute trigger file
    logger.info("Creating triggers...")
    if not execute_sql_file(conn, trigger_file, args):
        logger.error("Failed to create triggers")
        return False
    
    # Execute indexes file
    logger.info("Creating database indexes...")
    if not execute_sql_file(conn, indexes_file, args):
        logger.error("Failed to create indexes")
        return False
    
    logger.info("Database schema and triggers created successfully")
    return True


def truncate_tables(conn: psycopg2.extensions.connection, 
                    truncation_order: List[str], 
                    args: argparse.Namespace) -> None:
    """Truncate tables in the specified order."""
    reference_tables = ['hislist', 'businessunits', 'documenttypes']
    
    cursor = conn.cursor()
    
    for table in truncation_order:
        # Skip reference tables if requested
        if args.keep_reference_data and table in reference_tables:
            logger.info(f"Skipping reference table: {table}")
            continue
        
        # Log the operation
        if args.dry_run:
            logger.info(f"Would truncate table: {table}")
        else:
            logger.info(f"Truncating table: {table}")
            try:
                cursor.execute(f"TRUNCATE TABLE {table} CASCADE")
                if args.reset_sequences and table not in reference_tables:
                    # Get sequences for this table
                    cursor.execute(f"""
                        SELECT column_name, column_default
                        FROM information_schema.columns
                        WHERE table_name = %s
                        AND column_default LIKE 'nextval%%'
                    """, (table,))
                    
                    for column, default in cursor.fetchall():
                        # Extract sequence name from default value
                        # Format is typically: nextval('sequence_name'::regclass)
                        seq_name = default.split("'")[1]
                        logger.info(f"Resetting sequence: {seq_name}")
                        cursor.execute(f"ALTER SEQUENCE {seq_name} RESTART WITH 1")
            except Exception as e:
                logger.error(f"Error truncating {table}: {str(e)}")
                conn.rollback()
                continue
    
    if not args.dry_run:
        conn.commit()
    
    cursor.close()


def reinitialize_reference_data(conn: psycopg2.extensions.connection, args: argparse.Namespace) -> None:
    """Reinitialize the reference data tables if they were truncated."""
    if args.keep_reference_data or args.dry_run:
        return
    
    logger.info("Reinitializing reference data...")
    
    # Define reference data
    reference_data = {
        "hislist": [
            (1, 'qMS'),
            (2, 'Инфоклиника')
        ],
        "businessunits": [
            (1, 'ОО ФК "Хадасса Медикал ЛТД"'),
            (2, 'ООО "Медскан"'),
            (3, 'ООО "Клинический госпиталь на Яузе"')
        ],
        "documenttypes": [
            (1, 'Паспорт'),
            (2, 'Паспорт СССР'),
            (3, 'Заграничный паспорт РФ'),
            (4, 'Заграничный паспорт СССР'),
            (5, 'Свидетельство о рождении'),
            (6, 'Удостоверение личности офицера'),
            (7, 'Справка об освобождении из места лишения свободы'),
            (8, 'Военный билет'),
            (9, 'Дипломатический паспорт РФ'),
            (10, 'Иностранный паспорт'),
            (11, 'Свидетельство беженца'),
            (12, 'Вид на жительство'),
            (13, 'Удостоверение беженца'),
            (14, 'Временное удостоверение'),
            (15, 'Паспорт моряка'),
            (16, 'Военный билет офицера запаса'),
            (17, 'Иные документы')
        ]
    }
    
    cursor = conn.cursor()
    
    for table, data in reference_data.items():
        logger.info(f"Reinitializing {table}...")
        
        # Reset the sequence for this table
        cursor.execute(f"""
            SELECT pg_get_serial_sequence(%s, 'id')
        """, (table,))
        sequence = cursor.fetchone()[0]
        if sequence:
            cursor.execute(f"ALTER SEQUENCE {sequence} RESTART WITH 1")
        
        # Insert data
        for row in data:
            try:
                placeholders = ', '.join(['%s'] * len(row))
                cursor.execute(f"INSERT INTO {table} VALUES ({placeholders})", row)
            except Exception as e:
                logger.error(f"Error inserting into {table}: {str(e)}")
                conn.rollback()
                break
    
    conn.commit()
    cursor.close()


def test_connection_with_config():
    """Test connection using default configuration."""
    logger.info("Testing database connection with current configuration...")
    
    try:
        # Use PostgresConnector to test the connection
        from src.connectors.postgres_connector import PostgresConnector
        
        connector = PostgresConnector()  # Uses decrypted config automatically
        
        if connector.test_connection():
            logger.info("✓ Database connection test successful")
            return True
        else:
            logger.error("✗ Database connection test failed")
            return False
            
    except Exception as e:
        logger.error(f"✗ Database connection test failed: {e}")
        return False


def main():
    """Main function."""
    args = parse_args()
    
    # Validate arguments
    if args.reinitialize and args.keep_reference_data:
        logger.error("--reinitialize and --keep-reference-data cannot be used together")
        return 1
    
    # Test connection first
    if not test_connection_with_config():
        logger.error("Cannot proceed - database connection failed")
        logger.info("Check your database configuration with: python configurator.py --show-config")
        return 1
    
    if args.reinitialize:
        logger.info("Starting full database reinitialization...")
    else:
        logger.info("Starting database cleanup...")
    
    try:
        # Connect to the database
        conn = get_connection(args)
        
        if args.reinitialize:
            # Full reinitialization: drop everything and recreate
            drop_all_tables(conn, args)
            
            if not args.dry_run:
                if not reinitialize_database(conn, args):
                    logger.error("Failed to reinitialize database")
                    return 1
            
        else:
            # Regular cleanup: truncate tables
            # Get tables and their dependencies
            dependencies = get_tables_with_dependencies(conn)
            logger.info(f"Found {len(dependencies)} tables")
            
            # Determine truncation order
            truncation_order = get_truncation_order(dependencies)
            logger.info(f"Truncation order: {', '.join(truncation_order)}")
            
            # Truncate tables
            truncate_tables(conn, truncation_order, args)
            
            # Reinitialize reference data if needed
            reinitialize_reference_data(conn, args)
        
        # Close connection
        conn.close()
        
        if args.dry_run:
            logger.info("Dry run completed. No changes were made.")
        else:
            if args.reinitialize:
                logger.info("✓ Database reinitialization completed successfully.")
            else:
                logger.info("✓ Database cleanup completed successfully.")
        
    except Exception as e:
        logger.error(f"Error during database operation: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())