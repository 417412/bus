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

# Import configuration
from src.config.settings import DATABASE_CONFIG, LOGGING_CONFIG

# Set up logging
logging.basicConfig(
    level=getattr(logging, LOGGING_CONFIG.get("level", "INFO")),
    format=LOGGING_CONFIG.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("clear_database")


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Clear the database for testing purposes"
    )
    parser.add_argument(
        "--keep-reference-data", 
        action="store_true",
        help="Keep reference data in tables like hislist, businessunits, and documenttypes"
    )
    parser.add_argument(
        "--database", 
        type=str, 
        default=DATABASE_CONFIG["PostgreSQL"]["database"],
        help=f"Database name (default: {DATABASE_CONFIG['PostgreSQL']['database']})"
    )
    parser.add_argument(
        "--user", 
        type=str, 
        default=DATABASE_CONFIG["PostgreSQL"]["user"],
        help=f"Database user (default: {DATABASE_CONFIG['PostgreSQL']['user']})"
    )
    parser.add_argument(
        "--password", 
        type=str, 
        default=DATABASE_CONFIG["PostgreSQL"]["password"],
        help="Database password"
    )
    parser.add_argument(
        "--host", 
        type=str, 
        default=DATABASE_CONFIG["PostgreSQL"]["host"],
        help=f"Database host (default: {DATABASE_CONFIG['PostgreSQL']['host']})"
    )
    parser.add_argument(
        "--port", 
        type=int, 
        default=5432,
        help="Database port (default: 5432)"
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
    conn = psycopg2.connect(
        dbname=args.database,
        user=args.user,
        password=args.password,
        host=args.host,
        port=args.port
    )
    return conn


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


def main():
    """Main function."""
    args = parse_args()
    
    logger.info("Starting database cleanup...")
    
    try:
        # Connect to the database
        conn = get_connection(args)
        logger.info("Connected to the database")
        
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
            logger.info("Database cleanup completed successfully.")
        
    except Exception as e:
        logger.error(f"Error during database cleanup: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())