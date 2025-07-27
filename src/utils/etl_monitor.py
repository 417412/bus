#!/usr/bin/env python3
"""
ETL Monitoring utility.
Provides real-time monitoring of ETL processes and system health.
"""

import os
import sys
import time
import json
from datetime import datetime, timedelta
from typing import Dict, Any

# Add the parent directory to the path
parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent_dir)

from src.config.settings import setup_logger
from src.connectors.postgres_connector import PostgresConnector
from src.connectors.firebird_connector import FirebirdConnector
from src.connectors.yottadb_connector import YottaDBConnector
from src.repositories.postgres_repository import PostgresRepository
from src.repositories.firebird_repository import FirebirdRepository
from src.repositories.yottadb_repository import YottaDBRepository

def get_system_status():
    """Get comprehensive system status."""
    logger = setup_logger("etl_monitor", "monitor")
    status = {
        "timestamp": datetime.now().isoformat(),
        "connections": {},
        "repositories": {},
        "processing_status": {}
    }
    
    # Test connections
    logger.info("Testing connections...")
    
    # PostgreSQL
    try:
        pg_connector = PostgresConnector()
        if pg_connector.connect():
            status["connections"]["postgresql"] = {"status": "connected", "error": None}
            pg_connector.disconnect()
        else:
            status["connections"]["postgresql"] = {"status": "failed", "error": "Connection failed"}
    except Exception as e:
        status["connections"]["postgresql"] = {"status": "error", "error": str(e)}
    
    # Firebird
    try:
        fb_connector = FirebirdConnector()
        if fb_connector.connect():
            status["connections"]["firebird"] = {"status": "connected", "error": None}
            fb_connector.disconnect()
        else:
            status["connections"]["firebird"] = {"status": "failed", "error": "Connection failed"}
    except Exception as e:
        status["connections"]["firebird"] = {"status": "error", "error": str(e)}
    
    # YottaDB
    try:
        yottadb_connector = YottaDBConnector()
        if yottadb_connector.connect():
            status["connections"]["yottadb"] = {"status": "connected", "error": None}
            yottadb_connector.disconnect()
        else:
            status["connections"]["yottadb"] = {"status": "failed", "error": "Connection failed"}
    except Exception as e:
        status["connections"]["yottadb"] = {"status": "error", "error": str(e)}
    
    # Get repository status if connections are working
    if status["connections"]["postgresql"]["status"] == "connected":
        try:
            pg_connector = PostgresConnector()
            pg_connector.connect()
            pg_repo = PostgresRepository(pg_connector)
            
            # PostgreSQL stats
            status["repositories"]["postgresql"] = {
                "total_patients": pg_repo.get_total_patient_count(),
                "qms_patients": pg_repo.get_total_patient_count(source=1),
                "infoclinica_patients": pg_repo.get_total_patient_count(source=2)
            }
            
            pg_connector.disconnect()
        except Exception as e:
            status["repositories"]["postgresql"] = {"error": str(e)}
    
    if status["connections"]["firebird"]["status"] == "connected":
        try:
            fb_connector = FirebirdConnector()
            fb_connector.connect()
            fb_repo = FirebirdRepository(fb_connector)
            
            status["repositories"]["firebird"] = {
                "total_patients": fb_repo.get_total_patient_count()
            }
            
            fb_connector.disconnect()
        except Exception as e:
            status["repositories"]["firebird"] = {"error": str(e)}
    
    if status["connections"]["yottadb"]["status"] == "connected":
        try:
            yottadb_connector = YottaDBConnector()
            yottadb_connector.connect()
            yottadb_repo = YottaDBRepository(yottadb_connector)
            
            total_patients = yottadb_repo.get_total_patient_count()
            processed_count = len(yottadb_repo.get_processed_hisnumbers())
            
            status["repositories"]["yottadb"] = {
                "total_patients": total_patients,
                "processed_patients": processed_count,
                "unprocessed_patients": total_patients - processed_count,
                "completion_percent": (processed_count / total_patients * 100) if total_patients > 0 else 0
            }
            
            yottadb_connector.disconnect()
        except Exception as e:
            status["repositories"]["yottadb"] = {"error": str(e)}
    
    # Check for recent ETL activity
    if status["connections"]["postgresql"]["status"] == "connected":
        try:
            pg_connector = PostgresConnector()
            pg_connector.connect()
            
            # Check recent matching activity
            rows, columns = pg_connector.execute_query("""
                SELECT source, COUNT(*) as count
                FROM patient_matching_log 
                WHERE match_time > %s
                GROUP BY source
            """, (datetime.now() - timedelta(hours=1),))
            
            recent_activity = {}
            for source, count in rows:
                source_name = "qMS" if source == 1 else "Infoclinica"
                recent_activity[source_name] = count
            
            status["processing_status"]["recent_activity_1h"] = recent_activity
            
            pg_connector.disconnect()
        except Exception as e:
            status["processing_status"]["error"] = str(e)
    
    return status

def print_status_summary(status):
    """Print a human-readable status summary."""
    print("\n" + "="*60)
    print("ETL SYSTEM STATUS SUMMARY")
    print("="*60)
    print(f"Timestamp: {status['timestamp']}")
    
    print("\n🔌 CONNECTIONS:")
    for system, conn_status in status["connections"].items():
        status_icon = "✅" if conn_status["status"] == "connected" else "❌"
        print(f"  {status_icon} {system.upper()}: {conn_status['status']}")
        if conn_status.get("error"):
            print(f"     Error: {conn_status['error']}")
    
    print("\n📊 REPOSITORIES:")
    for system, repo_status in status["repositories"].items():
        if "error" in repo_status:
            print(f"  ❌ {system.upper()}: Error - {repo_status['error']}")
        else:
            print(f"  ✅ {system.upper()}:")
            if system == "yottadb":
                print(f"     Total: {repo_status.get('total_patients', 0)}")
                print(f"     Processed: {repo_status.get('processed_patients', 0)}")
                print(f"     Unprocessed: {repo_status.get('unprocessed_patients', 0)}")
                print(f"     Completion: {repo_status.get('completion_percent', 0):.1f}%")
            elif system == "postgresql":
                print(f"     Total patients: {repo_status.get('total_patients', 0)}")
                print(f"     qMS patients: {repo_status.get('qms_patients', 0)}")
                print(f"     Infoclinica patients: {repo_status.get('infoclinica_patients', 0)}")
            else:
                print(f"     Total patients: {repo_status.get('total_patients', 0)}")
    
    print("\n⚡ RECENT ACTIVITY (last hour):")
    recent_activity = status.get("processing_status", {}).get("recent_activity_1h", {})
    if recent_activity:
        for source, count in recent_activity.items():
            print(f"  {source}: {count} records processed")
    else:
        print("  No recent activity detected")

def monitor_loop(interval=60):
    """Run continuous monitoring loop."""
    logger = setup_logger("etl_monitor", "monitor")
    
    try:
        while True:
            status = get_system_status()
            
            # Clear screen (works on most terminals)
            os.system('clear' if os.name == 'posix' else 'cls')
            
            print_status_summary(status)
            
            print(f"\n🕐 Refreshing every {interval} seconds... (Ctrl+C to stop)")
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped by user.")

def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="ETL System Monitor")
    parser.add_argument("--json", action="store_true", help="Output status as JSON")
    parser.add_argument("--monitor", action="store_true", help="Run continuous monitoring")
    parser.add_argument("--interval", type=int, default=60, help="Monitoring interval in seconds")
    
    args = parser.parse_args()
    
    if args.monitor:
        monitor_loop(args.interval)
    else:
        status = get_system_status()
        
        if args.json:
            print(json.dumps(status, indent=2))
        else:
            print_status_summary(status)

if __name__ == "__main__":
    main()