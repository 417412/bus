#!/usr/bin/env python3
"""
Utility script to check and configure logging settings.
"""

import os
import sys
import argparse
from pathlib import Path

# Add the parent directory to the path so Python can find the modules
parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent_dir)

from src.config.settings import get_config_info, LOGS_DIR, STATE_DIR

def show_logging_info():
    """Display current logging configuration."""
    info = get_config_info()
    
    print("=== ETL Logging Configuration ===")
    print(f"Project Base Directory: {info['directories']['base_dir']}")
    print(f"Logs Directory: {info['directories']['logs_dir']}")
    print(f"State Directory: {info['directories']['state_dir']}")
    print(f"Log Level: {info['logging_config']['level']}")
    print()
    
    print("Log Files:")
    for name, path in info['logging_config']['files'].items():
        exists = "✓" if Path(path).exists() else "✗"
        size = ""
        if Path(path).exists():
            size_bytes = Path(path).stat().st_size
            if size_bytes > 1024*1024:
                size = f" ({size_bytes/1024/1024:.1f} MB)"
            elif size_bytes > 1024:
                size = f" ({size_bytes/1024:.1f} KB)"
            else:
                size = f" ({size_bytes} bytes)"
        
        print(f"  {exists} {name}: {path}{size}")
    
    print()
    print("Environment Variables:")
    print(f"  ETL_LOGS_DIR: {os.getenv('ETL_LOGS_DIR', 'Not set')}")
    print(f"  ETL_STATE_DIR: {os.getenv('ETL_STATE_DIR', 'Not set')}")
    print(f"  ETL_LOG_LEVEL: {os.getenv('ETL_LOG_LEVEL', 'Not set')}")

def clean_logs(older_than_days: int = 7):
    """Clean old log files."""
    import time
    from datetime import datetime, timedelta
    
    cutoff_time = time.time() - (older_than_days * 24 * 60 * 60)
    cleaned_files = []
    
    for log_file in LOGS_DIR.glob("*.log"):
        if log_file.stat().st_mtime < cutoff_time:
            log_file.unlink()
            cleaned_files.append(str(log_file))
    
    print(f"Cleaned {len(cleaned_files)} log files older than {older_than_days} days")
    for file in cleaned_files:
        print(f"  Removed: {file}")

def main():
    parser = argparse.ArgumentParser(description="ETL Logging Utility")
    parser.add_argument("--info", action="store_true", help="Show logging configuration")
    parser.add_argument("--clean", type=int, metavar="DAYS", help="Clean logs older than N days")
    parser.add_argument("--set-env", action="store_true", help="Show commands to set environment variables")
    
    args = parser.parse_args()
    
    if args.info or not any(vars(args).values()):
        show_logging_info()
    
    if args.clean:
        clean_logs(args.clean)
    
    if args.set_env:
        print("\nTo override logging configuration, set these environment variables:")
        print(f"export ETL_LOGS_DIR='/path/to/your/logs'")
        print(f"export ETL_STATE_DIR='/path/to/your/state'")
        print(f"export ETL_LOG_LEVEL='DEBUG'  # or INFO, WARNING, ERROR")
        print()
        print("Current values would be:")
        print(f"export ETL_LOGS_DIR='{LOGS_DIR}'")
        print(f"export ETL_STATE_DIR='{STATE_DIR}'")

if __name__ == "__main__":
    main()