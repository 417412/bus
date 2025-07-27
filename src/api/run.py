#!/usr/bin/env python3
"""
Script to run the FastAPI application.
"""

import os
import sys
import uvicorn
import logging.config

# Add the parent directory to the path
parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent_dir)

from src.api.config import API_CONFIG, LOGGING_CONFIG

def main():
    """Run the FastAPI application."""
    
    # Set up logging
    os.makedirs("logs", exist_ok=True)
    logging.config.dictConfig(LOGGING_CONFIG)
    
    # Run the application
    uvicorn.run(
        "src.api.main:app",
        host=API_CONFIG["host"],
        port=API_CONFIG["port"],
        reload=API_CONFIG["debug"],
        log_level="info"
    )

if __name__ == "__main__":
    main()