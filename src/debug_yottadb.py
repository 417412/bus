#!/usr/bin/env python3
import requests
import time
import sys
import os

# Add the parent directory to the path so Python can find the modules
parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent_dir)

from src.config.settings import DATABASE_CONFIG, setup_logger

# Set up logging
logger = setup_logger("debug_yottadb", "debug_yottadb")

def test_yottadb_api():
    api_url = DATABASE_CONFIG["YottaDB"]["api_url"]
    timeout_config = DATABASE_CONFIG["YottaDB"]["timeout"]
    connect_timeout_config = DATABASE_CONFIG["YottaDB"]["connect_timeout"]
    
    logger.info(f"Testing YottaDB API: {api_url}")
    logger.info(f"Timeouts: connect={connect_timeout_config}s, read={timeout_config}s")
    logger.info("Starting API call...")
    
    start_time = time.time()
    
    try:
        response = requests.get(
            api_url,
            timeout=(connect_timeout_config, timeout_config),
            headers={'User-Agent': 'Medical-ETL-Debug/1.0'}
        )
        
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info(f"API call completed in {duration:.1f} seconds")
        logger.info(f"Status code: {response.status_code}")
        logger.info(f"Content length: {len(response.text)} characters")
        
        if response.status_code == 200:
            lines = response.text.strip().split('\n')
            logger.info(f"Number of lines: {len(lines)}")
            
            # Show first few lines
            for i, line in enumerate(lines[:3]):
                if line.strip():
                    logger.info(f"Line {i+1}: {line}")
            
            return True
        else:
            logger.error(f"ERROR: API returned status {response.status_code}")
            return False
            
    except requests.exceptions.ConnectTimeout as e:
        logger.error(f"Connection timeout: {e}")
        return False
    except requests.exceptions.ReadTimeout as e:
        logger.error(f"Read timeout (API took longer than {timeout_config} seconds): {e}")
        return False
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False

if __name__ == "__main__":
    test_yottadb_api()