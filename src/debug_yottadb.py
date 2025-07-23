#!/usr/bin/env python3
import requests
import time
import sys
import os

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.config.settings import DATABASE_CONFIG

def test_yottadb_api():
    api_url = DATABASE_CONFIG["YottaDB"]["api_url"]
    timeout_config = DATABASE_CONFIG["YottaDB"]["timeout"]
    connect_timeout_config = DATABASE_CONFIG["YottaDB"]["connect_timeout"]
    
    print(f"Testing YottaDB API: {api_url}")
    print(f"Timeouts: connect={connect_timeout_config}s, read={timeout_config}s")
    print("Starting API call...")
    
    start_time = time.time()
    
    try:
        response = requests.get(
            api_url,
            timeout=(connect_timeout_config, timeout_config),
            headers={'User-Agent': 'Medical-ETL-Debug/1.0'}
        )
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"API call completed in {duration:.1f} seconds")
        print(f"Status code: {response.status_code}")
        print(f"Content length: {len(response.text)} characters")
        
        if response.status_code == 200:
            lines = response.text.strip().split('\n')
            print(f"Number of lines: {len(lines)}")
            
            # Show first few lines
            for i, line in enumerate(lines[:3]):
                if line.strip():
                    print(f"Line {i+1}: {line}")
            
            return True
        else:
            print(f"ERROR: API returned status {response.status_code}")
            return False
            
    except requests.exceptions.ConnectTimeout as e:
        print(f"Connection timeout: {e}")
        return False
    except requests.exceptions.ReadTimeout as e:
        print(f"Read timeout (API took longer than {timeout_config} seconds): {e}")
        return False
    except requests.exceptions.ConnectionError as e:
        print(f"Connection error: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False

if __name__ == "__main__":
    test_yottadb_api()