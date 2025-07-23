#!/usr/bin/env python3
import requests
import time
import sys
import os
import subprocess
import socket
from urllib.parse import urlparse

# Add the parent directory to the path so Python can find the modules
parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent_dir)

from src.config.settings import DATABASE_CONFIG, setup_logger

# Set up logging
logger = setup_logger("debug_yottadb", "debug_yottadb")

def ping_host(host: str) -> bool:
    """Ping the host to check basic network connectivity."""
    try:
        # Determine ping command based on OS
        if sys.platform.lower().startswith('win'):
            cmd = ['ping', '-n', '1', '-w', '5000', host]
        else:
            cmd = ['ping', '-c', '1', '-W', '5', host]
        
        logger.info(f"Pinging {host}...")
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
        
        if result.returncode == 0:
            logger.info(f"Ping to {host} successful")
            return True
        else:
            logger.warning(f"Ping to {host} failed")
            return False
            
    except Exception as e:
        logger.warning(f"Ping failed: {e}")
        return False

def test_tcp_connection(host: str, port: int = 80) -> bool:
    """Test TCP connectivity using socket."""
    try:
        logger.info(f"Testing TCP connection to {host}:{port}...")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            logger.info(f"TCP connection to {host}:{port} successful")
            return True
        else:
            logger.warning(f"TCP connection to {host}:{port} failed")
            return False
    except Exception as e:
        logger.error(f"TCP connection test failed: {e}")
        return False

def test_yottadb_connectivity():
    """Test YottaDB connectivity without calling the API."""
    api_url = DATABASE_CONFIG["YottaDB"]["api_url"]
    
    logger.info(f"Testing connectivity to YottaDB API: {api_url}")
    
    # Extract hostname
    try:
        parsed = urlparse(api_url)
        host = parsed.hostname
        port = parsed.port if parsed.port else (443 if parsed.scheme == 'https' else 80)
        
        logger.info(f"Extracted host: {host}, port: {port}")
        
        # Test ping
        if ping_host(host):
            logger.info("✓ Ping test passed")
            ping_ok = True
        else:
            logger.warning("✗ Ping test failed")
            ping_ok = False
        
        # Test TCP connection
        if test_tcp_connection(host, port):
            logger.info("✓ TCP connection test passed")
            tcp_ok = True
        else:
            logger.warning("✗ TCP connection test failed")
            tcp_ok = False
        
        if ping_ok or tcp_ok:
            logger.info("✓ Network connectivity confirmed")
            return True
        else:
            logger.error("✗ All connectivity tests failed")
            return False
            
    except Exception as e:
        logger.error(f"Error testing connectivity: {e}")
        return False

def test_yottadb_api():
    """Test the actual YottaDB API call (takes 2-3 minutes)."""
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

def main():
    """Main function with menu options."""
    logger.info("YottaDB Debug Utility")
    logger.info("====================")
    
    print("\nSelect test type:")
    print("1. Quick connectivity test (ping/tcp) - fast")
    print("2. Full API test - takes 2-3 minutes")
    print("3. Both tests")
    
    try:
        choice = input("\nEnter choice (1-3): ").strip()
        
        if choice == "1":
            logger.info("Running quick connectivity test...")
            if test_yottadb_connectivity():
                logger.info("✓ Connectivity test PASSED")
            else:
                logger.error("✗ Connectivity test FAILED")
                
        elif choice == "2":
            logger.info("Running full API test...")
            if test_yottadb_api():
                logger.info("✓ API test PASSED")
            else:
                logger.error("✗ API test FAILED")
                
        elif choice == "3":
            logger.info("Running both tests...")
            
            logger.info("\n--- Quick Connectivity Test ---")
            connectivity_ok = test_yottadb_connectivity()
            
            if connectivity_ok:
                logger.info("\n--- Full API Test ---")
                api_ok = test_yottadb_api()
                
                if connectivity_ok and api_ok:
                    logger.info("✓ All tests PASSED")
                else:
                    logger.error("✗ Some tests FAILED")
            else:
                logger.error("✗ Skipping API test due to connectivity failure")
        else:
            logger.error("Invalid choice")
            
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == "__main__":
    main()