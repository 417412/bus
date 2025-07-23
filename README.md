# Medical System ETL Bus

A comprehensive Extract, Transform, Load (ETL) service designed to synchronize patient data between multiple Healthcare Information Systems (HIS) and a centralized PostgreSQL database.

## Overview

This ETL Bus service facilitates seamless data integration between different medical systems:
- **qMS** (YottaDB) - Primary HIS via HTTP API
- **Infoclinica** (Firebird) - Secondary HIS with delta sync capabilities
- **PostgreSQL** - Centralized target database with patient matching and deduplication

## Architecture

The system follows a modular architecture with clear separation of concerns:
```
src/ 
├── config/ # Configuration settings 
├── connectors/ # Database/API connection handlers 
├── repositories/ # Data access layer 
├── etl/ # ETL services and transformers 
├── daemons/ # Background service processes 
├── triggers/ # Database triggers for patient matching 
└── utils/ # Utility functions and helpers
└── systemd/ # Service configuration files
```


## Key Features

### Multi-Source Data Integration
- **YottaDB (qMS)**: HTTP API-based data retrieval with full synchronization
- **Firebird (Infoclinica)**: Database connection with incremental delta sync
- **PostgreSQL**: Centralized storage with automated patient matching

### Intelligent Patient Matching
- Automatic patient deduplication based on document numbers
- Cross-system patient identity resolution
- Database triggers for real-time matching during data insertion

### Scalable Processing
- Batch processing with configurable batch sizes
- Resumable operations with state persistence
- Configurable sync intervals per data source

### Robust Error Handling
- Retry mechanisms for network timeouts
- Comprehensive logging and monitoring
- Graceful failure recovery

## Quick Start

### Prerequisites

- Python 3.8+
- PostgreSQL 12+
- Access to source systems (qMS API, Firebird database)

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd medical-etl-bus
   ```
2. **Create virtual environment**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```
3. **Install dependencies**
    ```bash
    pip install -r requirements.txt
    ```
4. **Database Setup**
    ```bash
    # Create PostgreSQL database and user
    sudo -u postgres createdb medical_system
    sudo -u postgres createuser medapp_user

    # Import schema
    psql -U medapp_user -d medical_system -f src/schema/schema.sql

    # Install triggers
    psql -U medapp_user -d medical_system -f src/triggers/matching_trigger.sql
    ```
5. **Configuration**
    ```bash
    # Edit configuration file
    cp src/config/settings.py.example src/config/settings.py
    # Update database connections and API endpoints
    ```

### Testing
    Run the comprehensive test suite:
    ```bash
    python src/test_etl.py

# Usage
## Manual Execution
### Initial Data Load
    ```bash
    # Load from Firebird (Infoclinica)
    python src/daemons/etl_daemon.py --source firebird --initial-load

    # Load from YottaDB (qMS)
    python src/daemons/etl_daemon.py --source yottadb --initial-load
    ```
### Delta Synchronization

    ```bash
    # Firebird delta sync (incremental changes)
    python src/daemons/etl_daemon.py --source firebird --delta-sync

    # YottaDB full sync (complete refresh)
    python src/daemons/etl_daemon.py --source yottadb --no-daemon
    ```

## Daemon Services
### Install systemd services

    ```bash
    # Copy service files
    sudo cp src/systemd/etl_*_daemon.service /etc/systemd/system/
    sudo systemctl daemon-reload

    # Enable and start services
    sudo systemctl enable etl_firebird_daemon.service
    sudo systemctl enable etl_yottadb_daemon.service
    sudo systemctl start etl_firebird_daemon.service
    sudo systemctl start etl_yottadb_daemon.service
    ```

### Monitor services

    ```bash
    # Check status
    sudo systemctl status etl_firebird_daemon.service
    sudo systemctl status etl_yottadb_daemon.service

    # View logs
    sudo journalctl -u etl_firebird_daemon.service -f
    sudo journalctl -u etl_yottadb_daemon.service -f
    ```

## Configuration

### Database Settings
    ```python
    DATABASE_CONFIG = {
        "PostgreSQL": {
            "host": "localhost",
            "database": "medical_system",
            "user": "medapp_user",
            "password": "your_password"
        },
        "Firebird": {
            "host": "firebird_server:3050",
            "database": "database_name",
            "user": "user",
            "password": "password"
        },
        "YottaDB": {
            "api_url": "http://yottadb_server/cgi-bin/qms_export_pat",
            "timeout": 300,
            "connect_timeout": 30,
            "max_retries": 2
        }
    }
    ```