#!/bin/bash

# Set HIS API endpoints
export YOTTADB_API_BASE="http://192.168.156.43"
export FIREBIRD_API_BASE="http://your-firebird-server"

# Set OAuth endpoints
export YOTTADB_TOKEN_URL="http://192.168.156.43/oauth/token"
export FIREBIRD_TOKEN_URL="http://your-firebird-server/oauth/token"

# Set OAuth credentials for YottaDB
export YOTTADB_CLIENT_ID="your_yottadb_client_id"
export YOTTADB_CLIENT_SECRET="your_yottadb_client_secret"
export YOTTADB_USERNAME="your_yottadb_api_user"
export YOTTADB_PASSWORD="your_yottadb_api_password"
export YOTTADB_SCOPE="patient_update"

# Set OAuth credentials for Firebird
export FIREBIRD_CLIENT_ID="your_firebird_client_id"
export FIREBIRD_CLIENT_SECRET="your_firebird_client_secret"
export FIREBIRD_USERNAME="your_firebird_api_user"
export FIREBIRD_PASSWORD="your_firebird_api_password"
export FIREBIRD_SCOPE="patient_update"

# Create logs directory
mkdir -p logs

# Run the application
echo "Starting Patient Credential Management API with OAuth authentication..."
python3 run.py