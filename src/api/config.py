# Update the HIS_API_CONFIG section:

# HIS System API Endpoints with OAuth Configuration
HIS_API_CONFIG = {
    "yottadb": {
        "base_url": os.getenv("YOTTADB_API_BASE", "http://192.168.156.43"),
        "credentials_endpoint": "/updatePatients/{hisnumber}/credentials",
        "create_endpoint": "/createPatients",
        "oauth": {
            "token_url": os.getenv("YOTTADB_TOKEN_URL", "http://192.168.156.43/token"),  # Fixed: /token not /oauth/token
            "client_id": os.getenv("YOTTADB_CLIENT_ID", "admin"),        # Fixed: admin instead of yottadb_client
            "client_secret": os.getenv("YOTTADB_CLIENT_SECRET", "secret"),  # Fixed: secret instead of yottadb_secret
            "username": os.getenv("YOTTADB_USERNAME", "admin"),          # Fixed: admin instead of api_user
            "password": os.getenv("YOTTADB_PASSWORD", "secret"),         # Fixed: secret instead of api_password
            "scope": os.getenv("YOTTADB_SCOPE", "")  # Fixed: empty scope since docs don't mention it
        }
    },
    "firebird": {
        "base_url": os.getenv("FIREBIRD_API_BASE", "http://192.168.160.141"),
        "credentials_endpoint": "/updatePatients/{hisnumber}/credentials",
        "create_endpoint": "/createPatients",
        "oauth": {
            "token_url": os.getenv("FIREBIRD_TOKEN_URL", "http://192.168.160.141/token"),  # Fixed: /token not /oauth/token
            "client_id": os.getenv("FIREBIRD_CLIENT_ID", "admin"),       # Fixed: admin instead of firebird_client
            "client_secret": os.getenv("FIREBIRD_CLIENT_SECRET", "secret"), # Fixed: secret instead of firebird_secret
            "username": os.getenv("FIREBIRD_USERNAME", "admin"),         # Fixed: admin instead of api_user
            "password": os.getenv("FIREBIRD_PASSWORD", "secret"),        # Fixed: secret instead of api_password
            "scope": os.getenv("FIREBIRD_SCOPE", "")  # Fixed: empty scope since docs don't mention it
        }
    }
}