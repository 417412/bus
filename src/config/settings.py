"""
Configuration settings for the medical system ETL application.
"""

# Database connection configurations
# ... existing code ...

DATABASE_CONFIG = {
    "PostgreSQL": {
        "host": "localhost",
        "database": "medical_system",
        "user": "medapp_user",
        "password": "the2zG6tbewA3"
    },
    "Firebird": {
        "host": "192.168.160.168:3050",
        "database": "099-1",            
        "user": "SYSDBA",
        "password": "masterkey",
        "charset": "UTF-8",
        "debug": False
    },
    "YottaDB": {
        # YottaDB specific configuration for HTTP API
        "api_url": "http://192.168.156.43/cgi-bin/qms_export_pat",
        "timeout": 300,  # 5 minutes read timeout (allowing extra time)
        "connect_timeout": 300,  # 30 seconds connection timeout
        "max_retries": 2,  # Fewer retries since each call is long
        "delimiter": "|"
    }
}

# ... rest of the code ...

# Document type mapping (used for display and data mapping)
DOCUMENT_TYPES = {
    1: 'Паспорт',
    2: 'Паспорт СССР',
    3: 'Заграничный паспорт РФ',
    4: 'Заграничный паспорт СССР', 
    5: 'Свидетельство о рождении',
    6: 'Удостоверение личности офицера',
    7: 'Справка об освобождении из места лишения свободы',
    8: 'Военный билет',
    9: 'Дипломатический паспорт РФ',
    10: 'Иностранный паспорт',
    11: 'Свидетельство беженца',
    12: 'Вид на жительство',
    13: 'Удостоверение беженца',
    14: 'Временное удостоверение',
    15: 'Паспорт моряка',
    16: 'Военный билет офицера запаса',
    17: 'Иные документы'
}

# Document type mapping for external systems
EXTERNAL_DOCUMENT_TYPE_MAPPING = {
    # Infoclinica document type -> Our system
    99: 17,  # Map Infoclinica's "Other" (99) to our "Other documents" (17)
}

# Logging configuration
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": "etl_test.log"
}