"""
Patient data model for the medical system ETL.
Provides a standardized way to handle patient data across different HIS systems.
"""

from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any, Union
from datetime import datetime, date
import re

@dataclass
class Patient:
    """
    Standard patient data model.
    
    This model represents a patient record that can come from any HIS system
    and be transformed for storage in the target PostgreSQL database.
    """
    # Core identifier
    hisnumber: str
    source: int  # 1=qMS, 2=Инфоклиника
    
    # Business information
    businessunit: int
    
    # Demographics
    lastname: Optional[str] = None
    name: Optional[str] = None
    surname: Optional[str] = None
    birthdate: Optional[Union[date, str]] = None
    
    # Document information
    documenttypes: Optional[int] = None
    document_number: Optional[int] = None
    
    # Contact information
    email: Optional[str] = None          # Contact email
    telephone: Optional[str] = None
    
    # HIS credentials
    his_password: Optional[str] = None
    login_email: Optional[str] = None    # Login email for HIS API
    
    # Internal tracking
    uuid: Optional[str] = None
    
    def __post_init__(self):
        """Validate and normalize data after initialization."""
        # Ensure hisnumber is string
        if self.hisnumber is not None:
            self.hisnumber = str(self.hisnumber)
        
        # Normalize birthdate
        if isinstance(self.birthdate, datetime):
            self.birthdate = self.birthdate.date().isoformat()
        elif isinstance(self.birthdate, date):
            self.birthdate = self.birthdate.isoformat()
        elif isinstance(self.birthdate, str) and '.' in self.birthdate:
            # Handle DD.MM.YYYY format
            parts = self.birthdate.split('.')
            if len(parts) == 3:
                day, month, year = parts
                self.birthdate = f"{year}-{month}-{day}"
        
        # Normalize document number
        if self.document_number is not None:
            if isinstance(self.document_number, str):
                # Extract digits only
                digits = re.sub(r'\D', '', self.document_number)
                self.document_number = int(digits) if digits else None
        
        # Validate source
        if self.source not in [1, 2]:
            raise ValueError(f"Invalid source: {self.source}. Must be 1 (qMS) or 2 (Инфоклиника)")
        
        # Validate businessunit
        if self.businessunit not in [1, 2, 3]:
            raise ValueError(f"Invalid businessunit: {self.businessunit}. Must be 1, 2, or 3")
    
    @classmethod
    def from_firebird_raw(cls, raw_data: Dict[str, Any]) -> 'Patient':
        """
        Create Patient from raw Firebird data.
        
        Args:
            raw_data: Raw dictionary from Firebird query
            
        Returns:
            Patient instance
        """
        return cls(
            hisnumber=str(raw_data.get('hisnumber', '')),
            source=raw_data.get('source', 2),
            businessunit=raw_data.get('businessunit', 2),
            lastname=raw_data.get('lastname'),
            name=raw_data.get('name'),
            surname=raw_data.get('surname'),
            birthdate=raw_data.get('birthdate'),
            documenttypes=raw_data.get('documenttypes'),
            document_number=raw_data.get('document_number'),
            email=raw_data.get('email'),
            telephone=raw_data.get('telephone'),
            his_password=raw_data.get('his_password'),
            login_email=raw_data.get('login_email')  # New field from cllogin
        )
    
    @classmethod
    def from_yottadb_raw(cls, raw_data: Dict[str, Any]) -> 'Patient':
        """
        Create Patient from raw YottaDB data.
        
        Args:
            raw_data: Raw dictionary from YottaDB API
            
        Returns:
            Patient instance
        """
        return cls(
            hisnumber=str(raw_data.get('hisnumber', '')),
            source=raw_data.get('source', 1),
            businessunit=raw_data.get('businessunit', 1),
            lastname=raw_data.get('lastname'),
            name=raw_data.get('name'),
            surname=raw_data.get('surname'),
            birthdate=raw_data.get('birthdate'),
            documenttypes=raw_data.get('documenttypes'),
            document_number=raw_data.get('document_number'),
            email=raw_data.get('email'),          # Contact email (first one)
            telephone=raw_data.get('telephone'),
            his_password=raw_data.get('his_password'),
            login_email=raw_data.get('login_email')  # Login email (second one)
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database operations."""
        return asdict(self)
    
    def to_patientsdet_dict(self) -> Dict[str, Any]:
        """Convert to dictionary suitable for patientsdet table."""
        data = self.to_dict()
        # Remove uuid as it will be set by trigger
        data.pop('uuid', None)
        return data
    
    def get_source_name(self) -> str:
        """Get human-readable source name."""
        return {1: 'qMS', 2: 'Инфоклиника'}.get(self.source, 'Unknown')
    
    def get_businessunit_name(self) -> str:
        """Get human-readable business unit name."""
        names = {
            1: 'ОО ФК "Хадасса Медикал ЛТД"',
            2: 'ООО "Медскан"',
            3: 'ООО "Клинический госпиталь на Яузе"'
        }
        return names.get(self.businessunit, 'Unknown')
    
    def has_document(self) -> bool:
        """Check if patient has valid document information."""
        return (self.documenttypes is not None and 
                self.document_number is not None and 
                self.document_number > 0)
    
    def has_contact_info(self) -> bool:
        """Check if patient has any contact information."""
        return bool(self.email or self.telephone)
    
    def has_login_credentials(self) -> bool:
        """Check if patient has login credentials."""
        return bool(self.login_email and self.his_password)
    
    def __str__(self) -> str:
        """String representation."""
        name_parts = [self.lastname, self.name, self.surname]
        full_name = ' '.join(filter(None, name_parts))
        return f"Patient({self.hisnumber}, {self.get_source_name()}, {full_name})"