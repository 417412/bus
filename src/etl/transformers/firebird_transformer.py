import logging
import re
from typing import Dict, Any, Optional
from datetime import datetime

class FirebirdTransformer:
    """Transform raw Firebird data into standardized format."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def normalize_document_number(self, document_number: Optional[str]) -> Optional[int]:
        """
        Extract digits and convert to integer.
        
        Args:
            document_number: Raw document number
            
        Returns:
            Normalized document number as integer or None
        """
        if not document_number:
            return None
        digits = re.sub(r'\D', '', str(document_number))
        if digits:
            try:
                return int(digits)
            except ValueError:
                return None
        return None
    
    def map_document_type(self, doc_type: Optional[int]) -> int:
        """
        Map Firebird document types to our system.
        
        Args:
            doc_type: Original document type ID
            
        Returns:
            Mapped document type ID
        """
        try:
            # Convert to integer first if it's a string
            if isinstance(doc_type, str):
                doc_type = int(doc_type)
        except (ValueError, TypeError):
            # Default to passport (type 1) if conversion fails
            return 1
            
        # Map type 99 to our "Other documents" type (17)
        if doc_type == 99:
            return 17
        elif doc_type == 88:
            return 17
        # Default to passport (type 1) if not specified or invalid
        elif doc_type is None or doc_type == 0:
            return 1
        
        # Check if the type is within our valid range (1-17)
        if 1 <= doc_type <= 17:
            return doc_type
        else:
            # If not in range, map to "Other documents"
            self.logger.warning(f"Document type {doc_type} outside valid range, mapping to 'Other documents' (17)")
            return 17
        
    def transform_patient(self, raw_patient: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform a raw patient record into standardized format.
        
        Args:
            raw_patient: Raw patient data from Firebird
            
        Returns:
            Standardized patient record
        """
        # Format birthdate
        birthdate = raw_patient.get('birthdate')
        if isinstance(birthdate, datetime):
            birthdate = birthdate.date().isoformat()
        elif isinstance(birthdate, str) and '.' in birthdate:
            parts = birthdate.split('.')
            if len(parts) == 3:
                day, month, year = parts
                birthdate = f"{year}-{month}-{day}"
        
        # Map document type
        doc_type = self.map_document_type(raw_patient.get('documenttypes'))
        
        # Normalize document number
        doc_number = self.normalize_document_number(raw_patient.get('document_number'))
        
        # Build standardized patient record
        return {
            "hisnumber": raw_patient.get('hisnumber'),
            "source": 2,  # Инфоклиника
            "businessunit": raw_patient.get('businessunit') or 2,
            "lastname": raw_patient.get('lastname'),
            "name": raw_patient.get('name'),
            "surname": raw_patient.get('surname'),
            "birthdate": birthdate,
            "documenttypes": doc_type,
            "document_number": doc_number,
            "email": raw_patient.get('email'),
            "telephone": raw_patient.get('telephone'),
            "his_password": raw_patient.get('his_password')
        }