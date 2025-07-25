import logging
import re
from typing import Dict, Any, Optional
from datetime import datetime
from src.config.settings import setup_logger

class YottaDBTransformer:
    """Transform raw YottaDB (qMS) data into standardized format."""
    
    def __init__(self):
        self.logger = setup_logger(__name__, "transformers")
        
        # Map for document types from qMS to our schema
        self.document_type_map = {
            '1': 2,    # Паспорт_гражданина_СССР -> Паспорт СССР
            '2': 4,    # Загранпаспорт_гражданина_СССР -> Заграничный паспорт СССР
            '3': 5,    # Свидетельство_о_рождении -> Свидетельство о рождении
            '4': 6,    # Удостоверение_личности_офицера -> Удостоверение личности офицера
            '5': 7,    # Справка_об_освобождении_из_места_лишения_свободы -> Справка об освобождении из места лишения свободы
            '6': 15,   # Паспорт_Минморфлота_России -> Паспорт моряка
            '7': 8,    # Военный_билет_солдата_(матроса,_сержанта,_старшины) -> Военный билет
            '9': 9,    # Дипломатический_паспорт_гражданина_Российской_Федерации -> Дипломатический паспорт РФ
            '10': 10,  # Иностранный_паспорт -> Иностранный паспорт
            '11': 11,  # Свидетельство_о_регистрации_ходатайства_лица_о_признании_его_беженцем -> Свидетельство беженца
            '12': 12,  # Вид_на_жительство -> Вид на жительство
            '13': 13,  # Удостоверение_беженца -> Удостоверение беженца
            '14': 17,  # Удостоверение_вынужденного_переселенца -> Иные документы
            '15': 14,  # Временное_удостоверение_личности_гражданина_Российской_Федерации -> Временное удостоверение
            '16': 1,   # Паспорт_гражданина_Российской_Федерации -> Паспорт
            '17': 3,   # Загранпаспорт_гражданина_Российской_Федерации -> Заграничный паспорт РФ
            '18': 17,  # Свидетельство_о_рождении,_выданное_уполномоченным_органом_иностранного_государства -> Иные документы
            '19': 15,  # Паспорт_моряка -> Паспорт моряка
            '20': 16,  # Военный_билет_офицера_запаса_вооруженных_сил -> Военный билет офицера запаса
            '47': 17,  # Без_документов -> Иные документы
            '51': 17,  # Виза -> Иные документы
            '52': 17,  # Разрешение_на_временное_проживание -> Иные документы
            '90': 17,  # Иные_документы,_выдаваемые_органами_МВД_России -> Иные документы
            '99': 17,  # Иные_документы,_удостоверяющие_личность_в_соответствии_с_законодательством -> Иные документы
            '100': 17, # Удостоверение_личности_военнослужащего -> Иные документы
            '101': 17, # Удостоверение -> Иные документы
            '289': 16, # Военный_билет_офицера_запаса_вооруженных_сил -> Военный билет офицера запаса
            '412': 17, # Миграционная_карта -> Иные документы
            '732': 17, # Медицинское_свидетельство_о_рождении_(для_детей_в_возрасте_до_1_месяца) -> Иные документы
            '735': 10, # Паспорт_иностранного_гражданина -> Иностранный паспорт
            '1374': 17 # Свидетельство_о_предоставлении_временного_убежища_на_территории_Российской_Федерации -> Иные документы
        }
    
    def normalize_document_number(self, series: Optional[str], number: Optional[str]) -> Optional[int]:
        """
        Normalize document number by combining series and number and extracting digits.
        
        Args:
            series: Document series
            number: Document number
            
        Returns:
            Normalized document number as integer or None
        """
        if not series and not number:
            return None
            
        # Combine series and number
        combined = f"{series or ''}{number or ''}"
        
        # Extract only digits
        digits = re.sub(r'\D', '', combined)
        
        if digits:
            try:
                return int(digits)
            except ValueError:
                return None
        return None
    
    def normalize_date(self, date_str: Optional[str]) -> Optional[str]:
        """
        Normalize date string to ISO format (YYYY-MM-DD).
        
        Args:
            date_str: Date string in YYYYMMDD format
            
        Returns:
            Date string in ISO format or None
        """
        if not date_str:
            return None
            
        # Remove any non-digit characters
        digits = re.sub(r'\D', '', date_str)
        
        # Check if we have exactly 8 digits for YYYYMMDD
        if len(digits) == 8:
            try:
                year = digits[0:4]
                month = digits[4:6]
                day = digits[6:8]
                
                # Basic validation
                if int(month) < 1 or int(month) > 12:
                    self.logger.warning(f"Invalid month in date: {date_str}")
                    return None
                if int(day) < 1 or int(day) > 31:
                    self.logger.warning(f"Invalid day in date: {date_str}")
                    return None
                    
                return f"{year}-{month}-{day}"
            except (ValueError, IndexError):
                self.logger.warning(f"Error parsing date: {date_str}")
                return None
        else:
            self.logger.warning(f"Date string doesn't have 8 digits: {date_str}")
            return None
    
    def map_document_type(self, doc_type: Optional[str]) -> int:
        """
        Map document type from qMS to our schema.
        
        Args:
            doc_type: Document type code from qMS
            
        Returns:
            Mapped document type ID according to our schema
        """
        if not doc_type:
            return 1  # Default to passport
            
        # Get mapped type or default to "Иные документы" (17)
        mapped_type = self.document_type_map.get(str(doc_type), 17)
        
        if mapped_type == 17 and str(doc_type) not in self.document_type_map:
            self.logger.warning(f"Unknown document type {doc_type}, mapping to 'Иные документы' (17)")
            
        return mapped_type
    
    def clean_phone_number(self, phone: Optional[str]) -> Optional[str]:
        """
        Clean and normalize phone number.
        
        Args:
            phone: Raw phone number
            
        Returns:
            Cleaned phone number with only digits or None
        """
        if not phone:
            return None
            
        # Extract only digits
        digits = re.sub(r'\D', '', phone)
        
        # Return None if no digits found
        if not digits:
            return None
            
        # Remove leading 8 if phone starts with 8 and has 11 digits (Russian format)
        if len(digits) == 11 and digits.startswith('8'):
            digits = '7' + digits[1:]
            
        return digits
        
    def transform_patient(self, raw_patient: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform a raw patient record into standardized format.
        """
        try:
            # Extract and transform fields
            birthdate = self.normalize_date(raw_patient.get('birthdate'))
            doc_type = self.map_document_type(raw_patient.get('documenttypes'))
            document_number = self.normalize_document_number(
                raw_patient.get('series'), 
                raw_patient.get('number')
            )
            
            # Clean phone number
            telephone = self.clean_phone_number(raw_patient.get('telephone'))
            
            # Keep the full hisnumber as provided by the API
            hisnumber = raw_patient.get('hisnumber', '')
            
            # Build standardized patient record
            return {
                "hisnumber": hisnumber,
                "source": raw_patient.get('source', 1),  # qMS = 1
                "businessunit": raw_patient.get('businessunit', 1),
                "lastname": raw_patient.get('lastname'),
                "name": raw_patient.get('name'),
                "surname": raw_patient.get('surname'),
                "birthdate": birthdate,
                "documenttypes": doc_type,
                "document_number": document_number,
                "email": raw_patient.get('email'),          # Contact email
                "telephone": telephone,
                "his_password": None,                       # No password for qMS patients
                "login_email": raw_patient.get('login_email')  # LOGIN EMAIL - ADD THIS
            }
        except Exception as e:
            self.logger.error(f"Error transforming patient {raw_patient.get('hisnumber', 'unknown')}: {e}")
            # Return a minimal record
            return {
                "hisnumber": str(raw_patient.get('hisnumber', '')),
                "source": raw_patient.get('source', 1),
                "businessunit": raw_patient.get('businessunit', 1),
                "documenttypes": 17,  # Default to "Иные документы"
                "lastname": raw_patient.get('lastname'),
                "name": raw_patient.get('name'),
                "surname": raw_patient.get('surname'),
                "login_email": raw_patient.get('login_email')  # ADD THIS TOO
            }