# src/etl/loaders/postgres_loader.py
from src.repositories.postgres_repository import PostgresRepository

class PostgresLoader:
    def __init__(self, repository: PostgresRepository):
        self.repository = repository
    
    def load_patient(self, patient_data):
        """Load a patient record into PostgreSQL."""
        return self.repository.insert_patient(patient_data)
        
    def load_patients(self, patients):
        """Load multiple patient records into PostgreSQL."""
        success_count = 0
        for patient in patients:
            if self.load_patient(patient):
                success_count += 1
        return success_count