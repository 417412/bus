-- Performance indexes for patient matching system
-- Run this separately from other SQL scripts due to CONCURRENTLY requirement

-- Drop existing indexes if they exist (for clean reinstall)
DROP INDEX IF EXISTS idx_patients_hisnumber_qms;
DROP INDEX IF EXISTS idx_patients_hisnumber_infoclinica;
DROP INDEX IF EXISTS idx_patients_document;
DROP INDEX IF EXISTS idx_patientsdet_uuid;
DROP INDEX IF EXISTS idx_protocols_uuid;

-- Create performance-critical indexes
CREATE INDEX idx_patients_hisnumber_qms ON patients(hisnumber_qms) WHERE hisnumber_qms IS NOT NULL;
CREATE INDEX idx_patients_hisnumber_infoclinica ON patients(hisnumber_infoclinica) WHERE hisnumber_infoclinica IS NOT NULL;
CREATE INDEX idx_patients_document ON patients(documenttypes, document_number) WHERE documenttypes IS NOT NULL AND document_number IS NOT NULL;
CREATE INDEX idx_patientsdet_uuid ON patientsdet(uuid);
CREATE INDEX idx_protocols_uuid ON protocols(uuid) WHERE uuid IS NOT NULL;

-- Additional helpful indexes
CREATE INDEX idx_patients_uuid ON patients(uuid);
CREATE INDEX idx_patient_matching_log_hisnumber ON patient_matching_log(hisnumber);
CREATE INDEX idx_patient_matching_log_source ON patient_matching_log(source);
CREATE INDEX idx_patient_matching_log_match_type ON patient_matching_log(match_type);