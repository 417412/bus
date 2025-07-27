-- Enhanced performance indexes for patient matching system with mobile app support
-- Run this separately from other SQL scripts due to CONCURRENTLY requirement

-- Drop existing indexes if they exist (for clean reinstall)
DROP INDEX IF EXISTS idx_patients_hisnumber_qms;
DROP INDEX IF EXISTS idx_patients_hisnumber_infoclinica;
DROP INDEX IF EXISTS idx_patients_document;
DROP INDEX IF EXISTS idx_patientsdet_uuid;
DROP INDEX IF EXISTS idx_protocols_uuid;
DROP INDEX IF EXISTS idx_mobile_app_users_qms;
DROP INDEX IF EXISTS idx_mobile_app_users_infoclinica;
DROP INDEX IF EXISTS idx_mobile_app_users_uuid;
DROP INDEX IF EXISTS idx_patients_matching_locked;

-- Create performance-critical indexes for existing tables
CREATE INDEX idx_patients_hisnumber_qms ON patients(hisnumber_qms) WHERE hisnumber_qms IS NOT NULL;
CREATE INDEX idx_patients_hisnumber_infoclinica ON patients(hisnumber_infoclinica) WHERE hisnumber_infoclinica IS NOT NULL;
CREATE INDEX idx_patients_document ON patients(documenttypes, document_number) WHERE documenttypes IS NOT NULL AND document_number IS NOT NULL;
CREATE INDEX idx_patientsdet_uuid ON patientsdet(uuid);
CREATE INDEX idx_protocols_uuid ON protocols(uuid) WHERE uuid IS NOT NULL;

-- New indexes for mobile app users table
CREATE INDEX idx_mobile_app_users_qms ON mobile_app_users(hisnumber_qms) WHERE hisnumber_qms IS NOT NULL;
CREATE INDEX idx_mobile_app_users_infoclinica ON mobile_app_users(hisnumber_infoclinica) WHERE hisnumber_infoclinica IS NOT NULL;
CREATE INDEX idx_mobile_app_users_uuid ON mobile_app_users(uuid);

-- Enhanced indexes for patients table
CREATE INDEX idx_patients_uuid ON patients(uuid);
CREATE INDEX idx_patients_matching_locked ON patients(matching_locked, uuid) WHERE matching_locked = FALSE;
CREATE INDEX idx_patients_mobile_registration ON patients(registered_via_mobile, uuid) WHERE registered_via_mobile = TRUE;

-- Enhanced indexes for matching log
CREATE INDEX idx_patient_matching_log_hisnumber ON patient_matching_log(hisnumber);
CREATE INDEX idx_patient_matching_log_source ON patient_matching_log(source);
CREATE INDEX idx_patient_matching_log_match_type ON patient_matching_log(match_type);
CREATE INDEX idx_patient_matching_log_mobile_uuid ON patient_matching_log(mobile_app_uuid) WHERE mobile_app_uuid IS NOT NULL;
CREATE INDEX idx_patient_matching_log_matched_patient ON patient_matching_log(matched_patient_uuid);
CREATE INDEX idx_patient_matching_log_time ON patient_matching_log(match_time);

-- Composite indexes for common query patterns
CREATE INDEX idx_patientsdet_hisnumber_source ON patientsdet(hisnumber, source);
CREATE INDEX idx_patientsdet_processed_at ON patientsdet(processed_at) WHERE processed_at IS NOT NULL;
CREATE INDEX idx_patients_created_updated ON patients(created_at, updated_at);

-- GIN index for JSONB details in matching log
CREATE INDEX idx_patient_matching_log_details ON patient_matching_log USING GIN(details);