-- HIS reference table
CREATE TABLE hislist (
    id SMALLSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

-- Initial data for HIS systems
INSERT INTO hislist (id, name) VALUES 
(1, 'qMS'),
(2, 'Инфоклиника');

-- Business units
CREATE TABLE businessunits (
    id SMALLSERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

-- Initial data for business units
INSERT INTO businessunits (id, name) VALUES 
(1, 'ОО ФК "Хадасса Медикал ЛТД"'),
(2, 'ООО "Медскан"'),
(3, 'ООО "Клинический госпиталь на Яузе"');

-- Type of patient document
CREATE TABLE documenttypes (
    id SMALLSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

-- Initial data for document types
INSERT INTO documenttypes (id, name) VALUES 
(1, 'Паспорт'),
(2, 'Паспорт СССР'),
(3, 'Заграничный паспорт РФ'),
(4, 'Заграничный паспорт СССР'),
(5, 'Свидетельство о рождении'),
(6, 'Удостоверение личности офицера'),
(7, 'Справка об освобождении из места лишения свободы'),
(8, 'Военный билет'),
(9, 'Дипломатический паспорт РФ'),
(10, 'Иностранный паспорт'),
(11, 'Свидетельство беженца'),
(12, 'Вид на жительство'),
(13, 'Удостоверение беженца'),
(14, 'Временное удостоверение'),
(15, 'Паспорт моряка'),
(16, 'Военный билет офицера запаса'),
(17, 'Иные документы');

-- Mobile app user registrations - stores HIS numbers for patients registered via mobile app
CREATE TABLE mobile_app_users (
    id SERIAL PRIMARY KEY,
    uuid UUID NOT NULL DEFAULT gen_random_uuid(),
    hisnumber_qms VARCHAR(255),
    hisnumber_infoclinica VARCHAR(255),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensure we have at least one HIS number
    CONSTRAINT mobile_app_users_has_hisnumber CHECK (
        hisnumber_qms IS NOT NULL OR hisnumber_infoclinica IS NOT NULL
    ),
    
    -- Ensure unique HIS numbers
    CONSTRAINT mobile_app_users_qms_unique UNIQUE (hisnumber_qms),
    CONSTRAINT mobile_app_users_infoclinica_unique UNIQUE (hisnumber_infoclinica),
    CONSTRAINT mobile_app_users_uuid_unique UNIQUE (uuid)
);

-- Consolidated patients table with additional fields including login emails
CREATE TABLE patients (
    id SERIAL PRIMARY KEY,
    uuid UUID NOT NULL DEFAULT gen_random_uuid(),
    documenttypes SMALLINT REFERENCES documenttypes(id),
    document_number BIGINT,
    
    -- Consolidated demographic data
    lastname TEXT,
    name TEXT,
    surname TEXT,
    birthdate DATE,
    
    -- HIS-specific identifiers
    hisnumber_qms VARCHAR(255),
    hisnumber_infoclinica VARCHAR(255),
    
    -- Contact info and credentials for each HIS
    email_qms VARCHAR(255),              -- Contact email for qMS
    telephone_qms VARCHAR(50),
    password_qms VARCHAR(100),
    login_qms VARCHAR(255),              -- Login email for qMS API
    
    email_infoclinica VARCHAR(255),      -- Contact email for Infoclinica
    telephone_infoclinica VARCHAR(50),
    password_infoclinica VARCHAR(100),
    login_infoclinica VARCHAR(255),      -- Login email for Infoclinica
    
    -- Source of canonical data
    primary_source SMALLINT REFERENCES hislist(id),
    
    -- Mobile app registration flag
    registered_via_mobile BOOLEAN NOT NULL DEFAULT FALSE,
    
    -- Matching protection flag to prevent re-matching already processed patients
    matching_locked BOOLEAN NOT NULL DEFAULT FALSE,
    matching_locked_at TIMESTAMP,
    matching_locked_reason TEXT,
    
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT patients_uuid_unique UNIQUE (uuid),
    CONSTRAINT patients_qms_unique UNIQUE (hisnumber_qms),
    CONSTRAINT patients_infoclinica_unique UNIQUE (hisnumber_infoclinica)
);

-- Raw patient details from various HIS systems
CREATE TABLE patientsdet (
    id SERIAL PRIMARY KEY,
    hisnumber VARCHAR(255) NOT NULL,
    source SMALLINT NOT NULL REFERENCES hislist(id),
    businessunit SMALLINT NOT NULL REFERENCES businessunits(id),
    lastname TEXT,
    name TEXT,
    surname TEXT,
    birthdate DATE,
    documenttypes SMALLINT REFERENCES documenttypes(id),
    document_number BIGINT,
    email VARCHAR(255),          -- Contact email
    telephone VARCHAR(50),
    his_password VARCHAR(100),
    login_email VARCHAR(255),    -- Login email for HIS API access
    uuid UUID REFERENCES patients(uuid),
    processed_at TIMESTAMP,      -- Track when record was processed
    CONSTRAINT unique_hisnumber_source UNIQUE (hisnumber, source)
);

-- Medical protocols
CREATE TABLE protocols (
    id BIGSERIAL PRIMARY KEY,
    uuid UUID NOT NULL REFERENCES patients(uuid),
    source SMALLINT NOT NULL REFERENCES hislist(id),
    businessunit SMALLINT NOT NULL REFERENCES businessunits(id),
    date TIMESTAMP NOT NULL,
    doctor TEXT,
    protocolname TEXT,
    servicename TEXT,
    servicecode VARCHAR(255),
    content JSONB
);

-- Create necessary indexes
CREATE INDEX idx_patientsdet_names ON patientsdet(lastname, name, surname);
CREATE INDEX idx_patientsdet_birthdate ON patientsdet(birthdate);
CREATE INDEX idx_patientsdet_document_number ON patientsdet(document_number);
CREATE INDEX idx_patientsdet_uuid ON patientsdet(uuid);
CREATE INDEX idx_patients_passport ON patients(document_number);
CREATE INDEX idx_protocols_uuid ON protocols(uuid);
CREATE INDEX idx_protocols_date ON protocols(date);

-- Audit log for patient matching
CREATE TABLE patient_matching_log (
    id SERIAL PRIMARY KEY,
    hisnumber VARCHAR(255) NOT NULL,
    source SMALLINT NOT NULL,
    match_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    match_type VARCHAR(30) NOT NULL, -- Extended to include mobile app types
    document_number BIGINT,
    created_uuid BOOLEAN NOT NULL DEFAULT FALSE,
    mobile_app_uuid UUID,  -- Reference to mobile_app_users if applicable
    matched_patient_uuid UUID,  -- Reference to matched patient
    details JSONB  -- Additional matching details
);

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;$$ LANGUAGE plpgsql;

-- Triggers for updated_at
CREATE TRIGGER trg_mobile_app_users_updated_at
    BEFORE UPDATE ON mobile_app_users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_patients_updated_at
    BEFORE UPDATE ON patients
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();