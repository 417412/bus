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

-- Consolidated patients table with additional fields
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
    email_qms VARCHAR(255),
    telephone_qms VARCHAR(50),
    password_qms VARCHAR(100),
    
    email_infoclinica VARCHAR(255),
    telephone_infoclinica VARCHAR(50),
    password_infoclinica VARCHAR(100),
    
    -- Source of canonical data
    primary_source SMALLINT REFERENCES hislist(id),
    
    CONSTRAINT patients_uuid_unique UNIQUE (uuid)
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
    email VARCHAR(255),
    telephone VARCHAR(50),
    his_password VARCHAR(100),
    uuid UUID REFERENCES patients(uuid),
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
    match_type VARCHAR(20) NOT NULL, -- 'NEW', 'MATCHED', 'MANUAL'
    document_number BIGINT,
    created_uuid BOOLEAN NOT NULL DEFAULT FALSE
);