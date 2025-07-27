-- Optimized patient matching trigger functions
-- This file contains only functions and triggers, no indexes

-- Optimized function for new patient processing
CREATE OR REPLACE FUNCTION process_new_patient()
RETURNS TRIGGER AS $$
DECLARE
    v_patient_uuid UUID;
    v_existing_patient UUID;
    v_match_type TEXT := 'NEW_NO_DOCUMENT';
BEGIN
    -- Skip processing if UUID is already provided
    IF NEW.uuid IS NOT NULL THEN
        RETURN NEW;
    END IF;
    
    -- FIRST: Quick check for existing patient with same HIS number and source
    -- Use the optimized indexes for fast lookup
    IF NEW.source = 1 THEN
        SELECT p.uuid INTO v_existing_patient
        FROM patients p
        WHERE p.hisnumber_qms = NEW.hisnumber
        LIMIT 1;  -- Only need to find one match
    ELSIF NEW.source = 2 THEN
        SELECT p.uuid INTO v_existing_patient
        FROM patients p
        WHERE p.hisnumber_infoclinica = NEW.hisnumber
        LIMIT 1;  -- Only need to find one match
    END IF;
    
    IF v_existing_patient IS NOT NULL THEN
        -- Patient already exists for this HIS, do minimal update
        v_patient_uuid := v_existing_patient;
        v_match_type := 'UPDATED_EXISTING';
        
        -- Only update the HIS-specific fields, don't touch demographics unless they're NULL
        IF NEW.source = 1 THEN
            UPDATE patients
            SET 
                email_qms = NEW.email,
                telephone_qms = NEW.telephone,
                password_qms = NEW.his_password,
                login_qms = NEW.login_email,
                -- Only update demographics if they're currently NULL
                lastname = CASE WHEN lastname IS NULL THEN NEW.lastname ELSE lastname END,
                name = CASE WHEN name IS NULL THEN NEW.name ELSE name END,
                surname = CASE WHEN surname IS NULL THEN NEW.surname ELSE surname END,
                birthdate = CASE WHEN birthdate IS NULL THEN NEW.birthdate ELSE birthdate END,
                documenttypes = CASE WHEN documenttypes IS NULL THEN NEW.documenttypes ELSE documenttypes END,
                document_number = CASE WHEN document_number IS NULL THEN NEW.document_number ELSE document_number END
            WHERE uuid = v_existing_patient;
        ELSE
            UPDATE patients
            SET 
                email_infoclinica = NEW.email,
                telephone_infoclinica = NEW.telephone,
                password_infoclinica = NEW.his_password,
                login_infoclinica = NEW.login_email,
                -- Only update demographics if they're currently NULL
                lastname = CASE WHEN lastname IS NULL THEN NEW.lastname ELSE lastname END,
                name = CASE WHEN name IS NULL THEN NEW.name ELSE name END,
                surname = CASE WHEN surname IS NULL THEN NEW.surname ELSE surname END,
                birthdate = CASE WHEN birthdate IS NULL THEN NEW.birthdate ELSE birthdate END,
                documenttypes = CASE WHEN documenttypes IS NULL THEN NEW.documenttypes ELSE documenttypes END,
                document_number = CASE WHEN document_number IS NULL THEN NEW.document_number ELSE document_number END
            WHERE uuid = v_existing_patient;
        END IF;
        
    ELSE
        -- SECOND: Look for existing patient with matching document (only if we have document info)
        IF NEW.document_number IS NOT NULL AND NEW.documenttypes IS NOT NULL THEN
            SELECT p.uuid INTO v_patient_uuid
            FROM patients p
            WHERE p.document_number = NEW.document_number 
            AND p.documenttypes = NEW.documenttypes
            LIMIT 1;  -- Only need one match
            
            IF v_patient_uuid IS NOT NULL THEN
                -- Found match by document - PRESERVE THE EXISTING UUID
                v_match_type := 'MATCHED_DOCUMENT';
                
                -- Update with HIS-specific data, preserve existing demographics
                IF NEW.source = 1 THEN
                    UPDATE patients
                    SET 
                        hisnumber_qms = NEW.hisnumber,
                        email_qms = NEW.email,
                        telephone_qms = NEW.telephone,
                        password_qms = NEW.his_password,
                        login_qms = NEW.login_email,
                        -- Use COALESCE to prefer existing data over new data
                        lastname = COALESCE(lastname, NEW.lastname),
                        name = COALESCE(name, NEW.name),
                        surname = COALESCE(surname, NEW.surname),
                        birthdate = COALESCE(birthdate, NEW.birthdate)
                    WHERE uuid = v_patient_uuid;
                ELSE
                    UPDATE patients
                    SET 
                        hisnumber_infoclinica = NEW.hisnumber,
                        email_infoclinica = NEW.email,
                        telephone_infoclinica = NEW.telephone,
                        password_infoclinica = NEW.his_password,
                        login_infoclinica = NEW.login_email,
                        -- Use COALESCE to prefer existing data over new data
                        lastname = COALESCE(lastname, NEW.lastname),
                        name = COALESCE(name, NEW.name),
                        surname = COALESCE(surname, NEW.surname),
                        birthdate = COALESCE(birthdate, NEW.birthdate)
                    WHERE uuid = v_patient_uuid;
                END IF;
            ELSE
                -- No match found, create new patient record
                v_match_type := 'NEW_WITH_DOCUMENT';
                
                INSERT INTO patients (
                    documenttypes, document_number,
                    lastname, name, surname, birthdate,
                    primary_source,
                    hisnumber_qms, email_qms, telephone_qms, password_qms, login_qms,
                    hisnumber_infoclinica, email_infoclinica, telephone_infoclinica, password_infoclinica, login_infoclinica
                ) 
                VALUES (
                    NEW.documenttypes, NEW.document_number,
                    NEW.lastname, NEW.name, NEW.surname, NEW.birthdate,
                    NEW.source,
                    CASE WHEN NEW.source = 1 THEN NEW.hisnumber END,
                    CASE WHEN NEW.source = 1 THEN NEW.email END,
                    CASE WHEN NEW.source = 1 THEN NEW.telephone END,
                    CASE WHEN NEW.source = 1 THEN NEW.his_password END,
                    CASE WHEN NEW.source = 1 THEN NEW.login_email END,
                    CASE WHEN NEW.source = 2 THEN NEW.hisnumber END,
                    CASE WHEN NEW.source = 2 THEN NEW.email END,
                    CASE WHEN NEW.source = 2 THEN NEW.telephone END,
                    CASE WHEN NEW.source = 2 THEN NEW.his_password END,
                    CASE WHEN NEW.source = 2 THEN NEW.login_email END
                )
                RETURNING uuid INTO v_patient_uuid;
            END IF;
        ELSE
            -- No document available, create new record
            INSERT INTO patients (
                lastname, name, surname, birthdate,
                primary_source,
                hisnumber_qms, email_qms, telephone_qms, password_qms, login_qms,
                hisnumber_infoclinica, email_infoclinica, telephone_infoclinica, password_infoclinica, login_infoclinica
            ) 
            VALUES (
                NEW.lastname, NEW.name, NEW.surname, NEW.birthdate,
                NEW.source,
                CASE WHEN NEW.source = 1 THEN NEW.hisnumber END,
                CASE WHEN NEW.source = 1 THEN NEW.email END,
                CASE WHEN NEW.source = 1 THEN NEW.telephone END,
                CASE WHEN NEW.source = 1 THEN NEW.his_password END,
                CASE WHEN NEW.source = 1 THEN NEW.login_email END,
                CASE WHEN NEW.source = 2 THEN NEW.hisnumber END,
                CASE WHEN NEW.source = 2 THEN NEW.email END,
                CASE WHEN NEW.source = 2 THEN NEW.telephone END,
                CASE WHEN NEW.source = 2 THEN NEW.his_password END,
                CASE WHEN NEW.source = 2 THEN NEW.login_email END
            )
            RETURNING uuid INTO v_patient_uuid;
        END IF;
    END IF;
    
    -- Set the UUID (either existing or newly created)
    NEW.uuid := v_patient_uuid;
    
    -- Simplified logging (batch this for better performance)
    INSERT INTO patient_matching_log (
        hisnumber, source, match_type, document_number, created_uuid
    ) VALUES (
        NEW.hisnumber, NEW.source, v_match_type, NEW.document_number, 
        CASE WHEN v_match_type LIKE 'NEW_%' THEN TRUE ELSE FALSE END
    );
    
    RETURN NEW;
END;$$ LANGUAGE plpgsql;

-- Optimized update function with better performance and UUID preservation
CREATE OR REPLACE FUNCTION update_patient_from_patientsdet()
RETURNS TRIGGER AS $$
DECLARE
    v_other_patient_uuid UUID;
    v_merged BOOLEAN := FALSE;
    v_doc_changed BOOLEAN := FALSE;
BEGIN
    -- Only process if we have a valid UUID
    IF NEW.uuid IS NULL THEN
        RETURN NEW;
    END IF;
    
    -- Quick check if document information has actually changed
    v_doc_changed := (OLD.document_number IS DISTINCT FROM NEW.document_number) OR 
                     (OLD.documenttypes IS DISTINCT FROM NEW.documenttypes);
    
    -- Only do expensive operations if document changed
    IF v_doc_changed AND NEW.document_number IS NOT NULL AND NEW.documenttypes IS NOT NULL THEN
        
        -- Fast lookup for existing patient with same document
        SELECT p.uuid INTO v_other_patient_uuid
        FROM patients p
        WHERE p.document_number = NEW.document_number 
        AND p.documenttypes = NEW.documenttypes
        AND p.uuid != NEW.uuid  -- Don't match with ourselves
        LIMIT 1;  -- Only need one match
        
        IF v_other_patient_uuid IS NOT NULL THEN
            -- MERGE THE TWO PATIENTS - PRESERVE THE OLDER UUID (lower value)
            DECLARE
                v_target_uuid UUID;
                v_source_uuid UUID;
            BEGIN
                -- Choose the UUID to keep (prefer the older/smaller one for consistency)
                IF v_other_patient_uuid < NEW.uuid THEN
                    v_target_uuid := v_other_patient_uuid;
                    v_source_uuid := NEW.uuid;
                ELSE
                    v_target_uuid := NEW.uuid;
                    v_source_uuid := v_other_patient_uuid;
                END IF;
                
                -- Update the target patient with data from both records
                IF NEW.source = 1 THEN
                    UPDATE patients
                    SET 
                        hisnumber_qms = NEW.hisnumber,
                        email_qms = NEW.email,
                        telephone_qms = NEW.telephone,
                        password_qms = NEW.his_password,
                        login_qms = NEW.login_email,
                        lastname = COALESCE(lastname, NEW.lastname),
                        name = COALESCE(name, NEW.name),
                        surname = COALESCE(surname, NEW.surname),
                        birthdate = COALESCE(birthdate, NEW.birthdate)
                    WHERE uuid = v_target_uuid;
                    
                    -- Transfer Infoclinica data from source record if target doesn't have it
                    UPDATE patients p1
                    SET
                        hisnumber_infoclinica = COALESCE(p1.hisnumber_infoclinica, p2.hisnumber_infoclinica),
                        email_infoclinica = COALESCE(p1.email_infoclinica, p2.email_infoclinica),
                        telephone_infoclinica = COALESCE(p1.telephone_infoclinica, p2.telephone_infoclinica),
                        password_infoclinica = COALESCE(p1.password_infoclinica, p2.password_infoclinica),
                        login_infoclinica = COALESCE(p1.login_infoclinica, p2.login_infoclinica)
                    FROM patients p2
                    WHERE p1.uuid = v_target_uuid AND p2.uuid = v_source_uuid;
                ELSE
                    UPDATE patients
                    SET 
                        hisnumber_infoclinica = NEW.hisnumber,
                        email_infoclinica = NEW.email,
                        telephone_infoclinica = NEW.telephone,
                        password_infoclinica = NEW.his_password,
                        login_infoclinica = NEW.login_email,
                        lastname = COALESCE(lastname, NEW.lastname),
                        name = COALESCE(name, NEW.name),
                        surname = COALESCE(surname, NEW.surname),
                        birthdate = COALESCE(birthdate, NEW.birthdate)
                    WHERE uuid = v_target_uuid;
                    
                    -- Transfer qMS data from source record if target doesn't have it
                    UPDATE patients p1
                    SET
                        hisnumber_qms = COALESCE(p1.hisnumber_qms, p2.hisnumber_qms),
                        email_qms = COALESCE(p1.email_qms, p2.email_qms),
                        telephone_qms = COALESCE(p1.telephone_qms, p2.telephone_qms),
                        password_qms = COALESCE(p1.password_qms, p2.password_qms),
                        login_qms = COALESCE(p1.login_qms, p2.login_qms)
                    FROM patients p2
                    WHERE p1.uuid = v_target_uuid AND p2.uuid = v_source_uuid;
                END IF;
                
                -- Update all references to point to the target UUID
                UPDATE patientsdet SET uuid = v_target_uuid WHERE uuid = v_source_uuid;
                
                -- Update protocols if any exist
                UPDATE protocols SET uuid = v_target_uuid WHERE uuid = v_source_uuid;
                
                -- Delete the source patient record
                DELETE FROM patients WHERE uuid = v_source_uuid;
                
                -- Update current record to use target UUID
                NEW.uuid := v_target_uuid;
                v_merged := TRUE;
                
                -- Log the merge
                INSERT INTO patient_matching_log (hisnumber, source, match_type, document_number, created_uuid)
                VALUES (NEW.hisnumber, NEW.source, 'MERGED_ON_UPDATE', NEW.document_number, FALSE);
            END;
        ELSE
            -- No existing patient with this document, just update current patient
            UPDATE patients
            SET documenttypes = NEW.documenttypes, document_number = NEW.document_number
            WHERE uuid = NEW.uuid;
        END IF;
    END IF;
    
    -- Regular update (only if we didn't merge)
    IF NOT v_merged THEN
        -- Single optimized update statement
        IF NEW.source = 1 THEN
            UPDATE patients
            SET 
                lastname = NEW.lastname, name = NEW.name, surname = NEW.surname, birthdate = NEW.birthdate,
                documenttypes = NEW.documenttypes, document_number = NEW.document_number,
                hisnumber_qms = NEW.hisnumber, email_qms = NEW.email, telephone_qms = NEW.telephone,
                password_qms = NEW.his_password, login_qms = NEW.login_email
            WHERE uuid = NEW.uuid;
        ELSE
            UPDATE patients
            SET 
                lastname = NEW.lastname, name = NEW.name, surname = NEW.surname, birthdate = NEW.birthdate,
                documenttypes = NEW.documenttypes, document_number = NEW.document_number,
                hisnumber_infoclinica = NEW.hisnumber, email_infoclinica = NEW.email, telephone_infoclinica = NEW.telephone,
                password_infoclinica = NEW.his_password, login_infoclinica = NEW.login_email
            WHERE uuid = NEW.uuid;
        END IF;
        
        -- Simplified logging (only log if document actually changed)
        IF v_doc_changed THEN
            INSERT INTO patient_matching_log (hisnumber, source, match_type, document_number, created_uuid)
            VALUES (NEW.hisnumber, NEW.source, 'REGULAR_UPDATE', NEW.document_number, FALSE);
        END IF;
    END IF;
    
    RETURN NEW;
END;$$ LANGUAGE plpgsql;

-- Create optimized triggers
DROP TRIGGER IF EXISTS trg_process_new_patient ON patientsdet;
CREATE TRIGGER trg_process_new_patient
    BEFORE INSERT ON patientsdet
    FOR EACH ROW
    EXECUTE FUNCTION process_new_patient();

DROP TRIGGER IF EXISTS trg_update_patient ON patientsdet;
CREATE TRIGGER trg_update_patient
    AFTER UPDATE ON patientsdet
    FOR EACH ROW
    EXECUTE FUNCTION update_patient_from_patientsdet();

-- Performance monitoring view (fixed for PostgreSQL compatibility)
CREATE OR REPLACE VIEW trigger_performance_stats AS
SELECT 
    schemaname,
    relname as tablename,
    n_tup_ins as inserts,
    n_tup_upd as updates,
    n_tup_del as deletes,
    seq_scan,
    seq_tup_read,
    idx_scan,
    idx_tup_fetch
FROM pg_stat_user_tables 
WHERE relname IN ('patients', 'patientsdet', 'patient_matching_log');