-- Enhanced patient matching trigger functions with mobile app user support
-- This file contains only functions and triggers, no indexes

-- Function to find mobile app user by HIS number
CREATE OR REPLACE FUNCTION find_mobile_app_user(p_hisnumber VARCHAR, p_source SMALLINT)
RETURNS UUID AS $$DECLARE
    v_mobile_uuid UUID;
BEGIN
    IF p_source = 1 THEN  -- qMS
        SELECT uuid INTO v_mobile_uuid
        FROM mobile_app_users
        WHERE hisnumber_qms = p_hisnumber
        LIMIT 1;
    ELSIF p_source = 2 THEN  -- Infoclinica
        SELECT uuid INTO v_mobile_uuid
        FROM mobile_app_users
        WHERE hisnumber_infoclinica = p_hisnumber
        LIMIT 1;
    END IF;
    
    RETURN v_mobile_uuid;
END;$$ LANGUAGE plpgsql;

-- Function to update mobile app user with new HIS number
CREATE OR REPLACE FUNCTION update_mobile_app_user(p_uuid UUID, p_hisnumber VARCHAR, p_source SMALLINT)
RETURNS BOOLEAN AS $$BEGIN
    IF p_source = 1 THEN  -- qMS
        UPDATE mobile_app_users
        SET hisnumber_qms = p_hisnumber,
            updated_at = CURRENT_TIMESTAMP
        WHERE uuid = p_uuid;
    ELSIF p_source = 2 THEN  -- Infoclinica
        UPDATE mobile_app_users
        SET hisnumber_infoclinica = p_hisnumber,
            updated_at = CURRENT_TIMESTAMP
        WHERE uuid = p_uuid;
    END IF;
    
    RETURN FOUND;
END;$$ LANGUAGE plpgsql;

-- Enhanced function for new patient processing with mobile app support
CREATE OR REPLACE FUNCTION process_new_patient()
RETURNS TRIGGER AS $$DECLARE
    v_patient_uuid UUID;
    v_existing_patient UUID;
    v_mobile_app_uuid UUID;
    v_match_type TEXT := 'NEW_NO_DOCUMENT';
    v_is_mobile_match BOOLEAN := FALSE;
    v_existing_mobile_patient UUID;
BEGIN
    -- Skip processing if UUID is already provided
    IF NEW.uuid IS NOT NULL THEN
        RETURN NEW;
    END IF;
    
    -- PROTECTION: Check if matching is locked for existing patients
    -- This prevents re-matching of already processed patients
    
    -- FIRST PRIORITY: Check for mobile app user registration
    v_mobile_app_uuid := find_mobile_app_user(NEW.hisnumber, NEW.source);
    
    IF v_mobile_app_uuid IS NOT NULL THEN
        -- Found mobile app registration, check if patient already exists
        SELECT uuid INTO v_existing_mobile_patient
        FROM patients p
        WHERE p.uuid = v_mobile_app_uuid
        AND p.matching_locked = FALSE  -- Only process unlocked patients
        LIMIT 1;
        
        IF v_existing_mobile_patient IS NOT NULL THEN
            -- Update existing mobile app patient
            v_patient_uuid := v_existing_mobile_patient;
            v_match_type := 'MOBILE_APP_UPDATE';
            v_is_mobile_match := TRUE;
            
            -- Update the existing patient with HIS-specific data
            IF NEW.source = 1 THEN
                UPDATE patients
                SET 
                    hisnumber_qms = NEW.hisnumber,
                    email_qms = NEW.email,
                    telephone_qms = NEW.telephone,
                    password_qms = NEW.his_password,
                    login_qms = NEW.login_email,
                    -- Update demographics only if they're currently NULL (preserve existing data)
                    lastname = CASE WHEN lastname IS NULL THEN NEW.lastname ELSE lastname END,
                    name = CASE WHEN name IS NULL THEN NEW.name ELSE name END,
                    surname = CASE WHEN surname IS NULL THEN NEW.surname ELSE surname END,
                    birthdate = CASE WHEN birthdate IS NULL THEN NEW.birthdate ELSE birthdate END,
                    documenttypes = CASE WHEN documenttypes IS NULL THEN NEW.documenttypes ELSE documenttypes END,
                    document_number = CASE WHEN document_number IS NULL THEN NEW.document_number ELSE document_number END,
                    primary_source = CASE WHEN primary_source IS NULL THEN NEW.source ELSE primary_source END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE uuid = v_patient_uuid;
            ELSE
                UPDATE patients
                SET 
                    hisnumber_infoclinica = NEW.hisnumber,
                    email_infoclinica = NEW.email,
                    telephone_infoclinica = NEW.telephone,
                    password_infoclinica = NEW.his_password,
                    login_infoclinica = NEW.login_email,
                    -- Update demographics only if they're currently NULL (preserve existing data)
                    lastname = CASE WHEN lastname IS NULL THEN NEW.lastname ELSE lastname END,
                    name = CASE WHEN name IS NULL THEN NEW.name ELSE name END,
                    surname = CASE WHEN surname IS NULL THEN NEW.surname ELSE surname END,
                    birthdate = CASE WHEN birthdate IS NULL THEN NEW.birthdate ELSE birthdate END,
                    documenttypes = CASE WHEN documenttypes IS NULL THEN NEW.documenttypes ELSE documenttypes END,
                    document_number = CASE WHEN document_number IS NULL THEN NEW.document_number ELSE document_number END,
                    primary_source = CASE WHEN primary_source IS NULL THEN NEW.source ELSE primary_source END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE uuid = v_patient_uuid;
            END IF;
        ELSE
            -- Create new patient for mobile app user
            v_match_type := 'MOBILE_APP_NEW';
            v_is_mobile_match := TRUE;
            
            INSERT INTO patients (
                uuid, -- Use the mobile app UUID
                documenttypes, document_number,
                lastname, name, surname, birthdate,
                primary_source, registered_via_mobile,
                hisnumber_qms, email_qms, telephone_qms, password_qms, login_qms,
                hisnumber_infoclinica, email_infoclinica, telephone_infoclinica, password_infoclinica, login_infoclinica
            ) 
            VALUES (
                v_mobile_app_uuid, -- Use mobile app UUID
                NEW.documenttypes, NEW.document_number,
                NEW.lastname, NEW.name, NEW.surname, NEW.birthdate,
                NEW.source, TRUE,
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
            );
            
            v_patient_uuid := v_mobile_app_uuid;
        END IF;
    ELSE
        -- SECOND PRIORITY: Quick check for existing patient with same HIS number and source
        -- Use the optimized indexes for fast lookup
        IF NEW.source = 1 THEN
            SELECT p.uuid INTO v_existing_patient
            FROM patients p
            WHERE p.hisnumber_qms = NEW.hisnumber
            AND p.matching_locked = FALSE  -- Only process unlocked patients
            LIMIT 1;
        ELSIF NEW.source = 2 THEN
            SELECT p.uuid INTO v_existing_patient
            FROM patients p
            WHERE p.hisnumber_infoclinica = NEW.hisnumber
            AND p.matching_locked = FALSE  -- Only process unlocked patients
            LIMIT 1;
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
                    document_number = CASE WHEN document_number IS NULL THEN NEW.document_number ELSE document_number END,
                    updated_at = CURRENT_TIMESTAMP
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
                    document_number = CASE WHEN document_number IS NULL THEN NEW.document_number ELSE document_number END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE uuid = v_existing_patient;
            END IF;
        ELSE
            -- THIRD PRIORITY: Look for existing patient with matching document (only if we have document info)
            IF NEW.document_number IS NOT NULL AND NEW.documenttypes IS NOT NULL THEN
                SELECT p.uuid INTO v_patient_uuid
                FROM patients p
                WHERE p.document_number = NEW.document_number 
                AND p.documenttypes = NEW.documenttypes
                AND p.matching_locked = FALSE  -- Only process unlocked patients
                LIMIT 1;
                
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
                            birthdate = COALESCE(birthdate, NEW.birthdate),
                            primary_source = COALESCE(primary_source, NEW.source),
                            updated_at = CURRENT_TIMESTAMP
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
                            birthdate = COALESCE(birthdate, NEW.birthdate),
                            primary_source = COALESCE(primary_source, NEW.source),
                            updated_at = CURRENT_TIMESTAMP
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
    END IF;
    
    -- Set the UUID (either existing or newly created)
    NEW.uuid := v_patient_uuid;
    NEW.processed_at := CURRENT_TIMESTAMP;
    
    -- Enhanced logging
    INSERT INTO patient_matching_log (
        hisnumber, source, match_type, document_number, created_uuid,
        mobile_app_uuid, matched_patient_uuid,
        details
    ) VALUES (
        NEW.hisnumber, NEW.source, v_match_type, NEW.document_number, 
        CASE WHEN v_match_type LIKE 'NEW_%' OR v_match_type = 'MOBILE_APP_NEW' THEN TRUE ELSE FALSE END,
        CASE WHEN v_is_mobile_match THEN v_mobile_app_uuid ELSE NULL END,
        v_patient_uuid,
        jsonb_build_object(
            'is_mobile_match', v_is_mobile_match,
            'hisnumber', NEW.hisnumber,
            'source', NEW.source,
            'has_document', (NEW.document_number IS NOT NULL AND NEW.documenttypes IS NOT NULL)
        )
    );
    
    RETURN NEW;
END;$$ LANGUAGE plpgsql;

-- Enhanced update function with mobile app support and better performance
CREATE OR REPLACE FUNCTION update_patient_from_patientsdet()
RETURNS TRIGGER AS $$
DECLARE
    v_other_patient_uuid UUID;
    v_merged BOOLEAN := FALSE;
    v_doc_changed BOOLEAN := FALSE;
    v_is_locked BOOLEAN := FALSE;
BEGIN
    -- Only process if we have a valid UUID
    IF NEW.uuid IS NULL THEN
        RETURN NEW;
    END IF;
    
    -- Check if patient matching is locked
    SELECT matching_locked INTO v_is_locked
    FROM patients
    WHERE uuid = NEW.uuid;
    
    IF v_is_locked THEN
        -- Patient is locked from re-matching, only update the patientsdet record
        NEW.processed_at := CURRENT_TIMESTAMP;
        RETURN NEW;
    END IF;
    
    -- Quick check if document information has actually changed
    v_doc_changed := (OLD.document_number IS DISTINCT FROM NEW.document_number) OR 
                     (OLD.documenttypes IS DISTINCT FROM NEW.documenttypes);
    
    -- Only do expensive operations if document changed and we have document info
    IF v_doc_changed AND NEW.document_number IS NOT NULL AND NEW.documenttypes IS NOT NULL THEN
        
        -- Fast lookup for existing patient with same document
        SELECT p.uuid INTO v_other_patient_uuid
        FROM patients p
        WHERE p.document_number = NEW.document_number 
        AND p.documenttypes = NEW.documenttypes
        AND p.uuid != NEW.uuid  -- Don't match with ourselves
        AND p.matching_locked = FALSE  -- Only consider unlocked patients
        LIMIT 1;
        
        IF v_other_patient_uuid IS NOT NULL THEN
            -- MERGE THE TWO PATIENTS - PRESERVE THE OLDER UUID (lower value)
            DECLARE
                v_target_uuid UUID;
                v_source_uuid UUID;
                v_target_is_mobile BOOLEAN;
                v_source_is_mobile BOOLEAN;
            BEGIN
                -- Get mobile app status for both patients
                SELECT registered_via_mobile INTO v_target_is_mobile
                FROM patients WHERE uuid = v_other_patient_uuid;
                
                SELECT registered_via_mobile INTO v_source_is_mobile
                FROM patients WHERE uuid = NEW.uuid;
                
                -- Choose the UUID to keep (prefer mobile app registrations, then older UUID)
                IF v_target_is_mobile AND NOT v_source_is_mobile THEN
                    v_target_uuid := v_other_patient_uuid;
                    v_source_uuid := NEW.uuid;
                ELSIF v_source_is_mobile AND NOT v_target_is_mobile THEN
                    v_target_uuid := NEW.uuid;
                    v_source_uuid := v_other_patient_uuid;
                ELSIF v_other_patient_uuid < NEW.uuid THEN
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
                        birthdate = COALESCE(birthdate, NEW.birthdate),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE uuid = v_target_uuid;
                    
                    -- Transfer Infoclinica data from source record if target doesn't have it
                    UPDATE patients p1
                    SET
                        hisnumber_infoclinica = COALESCE(p1.hisnumber_infoclinica, p2.hisnumber_infoclinica),
                        email_infoclinica = COALESCE(p1.email_infoclinica, p2.email_infoclinica),
                        telephone_infoclinica = COALESCE(p1.telephone_infoclinica, p2.telephone_infoclinica),
                        password_infoclinica = COALESCE(p1.password_infoclinica, p2.password_infoclinica),
                        login_infoclinica = COALESCE(p1.login_infoclinica, p2.login_infoclinica),
                        registered_via_mobile = p1.registered_via_mobile OR p2.registered_via_mobile,
                        updated_at = CURRENT_TIMESTAMP
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
                        birthdate = COALESCE(birthdate, NEW.birthdate),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE uuid = v_target_uuid;
                    
                    -- Transfer qMS data from source record if target doesn't have it
                    UPDATE patients p1
                    SET
                        hisnumber_qms = COALESCE(p1.hisnumber_qms, p2.hisnumber_qms),
                        email_qms = COALESCE(p1.email_qms, p2.email_qms),
                        telephone_qms = COALESCE(p1.telephone_qms, p2.telephone_qms),
                        password_qms = COALESCE(p1.password_qms, p2.password_qms),
                        login_qms = COALESCE(p1.login_qms, p2.login_qms),
                        registered_via_mobile = p1.registered_via_mobile OR p2.registered_via_mobile,
                        updated_at = CURRENT_TIMESTAMP
                    FROM patients p2
                    WHERE p1.uuid = v_target_uuid AND p2.uuid = v_source_uuid;
                END IF;
                
                -- Update mobile_app_users table if needed
                UPDATE mobile_app_users 
                SET uuid = v_target_uuid,
                    updated_at = CURRENT_TIMESTAMP
                WHERE uuid = v_source_uuid;
                
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
                INSERT INTO patient_matching_log (
                    hisnumber, source, match_type, document_number, created_uuid,
                    matched_patient_uuid, details
                )
                VALUES (
                    NEW.hisnumber, NEW.source, 'MERGED_ON_UPDATE', NEW.document_number, FALSE,
                    v_target_uuid,
                    jsonb_build_object(
                        'merged_from', v_source_uuid,
                        'merged_to', v_target_uuid,
                        'document_based_merge', true
                    )
                );
            END;
        ELSE
            -- No existing patient with this document, just update current patient
            UPDATE patients
            SET 
                documenttypes = NEW.documenttypes, 
                document_number = NEW.document_number,
                updated_at = CURRENT_TIMESTAMP
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
                password_qms = NEW.his_password, login_qms = NEW.login_email,
                updated_at = CURRENT_TIMESTAMP
            WHERE uuid = NEW.uuid;
        ELSE
            UPDATE patients
            SET 
                lastname = NEW.lastname, name = NEW.name, surname = NEW.surname, birthdate = NEW.birthdate,
                documenttypes = NEW.documenttypes, document_number = NEW.document_number,
                hisnumber_infoclinica = NEW.hisnumber, email_infoclinica = NEW.email, telephone_infoclinica = NEW.telephone,
                password_infoclinica = NEW.his_password, login_infoclinica = NEW.login_email,
                updated_at = CURRENT_TIMESTAMP
            WHERE uuid = NEW.uuid;
        END IF;
        
        -- Simplified logging (only log if document actually changed)
        IF v_doc_changed THEN
            INSERT INTO patient_matching_log (
                hisnumber, source, match_type, document_number, created_uuid,
                matched_patient_uuid, details
            )
            VALUES (
                NEW.hisnumber, NEW.source, 'REGULAR_UPDATE', NEW.document_number, FALSE,
                NEW.uuid,
                jsonb_build_object('document_changed', true)
            );
        END IF;
    END IF;
    
    -- Update processed timestamp
    NEW.processed_at := CURRENT_TIMESTAMP;
    
    RETURN NEW;
END;$$ LANGUAGE plpgsql;

-- Function to lock patient from further matching
CREATE OR REPLACE FUNCTION lock_patient_matching(p_uuid UUID, p_reason TEXT DEFAULT 'Manual lock')
RETURNS BOOLEAN AS $$
BEGIN
    UPDATE patients
    SET 
        matching_locked = TRUE,
        matching_locked_at = CURRENT_TIMESTAMP,
        matching_locked_reason = p_reason,
        updated_at = CURRENT_TIMESTAMP
    WHERE uuid = p_uuid;
    
    RETURN FOUND;
END;$$ LANGUAGE plpgsql;

-- Function to unlock patient matching
CREATE OR REPLACE FUNCTION unlock_patient_matching(p_uuid UUID)
RETURNS BOOLEAN AS $$BEGIN
    UPDATE patients
    SET 
        matching_locked = FALSE,
        matching_locked_at = NULL,
        matching_locked_reason = NULL,
        updated_at = CURRENT_TIMESTAMP
    WHERE uuid = p_uuid;
    
    RETURN FOUND;
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

-- Performance monitoring view (enhanced for mobile app support)
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
WHERE relname IN ('patients', 'patientsdet', 'patient_matching_log', 'mobile_app_users');

-- View for mobile app matching statistics
CREATE OR REPLACE VIEW mobile_app_matching_stats AS
SELECT 
    COUNT(*) as total_mobile_users,
    COUNT(CASE WHEN hisnumber_qms IS NOT NULL AND hisnumber_infoclinica IS NOT NULL THEN 1 END) as both_his_registered,
    COUNT(CASE WHEN hisnumber_qms IS NOT NULL AND hisnumber_infoclinica IS NULL THEN 1 END) as qms_only,
    COUNT(CASE WHEN hisnumber_qms IS NULL AND hisnumber_infoclinica IS NOT NULL THEN 1 END) as infoclinica_only
FROM mobile_app_users;

-- View for patient matching statistics
CREATE OR REPLACE VIEW patient_matching_stats AS
SELECT 
    match_type,
    COUNT(*) as count,
    COUNT(CASE WHEN created_uuid THEN 1 END) as new_patients_created,
    COUNT(CASE WHEN mobile_app_uuid IS NOT NULL THEN 1 END) as mobile_app_matches
FROM patient_matching_log
GROUP BY match_type
ORDER BY count DESC;