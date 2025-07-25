-- Function called by trigger when a new patient is inserted
CREATE OR REPLACE FUNCTION process_new_patient()
RETURNS TRIGGER AS $$
DECLARE
    v_patient_uuid UUID;
    v_his_name VARCHAR(255);
    v_existing_patient UUID;
BEGIN
    -- Skip processing if UUID is already provided
    IF NEW.uuid IS NOT NULL THEN
        RETURN NEW;
    END IF;
    
    -- Get HIS name
    SELECT name INTO v_his_name FROM hislist WHERE id = NEW.source;
    
    -- Debug logging to see what's happening
    RAISE NOTICE 'Processing patient: hisnumber=%, source=%, his_name=%, document_number=%, document_type=%', 
        NEW.hisnumber, NEW.source, v_his_name, NEW.document_number, NEW.documenttypes;
    
    -- FIRST: Check if we already have a patient record with same HIS number and source
    -- This prevents duplicate patients in the patients table
    SELECT p.uuid INTO v_existing_patient
    FROM patients p
    WHERE (
        (NEW.source = 1 AND p.hisnumber_qms = NEW.hisnumber) OR
        (NEW.source = 2 AND p.hisnumber_infoclinica = NEW.hisnumber)
    );
    
    IF v_existing_patient IS NOT NULL THEN
        -- Patient already exists for this HIS, just update and use existing UUID
        RAISE NOTICE 'Found existing patient with same HIS number, UUID: %', v_existing_patient;
        
        IF NEW.source = 1 THEN  -- qMS
            UPDATE patients
            SET 
                hisnumber_qms = NEW.hisnumber,
                email_qms = NEW.email,
                telephone_qms = NEW.telephone,
                password_qms = NEW.his_password,
                login_qms = NEW.login_email,
                lastname = COALESCE(NEW.lastname, lastname),
                name = COALESCE(NEW.name, name),
                surname = COALESCE(NEW.surname, surname),
                birthdate = COALESCE(NEW.birthdate, birthdate),
                documenttypes = COALESCE(NEW.documenttypes, documenttypes),
                document_number = COALESCE(NEW.document_number, document_number)
            WHERE uuid = v_existing_patient;
        ELSIF NEW.source = 2 THEN  -- Инфоклиника
            UPDATE patients
            SET 
                hisnumber_infoclinica = NEW.hisnumber,
                email_infoclinica = NEW.email,
                telephone_infoclinica = NEW.telephone,
                password_infoclinica = NEW.his_password,
                login_infoclinica = NEW.login_email,
                lastname = COALESCE(NEW.lastname, lastname),
                name = COALESCE(NEW.name, name),
                surname = COALESCE(NEW.surname, surname),
                birthdate = COALESCE(NEW.birthdate, birthdate),
                documenttypes = COALESCE(NEW.documenttypes, documenttypes),
                document_number = COALESCE(NEW.document_number, document_number)
            WHERE uuid = v_existing_patient;
        END IF;
        
        NEW.uuid := v_existing_patient;
        
        INSERT INTO patient_matching_log (
            hisnumber, source, match_type, document_number, created_uuid
        ) VALUES (
            NEW.hisnumber, NEW.source, 'UPDATED_EXISTING', NEW.document_number, FALSE
        );
        
        RETURN NEW;
    END IF;
    
    -- SECOND: Look for existing patient with matching document if document is provided
    IF NEW.document_number IS NOT NULL AND NEW.documenttypes IS NOT NULL THEN
        SELECT p.uuid
        INTO v_patient_uuid
        FROM patients p
        WHERE p.document_number = NEW.document_number 
        AND p.documenttypes = NEW.documenttypes;
        
        IF v_patient_uuid IS NOT NULL THEN
            -- Found match by document
            RAISE NOTICE 'Found existing patient with matching document, UUID: %', v_patient_uuid;
            
            IF NEW.source = 1 THEN  -- qMS
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
                WHERE uuid = v_patient_uuid;
            ELSIF NEW.source = 2 THEN  -- Инфоклиника
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
                WHERE uuid = v_patient_uuid;
            END IF;
            
            INSERT INTO patient_matching_log (
                hisnumber, source, match_type, document_number, created_uuid
            ) VALUES (
                NEW.hisnumber, NEW.source, 'MATCHED_DOCUMENT', NEW.document_number, FALSE
            );
        ELSE
            -- No match found, create new patient record
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
                CASE WHEN NEW.source = 1 THEN NEW.hisnumber ELSE NULL END,
                CASE WHEN NEW.source = 1 THEN NEW.email ELSE NULL END,
                CASE WHEN NEW.source = 1 THEN NEW.telephone ELSE NULL END,
                CASE WHEN NEW.source = 1 THEN NEW.his_password ELSE NULL END,
                CASE WHEN NEW.source = 1 THEN NEW.login_email ELSE NULL END,
                CASE WHEN NEW.source = 2 THEN NEW.hisnumber ELSE NULL END,
                CASE WHEN NEW.source = 2 THEN NEW.email ELSE NULL END,
                CASE WHEN NEW.source = 2 THEN NEW.telephone ELSE NULL END,
                CASE WHEN NEW.source = 2 THEN NEW.his_password ELSE NULL END,
                CASE WHEN NEW.source = 2 THEN NEW.login_email ELSE NULL END
            )
            RETURNING uuid INTO v_patient_uuid;
            
            INSERT INTO patient_matching_log (
                hisnumber, source, match_type, document_number, created_uuid
            ) VALUES (
                NEW.hisnumber, NEW.source, 'NEW_WITH_DOCUMENT', NEW.document_number, TRUE
            );
        END IF;
    ELSE
        -- No document available, create new record
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
            CASE WHEN NEW.source = 1 THEN NEW.hisnumber ELSE NULL END,
            CASE WHEN NEW.source = 1 THEN NEW.email ELSE NULL END,
            CASE WHEN NEW.source = 1 THEN NEW.telephone ELSE NULL END,
            CASE WHEN NEW.source = 1 THEN NEW.his_password ELSE NULL END,
            CASE WHEN NEW.source = 1 THEN NEW.login_email ELSE NULL END,
            CASE WHEN NEW.source = 2 THEN NEW.hisnumber ELSE NULL END,
            CASE WHEN NEW.source = 2 THEN NEW.email ELSE NULL END,
            CASE WHEN NEW.source = 2 THEN NEW.telephone ELSE NULL END,
            CASE WHEN NEW.source = 2 THEN NEW.his_password ELSE NULL END,
            CASE WHEN NEW.source = 2 THEN NEW.login_email ELSE NULL END
        )
        RETURNING uuid INTO v_patient_uuid;
        
        INSERT INTO patient_matching_log (
            hisnumber, source, match_type, document_number, created_uuid
        ) VALUES (
            NEW.hisnumber, NEW.source, 'NEW_NO_DOCUMENT', NULL, TRUE
        );
    END IF;
    
    NEW.uuid := v_patient_uuid;
    RETURN NEW;
END;$$ LANGUAGE plpgsql;

-- Function to handle updates to patientsdet - THIS IS THE KEY FIX
CREATE OR REPLACE FUNCTION update_patient_from_patientsdet()
RETURNS TRIGGER AS $$
DECLARE
    v_his_name VARCHAR(255);
    v_other_patient_uuid UUID;
    v_merged BOOLEAN := FALSE;
BEGIN
    -- Only process if we have a valid UUID
    IF NEW.uuid IS NULL THEN
        RETURN NEW;
    END IF;
    
    -- Get HIS name
    SELECT name INTO v_his_name FROM hislist WHERE id = NEW.source;
    
    RAISE NOTICE 'UPDATE: Processing patient update: hisnumber=%, source=%, old_doc=% -> new_doc=%, old_type=% -> new_type=%', 
        NEW.hisnumber, NEW.source, OLD.document_number, NEW.document_number, OLD.documenttypes, NEW.documenttypes;
    
    -- KEY LOGIC: Check if document information has changed
    IF (OLD.document_number IS DISTINCT FROM NEW.document_number) OR 
       (OLD.documenttypes IS DISTINCT FROM NEW.documenttypes) THEN
        
        RAISE NOTICE 'Document information changed, checking for existing matches...';
        
        -- If we now have valid document info, check if another patient exists with this document
        IF NEW.document_number IS NOT NULL AND NEW.documenttypes IS NOT NULL THEN
            SELECT p.uuid 
            INTO v_other_patient_uuid
            FROM patients p
            WHERE p.document_number = NEW.document_number 
            AND p.documenttypes = NEW.documenttypes
            AND p.uuid <> OLD.uuid;  -- Don't match with ourselves
            
            IF v_other_patient_uuid IS NOT NULL THEN
                RAISE NOTICE 'Found another patient with same document! Merging UUID % into UUID %', OLD.uuid, v_other_patient_uuid;
                
                -- MERGE THE TWO PATIENTS
                -- First, update the target patient with HIS-specific information from current patient
                IF NEW.source = 1 THEN  -- qMS
                    UPDATE patients
                    SET 
                        hisnumber_qms = NEW.hisnumber,
                        email_qms = NEW.email,
                        telephone_qms = NEW.telephone,
                        password_qms = NEW.his_password,
                        login_qms = NEW.login_email,
                        -- Keep the best demographic data
                        lastname = COALESCE(lastname, NEW.lastname),
                        name = COALESCE(name, NEW.name),
                        surname = COALESCE(surname, NEW.surname),
                        birthdate = COALESCE(birthdate, NEW.birthdate)
                    WHERE uuid = v_other_patient_uuid;
                    
                    -- Transfer any Infoclinica data from the old record to the merged one
                    UPDATE patients p1
                    SET
                        hisnumber_infoclinica = COALESCE(p1.hisnumber_infoclinica, p2.hisnumber_infoclinica),
                        email_infoclinica = COALESCE(p1.email_infoclinica, p2.email_infoclinica),
                        telephone_infoclinica = COALESCE(p1.telephone_infoclinica, p2.telephone_infoclinica),
                        password_infoclinica = COALESCE(p1.password_infoclinica, p2.password_infoclinica),
                        login_infoclinica = COALESCE(p1.login_infoclinica, p2.login_infoclinica)
                    FROM patients p2
                    WHERE p1.uuid = v_other_patient_uuid AND p2.uuid = OLD.uuid;
                    
                ELSIF NEW.source = 2 THEN  -- Инфоклиника
                    UPDATE patients
                    SET 
                        hisnumber_infoclinica = NEW.hisnumber,
                        email_infoclinica = NEW.email,
                        telephone_infoclinica = NEW.telephone,
                        password_infoclinica = NEW.his_password,
                        login_infoclinica = NEW.login_email,
                        -- Keep the best demographic data
                        lastname = COALESCE(lastname, NEW.lastname),
                        name = COALESCE(name, NEW.name),
                        surname = COALESCE(surname, NEW.surname),
                        birthdate = COALESCE(birthdate, NEW.birthdate)
                    WHERE uuid = v_other_patient_uuid;
                    
                    -- Transfer any qMS data from the old record to the merged one
                    UPDATE patients p1
                    SET
                        hisnumber_qms = COALESCE(p1.hisnumber_qms, p2.hisnumber_qms),
                        email_qms = COALESCE(p1.email_qms, p2.email_qms),
                        telephone_qms = COALESCE(p1.telephone_qms, p2.telephone_qms),
                        password_qms = COALESCE(p1.password_qms, p2.password_qms),
                        login_qms = COALESCE(p1.login_qms, p2.login_qms)
                    FROM patients p2
                    WHERE p1.uuid = v_other_patient_uuid AND p2.uuid = OLD.uuid;
                END IF;
                
                -- Update ALL patientsdet records that reference the old uuid
                UPDATE patientsdet
                SET uuid = v_other_patient_uuid
                WHERE uuid = OLD.uuid;
                
                -- Update protocols if any
                UPDATE protocols
                SET uuid = v_other_patient_uuid
                WHERE uuid = OLD.uuid;
                
                -- Delete the old patient record (it's been merged)
                DELETE FROM patients
                WHERE uuid = OLD.uuid;
                
                -- Update the current record's uuid to point to the merged patient
                NEW.uuid := v_other_patient_uuid;
                
                -- Log the merge
                INSERT INTO patient_matching_log (
                    hisnumber, source, match_type, document_number, created_uuid
                ) VALUES (
                    NEW.hisnumber, NEW.source, 'MERGED_ON_UPDATE', NEW.document_number, FALSE
                );
                
                v_merged := TRUE;
                RAISE NOTICE 'Successfully merged patients!';
            ELSE
                -- No existing patient with this document, just update our current patient
                RAISE NOTICE 'No existing patient found with document %, updating current patient', NEW.document_number;
                UPDATE patients
                SET 
                    documenttypes = NEW.documenttypes,
                    document_number = NEW.document_number
                WHERE uuid = OLD.uuid;
            END IF;
        END IF;
    END IF;
    
    -- If we didn't merge, perform a regular update of the patient record
    IF NOT v_merged THEN
        IF NEW.source = 1 THEN  -- qMS
            UPDATE patients
            SET 
                lastname = NEW.lastname,
                name = NEW.name,
                surname = NEW.surname,
                birthdate = NEW.birthdate,
                documenttypes = NEW.documenttypes,
                document_number = NEW.document_number,
                hisnumber_qms = NEW.hisnumber,
                email_qms = NEW.email,
                telephone_qms = NEW.telephone,
                password_qms = NEW.his_password,
                login_qms = NEW.login_email
            WHERE uuid = NEW.uuid;
        ELSIF NEW.source = 2 THEN  -- Инфоклиника
            UPDATE patients
            SET 
                lastname = NEW.lastname,
                name = NEW.name,
                surname = NEW.surname,
                birthdate = NEW.birthdate,
                documenttypes = NEW.documenttypes,
                document_number = NEW.document_number,
                hisnumber_infoclinica = NEW.hisnumber,
                email_infoclinica = NEW.email,
                telephone_infoclinica = NEW.telephone,
                password_infoclinica = NEW.his_password,
                login_infoclinica = NEW.login_email
            WHERE uuid = NEW.uuid;
        END IF;
        
        -- Log the regular update
        INSERT INTO patient_matching_log (
            hisnumber, source, match_type, document_number, created_uuid
        ) VALUES (
            NEW.hisnumber, NEW.source, 'REGULAR_UPDATE', NEW.document_number, FALSE
        );
    END IF;
    
    RETURN NEW;
END;$$ LANGUAGE plpgsql;

-- Create triggers
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