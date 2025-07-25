-- Function called by trigger when a new patient is inserted
CREATE OR REPLACE FUNCTION process_new_patient()
RETURNS TRIGGER AS $$
DECLARE
    v_patient_uuid UUID;
    v_his_name VARCHAR(255);
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
    
    -- Look for existing patient with matching document if document is provided
    IF NEW.document_number IS NOT NULL AND NEW.documenttypes IS NOT NULL THEN
        -- Try to find matching patient by document type and number
        SELECT p.uuid
        INTO v_patient_uuid
        FROM patients p
        WHERE p.document_number = NEW.document_number 
        AND p.documenttypes = NEW.documenttypes;
        
        IF v_patient_uuid IS NOT NULL THEN
            -- Found match by document
            RAISE NOTICE 'Found existing patient with UUID: %', v_patient_uuid;
            
            -- Update patient record with HIS-specific data and handle conflicts
            IF NEW.source = 1 THEN  -- qMS - use source ID directly instead of name comparison
                RAISE NOTICE 'Updating qMS fields for existing patient';
                UPDATE patients
                SET 
                    -- Update HIS-specific fields including login
                    hisnumber_qms = NEW.hisnumber,
                    email_qms = NEW.email,
                    telephone_qms = NEW.telephone,
                    password_qms = NEW.his_password,
                    login_qms = NEW.login_email,
                    
                    -- Update demographic fields if they're empty
                    lastname = COALESCE(lastname, NEW.lastname),
                    name = COALESCE(name, NEW.name),
                    surname = COALESCE(surname, NEW.surname),
                    birthdate = COALESCE(birthdate, NEW.birthdate),
                    
                    -- Set primary source if it's not set yet
                    primary_source = COALESCE(primary_source, NEW.source)
                WHERE uuid = v_patient_uuid;
            ELSIF NEW.source = 2 THEN  -- Инфоклиника - use source ID directly
                RAISE NOTICE 'Updating Infoclinica fields for existing patient';
                UPDATE patients
                SET 
                    -- Update HIS-specific fields including login
                    hisnumber_infoclinica = NEW.hisnumber,
                    email_infoclinica = NEW.email,
                    telephone_infoclinica = NEW.telephone,
                    password_infoclinica = NEW.his_password,
                    login_infoclinica = NEW.login_email,
                    
                    -- Update demographic fields if they're empty
                    lastname = COALESCE(lastname, NEW.lastname),
                    name = COALESCE(name, NEW.name),
                    surname = COALESCE(surname, NEW.surname),
                    birthdate = COALESCE(birthdate, NEW.birthdate),
                    
                    -- Set primary source if it's not set yet
                    primary_source = COALESCE(primary_source, NEW.source)
                WHERE uuid = v_patient_uuid;
            END IF;
            
            -- Log the match
            INSERT INTO patient_matching_log (
                hisnumber, source, match_type, document_number, created_uuid
            ) VALUES (
                NEW.hisnumber, NEW.source, 'MATCHED', NEW.document_number, FALSE
            );
        ELSE
            -- No match found, create new patient record
            RAISE NOTICE 'Creating new patient with source: %', NEW.source;
            
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
                CASE WHEN NEW.source = 1 THEN NEW.hisnumber ELSE NULL END,           -- qMS
                CASE WHEN NEW.source = 1 THEN NEW.email ELSE NULL END,               -- qMS
                CASE WHEN NEW.source = 1 THEN NEW.telephone ELSE NULL END,           -- qMS
                CASE WHEN NEW.source = 1 THEN NEW.his_password ELSE NULL END,        -- qMS
                CASE WHEN NEW.source = 1 THEN NEW.login_email ELSE NULL END,         -- qMS login
                CASE WHEN NEW.source = 2 THEN NEW.hisnumber ELSE NULL END,           -- Инфоклиника
                CASE WHEN NEW.source = 2 THEN NEW.email ELSE NULL END,               -- Инфоклиника
                CASE WHEN NEW.source = 2 THEN NEW.telephone ELSE NULL END,           -- Инфоклиника
                CASE WHEN NEW.source = 2 THEN NEW.his_password ELSE NULL END,        -- Инфоклиника
                CASE WHEN NEW.source = 2 THEN NEW.login_email ELSE NULL END          -- Инфоклиника login
            )
            RETURNING uuid INTO v_patient_uuid;
            
            RAISE NOTICE 'Created new patient with UUID: %', v_patient_uuid;
            
            -- Log the new patient creation
            INSERT INTO patient_matching_log (
                hisnumber, source, match_type, document_number, created_uuid
            ) VALUES (
                NEW.hisnumber, NEW.source, 'NEW', NEW.document_number, TRUE
            );
        END IF;
    ELSE
        -- No document available, always create a new record
        RAISE NOTICE 'Creating new patient without document, source: %', NEW.source;
        
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
            CASE WHEN NEW.source = 1 THEN NEW.hisnumber ELSE NULL END,           -- qMS
            CASE WHEN NEW.source = 1 THEN NEW.email ELSE NULL END,               -- qMS
            CASE WHEN NEW.source = 1 THEN NEW.telephone ELSE NULL END,           -- qMS
            CASE WHEN NEW.source = 1 THEN NEW.his_password ELSE NULL END,        -- qMS
            CASE WHEN NEW.source = 1 THEN NEW.login_email ELSE NULL END,         -- qMS login
            CASE WHEN NEW.source = 2 THEN NEW.hisnumber ELSE NULL END,           -- Инфоклиника
            CASE WHEN NEW.source = 2 THEN NEW.email ELSE NULL END,               -- Инфоклиника
            CASE WHEN NEW.source = 2 THEN NEW.telephone ELSE NULL END,           -- Инфоклиника
            CASE WHEN NEW.source = 2 THEN NEW.his_password ELSE NULL END,        -- Инфоклиника
            CASE WHEN NEW.source = 2 THEN NEW.login_email ELSE NULL END          -- Инфоклиника login
        )
        RETURNING uuid INTO v_patient_uuid;
        
        RAISE NOTICE 'Created new patient without document, UUID: %', v_patient_uuid;
        
        -- Log the new patient creation
        INSERT INTO patient_matching_log (
                hisnumber, source, match_type, document_number, created_uuid
            ) VALUES (
                NEW.hisnumber, NEW.source, 'NEW_NO_DOCUMENT', NULL, TRUE
            );
    END IF;
    
    -- Assign the UUID to the new patient record
    NEW.uuid := v_patient_uuid;
    
    RETURN NEW;
END;$$ LANGUAGE plpgsql;

-- Create trigger on patientsdet table for INSERTs
DROP TRIGGER IF EXISTS trg_process_new_patient ON patientsdet;
CREATE TRIGGER trg_process_new_patient
BEFORE INSERT ON patientsdet
FOR EACH ROW
EXECUTE FUNCTION process_new_patient();

-- Function to handle updates to patientsdet
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
    
    -- Handle document updates: If document was NULL and now has a value
    IF (OLD.document_number IS NULL OR OLD.documenttypes IS NULL) AND 
       NEW.document_number IS NOT NULL AND NEW.documenttypes IS NOT NULL THEN
        
        -- Check if another patient exists with this document
        SELECT p.uuid 
        INTO v_other_patient_uuid
        FROM patients p
        WHERE p.document_number = NEW.document_number 
        AND p.documenttypes = NEW.documenttypes
        AND p.uuid <> OLD.uuid;
        
        IF v_other_patient_uuid IS NOT NULL THEN
            -- We found another patient with the same document
            -- We need to merge the records
            
            -- First, update the HIS-specific information based on source
            IF NEW.source = 1 THEN  -- qMS
                UPDATE patients
                SET 
                    hisnumber_qms = NEW.hisnumber,
                    email_qms = NEW.email,
                    telephone_qms = NEW.telephone,
                    password_qms = NEW.his_password,
                    login_qms = NEW.login_email
                WHERE uuid = v_other_patient_uuid;
                
                -- Transfer any missing data from the old record to the merged one
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
                    login_infoclinica = NEW.login_email
                WHERE uuid = v_other_patient_uuid;
                
                -- Transfer any missing data from the old record to the merged one
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
            
            -- Update demographic data if better quality in the new record
            UPDATE patients
            SET
                lastname = CASE WHEN NEW.lastname IS NOT NULL THEN NEW.lastname ELSE lastname END,
                name = CASE WHEN NEW.name IS NOT NULL THEN NEW.name ELSE name END,
                surname = CASE WHEN NEW.surname IS NOT NULL THEN NEW.surname ELSE surname END,
                birthdate = CASE WHEN NEW.birthdate IS NOT NULL THEN NEW.birthdate ELSE birthdate END
            WHERE uuid = v_other_patient_uuid;
            
            -- Update all records in patientsdet that reference the old uuid
            UPDATE patientsdet
            SET uuid = v_other_patient_uuid
            WHERE uuid = OLD.uuid;
            
            -- Update protocols if any
            UPDATE protocols
            SET uuid = v_other_patient_uuid
            WHERE uuid = OLD.uuid;
            
            -- Delete the old patient record
            DELETE FROM patients
            WHERE uuid = OLD.uuid;
            
            -- Update the current record's uuid
            NEW.uuid := v_other_patient_uuid;
            
            -- Log the merge
            INSERT INTO patient_matching_log (
                hisnumber, source, match_type, document_number, created_uuid
            ) VALUES (
                NEW.hisnumber, NEW.source, 'MERGED', NEW.document_number, FALSE
            );
            
            v_merged := TRUE;
        ELSE
            -- No other patient found with this document, just update the current patient
            UPDATE patients
            SET 
                documenttypes = NEW.documenttypes,
                document_number = NEW.document_number
            WHERE uuid = OLD.uuid;
        END IF;
    END IF;
    
    -- If we didn't merge, perform a regular update
    IF NOT v_merged THEN
        -- Update the consolidated patient record based on HIS source
        IF NEW.source = 1 THEN  -- qMS
            UPDATE patients
            SET 
                -- Always update demographic data with the latest values
                lastname = NEW.lastname,
                name = NEW.name,
                surname = NEW.surname,
                birthdate = NEW.birthdate,
                documenttypes = NEW.documenttypes,
                document_number = NEW.document_number,
                
                -- Update HIS-specific fields including login
                hisnumber_qms = NEW.hisnumber,
                email_qms = NEW.email,
                telephone_qms = NEW.telephone,
                password_qms = NEW.his_password,
                login_qms = NEW.login_email
            WHERE uuid = NEW.uuid;
        ELSIF NEW.source = 2 THEN  -- Инфоклиника
            UPDATE patients
            SET 
                -- Always update demographic data with the latest values
                lastname = NEW.lastname,
                name = NEW.name,
                surname = NEW.surname,
                birthdate = NEW.birthdate,
                documenttypes = NEW.documenttypes,
                document_number = NEW.document_number,
                
                -- Update HIS-specific fields including login
                hisnumber_infoclinica = NEW.hisnumber,
                email_infoclinica = NEW.email,
                telephone_infoclinica = NEW.telephone,
                password_infoclinica = NEW.his_password,
                login_infoclinica = NEW.login_email
            WHERE uuid = NEW.uuid;
        END IF;
        
        -- Log the update
        INSERT INTO patient_matching_log (
            hisnumber, source, match_type, document_number, created_uuid
        ) VALUES (
            NEW.hisnumber, NEW.source, 'UPDATED', NEW.document_number, FALSE
        );
    END IF;
    
    RETURN NEW;
END;$$ LANGUAGE plpgsql;

-- Create trigger for updates
DROP TRIGGER IF EXISTS trg_update_patient ON patientsdet;
CREATE TRIGGER trg_update_patient
AFTER UPDATE ON patientsdet
FOR EACH ROW
EXECUTE FUNCTION update_patient_from_patientsdet();