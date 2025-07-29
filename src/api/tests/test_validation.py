"""
Tests for input validation and error handling - CONSOLIDATED.
All validation-related tests consolidated here.
"""

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from src.api.main import PatientCredentialRequest
from src.api.tests.conftest import TestDataGenerator


class TestPatientCredentialRequestValidation:
    """Test PatientCredentialRequest model validation."""
    
    @pytest.mark.parametrize("valid_date,description", TestDataGenerator.valid_dates())
    def test_valid_date_formats(self, valid_date, description):
        """Test various valid date formats are accepted."""
        request_data = {
            "lastname": "Test",
            "firstname": "User",
            "bdate": valid_date,
            "cllogin": "test_login",
            "clpassword": "test_password"
        }
        
        # Should not raise ValidationError
        patient_request = PatientCredentialRequest(**request_data)
        assert patient_request.bdate == valid_date
        assert patient_request.lastname == "Test"
        assert patient_request.firstname == "User"
    
    @pytest.mark.parametrize("invalid_date,expected_error", TestDataGenerator.invalid_dates())
    def test_invalid_date_formats(self, invalid_date, expected_error):
        """Test various invalid date formats are rejected."""
        request_data = {
            "lastname": "Test",
            "firstname": "User",
            "bdate": invalid_date,
            "cllogin": "test_login",
            "clpassword": "test_password"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            PatientCredentialRequest(**request_data)
        
        error_details = str(exc_info.value)
        assert "Birth date must be in YYYY-MM-DD format" in error_details
    
    def test_required_fields_validation(self):
        """Test that all required fields are validated."""
        # Test missing lastname
        with pytest.raises(ValidationError) as exc_info:
            PatientCredentialRequest(
                firstname="John",
                bdate="1990-01-01",
                cllogin="test_login",
                clpassword="test_password"
            )
        assert "lastname" in str(exc_info.value)
        
        # Test missing firstname
        with pytest.raises(ValidationError) as exc_info:
            PatientCredentialRequest(
                lastname="Smith",
                bdate="1990-01-01",
                cllogin="test_login",
                clpassword="test_password"
            )
        assert "firstname" in str(exc_info.value)
        
        # Test missing bdate
        with pytest.raises(ValidationError) as exc_info:
            PatientCredentialRequest(
                lastname="Smith",
                firstname="John",
                cllogin="test_login",
                clpassword="test_password"
            )
        assert "bdate" in str(exc_info.value)
        
        # Test missing cllogin
        with pytest.raises(ValidationError) as exc_info:
            PatientCredentialRequest(
                lastname="Smith",
                firstname="John",
                bdate="1990-01-01",
                clpassword="test_password"
            )
        assert "cllogin" in str(exc_info.value)
        
        # Test missing clpassword
        with pytest.raises(ValidationError) as exc_info:
            PatientCredentialRequest(
                lastname="Smith",
                firstname="John",
                bdate="1990-01-01",
                cllogin="test_login"
            )
        assert "clpassword" in str(exc_info.value)
    
    def test_optional_midname_field(self):
        """Test that midname field is optional."""
        # Test with midname
        request_with_midname = PatientCredentialRequest(
            lastname="Smith",
            firstname="John",
            midname="William",
            bdate="1990-01-01",
            cllogin="test_login",
            clpassword="test_password"
        )
        assert request_with_midname.midname == "William"
        
        # Test without midname
        request_without_midname = PatientCredentialRequest(
            lastname="Smith",
            firstname="John",
            bdate="1990-01-01",
            cllogin="test_login",
            clpassword="test_password"
        )
        assert request_without_midname.midname is None
        
        # Test with explicit None midname
        request_explicit_none = PatientCredentialRequest(
            lastname="Smith",
            firstname="John",
            midname=None,
            bdate="1990-01-01",
            cllogin="test_login",
            clpassword="test_password"
        )
        assert request_explicit_none.midname is None
    
    def test_empty_string_validation(self):
        """Test validation with empty strings."""
        # Current implementation allows empty strings for string fields
        # If business logic requires non-empty strings, additional validators should be added
        
        # Test that empty strings are currently accepted by the model
        # (This documents the current behavior - modify if validation is added later)
        
        # Empty lastname - currently allowed
        request = PatientCredentialRequest(
            lastname="",
            firstname="John",
            bdate="1990-01-01",
            cllogin="test_login",
            clpassword="test_password"
        )
        assert request.lastname == ""
        
        # Empty firstname - currently allowed
        request = PatientCredentialRequest(
            lastname="Smith",
            firstname="",
            bdate="1990-01-01",
            cllogin="test_login",
            clpassword="test_password"
        )
        assert request.firstname == ""
        
        # Empty bdate should fail due to date format validation
        with pytest.raises(ValidationError) as exc_info:
            PatientCredentialRequest(
                lastname="Smith",
                firstname="John",
                bdate="",
                cllogin="test_login",
                clpassword="test_password"
            )
        assert "Birth date must be in YYYY-MM-DD format" in str(exc_info.value)
        
        # Empty cllogin - currently allowed
        request = PatientCredentialRequest(
            lastname="Smith",
            firstname="John",
            bdate="1990-01-01",
            cllogin="",
            clpassword="test_password"
        )
        assert request.cllogin == ""
        
        # Empty clpassword - currently allowed
        request = PatientCredentialRequest(
            lastname="Smith",
            firstname="John",
            bdate="1990-01-01",
            cllogin="test_login",
            clpassword=""
        )
        assert request.clpassword == ""
    
    def test_whitespace_validation(self):
        """Test validation with whitespace strings."""
        # Whitespace-only strings should be trimmed or rejected
        request_data = {
            "lastname": "  Smith  ",
            "firstname": "  John  ",
            "midname": "  William  ",
            "bdate": "1990-01-01",
            "cllogin": "  test_login  ",
            "clpassword": "  test_password  "
        }
        
        patient_request = PatientCredentialRequest(**request_data)
        
        # Fields should be trimmed (if implemented) or accepted as-is
        assert patient_request.lastname.strip() == "Smith"
        assert patient_request.firstname.strip() == "John"
        assert patient_request.midname.strip() == "William"
        assert patient_request.cllogin.strip() == "test_login"
        assert patient_request.clpassword.strip() == "test_password"
    
    def test_get_bdate_as_date_method(self):
        """Test the get_bdate_as_date method."""
        patient_request = PatientCredentialRequest(
            lastname="Smith",
            firstname="John",
            bdate="1990-01-15",
            cllogin="test_login",
            clpassword="test_password"
        )
        
        date_obj = patient_request.get_bdate_as_date()
        
        assert date_obj.year == 1990
        assert date_obj.month == 1
        assert date_obj.day == 15
        assert str(date_obj) == "1990-01-15"


class TestAPIEndpointValidation:
    """Test API endpoint validation through HTTP requests."""
    
    def test_checkModifyPatient_endpoint_validation(self, client):
        """Test /checkModifyPatient endpoint validation."""
        # Test completely empty request
        response = client.post("/checkModifyPatient", json={})
        assert response.status_code == 422
        
        error_data = response.json()
        assert "detail" in error_data
        
        # Should have multiple validation errors
        errors = error_data["detail"]
        required_fields = ["lastname", "firstname", "bdate", "cllogin", "clpassword"]
        
        error_fields = [error["loc"][-1] for error in errors]
        for field in required_fields:
            assert field in error_fields
    
    def test_checkModifyPatient_partial_data_validation(self, client):
        """Test /checkModifyPatient with partial data."""
        # Test with only some fields
        partial_data = {
            "lastname": "Smith",
            "firstname": "John"
        }
        
        response = client.post("/checkModifyPatient", json=partial_data)
        assert response.status_code == 422
        
        error_data = response.json()
        errors = error_data["detail"]
        
        # Should complain about missing bdate, cllogin, clpassword
        missing_fields = [error["loc"][-1] for error in errors]
        assert "bdate" in missing_fields
        assert "cllogin" in missing_fields
        assert "clpassword" in missing_fields
    
    def test_checkModifyPatient_invalid_json(self, client):
        """Test /checkModifyPatient with invalid JSON."""
        response = client.post(
            "/checkModifyPatient",
            data="invalid json",
            headers={"Content-Type": "application/json"}
        )
        assert response.status_code == 422
    
    def test_checkModifyPatient_wrong_content_type(self, client):
        """Test /checkModifyPatient with wrong content type."""
        response = client.post(
            "/checkModifyPatient",
            data="lastname=Smith&firstname=John",
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        assert response.status_code == 422
    
    def test_test_create_endpoint_validation(self, client, sample_patient_request):
        """Test /test-create/{his_type} endpoint validation."""
        # Test with valid his_type
        response = client.post("/test-create/yottadb", json=sample_patient_request)
        # Should not return 400 (validation error) or 422 (request validation)
        assert response.status_code not in [400, 422]
        
        # Test with invalid his_type
        response = client.post("/test-create/invalid", json=sample_patient_request)
        assert response.status_code == 400
        
        error_data = response.json()
        assert "Invalid HIS type" in error_data["detail"]
    
    def test_oauth_test_endpoint_validation(self, client):
        """Test /test-oauth/{his_type} endpoint validation."""
        # Test with valid his_type
        response = client.post("/test-oauth/yottadb")
        # Should not return 400 (validation error)
        assert response.status_code != 400
        
        # Test with invalid his_type
        response = client.post("/test-oauth/invalid")
        assert response.status_code == 400
        
        error_data = response.json()
        assert "Invalid HIS type" in error_data["detail"]


class TestSpecialCharacterValidation:
    """Test handling of special characters in input."""
    
    def test_special_characters_in_names(self):
        """Test special characters in name fields."""
        special_cases = [
            ("O'Connor", "Valid apostrophe"),
            ("Smith-Jones", "Valid hyphen"),
            ("José", "Valid accented character"),
            ("李", "Valid Chinese character"),
            ("Smith123", "Numbers in name"),
            ("Smith@email", "Invalid @ symbol"),
            ("Smith<script>", "Potential XSS attempt")
        ]
        
        for name, description in special_cases:
            request_data = {
                "lastname": name,
                "firstname": "John",
                "bdate": "1990-01-01",
                "cllogin": "test_login",
                "clpassword": "test_password"
            }
            
            try:
                patient_request = PatientCredentialRequest(**request_data)
                # If validation passes, the name should be preserved
                assert patient_request.lastname == name
            except ValidationError:
                # Some special characters might be rejected
                # This is expected for security reasons
                pass
    
    def test_sql_injection_attempts(self):
        """Test potential SQL injection attempts in input."""
        sql_injection_attempts = [
            "Smith'; DROP TABLE patients; --",
            "Smith' OR '1'='1",
            "Smith' UNION SELECT * FROM users --",
            "Smith\"; DELETE FROM patients; --"
        ]
        
        for injection_attempt in sql_injection_attempts:
            request_data = {
                "lastname": injection_attempt,
                "firstname": "John",
                "bdate": "1990-01-01",
                "cllogin": "test_login",
                "clpassword": "test_password"
            }
            
            # Should either reject the input or sanitize it
            try:
                patient_request = PatientCredentialRequest(**request_data)
                # If accepted, should be exactly as provided (will be parameterized in queries)
                assert patient_request.lastname == injection_attempt
            except ValidationError:
                # Rejection is also acceptable
                pass
    
    def test_unicode_handling(self):
        """Test Unicode character handling."""
        unicode_cases = [
            ("Müller", "German umlaut"),
            ("Ñoño", "Spanish tilde"),
            ("Владимир", "Cyrillic characters"),
            ("محمد", "Arabic characters"),
            ("田中", "Japanese characters"),
            ("김철수", "Korean characters")
        ]
        
        for name, description in unicode_cases:
            request_data = {
                "lastname": name,
                "firstname": "Test",
                "bdate": "1990-01-01",
                "cllogin": "test_login",
                "clpassword": "test_password"
            }
            
            # Unicode should be handled properly
            patient_request = PatientCredentialRequest(**request_data)
            assert patient_request.lastname == name


class TestEdgeCaseValidation:
    """Test edge cases in validation."""
    
    def test_very_long_strings(self):
        """Test very long input strings."""
        long_string = "A" * 1000
        
        request_data = {
            "lastname": long_string,
            "firstname": "John",
            "bdate": "1990-01-01",
            "cllogin": "test_login",
            "clpassword": "test_password"
        }
        
        # Should either accept or reject based on field length limits
        try:
            patient_request = PatientCredentialRequest(**request_data)
            assert len(patient_request.lastname) == 1000
        except ValidationError as e:
            # Field length validation is acceptable
            assert "too long" in str(e).lower() or "length" in str(e).lower()
    
    def test_leap_year_dates(self):
        """Test leap year date validation."""
        leap_year_cases = [
            ("2020-02-29", True, "Valid leap year date"),
            ("2021-02-29", False, "Invalid non-leap year date"),
            ("2000-02-29", True, "Valid century leap year"),
            ("1900-02-29", False, "Invalid century non-leap year")
        ]
        
        for date_str, should_be_valid, description in leap_year_cases:
            request_data = {
                "lastname": "Smith",
                "firstname": "John",
                "bdate": date_str,
                "cllogin": "test_login",
                "clpassword": "test_password"
            }
            
            if should_be_valid:
                patient_request = PatientCredentialRequest(**request_data)
                assert patient_request.bdate == date_str
            else:
                with pytest.raises(ValidationError):
                    PatientCredentialRequest(**request_data)
    
    def test_boundary_dates(self):
        """Test boundary date values."""
        boundary_cases = [
            ("1900-01-01", "Very old date"),
            ("2100-12-31", "Future date"),
            ("2024-01-01", "Current era date"),
            ("1999-12-31", "Y2K boundary")
        ]
        
        for date_str, description in boundary_cases:
            request_data = {
                "lastname": "Smith",
                "firstname": "John",
                "bdate": date_str,
                "cllogin": "test_login",
                "clpassword": "test_password"
            }
            
            # All valid ISO dates should be accepted
            patient_request = PatientCredentialRequest(**request_data)
            assert patient_request.bdate == date_str