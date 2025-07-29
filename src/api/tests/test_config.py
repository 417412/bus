import pytest
from datetime import datetime, timedelta
from unittest.mock import patch, AsyncMock

class TestAPIConfiguration:
    """Test the actual configuration system."""
    
    def test_his_api_config_structure(self):
        """Test HIS API configuration matches expected structure."""
        from src.api.config import HIS_API_CONFIG
        
        for his_type in ['yottadb', 'firebird']:
            config = HIS_API_CONFIG[his_type]
            
            # Test structure matches actual config.py
            assert 'base_url' in config
            assert 'credentials_endpoint' in config
            assert 'create_endpoint' in config
            assert 'oauth' in config
            
            oauth_config = config['oauth']
            assert 'token_url' in oauth_config
            assert 'username' in oauth_config
            assert 'password' in oauth_config
            # These should be empty strings as per implementation
            assert oauth_config.get('client_id') == ""
            assert oauth_config.get('client_secret') == ""
            assert oauth_config.get('scope') == ""
    
    def test_mobile_app_config_defaults(self):
        """Test mobile app configuration defaults."""
        from src.api.config import MOBILE_APP_CONFIG
        
        # Test actual default values
        assert 'registration_enabled' in MOBILE_APP_CONFIG
        assert 'auto_register_on_create' in MOBILE_APP_CONFIG
        assert 'require_both_his' in MOBILE_APP_CONFIG
    
    def test_config_validation(self):
        """Test the validate_config function."""
        from src.api.config import validate_config
        
        issues = validate_config()
        # Should return a list of issues or empty list
        assert isinstance(issues, list)