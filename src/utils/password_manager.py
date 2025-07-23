#!/usr/bin/env python3
"""
Password encryption/decryption utility for the medical system ETL application.
Uses Fernet symmetric encryption with a key derived from the system.
"""

import os
import base64
import hashlib
from pathlib import Path
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from typing import Optional, Union
import platform
import getpass

class PasswordManager:
    """Manages password encryption and decryption for configuration files."""
    
    def __init__(self, key_file: Optional[Union[str, Path]] = None):
        """
        Initialize the password manager.
        
        Args:
            key_file: Optional path to store the encryption key file
        """
        self.key_file = Path(key_file) if key_file else Path.home() / ".etl_key"
        self._cipher = None
    
    def _generate_key_from_system(self) -> bytes:
        """Generate a deterministic key based on system characteristics."""
        # Combine system information to create a unique seed
        system_info = f"{platform.node()}{platform.system()}{platform.machine()}"
        
        # Add current user
        try:
            system_info += getpass.getuser()
        except:
            system_info += "default_user"
        
        # Use PBKDF2 to derive a key from the system info
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=b'etl_medical_system_salt',  # Fixed salt for deterministic key
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(system_info.encode()))
        return key
    
    def _get_or_create_key(self) -> bytes:
        """Get existing key or create a new one."""
        if self.key_file.exists():
            try:
                with open(self.key_file, 'rb') as f:
                    return f.read()
            except Exception:
                pass
        
        # Generate new key
        key = self._generate_key_from_system()
        
        # Save key securely
        try:
            # Create directory if it doesn't exist
            self.key_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Write key file with restricted permissions
            with open(self.key_file, 'wb') as f:
                f.write(key)
            
            # Set restrictive permissions (owner read/write only)
            if os.name != 'nt':  # Unix-like systems
                os.chmod(self.key_file, 0o600)
                
        except Exception as e:
            # If we can't save the key, still return it for this session
            print(f"Warning: Could not save encryption key: {e}")
        
        return key
    
    def _get_cipher(self) -> Fernet:
        """Get or create the cipher object."""
        if self._cipher is None:
            key = self._get_or_create_key()
            self._cipher = Fernet(key)
        return self._cipher
    
    def encrypt_password(self, password: str) -> str:
        """
        Encrypt a password.
        
        Args:
            password: Plain text password
            
        Returns:
            Encrypted password as base64 string with prefix
        """
        if not password:
            return password
        
        cipher = self._get_cipher()
        encrypted_bytes = cipher.encrypt(password.encode())
        encrypted_b64 = base64.urlsafe_b64encode(encrypted_bytes).decode()
        
        # Add prefix to identify encrypted passwords
        return f"ENC:{encrypted_b64}"
    
    def decrypt_password(self, encrypted_password: str) -> str:
        """
        Decrypt a password.
        
        Args:
            encrypted_password: Encrypted password string
            
        Returns:
            Decrypted plain text password
        """
        if not encrypted_password:
            return encrypted_password
        
        # Check if it's actually encrypted
        if not encrypted_password.startswith("ENC:"):
            # Not encrypted, return as-is (for backward compatibility)
            return encrypted_password
        
        try:
            # Remove prefix and decode
            encrypted_b64 = encrypted_password[4:]  # Remove "ENC:" prefix
            encrypted_bytes = base64.urlsafe_b64decode(encrypted_b64.encode())
            
            # Decrypt
            cipher = self._get_cipher()
            decrypted_bytes = cipher.decrypt(encrypted_bytes)
            return decrypted_bytes.decode()
        except Exception as e:
            raise ValueError(f"Failed to decrypt password: {e}")
    
    def is_encrypted(self, password: str) -> bool:
        """Check if a password is encrypted."""
        return isinstance(password, str) and password.startswith("ENC:")
    
    def encrypt_config_passwords(self, config: dict) -> dict:
        """
        Recursively encrypt all password fields in a configuration dictionary.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Configuration dictionary with encrypted passwords
        """
        import copy
        
        def encrypt_recursive(obj):
            if isinstance(obj, dict):
                result = {}
                for key, value in obj.items():
                    if key.lower() == 'password' and isinstance(value, str) and not self.is_encrypted(value):
                        result[key] = self.encrypt_password(value)
                    else:
                        result[key] = encrypt_recursive(value)
                return result
            elif isinstance(obj, list):
                return [encrypt_recursive(item) for item in obj]
            else:
                return obj
        
        return encrypt_recursive(config)
    
    def decrypt_config_passwords(self, config: dict) -> dict:
        """
        Recursively decrypt all password fields in a configuration dictionary.
        
        Args:
            config: Configuration dictionary with encrypted passwords
            
        Returns:
            Configuration dictionary with decrypted passwords
        """
        import copy
        
        def decrypt_recursive(obj):
            if isinstance(obj, dict):
                result = {}
                for key, value in obj.items():
                    if key.lower() == 'password' and isinstance(value, str) and self.is_encrypted(value):
                        result[key] = self.decrypt_password(value)
                    else:
                        result[key] = decrypt_recursive(value)
                return result
            elif isinstance(obj, list):
                return [decrypt_recursive(item) for item in obj]
            else:
                return obj
        
        return decrypt_recursive(config)

# Global instance for easy access
_password_manager = None

def get_password_manager() -> PasswordManager:
    """Get the global password manager instance."""
    global _password_manager
    if _password_manager is None:
        _password_manager = PasswordManager()
    return _password_manager

def encrypt_password(password: str) -> str:
    """Convenience function to encrypt a password."""
    return get_password_manager().encrypt_password(password)

def decrypt_password(encrypted_password: str) -> str:
    """Convenience function to decrypt a password."""
    return get_password_manager().decrypt_password(encrypted_password)

def is_password_encrypted(password: str) -> bool:
    """Convenience function to check if a password is encrypted."""
    return get_password_manager().is_encrypted(password)