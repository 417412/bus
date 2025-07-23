"""
Configuration management utilities for updating settings.py
"""

import os
import sys
import re
import ast
import json
from pathlib import Path
from typing import Dict, Any, Union
from src.utils.password_manager import get_password_manager

# Add the parent directory to the path so Python can find the modules
parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent_dir)

from src.config.settings import get_config_info

class ConfigManager:
    """Manages configuration updates to settings.py"""
    
    def __init__(self, settings_file: str = None):
        if settings_file is None:
            # Default to the settings.py file
            self.settings_file = Path(__file__).parent.parent / "config" / "settings.py"
        else:
            self.settings_file = Path(settings_file)
    
    def read_settings(self) -> str:
        """Read the current settings file content."""
        return self.settings_file.read_text(encoding='utf-8')
    
    def write_settings(self, content: str) -> None:
        """Write content to the settings file."""
        # Create backup first
        backup_file = self.settings_file.with_suffix('.py.backup')
        if self.settings_file.exists():
            self.settings_file.rename(backup_file)
        
        try:
            self.settings_file.write_text(content, encoding='utf-8')
        except Exception as e:
            # Restore backup on error if it exists
            if backup_file.exists():
                backup_file.rename(self.settings_file)
            raise e
    
    def update_path_variable(self, var_name: str, new_path: str) -> None:
        """Update a path variable in settings.py"""
        content = self.read_settings()
        
        # Pattern to match the variable assignment
        pattern = rf'^({var_name}\s*=\s*)(.+)$'
        
        new_value = f'Path("{new_path}")'
        replacement = rf'\1{new_value}'
        
        new_content = re.sub(pattern, replacement, content, flags=re.MULTILINE)
        
        if new_content == content:
            raise ValueError(f"Variable {var_name} not found in settings file")
        
        self.write_settings(new_content)
    
    def update_dict_variable(self, var_name: str, new_value: Dict[str, Any]) -> bool:
        """
        Update a dictionary variable in settings.py.
        Automatically encrypts passwords in database configurations.
        """
        # If this is database config, encrypt passwords before processing
        if var_name == "DATABASE_CONFIG":
            password_manager = get_password_manager()
            new_value = password_manager.encrypt_config_passwords(new_value)
        
        content = self.read_settings()
        
        # Find the variable assignment - handle multiline dictionaries
        # First try to find the start of the variable
        start_pattern = rf'^({var_name}\s*=\s*\{{)'
        start_match = re.search(start_pattern, content, flags=re.MULTILINE)
        
        if not start_match:
            raise ValueError(f"Dictionary variable {var_name} not found in settings file")
        
        # Find the matching closing brace
        start_pos = start_match.start()
        brace_count = 0
        current_pos = start_match.end() - 1  # Position of opening brace
        end_pos = None
        
        for i, char in enumerate(content[current_pos:], current_pos):
            if char == '{':
                brace_count += 1
            elif char == '}':
                brace_count -= 1
                if brace_count == 0:
                    end_pos = i + 1
                    break
        
        if end_pos is None:
            raise ValueError(f"Could not find closing brace for {var_name}")
        
        # Format the new dictionary nicely
        formatted_dict = self._format_dict(new_value, indent=4)  # Changed new_dict to new_value
        
        new_assignment = f"{var_name} = {formatted_dict}"
        
        new_content = content[:start_pos] + new_assignment + content[end_pos:]
        self.write_settings(new_content)
        return True  # Add return statement
    
    def update_simple_variable(self, var_name: str, new_value: Union[str, int, float, bool]) -> None:
        """Update a simple variable in settings.py"""
        content = self.read_settings()
        
        # Pattern to match the variable assignment
        pattern = rf'^({var_name}\s*=\s*)(.+)$'
        
        # Format the new value appropriately
        if isinstance(new_value, str):
            formatted_value = f'"{new_value}"'
        else:
            formatted_value = str(new_value)
        
        replacement = rf'\1{formatted_value}'
        
        new_content = re.sub(pattern, replacement, content, flags=re.MULTILINE)
        
        if new_content == content:
            raise ValueError(f"Variable {var_name} not found in settings file")
        
        self.write_settings(new_content)
    
    def _format_dict(self, d: Dict[str, Any], indent: int = 4) -> str:
        """Format a dictionary for writing to Python file."""
        lines = ["{"]
        
        for key, value in d.items():
            if isinstance(value, dict):
                # Nested dictionary
                nested = self._format_dict(value, indent + 4)
                lines.append(f"{' ' * indent}\"{key}\": {nested},")
            elif isinstance(value, str):
                lines.append(f"{' ' * indent}\"{key}\": \"{value}\",")
            elif isinstance(value, (int, float, bool)):
                lines.append(f"{' ' * indent}\"{key}\": {value},")
            else:
                lines.append(f"{' ' * indent}\"{key}\": {repr(value)},")
        
        lines.append("}")
        return "\n".join(lines)
    
    def validate_settings(self) -> bool:
        """Validate that the settings file is syntactically correct."""
        try:
            content = self.read_settings()
            ast.parse(content)
            return True
        except SyntaxError as e:
            print(f"Syntax error in settings file: {e}")
            return False
        except Exception as e:
            print(f"Error validating settings file: {e}")
            return False