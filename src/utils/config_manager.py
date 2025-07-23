"""
Configuration management utilities for updating settings.py
"""

import re
import ast
import json
from pathlib import Path
from typing import Dict, Any, Union
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
        self.settings_file.rename(backup_file)
        
        try:
            self.settings_file.write_text(content, encoding='utf-8')
        except Exception as e:
            # Restore backup on error
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
    
    def update_dict_variable(self, var_name: str, new_dict: Dict[str, Any]) -> None:
        """Update a dictionary variable in settings.py"""
        content = self.read_settings()
        
        # Find the variable assignment
        pattern = rf'^({var_name}\s*=\s*)(\{{[^}}]*\}})$'
        matches = re.search(pattern, content, flags=re.MULTILINE | re.DOTALL)
        
        if not matches:
            # Try multiline dictionary pattern
            pattern = rf'^({var_name}\s*=\s*\{{)(.*?)(^\}})$'
            matches = re.search(pattern, content, flags=re.MULTILINE | re.DOTALL)
        
        if not matches:
            raise ValueError(f"Dictionary variable {var_name} not found in settings file")
        
        # Format the new dictionary nicely
        formatted_dict = self._format_dict(new_dict, indent=4)
        
        start_pos = matches.start()
        end_pos = matches.end()
        
        new_assignment = f"{var_name} = {formatted_dict}"
        
        new_content = content[:start_pos] + new_assignment + content[end_pos:]
        self.write_settings(new_content)
    
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