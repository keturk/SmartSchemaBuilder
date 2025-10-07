"""
MIT License

Smart Schema Builder

Copyright (c) 2023 Kamil Ercan Turkarslan

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import os
import re
import logging
from typing import Any, List, Optional, Union, Callable
from pathlib import Path

from common.exceptions import ValidationError, ConfigurationError


class InputValidator:
    """Comprehensive input validation utilities."""
    
    @staticmethod
    def validate_file_path(file_path: Union[str, Path], must_exist: bool = True) -> Path:
        """
        Validate a file path.
        
        Args:
            file_path: Path to validate
            must_exist: Whether the file must exist
            
        Returns:
            Path object if valid
            
        Raises:
            ValidationError: If path is invalid
        """
        try:
            path = Path(file_path).resolve()
            
            if must_exist and not path.exists():
                raise ValidationError(
                    f"File does not exist: {file_path}",
                    field="file_path",
                    value=str(file_path)
                )
            
            if must_exist and not path.is_file():
                raise ValidationError(
                    f"Path is not a file: {file_path}",
                    field="file_path",
                    value=str(file_path)
                )
            
            return path
        except Exception as e:
            raise ValidationError(
                f"Invalid file path: {file_path} - {str(e)}",
                field="file_path",
                value=str(file_path)
            )
    
    @staticmethod
    def validate_directory_path(dir_path: Union[str, Path], must_exist: bool = True) -> Path:
        """
        Validate a directory path.
        
        Args:
            dir_path: Directory path to validate
            must_exist: Whether the directory must exist
            
        Returns:
            Path object if valid
            
        Raises:
            ValidationError: If path is invalid
        """
        try:
            path = Path(dir_path).resolve()
            
            if must_exist and not path.exists():
                raise ValidationError(
                    f"Directory does not exist: {dir_path}",
                    field="directory_path",
                    value=str(dir_path)
                )
            
            if must_exist and not path.is_dir():
                raise ValidationError(
                    f"Path is not a directory: {dir_path}",
                    field="directory_path",
                    value=str(dir_path)
                )
            
            return path
        except Exception as e:
            raise ValidationError(
                f"Invalid directory path: {dir_path} - {str(e)}",
                field="directory_path",
                value=str(dir_path)
            )
    
    @staticmethod
    def validate_database_type(db_type: str) -> str:
        """
        Validate database type.
        
        Args:
            db_type: Database type to validate
            
        Returns:
            Validated database type
            
        Raises:
            ValidationError: If database type is invalid
        """
        valid_types = ['postgresql', 'mysql', 'sqlserver']
        db_type_lower = db_type.lower()
        
        if db_type_lower not in valid_types:
            raise ValidationError(
                f"Invalid database type: {db_type}. Must be one of: {', '.join(valid_types)}",
                field="database_type",
                value=db_type
            )
        
        return db_type_lower
    
    @staticmethod
    def validate_port(port: Union[int, str]) -> int:
        """
        Validate port number.
        
        Args:
            port: Port number to validate
            
        Returns:
            Validated port number
            
        Raises:
            ValidationError: If port is invalid
        """
        try:
            port_int = int(port)
            if not (1 <= port_int <= 65535):
                raise ValidationError(
                    f"Invalid port number: {port}. Must be between 1 and 65535",
                    field="port",
                    value=port
                )
            return port_int
        except (ValueError, TypeError):
            raise ValidationError(
                f"Invalid port number: {port}. Must be a valid integer",
                field="port",
                value=port
            )
    
    @staticmethod
    def validate_host(host: str) -> str:
        """
        Validate host address.
        
        Args:
            host: Host address to validate
            
        Returns:
            Validated host address
            
        Raises:
            ValidationError: If host is invalid
        """
        if not host or not isinstance(host, str):
            raise ValidationError(
                f"Invalid host address: {host}. Must be a non-empty string",
                field="host",
                value=host
            )
        
        # Basic host validation (IP or hostname)
        host_pattern = re.compile(
            r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$|'
            r'^[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*$'
        )
        
        if not host_pattern.match(host):
            raise ValidationError(
                f"Invalid host address format: {host}",
                field="host",
                value=host
            )
        
        return host
    
    @staticmethod
    def validate_csv_file(csv_file: Union[str, Path]) -> Path:
        """
        Validate CSV file.
        
        Args:
            csv_file: CSV file path to validate
            
        Returns:
            Path object if valid
            
        Raises:
            ValidationError: If CSV file is invalid
        """
        path = InputValidator.validate_file_path(csv_file, must_exist=True)
        
        if not path.suffix.lower() == '.csv':
            raise ValidationError(
                f"File is not a CSV file: {csv_file}",
                field="csv_file",
                value=str(csv_file)
            )
        
        # Check if file is readable
        if not os.access(path, os.R_OK):
            raise ValidationError(
                f"CSV file is not readable: {csv_file}",
                field="csv_file",
                value=str(csv_file)
            )
        
        return path
    
    @staticmethod
    def validate_table_name(table_name: str) -> str:
        """
        Validate table name.
        
        Args:
            table_name: Table name to validate
            
        Returns:
            Validated table name
            
        Raises:
            ValidationError: If table name is invalid
        """
        if not table_name or not isinstance(table_name, str):
            raise ValidationError(
                f"Invalid table name: {table_name}. Must be a non-empty string",
                field="table_name",
                value=table_name
            )
        
        # Check for valid table name characters
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
            raise ValidationError(
                f"Invalid table name format: {table_name}. Must start with letter or underscore and contain only letters, numbers, and underscores",
                field="table_name",
                value=table_name
            )
        
        # Check length
        if len(table_name) > 63:  # PostgreSQL limit
            raise ValidationError(
                f"Table name too long: {table_name}. Maximum length is 63 characters",
                field="table_name",
                value=table_name
            )
        
        return table_name.lower()
    
    @staticmethod
    def validate_column_name(column_name: str) -> str:
        """
        Validate column name.
        
        Args:
            column_name: Column name to validate
            
        Returns:
            Validated column name
            
        Raises:
            ValidationError: If column name is invalid
        """
        if not column_name or not isinstance(column_name, str):
            raise ValidationError(
                f"Invalid column name: {column_name}. Must be a non-empty string",
                field="column_name",
                value=column_name
            )
        
        # Check for valid column name characters
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', column_name):
            raise ValidationError(
                f"Invalid column name format: {column_name}. Must start with letter or underscore and contain only letters, numbers, and underscores",
                field="column_name",
                value=column_name
            )
        
        # Check length
        if len(column_name) > 63:  # PostgreSQL limit
            raise ValidationError(
                f"Column name too long: {column_name}. Maximum length is 63 characters",
                field="column_name",
                value=column_name
            )
        
        return column_name.lower()
    
    @staticmethod
    def validate_positive_integer(value: Any, field_name: str = "value") -> int:
        """
        Validate positive integer.
        
        Args:
            value: Value to validate
            field_name: Name of the field being validated
            
        Returns:
            Validated positive integer
            
        Raises:
            ValidationError: If value is not a positive integer
        """
        try:
            int_value = int(value)
            if int_value <= 0:
                raise ValidationError(
                    f"Value must be positive: {value}",
                    field=field_name,
                    value=value
                )
            return int_value
        except (ValueError, TypeError):
            raise ValidationError(
                f"Invalid integer value: {value}",
                field=field_name,
                value=value
            )
    
    @staticmethod
    def validate_ai_provider(provider: str) -> str:
        """
        Validate AI provider.
        
        Args:
            provider: AI provider to validate
            
        Returns:
            Validated AI provider
            
        Raises:
            ValidationError: If AI provider is invalid
        """
        valid_providers = ['openai', 'ollama', 'none']
        provider_lower = provider.lower()
        
        if provider_lower not in valid_providers:
            raise ValidationError(
                f"Invalid AI provider: {provider}. Must be one of: {', '.join(valid_providers)}",
                field="ai_provider",
                value=provider
            )
        
        return provider_lower
    
    @staticmethod
    def validate_environment_variable(
        var_name: str, 
        required: bool = True,
        default_value: Optional[str] = None
    ) -> Optional[str]:
        """
        Validate environment variable.
        
        Args:
            var_name: Name of the environment variable
            required: Whether the variable is required
            default_value: Default value if not set
            
        Returns:
            Environment variable value or default
            
        Raises:
            ConfigurationError: If required variable is not set
        """
        value = os.environ.get(var_name, default_value)
        
        if required and value is None:
            raise ConfigurationError(
                f"Required environment variable not set: {var_name}",
                config_key=var_name
            )
        
        return value


def validate_inputs(**validations) -> dict:
    """
    Validate multiple inputs at once.
    
    Args:
        **validations: Dictionary of field_name: (value, validator_function) pairs
        
    Returns:
        Dictionary of validated values
        
    Raises:
        ValidationError: If any validation fails
    """
    validated = {}
    
    for field_name, (value, validator) in validations.items():
        try:
            if callable(validator):
                validated[field_name] = validator(value)
            else:
                validated[field_name] = value
        except Exception as e:
            raise ValidationError(
                f"Validation failed for {field_name}: {str(e)}",
                field=field_name,
                value=value
            )
    
    return validated
