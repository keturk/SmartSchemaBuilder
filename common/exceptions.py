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
import logging
from typing import Optional, Dict, Any
from enum import Enum


class ErrorSeverity(Enum):
    """Error severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class SmartSchemaBuilderError(Exception):
    """Base exception class for SmartSchemaBuilder."""
    
    def __init__(
        self, 
        message: str, 
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        error_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        recoverable: bool = True,
        **kwargs
    ):
        super().__init__(message)
        self.message = message
        self.severity = severity
        self.error_code = error_code
        self.details = details or {}
        self.recoverable = recoverable
        
        # Log the error
        self._log_error()
    
    def _log_error(self):
        """Log the error with appropriate level."""
        log_level = {
            ErrorSeverity.LOW: logging.INFO,
            ErrorSeverity.MEDIUM: logging.WARNING,
            ErrorSeverity.HIGH: logging.ERROR,
            ErrorSeverity.CRITICAL: logging.CRITICAL
        }.get(self.severity, logging.ERROR)
        
        logging.log(log_level, f"[{self.error_code or 'UNKNOWN'}] {self.message}")
        if self.details:
            logging.log(log_level, f"Details: {self.details}")


class ConfigurationError(SmartSchemaBuilderError):
    """Configuration-related errors."""
    
    def __init__(self, message: str, config_key: Optional[str] = None, **kwargs):
        super().__init__(
            message, 
            severity=ErrorSeverity.HIGH,
            error_code="CONFIG_ERROR",
            details={"config_key": config_key},
            **kwargs
        )


class DatabaseError(SmartSchemaBuilderError):
    """Database-related errors."""
    
    def __init__(self, message: str, database_type: Optional[str] = None, **kwargs):
        super().__init__(
            message,
            severity=ErrorSeverity.HIGH,
            error_code="DATABASE_ERROR",
            details={"database_type": database_type},
            **kwargs
        )


class ConnectionError(DatabaseError):
    """Database connection errors."""
    
    def __init__(self, message: str, host: Optional[str] = None, port: Optional[int] = None, **kwargs):
        super().__init__(
            message,
            details={"host": host, "port": port},
            **kwargs
        )


class AIProviderError(SmartSchemaBuilderError):
    """AI provider-related errors."""
    
    def __init__(self, message: str, provider: Optional[str] = None, **kwargs):
        super().__init__(
            message,
            severity=ErrorSeverity.MEDIUM,
            error_code="AI_PROVIDER_ERROR",
            details={"provider": provider},
            **kwargs
        )


class FileProcessingError(SmartSchemaBuilderError):
    """File processing errors."""
    
    def __init__(self, message: str, file_path: Optional[str] = None, **kwargs):
        super().__init__(
            message,
            severity=ErrorSeverity.MEDIUM,
            error_code="FILE_PROCESSING_ERROR",
            details={"file_path": file_path},
            **kwargs
        )


class CSVProcessingError(FileProcessingError):
    """CSV file processing errors."""
    
    def __init__(self, message: str, csv_file: Optional[str] = None, **kwargs):
        if csv_file is not None:
            kwargs['details'] = kwargs.get('details', {})
            kwargs['details']['csv_file'] = csv_file
        super().__init__(message, **kwargs)


class SQLGenerationError(SmartSchemaBuilderError):
    """SQL generation errors."""
    
    def __init__(self, message: str, table_name: Optional[str] = None, **kwargs):
        if table_name is not None:
            kwargs['details'] = kwargs.get('details', {})
            kwargs['details']['table_name'] = table_name
        kwargs['severity'] = ErrorSeverity.HIGH
        kwargs['error_code'] = "SQL_GENERATION_ERROR"
        super().__init__(message, **kwargs)


class ValidationError(SmartSchemaBuilderError):
    """Data validation errors."""
    
    def __init__(self, message: str, field: Optional[str] = None, value: Optional[Any] = None, **kwargs):
        if field is not None or value is not None:
            kwargs['details'] = kwargs.get('details', {})
            if field is not None:
                kwargs['details']['field'] = field
            if value is not None:
                kwargs['details']['value'] = value
        kwargs['severity'] = ErrorSeverity.MEDIUM
        kwargs['error_code'] = "VALIDATION_ERROR"
        super().__init__(message, **kwargs)


class NetworkError(SmartSchemaBuilderError):
    """Network-related errors."""
    
    def __init__(self, message: str, url: Optional[str] = None, **kwargs):
        if url is not None:
            kwargs['details'] = kwargs.get('details', {})
            kwargs['details']['url'] = url
        kwargs['severity'] = ErrorSeverity.MEDIUM
        kwargs['error_code'] = "NETWORK_ERROR"
        super().__init__(message, **kwargs)


class RetryableError(SmartSchemaBuilderError):
    """Errors that can be retried."""
    
    def __init__(self, message: str, max_retries: int = 3, **kwargs):
        kwargs['details'] = kwargs.get('details', {})
        kwargs['details']['max_retries'] = max_retries
        kwargs['severity'] = ErrorSeverity.MEDIUM
        kwargs['error_code'] = "RETRYABLE_ERROR"
        super().__init__(message, **kwargs)


class FatalError(SmartSchemaBuilderError):
    """Fatal errors that cannot be recovered from."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            severity=ErrorSeverity.CRITICAL,
            error_code="FATAL_ERROR",
            recoverable=False,
            **kwargs
        )


# Database-specific exceptions
class DatabaseConnectionError(SmartSchemaBuilderError):
    """Database connection error."""
    
    def __init__(self, message: str, host: str = None, port: int = None, database: str = None, **kwargs):
        details = kwargs.get('details', {})
        if host:
            details['host'] = host
        if port:
            details['port'] = port
        if database:
            details['database'] = database
        super().__init__(
            message,
            severity=ErrorSeverity.ERROR,
            error_code="DB_CONNECTION_ERROR",
            details=details,
            **kwargs
        )


class DatabaseQueryError(SmartSchemaBuilderError):
    """Database query execution error."""
    
    def __init__(self, message: str, query: str = None, **kwargs):
        details = kwargs.get('details', {})
        if query:
            details['query'] = query
        super().__init__(
            message,
            severity=ErrorSeverity.ERROR,
            error_code="DB_QUERY_ERROR",
            details=details,
            **kwargs
        )


class DatabaseSchemaError(SmartSchemaBuilderError):
    """Database schema-related error."""
    
    def __init__(self, message: str, table_name: str = None, column_name: str = None, **kwargs):
        details = kwargs.get('details', {})
        if table_name:
            details['table_name'] = table_name
        if column_name:
            details['column_name'] = column_name
        super().__init__(
            message,
            severity=ErrorSeverity.ERROR,
            error_code="DB_SCHEMA_ERROR",
            details=details,
            **kwargs
        )
