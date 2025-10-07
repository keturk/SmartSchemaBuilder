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
import time
import functools
from typing import Callable, Any, Optional, Type, Union, List
from contextlib import contextmanager

from common.exceptions import (
    SmartSchemaBuilderError, 
    RetryableError, 
    FatalError,
    ErrorSeverity
)


class ErrorHandler:
    """Comprehensive error handling utility."""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        self.error_counts = {}
        self.max_errors_per_type = 10
    
    def handle_error(
        self, 
        error: Exception, 
        context: Optional[str] = None,
        fallback_value: Any = None,
        reraise: bool = True
    ) -> Any:
        """
        Handle an error with appropriate logging and recovery.
        
        Args:
            error: The exception to handle
            context: Additional context about where the error occurred
            fallback_value: Value to return if error is handled gracefully
            reraise: Whether to reraise the exception after handling
            
        Returns:
            The fallback value if error is handled gracefully, None otherwise
        """
        error_type = type(error).__name__
        error_key = f"{error_type}:{context or 'unknown'}"
        
        # Track error frequency
        self.error_counts[error_key] = self.error_counts.get(error_key, 0) + 1
        
        # Log the error with context
        log_message = f"Error in {context or 'unknown context'}: {str(error)}"
        
        if isinstance(error, SmartSchemaBuilderError):
            # Use the error's own severity
            severity = error.severity
            if severity == ErrorSeverity.CRITICAL:
                self.logger.critical(log_message)
            elif severity == ErrorSeverity.HIGH:
                self.logger.error(log_message)
            elif severity == ErrorSeverity.MEDIUM:
                self.logger.warning(log_message)
            else:
                self.logger.info(log_message)
        else:
            # Default to error level for unknown exceptions
            self.logger.error(log_message)
        
        # Check if we should stop due to too many errors
        if self.error_counts[error_key] > self.max_errors_per_type:
            self.logger.critical(f"Too many {error_type} errors ({self.error_counts[error_key]}). Stopping.")
            raise FatalError(f"Too many {error_type} errors. Process terminated.")
        
        # Handle recoverable errors
        if isinstance(error, SmartSchemaBuilderError) and error.recoverable:
            self.logger.info(f"Error is recoverable. Using fallback value: {fallback_value}")
            return fallback_value
        
        # Reraise if requested
        if reraise:
            raise error
        
        return fallback_value
    
    def retry_on_failure(
        self,
        max_retries: int = 3,
        delay: float = 1.0,
        backoff_factor: float = 2.0,
        exceptions: tuple = (Exception,)
    ):
        """
        Decorator to retry a function on failure.
        
        Args:
            max_retries: Maximum number of retry attempts
            delay: Initial delay between retries in seconds
            backoff_factor: Multiplier for delay between retries
            exceptions: Tuple of exception types to retry on
        """
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                last_exception = None
                current_delay = delay
                
                for attempt in range(max_retries + 1):
                    try:
                        return func(*args, **kwargs)
                    except exceptions as e:
                        last_exception = e
                        
                        if attempt == max_retries:
                            self.logger.error(f"Function {func.__name__} failed after {max_retries} retries: {e}")
                            raise e
                        
                        self.logger.warning(
                            f"Function {func.__name__} failed (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                            f"Retrying in {current_delay:.1f} seconds..."
                        )
                        
                        time.sleep(current_delay)
                        current_delay *= backoff_factor
                
                # This should never be reached, but just in case
                raise last_exception
            
            return wrapper
        return decorator
    
    def safe_execute(
        self,
        func: Callable,
        *args,
        fallback_value: Any = None,
        context: Optional[str] = None,
        **kwargs
    ) -> Any:
        """
        Safely execute a function with error handling.
        
        Args:
            func: Function to execute
            *args: Positional arguments for the function
            fallback_value: Value to return if function fails
            context: Context description for error logging
            **kwargs: Keyword arguments for the function
            
        Returns:
            Function result or fallback value if error occurs
        """
        try:
            return func(*args, **kwargs)
        except Exception as e:
            return self.handle_error(e, context, fallback_value, reraise=False)
    
    @contextmanager
    def error_context(self, context: str):
        """
        Context manager for error handling with automatic context.
        
        Args:
            context: Description of the operation being performed
        """
        self.logger.debug(f"Starting operation: {context}")
        try:
            yield
            self.logger.debug(f"Completed operation: {context}")
        except Exception as e:
            self.logger.error(f"Failed operation: {context} - {e}")
            raise
    
    def validate_input(
        self,
        value: Any,
        validator: Callable[[Any], bool],
        error_message: str,
        field_name: Optional[str] = None
    ) -> Any:
        """
        Validate input with custom validator.
        
        Args:
            value: Value to validate
            validator: Function that returns True if value is valid
            error_message: Error message if validation fails
            field_name: Name of the field being validated
            
        Returns:
            The validated value
            
        Raises:
            ValidationError: If validation fails
        """
        if not validator(value):
            from common.exceptions import ValidationError
            raise ValidationError(
                f"{error_message} (field: {field_name or 'unknown'})",
                field=field_name,
                value=value
            )
        return value
    
    def get_error_summary(self) -> dict:
        """Get a summary of errors encountered."""
        return {
            "total_errors": sum(self.error_counts.values()),
            "error_types": len(self.error_counts),
            "error_breakdown": dict(self.error_counts)
        }


# Global error handler instance
error_handler = ErrorHandler()


def handle_errors(
    fallback_value: Any = None,
    context: Optional[str] = None,
    reraise: bool = True
):
    """
    Decorator for automatic error handling.
    
    Args:
        fallback_value: Value to return if function fails
        context: Context description for error logging
        reraise: Whether to reraise exceptions after handling
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                return error_handler.handle_error(
                    e, 
                    context or func.__name__, 
                    fallback_value, 
                    reraise
                )
        return wrapper
    return decorator


def retry_on_failure(
    max_retries: int = 3,
    delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,)
):
    """
    Decorator for retrying functions on failure.
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff_factor: Multiplier for delay between retries
        exceptions: Tuple of exception types to retry on
    """
    return error_handler.retry_on_failure(
        max_retries, delay, backoff_factor, exceptions
    )


def safe_execute(
    func: Callable,
    *args,
    fallback_value: Any = None,
    context: Optional[str] = None,
    **kwargs
) -> Any:
    """
    Safely execute a function with error handling.
    
    Args:
        func: Function to execute
        *args: Positional arguments for the function
        fallback_value: Value to return if function fails
        context: Context description for error logging
        **kwargs: Keyword arguments for the function
        
    Returns:
        Function result or fallback value if error occurs
    """
    return error_handler.safe_execute(
        func, *args, fallback_value=fallback_value, context=context, **kwargs
    )
