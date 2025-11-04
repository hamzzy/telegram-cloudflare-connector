"""
Error classification system for distinguishing between different error types.
"""
from enum import Enum
from typing import Optional


class ErrorType(Enum):
    """Classification of error types."""
    RETRYABLE = "retryable"  # Network, transient DB errors
    FATAL = "fatal"  # Authentication, validation errors
    RATE_LIMIT = "rate_limit"  # API rate limiting
    TIMEOUT = "timeout"  # Request timeouts
    CONNECTION = "connection"  # Connection errors


class CategorizedError(Exception):
    """Exception with error classification."""
    
    def __init__(self, message: str, error_type: ErrorType, original_error: Optional[Exception] = None):
        super().__init__(message)
        self.error_type = error_type
        self.original_error = original_error
    
    def is_retryable(self) -> bool:
        """Check if this error should be retried."""
        return self.error_type in [ErrorType.RETRYABLE, ErrorType.RATE_LIMIT, ErrorType.CONNECTION, ErrorType.TIMEOUT]


def classify_error(error: Exception) -> ErrorType:
    """
    Classify an error based on its type and message.
    
    Args:
        error: The exception to classify
        
    Returns:
        ErrorType classification
    """
    error_str = str(error).lower()
    error_type = type(error).__name__
    
    # Rate limiting errors
    if any(term in error_str for term in ["rate limit", "floodwait", "too many requests", "429"]):
        return ErrorType.RATE_LIMIT
    
    # Connection errors
    if any(term in error_str for term in ["connection", "timeout", "network", "unreachable"]):
        return ErrorType.CONNECTION
    
    if error_type in ["ConnectionError", "TimeoutError", "OSError"]:
        return ErrorType.CONNECTION
    
    # Database errors
    if "psycopg2" in error_type or "database" in error_str:
        # Transient DB errors are retryable
        if any(term in error_str for term in ["connection", "timeout", "broken", "closed"]):
            return ErrorType.RETRYABLE
        # Other DB errors might be fatal
        return ErrorType.FATAL
    
    # Telegram API errors
    if "telethon" in error_type.lower() or "telegram" in error_str:
        if "auth" in error_str or "unauthorized" in error_str:
            return ErrorType.FATAL
        if "flood" in error_str or "wait" in error_str:
            return ErrorType.RATE_LIMIT
    
    # Validation errors are fatal
    if "validation" in error_str or "schema" in error_str:
        return ErrorType.FATAL
    
    # Default to retryable for unknown errors (network issues, etc.)
    return ErrorType.RETRYABLE

