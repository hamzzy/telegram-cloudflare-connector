"""
Retry logic with exponential backoff using tenacity library.
"""
import asyncio
import logging
import re
from typing import Callable, TypeVar, Optional
from tenacity import (
    stop_after_attempt,
    retry_if_exception,
    before_sleep_log,
    after_log,
    AsyncRetrying,
    Retrying
)
from errors import ErrorType, classify_error

T = TypeVar('T')
logger = logging.getLogger(__name__)


def _extract_wait_time(exception: Exception) -> Optional[float]:
    """Extract wait time from rate limit error messages or exception attributes."""
    # Check if exception has seconds attribute (Telethon FloodWaitError)
    if hasattr(exception, 'seconds'):
        wait_seconds = float(exception.seconds)
        # Add small buffer
        return wait_seconds + 1
    
    # Check error message for patterns
    error_message = str(exception).lower()
    patterns = [
        r'wait\s+(\d+)\s+seconds?',
        r'floodwait\s+(\d+)',
        r'(\d+)\s+seconds?',
        r'flood.*wait.*?(\d+)',
        r'rate.*limit.*?(\d+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, error_message)
        if match:
            wait_seconds = int(match.group(1))
            # Add small buffer
            return float(wait_seconds + 1)
    
    return None


def _should_retry(exception: Exception, error_type_filter: Optional[ErrorType] = None) -> bool:
    """Determine if an exception should be retried."""
    error_type = classify_error(exception)
    
    # Check if we should retry this error
    if error_type_filter and error_type != error_type_filter:
        return False
    
    # Check if error type is retryable
    if hasattr(error_type, 'is_retryable'):
        return error_type.is_retryable()
    
    # Default: only retry retryable error types
    return error_type in [ErrorType.RETRYABLE, ErrorType.RATE_LIMIT, ErrorType.CONNECTION, ErrorType.TIMEOUT]


class RateLimitError(Exception):
    """Special exception for rate limit errors that need custom wait time."""
    def __init__(self, message: str, wait_time: Optional[float] = None):
        super().__init__(message)
        self.wait_time = wait_time


def retry_with_backoff(
    func: Callable,
    max_retries: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    backoff_factor: float = 2.0,
    error_type_filter: Optional[ErrorType] = None
):
    """
    Retry a function with exponential backoff using tenacity.
    
    Args:
        func: Function (sync or async) to retry
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
        backoff_factor: Multiplier for exponential backoff (not used with tenacity, kept for compatibility)
        error_type_filter: Only retry if error matches this type (None = retry all retryable errors)
        
    Returns:
        Result of the function call
        
    Raises:
        The last exception if all retries fail
    """
    def retry_condition(exception):
        """Check if exception should be retried."""
        if isinstance(exception, RateLimitError):
            return True
        return _should_retry(exception, error_type_filter)
    
    def wait_func(retry_state):
        """Custom wait function that handles rate limits."""
        exception = retry_state.outcome.exception()
        if exception and isinstance(exception, RateLimitError) and exception.wait_time:
            wait_time = min(exception.wait_time, max_delay)
            logger.warning(f"Rate limit detected. Waiting {wait_time:.1f}s...")
            return wait_time
        # Use exponential backoff for other retries
        return min(initial_delay * (2 ** retry_state.attempt_number), max_delay)
    
    # Create retry configuration
    retry_config = {
        'stop': stop_after_attempt(max_retries + 1),
        'wait': wait_func,
        'retry': retry_if_exception(retry_condition),
        'before_sleep': before_sleep_log(logger, logging.WARNING),
        'after': after_log(logger, logging.ERROR),
        'reraise': True
    }
    
    # Wrap the function and handle errors
    def wrap_func():
        try:
            return func()
        except Exception as e:
            error_type = classify_error(e)
            if error_type == ErrorType.RATE_LIMIT:
                wait_time = _extract_wait_time(e)
                if wait_time:
                    raise RateLimitError(str(e), wait_time)
            raise
    
    # Handle async vs sync
    if asyncio.iscoroutinefunction(func):
        async def async_wrapper():
            async def wrapped():
                try:
                    return await func()
                except Exception as e:
                    error_type = classify_error(e)
                    if error_type == ErrorType.RATE_LIMIT:
                        wait_time = _extract_wait_time(e)
                        if wait_time:
                            raise RateLimitError(str(e), wait_time)
                    raise
            
            async for attempt in AsyncRetrying(**retry_config):
                with attempt:
                    return await wrapped()
        
        return async_wrapper()
    else:
        for attempt in Retrying(**retry_config):
            with attempt:
                return wrap_func()

