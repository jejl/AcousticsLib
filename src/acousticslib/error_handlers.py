"""Error handling decorators for repository and service layers.

These decorators are framework-agnostic. UI-specific handlers (e.g. those that
call st.error()) must be implemented in each Streamlit application using the
exception types from acousticslib.exceptions.
"""
import functools
from typing import Any, Callable, Optional, Tuple

from loguru import logger

from .exceptions import (
    AcousticsError,
    AuthenticationError,
    AuthorizationError,
    DatabaseError,
    DuplicateError,
    NotFoundError,
    ValidationError,
)


def handle_repository_errors(func: Callable) -> Callable:
    """Decorator for repository methods that converts DB exceptions to typed errors.

    Inspects the exception message to distinguish duplicate-key errors,
    FK constraint violations, and generic database failures.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (DuplicateError, NotFoundError, DatabaseError):
            raise
        except Exception as exc:
            msg = str(exc).lower()
            if "duplicate" in msg or "unique" in msg:
                logger.error(f"Duplicate error in {func.__name__}: {exc}")
                raise DuplicateError(f"Record already exists: {exc}") from exc
            if "foreign key" in msg or "constraint" in msg:
                logger.error(f"Constraint violation in {func.__name__}: {exc}")
                raise DatabaseError(f"Database constraint violation: {exc}") from exc
            logger.error(f"Database error in {func.__name__}: {exc}")
            raise DatabaseError(f"Database operation failed: {exc}") from exc

    return wrapper


def handle_service_errors(user_message: str = "Operation failed") -> Callable:
    """Decorator for service methods that returns a standardised (success, msg, data) tuple.

    The decorated function may return any value; if it does not already return a
    3-tuple the result is wrapped as ``(True, "Operation successful", result)``.

    On error the tuple is always ``(False, str, None)``.  The ``user_message``
    parameter is used for unexpected (non-AcousticsError) exceptions so that
    internal details are not exposed to callers.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Tuple[bool, str, Optional[Any]]:
            try:
                result = func(*args, **kwargs)
                if isinstance(result, tuple):
                    return result
                return True, "Operation successful", result

            except ValidationError as exc:
                logger.warning(f"Validation error in {func.__name__}: {exc}")
                return False, str(exc), None

            except DuplicateError as exc:
                logger.warning(f"Duplicate error in {func.__name__}: {exc}")
                return False, str(exc), None

            except NotFoundError as exc:
                logger.warning(f"Not found in {func.__name__}: {exc}")
                return False, str(exc), None

            except DatabaseError as exc:
                logger.error(f"Database error in {func.__name__}: {exc}")
                return False, "Database error occurred. Please try again.", None

            except AuthenticationError as exc:
                logger.warning(f"Authentication error in {func.__name__}: {exc}")
                return False, str(exc), None

            except AuthorizationError as exc:
                logger.warning(f"Authorization error in {func.__name__}: {exc}")
                return False, str(exc), None

            except AcousticsError as exc:
                logger.error(f"Application error in {func.__name__}: {exc}")
                return False, str(exc), None

            except Exception as exc:
                logger.exception(f"Unexpected error in {func.__name__}: {exc}")
                return False, user_message, None

        return wrapper
    return decorator


def log_and_return_error(
    logger_func: Callable,
    message: str,
    exception: Exception,
) -> Tuple[bool, str, None]:
    """Log an error and return a standard ``(False, message, None)`` tuple.

    Useful inside service methods that need to bail out with an error without
    raising, for example after detecting a validation problem in a loop.
    """
    logger_func(f"{message}: {exception}")
    return False, message, None
