"""Tests for acousticslib.error_handlers decorators."""
import pytest

from acousticslib.error_handlers import (
    handle_repository_errors,
    handle_service_errors,
    log_and_return_error,
)
from acousticslib.exceptions import (
    AcousticsError,
    AuthenticationError,
    AuthorizationError,
    DatabaseError,
    DuplicateError,
    NotFoundError,
    ValidationError,
)


# ---------------------------------------------------------------------------
# handle_repository_errors
# ---------------------------------------------------------------------------

class TestHandleRepositoryErrors:
    def test_passes_through_return_value(self):
        @handle_repository_errors
        def fn():
            return 42
        assert fn() == 42

    def test_reraises_duplicate_error_unchanged(self):
        @handle_repository_errors
        def fn():
            raise DuplicateError("already exists")
        with pytest.raises(DuplicateError, match="already exists"):
            fn()

    def test_reraises_not_found_error_unchanged(self):
        @handle_repository_errors
        def fn():
            raise NotFoundError("missing")
        with pytest.raises(NotFoundError):
            fn()

    def test_reraises_database_error_unchanged(self):
        @handle_repository_errors
        def fn():
            raise DatabaseError("db down")
        with pytest.raises(DatabaseError, match="db down"):
            fn()

    def test_converts_duplicate_keyword_exception(self):
        @handle_repository_errors
        def fn():
            raise Exception("Duplicate entry 'x' for key 'PRIMARY'")
        with pytest.raises(DuplicateError):
            fn()

    def test_converts_unique_keyword_exception(self):
        @handle_repository_errors
        def fn():
            raise Exception("UNIQUE constraint failed: users.username")
        with pytest.raises(DuplicateError):
            fn()

    def test_converts_foreign_key_exception(self):
        @handle_repository_errors
        def fn():
            raise Exception("foreign key constraint fails")
        with pytest.raises(DatabaseError):
            fn()

    def test_converts_generic_exception_to_database_error(self):
        @handle_repository_errors
        def fn():
            raise RuntimeError("connection refused")
        with pytest.raises(DatabaseError):
            fn()

    def test_preserves_function_name(self):
        @handle_repository_errors
        def my_repository_method():
            pass
        assert my_repository_method.__name__ == "my_repository_method"


# ---------------------------------------------------------------------------
# handle_service_errors
# ---------------------------------------------------------------------------

class TestHandleServiceErrors:
    def test_wraps_plain_return_in_tuple(self):
        @handle_service_errors("failed")
        def fn():
            return [1, 2, 3]
        ok, msg, data = fn()
        assert ok is True
        assert data == [1, 2, 3]

    def test_passes_through_tuple_unchanged(self):
        @handle_service_errors("failed")
        def fn():
            return True, "all good", {"key": "value"}
        ok, msg, data = fn()
        assert ok is True
        assert msg == "all good"
        assert data == {"key": "value"}

    def test_returns_false_on_validation_error(self):
        @handle_service_errors("failed")
        def fn():
            raise ValidationError("name too short")
        ok, msg, data = fn()
        assert ok is False
        assert "name too short" in msg
        assert data is None

    def test_returns_false_on_duplicate_error(self):
        @handle_service_errors("failed")
        def fn():
            raise DuplicateError("already exists")
        ok, msg, data = fn()
        assert ok is False
        assert data is None

    def test_returns_false_on_not_found_error(self):
        @handle_service_errors("failed")
        def fn():
            raise NotFoundError("not found")
        ok, msg, data = fn()
        assert ok is False

    def test_returns_generic_message_on_database_error(self):
        @handle_service_errors("operation failed")
        def fn():
            raise DatabaseError("internal detail")
        ok, msg, data = fn()
        assert ok is False
        assert "internal detail" not in msg   # DB details hidden from caller
        assert "Database error" in msg

    def test_returns_user_message_on_unexpected_exception(self):
        @handle_service_errors("custom failure message")
        def fn():
            raise RuntimeError("something unexpected")
        ok, msg, data = fn()
        assert ok is False
        assert msg == "custom failure message"

    def test_returns_false_on_authentication_error(self):
        @handle_service_errors("failed")
        def fn():
            raise AuthenticationError("bad credentials")
        ok, msg, _ = fn()
        assert ok is False
        assert "bad credentials" in msg

    def test_returns_false_on_authorization_error(self):
        @handle_service_errors("failed")
        def fn():
            raise AuthorizationError("not allowed")
        ok, msg, _ = fn()
        assert ok is False

    def test_preserves_function_name(self):
        @handle_service_errors("failed")
        def my_service_method():
            return True, "ok", None
        assert my_service_method.__name__ == "my_service_method"

    def test_none_return_wrapped(self):
        @handle_service_errors("failed")
        def fn():
            return None
        ok, msg, data = fn()
        assert ok is True
        assert data is None


# ---------------------------------------------------------------------------
# log_and_return_error
# ---------------------------------------------------------------------------

class TestLogAndReturnError:
    def test_returns_false_tuple(self):
        ok, msg, data = log_and_return_error(
            lambda m: None, "something failed", Exception("detail")
        )
        assert ok is False
        assert msg == "something failed"
        assert data is None

    def test_calls_logger_function(self):
        calls = []
        log_and_return_error(calls.append, "error msg", Exception("exc"))
        assert len(calls) == 1
        assert "error msg" in calls[0]
