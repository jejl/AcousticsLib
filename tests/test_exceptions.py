"""Tests for acousticslib.exceptions — hierarchy, message, and isinstance relationships."""
import pytest

from acousticslib.exceptions import (
    AcousticsError,
    AmbiguousObservationError,
    AuthenticationError,
    AuthorizationError,
    ConfigurationError,
    DatabaseError,
    DuplicateError,
    FileOperationError,
    NotFoundError,
    RecorderNotFoundError,
    UserNotFoundError,
    ValidationError,
    WavMetadataError,
)


class TestExceptionHierarchy:
    def test_all_are_acoustics_error(self):
        for cls in (
            DatabaseError, DuplicateError, NotFoundError, RecorderNotFoundError,
            UserNotFoundError, ValidationError, AuthenticationError,
            AuthorizationError, ConfigurationError, FileOperationError,
            WavMetadataError, AmbiguousObservationError,
        ):
            assert issubclass(cls, AcousticsError)

    def test_duplicate_is_database_error(self):
        assert issubclass(DuplicateError, DatabaseError)

    def test_not_found_is_database_error(self):
        assert issubclass(NotFoundError, DatabaseError)

    def test_recorder_not_found_is_not_found(self):
        assert issubclass(RecorderNotFoundError, NotFoundError)

    def test_user_not_found_is_not_found(self):
        assert issubclass(UserNotFoundError, NotFoundError)

    def test_ambiguous_observation_is_wav_metadata_error(self):
        assert issubclass(AmbiguousObservationError, WavMetadataError)

    def test_wav_metadata_is_acoustics_error_not_database(self):
        assert issubclass(WavMetadataError, AcousticsError)
        assert not issubclass(WavMetadataError, DatabaseError)


class TestExceptionMessages:
    def test_message_is_preserved(self):
        exc = DatabaseError("something went wrong")
        assert str(exc) == "something went wrong"

    def test_can_be_raised_and_caught_as_base(self):
        with pytest.raises(AcousticsError):
            raise DuplicateError("duplicate")

    def test_can_be_raised_and_caught_as_database_error(self):
        with pytest.raises(DatabaseError):
            raise NotFoundError("not found")

    def test_recorder_not_found_caught_as_not_found(self):
        with pytest.raises(NotFoundError):
            raise RecorderNotFoundError("recorder 42 not found")

    def test_ambiguous_observation_caught_as_wav_metadata_error(self):
        with pytest.raises(WavMetadataError):
            raise AmbiguousObservationError("two windows matched")
