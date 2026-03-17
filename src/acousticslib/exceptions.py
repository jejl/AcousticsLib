"""Custom exception types for acousticslib.

Hierarchy:
    AcousticsError
    ├── DatabaseError
    │   ├── DuplicateError
    │   └── NotFoundError
    │       ├── RecorderNotFoundError
    │       └── UserNotFoundError
    ├── ValidationError
    ├── AuthenticationError
    ├── AuthorizationError
    ├── ConfigurationError
    ├── FileOperationError
    └── WavMetadataError
        └── AmbiguousObservationError

Note: UI-specific error handlers (e.g. Streamlit st.error() integration) are
intentionally excluded from this shared library and should be implemented in
each application that uses it.
"""


class AcousticsError(Exception):
    """Base exception for all acousticslib errors."""


class DatabaseError(AcousticsError):
    """Database operation failed."""


class DuplicateError(DatabaseError):
    """Record already exists (duplicate constraint violation)."""


class NotFoundError(DatabaseError):
    """Requested resource not found."""


class RecorderNotFoundError(NotFoundError):
    """Recorder not found in database."""


class UserNotFoundError(NotFoundError):
    """User not found in database."""


class ValidationError(AcousticsError):
    """Data validation failed."""


class AuthenticationError(AcousticsError):
    """Authentication failed."""


class AuthorizationError(AcousticsError):
    """User not authorized to perform this action."""


class ConfigurationError(AcousticsError):
    """Application configuration error."""


class FileOperationError(AcousticsError):
    """File operation failed."""


class WavMetadataError(AcousticsError):
    """WAV file metadata could not be extracted."""


class AmbiguousObservationError(WavMetadataError):
    """A timestamp matched multiple observation windows."""
