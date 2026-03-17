"""acousticslib — shared library for acoustic data management.

Public API:
    Exceptions:     AcousticsError, WavMetadataError, AmbiguousObservationError, ...
    Time utilities: localize_hobart, parse_guano_timestamp, match_observation_window
    Audio metadata: WavMetadata, read_wav_metadata
    Processing:     acousticslib.processing.{hardware, fft, doa, correlation}
"""

from .exceptions import (
    AcousticsError,
    DatabaseError,
    ValidationError,
    NotFoundError,
    DuplicateError,
    AuthenticationError,
    AuthorizationError,
    ConfigurationError,
    FileOperationError,
    RecorderNotFoundError,
    UserNotFoundError,
    WavMetadataError,
    AmbiguousObservationError,
)
from .time_utils import localize_hobart, parse_guano_timestamp, match_observation_window
from .audio.metadata import WavMetadata, read_wav_metadata

__all__ = [
    # Exceptions
    "AcousticsError",
    "DatabaseError",
    "ValidationError",
    "NotFoundError",
    "DuplicateError",
    "AuthenticationError",
    "AuthorizationError",
    "ConfigurationError",
    "FileOperationError",
    "RecorderNotFoundError",
    "UserNotFoundError",
    "WavMetadataError",
    "AmbiguousObservationError",
    # Time utilities
    "localize_hobart",
    "parse_guano_timestamp",
    "match_observation_window",
    # Audio metadata
    "WavMetadata",
    "read_wav_metadata",
]
