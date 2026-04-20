"""acousticslib.database — SQLAlchemy database layer.

Provides:
    get_session()   Context manager yielding a scoped SQLAlchemy Session.
                    Use this in all repository methods.

Repositories (one module per table group):
    recorder        Recorder CRUD
    observation     LocationLog (observations) with tz-aware datetimes
    results         ResultsBats / ResultsBitterns / ResultsCurlews (with allowlist)
    kit             Kit, KitRecorder, KitCustodian
    people          People, Owner, Custodian
    service_notes   ServiceNotes
    users           Users (bcrypt auth)
    reference       ClassifierStatus, DataAvailabilityStatus (lookup tables)
"""

from .connection import get_session
from .repositories import (
    CallLibraryRepository,
    ClassifierStatusRepository,
    CustodianRepository,
    DataAvailabilityRepository,
    KitCustodianRepository,
    KitMaintenanceRepository,
    KitRecorderRepository,
    KitRepository,
    MetadataRepository,
    ObservationRepository,
    ObservingProgramsRepository,
    OwnerRepository,
    PeopleRepository,
    RecorderRepository,
    ResultsRepository,
    ServiceNotesRepository,
    SoundClassCategoryRepository,
    SoundClassClassifierRepository,
    SoundClassConfigRepository,
    SoundClassUserConfigRepository,
    SoundClassificationRepository,
    UserRepository,
)

__all__ = [
    "CallLibraryRepository",
    "ClassifierStatusRepository",
    "CustodianRepository",
    "DataAvailabilityRepository",
    "KitCustodianRepository",
    "KitMaintenanceRepository",
    "KitRecorderRepository",
    "KitRepository",
    "MetadataRepository",
    "ObservationRepository",
    "ObservingProgramsRepository",
    "OwnerRepository",
    "PeopleRepository",
    "RecorderRepository",
    "ResultsRepository",
    "ServiceNotesRepository",
    "SoundClassCategoryRepository",
    "SoundClassClassifierRepository",
    "SoundClassConfigRepository",
    "SoundClassUserConfigRepository",
    "SoundClassificationRepository",
    "UserRepository",
    "get_session",
]
