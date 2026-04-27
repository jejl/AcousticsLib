"""acousticslib.database.repositories — one module per table group."""
from .docs import DocsRepository
from .kit import KitRepository, KitRecorderRepository, KitCustodianRepository
from .kit_maintenance import KitMaintenanceRepository
from .metadata import MetadataRepository
from .observation import ObservationRepository
from .people import PeopleRepository, OwnerRepository, CustodianRepository
from .recorder import RecorderRepository
from .reference import ClassifierStatusRepository, DataAvailabilityRepository, ObservingProgramsRepository
from .results import ResultsRepository
from .service_notes import ServiceNotesRepository
from .soundclass import (
    SoundClassConfigRepository,
    SoundClassUserConfigRepository,
    SoundClassCategoryRepository,
    SoundClassificationRepository,
    CallLibraryRepository,
    SoundClassClassifierRepository,
)
from .users import UserRepository
from .weather import WeatherRepository

__all__ = [
    "CallLibraryRepository",
    "DocsRepository",
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
    "WeatherRepository",
]
