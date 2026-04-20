# AcousticsLib

Shared Python library for managing CallTrackers logistics, hardware 
maintanance, observations, data analysis and reporting. Consumed by 
**CallTrackersAdmin** (Streamlit admin app) and **SoundClass** 
(Streamlit classification app), both of which install it from this git 
repository. The library will also be used to process data from multi-microphone 
array recordings.

## Package layout

```
src/acousticslib/
├── audio/              WAV metadata extraction, DSP filters, spectrogram generation, file I/O
├── database/
│   ├── connection.py   SQLAlchemy session factory (get_session)
│   └── repositories/   One repository class per table group (see below)
├── processing/         Signal processing: FFT, correlation, direction-of-arrival, hardware loaders
├── email.py            SMTP email helpers
├── error_handlers.py   @handle_repository_errors / @handle_service_errors decorators
├── exceptions.py       Exception hierarchy (AcousticsError and subclasses)
├── files.py            File utility helpers
├── geography.py        NatureTrackers grid square lookup (Tasmania shapefile)
├── password_reset.py   Password-reset token generation and verification
├── password_validation.py  Strength rules (10+ chars, upper, lower, digit, symbol)
└── time_utils.py       Hobart-timezone helpers, GUANO timestamp parsing
```

## Repositories

All SQL lives in `database/repositories/`. Each class is a collection of `@staticmethod` methods decorated with `@handle_repository_errors`. Import from the package level:

```python
from acousticslib.database.repositories import (
    RecorderRepository, ObservationRepository, MetadataRepository,
    ResultsRepository, KitRepository, UserRepository, ...
)
```

| Repository | Table(s) |
|---|---|
| `RecorderRepository` | `Recorder` |
| `PeopleRepository`, `OwnerRepository`, `CustodianRepository` | `People`, `Owner`, `Custodian` |
| `KitRepository`, `KitRecorderRepository`, `KitCustodianRepository` | `Kit`, `KitRecorder`, `KitCustodian` |
| `KitMaintenanceRepository` | `KitMaintenance`, `KitMaintenanceTask` |
| `ServiceNotesRepository` | `ServiceNotes` |
| `UserRepository` | `users` |
| `ObservationRepository` | `LocationLog` |
| `MetadataRepository` | `Metadata` |
| `ResultsRepository` | `ResultsBats`, `ResultsBitterns`, `ResultsCurlews` |
| `ClassifierStatusRepository`, `DataAvailabilityRepository`, `ObservingProgramsRepository` | lookup tables |
| `SoundClassificationRepository`, `SoundClassConfigRepository`, … | SoundClass classification tables |
| `CallLibraryRepository`, `SoundClassClassifierRepository` | call library, classifier config |

`ResultsRepository` validates table names against `_TABLE_ALLOWLIST` before use — do not add dynamic table methods without the same check.

## Development

### Environment

```bash
cd /path/to/AcousticsLib
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"   # or: uv sync
```

### Tests

```bash
pytest tests/ -q
```

Tests use `unittest.mock` to patch repository methods; they do not require a live database.

### Updating downstream consumers

Both CallTrackersAdmin and SoundClass install acousticslib via `uv` from git. After pushing changes here:

```bash
# In CallTrackersAdmin or SoundClass repo root:
uv sync --upgrade-package acousticslib
```

## Error handling

- **Repositories** raise `DatabaseError`, `DuplicateError`, `NotFoundError` (all subclass `AcousticsError`). The `@handle_repository_errors` decorator catches unexpected exceptions and re-raises them as `DatabaseError`.
- **Services** in downstream apps wrap methods with `@handle_service_errors("message")` → always returns `(bool, str, Any)`.
