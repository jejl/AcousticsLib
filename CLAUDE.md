# CallTrackersLib — Claude Code Context

## What this project is

CallTrackersLib is the shared Python library for the CallTrackers bat acoustic monitoring project. It provides database repositories, audio processing, geography, email, and utility functions consumed by CallTrackersAdmin and CallTrackersProcessing.

**Formerly called:** CallTrackersLib / CallTrackersLib. References to the old name in comments, docs, or git history are stale — the library is now `calltrackerslib`.

## Package layout

```
src/calltrackerslib/
├── audio/
│   ├── io.py           WAV file I/O helpers
│   ├── metadata.py     GUANO metadata extraction from WAV headers
│   ├── filters.py      DSP filter utilities
│   └── spectrograms.py Spectrogram generation
├── database/
│   ├── connection.py   SQLAlchemy session factory — get_session()
│   └── repositories/   One repository class per table group (see below)
├── processing/
│   ├── fft.py          FFT and spectral processing helpers
│   ├── correlation.py  Cross-correlation utilities
│   ├── doa.py          Direction-of-arrival estimation
│   └── hardware.py     Hardware-specific data loaders
├── email.py            SMTP email helpers
├── error_handlers.py   @handle_repository_errors / @handle_service_errors decorators
├── exceptions.py       Exception hierarchy (CallTrackersError and subclasses)
├── files.py            File utility helpers
├── geography.py        NatureTrackers grid square lookup (Tasmania MGA Zone 55 shapefile)
├── password_reset.py   Token generation and verification
├── password_validation.py  Strength rules (10+ chars, upper, lower, digit, symbol)
└── time_utils.py       Hobart-timezone helpers, GUANO timestamp parsing
```

## Repositories

All SQL lives in `database/repositories/`. Each class is a collection of `@staticmethod` methods decorated with `@handle_repository_errors`. Import from the package level:

```python
from calltrackerslib.database.repositories import (
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
| `RecorderNoiseRepository` | `RecorderNoise` |
| `SDCardRepository` | SD card registration |
| `WeatherRepository` | weather data |
| `SoundClassificationRepository`, `SoundClassConfigRepository`, `SoundClassUserConfigRepository`, `SoundClassCategoryRepository` | SoundClass tables |
| `CallLibraryRepository`, `SoundClassClassifierRepository` | call library, classifier config |
| `ClassifierRegistryRepository`, `ClassifierTypeRepository` | classifier registry |
| `DocsRepository` | in-app documentation sections |

**SQL injection guard:** `ResultsRepository` validates `table_name` against `_TABLE_ALLOWLIST = {"ResultsBats", "ResultsBitterns", "ResultsCurlews"}`. Do not add dynamic table methods without this check.

## Error handling pattern

- **Repositories** raise `DatabaseError`, `DuplicateError`, `NotFoundError` (all subclass `CallTrackersError`)
- `@handle_repository_errors` catches unexpected exceptions and re-raises as `DatabaseError`
- **Downstream services** (in CallTrackersAdmin) use `@handle_service_errors("message")` → always returns `(bool, str, Any)`

## Running tests

```bash
cd /Users/jlovell/Library/CloudStorage/Dropbox/Projects/Shared/CallTrackersLib
.venv/bin/python -m pytest tests/ -q
```

Tests use `unittest.mock` to patch repository methods — no live database required.

## Development and updating downstream consumers

```bash
# After pushing changes here, in each consumer repo:
cd /path/to/CallTrackersAdmin   # or CallTrackersProcessing
uv sync --upgrade-package calltrackerslib
```

Both CallTrackersAdmin and CallTrackersProcessing install this library via `uv` from git.

## Related projects

- **CallTrackersAdmin** (`Dropbox/Projects/Current/CallTrackers/Software/CallTrackersAdmin`) — primary consumer; Streamlit web app
- **CallTrackersProcessing** (`Dropbox/Projects/Current/CallTrackers/Software/CallTrackersProcessing`) — pipeline scripts; newer scripts use this lib, older ones use `db_utils.py` directly
- **AcousticArrayLib** (`Dropbox/Projects/Current/RecorderPrototype/Software/AcousticArrayLib`) — independent; the `processing/` subpackage here (FFT, correlation, DOA) overlaps in domain but the two libraries are not yet integrated
