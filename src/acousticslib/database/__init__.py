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

__all__ = ["get_session"]
