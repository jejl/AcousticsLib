"""SQLAlchemy engine and session management.

The engine is created lazily on first use so that importing acousticslib does
not raise at import time when DB environment variables are absent (e.g. during
local development without a database, or in test environments).

Session strategy — ``scoped_session``:
    Each call to ``Session()`` returns the *same* Session object for the current
    thread.  The ``get_session()`` context manager commits on clean exit,
    rolls back on exception, and calls ``Session.remove()`` in ``finally`` to
    return the connection to the pool.  This is the correct pattern for
    Streamlit, where each script-run is a single thread and connections must
    be released between runs.

Environment variables (all required except DB_PORT):
    CALLTRACKERS_DB_HOST
    CALLTRACKERS_DB_PORT      (default: 3306)
    CALLTRACKERS_DB_USERNAME
    CALLTRACKERS_DB_PASSWORD
    CALLTRACKERS_DB_NAME
"""
import os
from contextlib import contextmanager
from typing import Generator

from loguru import logger
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session, scoped_session, sessionmaker

from ..exceptions import ConfigurationError, DatabaseError

# ---------------------------------------------------------------------------
# Lazy engine + session factory
# ---------------------------------------------------------------------------

_engine = None
_Session: scoped_session | None = None


def _build_url() -> str:
    """Read env vars and return a SQLAlchemy connection URL.

    Raises:
        ConfigurationError: If any required variable is missing.
    """
    host = os.getenv("CALLTRACKERS_DB_HOST")
    port = os.getenv("CALLTRACKERS_DB_PORT", "3306")
    user = os.getenv("CALLTRACKERS_DB_USERNAME")
    password = os.getenv("CALLTRACKERS_DB_PASSWORD")
    dbname = os.getenv("CALLTRACKERS_DB_NAME")

    missing = [k for k, v in {
        "CALLTRACKERS_DB_HOST": host,
        "CALLTRACKERS_DB_USERNAME": user,
        "CALLTRACKERS_DB_PASSWORD": password,
        "CALLTRACKERS_DB_NAME": dbname,
    }.items() if not v]

    if missing:
        raise ConfigurationError(
            f"Missing required database environment variable(s): {', '.join(missing)}"
        )

    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{dbname}?charset=utf8mb4"


def _get_session_factory() -> scoped_session:
    """Return the scoped_session factory, creating the engine on first call."""
    global _engine, _Session
    if _Session is None:
        url = _build_url()
        _engine = create_engine(url, pool_pre_ping=True)
        factory = sessionmaker(bind=_engine, autocommit=False, autoflush=False)
        _Session = scoped_session(factory)
        logger.debug("SQLAlchemy engine created")
    return _Session


# ---------------------------------------------------------------------------
# Public context manager
# ---------------------------------------------------------------------------

@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Yield a thread-local SQLAlchemy Session, committing on clean exit.

    Usage::

        from acousticslib.database import get_session
        from sqlalchemy import text

        with get_session() as session:
            rows = session.execute(
                text("SELECT * FROM calltrackers.Recorder")
            ).mappings().all()

    Write operations are committed automatically on clean exit.  An exception
    triggers a rollback.  The session is always removed from the scoped registry
    (returning the connection to the pool) in the ``finally`` block.
    """
    Session = _get_session_factory()
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        Session.remove()
