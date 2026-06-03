"""Repository for the ClassifierRegistry table.

Tracks every distinct Classifier_Name value ever encountered during results
import, together with when it was first and last seen and whether it is the
currently active version for its classifier type.

The table is created automatically on first write if it does not already
exist (``CREATE TABLE IF NOT EXISTS``), so no manual migration is required.
"""
import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from ...error_handlers import handle_repository_errors
from ..connection import get_session

_CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS calltrackers.ClassifierRegistry (
        id           INT AUTO_INCREMENT PRIMARY KEY,
        classifier_name VARCHAR(255) NOT NULL,
        classifier_type VARCHAR(20)  NOT NULL,
        first_seen   DATETIME        NOT NULL,
        last_seen    DATETIME        NOT NULL,
        is_current   TINYINT(1)      NOT NULL DEFAULT 0,
        UNIQUE KEY uq_clf_name (classifier_name),
        INDEX        idx_clf_type (classifier_type)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


class ClassifierRegistryRepository:
    """Data access for calltrackers.ClassifierRegistry.

    One row per unique ``Classifier_Name`` string encountered in an imported
    classifier CSV.  The ``is_current`` flag marks the newest known build for
    each ``classifier_type`` (``'bat'``, ``'bittern'``, or ``'curlew'``).

    The first time a classifier name is seen it is auto-promoted to current
    (and all previous entries of the same type are demoted).  Admins can
    override ``is_current`` manually via :meth:`set_current`.
    """

    @staticmethod
    def _ensure_table(session) -> None:
        session.execute(text(_CREATE_TABLE_SQL))

    # ------------------------------------------------------------------ #
    # Read                                                                 #
    # ------------------------------------------------------------------ #

    @staticmethod
    @handle_repository_errors
    def get_all() -> List[Dict[str, Any]]:
        """Return all registry rows ordered by type and last_seen descending."""
        with get_session() as session:
            ClassifierRegistryRepository._ensure_table(session)
            return session.execute(
                text(
                    "SELECT id, classifier_name, classifier_type, "
                    "first_seen, last_seen, is_current "
                    "FROM calltrackers.ClassifierRegistry "
                    "ORDER BY classifier_type, last_seen DESC"
                )
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_by_name(classifier_name: str) -> Optional[Dict[str, Any]]:
        """Return the registry row for *classifier_name*, or None if unknown."""
        with get_session() as session:
            ClassifierRegistryRepository._ensure_table(session)
            return session.execute(
                text(
                    "SELECT id, classifier_name, classifier_type, "
                    "first_seen, last_seen, is_current "
                    "FROM calltrackers.ClassifierRegistry "
                    "WHERE classifier_name = :name"
                ),
                {"name": classifier_name},
            ).mappings().first()

    @staticmethod
    @handle_repository_errors
    def get_current_by_type() -> Dict[str, str]:
        """Return ``{classifier_type: classifier_name}`` for all current entries."""
        with get_session() as session:
            ClassifierRegistryRepository._ensure_table(session)
            rows = session.execute(
                text(
                    "SELECT classifier_type, classifier_name "
                    "FROM calltrackers.ClassifierRegistry "
                    "WHERE is_current = 1"
                )
            ).mappings().all()
            return {row["classifier_type"]: row["classifier_name"] for row in rows}

    # ------------------------------------------------------------------ #
    # Write                                                                #
    # ------------------------------------------------------------------ #

    @staticmethod
    @handle_repository_errors
    def upsert(
        classifier_name: str,
        classifier_type: str,
        at: Optional[datetime.datetime] = None,
    ) -> bool:
        """Insert or update a classifier entry.

        Args:
            classifier_name: Unique classifier build string.
            classifier_type: ``'bat'``, ``'bittern'``, or ``'curlew'``.
            at: The datetime to record as the classifier run time.  Defaults to
                ``datetime.datetime.now()`` when not supplied.  Pass the time
                derived from the ``Upload_Key`` field of the imported rows to
                record when the classifier was actually run rather than when
                results were loaded.

        If *classifier_name* is new:
            - Inserts a row with ``first_seen = last_seen = at``.
            - Auto-promotes it to ``is_current = 1`` and demotes all other
              entries of the same *classifier_type*.

        If *classifier_name* already exists, updates ``last_seen`` to
        ``GREATEST(last_seen, at)`` so it never goes backwards.

        Returns:
            ``True`` if the entry was newly created; ``False`` if it already
            existed (even if ``last_seen`` was updated).
        """
        now = at or datetime.datetime.now()
        with get_session() as session:
            ClassifierRegistryRepository._ensure_table(session)

            existing = session.execute(
                text(
                    "SELECT id FROM calltrackers.ClassifierRegistry "
                    "WHERE classifier_name = :name"
                ),
                {"name": classifier_name},
            ).mappings().first()

            if existing is None:
                # New classifier — insert and auto-promote.
                session.execute(
                    text(
                        "INSERT INTO calltrackers.ClassifierRegistry "
                        "(classifier_name, classifier_type, first_seen, last_seen, is_current) "
                        "VALUES (:name, :ctype, :now, :now, 1)"
                    ),
                    {"name": classifier_name, "ctype": classifier_type, "now": now},
                )
                # Demote all previous entries of the same type.
                session.execute(
                    text(
                        "UPDATE calltrackers.ClassifierRegistry "
                        "SET is_current = 0 "
                        "WHERE classifier_type = :ctype AND classifier_name != :name"
                    ),
                    {"ctype": classifier_type, "name": classifier_name},
                )
                return True
            else:
                # Update last_seen only if the new time is later.
                session.execute(
                    text(
                        "UPDATE calltrackers.ClassifierRegistry "
                        "SET last_seen = GREATEST(last_seen, :now) "
                        "WHERE classifier_name = :name"
                    ),
                    {"now": now, "name": classifier_name},
                )
                return False

    @staticmethod
    @handle_repository_errors
    def set_current(classifier_name: str) -> None:
        """Manually mark *classifier_name* as current for its type.

        All other entries of the same ``classifier_type`` are demoted.
        Raises :class:`ValueError` if *classifier_name* is not in the registry.
        """
        with get_session() as session:
            ClassifierRegistryRepository._ensure_table(session)

            row = session.execute(
                text(
                    "SELECT classifier_type FROM calltrackers.ClassifierRegistry "
                    "WHERE classifier_name = :name"
                ),
                {"name": classifier_name},
            ).mappings().first()

            if row is None:
                raise ValueError(
                    f"Classifier '{classifier_name}' not found in ClassifierRegistry."
                )

            ctype = row["classifier_type"]
            session.execute(
                text(
                    "UPDATE calltrackers.ClassifierRegistry "
                    "SET is_current = 0 WHERE classifier_type = :ctype"
                ),
                {"ctype": ctype},
            )
            session.execute(
                text(
                    "UPDATE calltrackers.ClassifierRegistry "
                    "SET is_current = 1 WHERE classifier_name = :name"
                ),
                {"name": classifier_name},
            )
