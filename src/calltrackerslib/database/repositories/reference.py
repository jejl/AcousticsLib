"""Repositories for read-only lookup / reference tables.

Covers:
    ClassifierStatus         Processing status codes for LocationLog processed_* columns
    DataAvailabilityStatus   Codes for acoustic_on_NAS / ultrasonic_on_NAS etc.
    ObservingPrograms        Program definitions with date ranges and directories
"""
from typing import Any, Dict, List

from sqlalchemy import text

from ...error_handlers import handle_repository_errors
from ...exceptions import ValidationError
from ..connection import get_session


class ClassifierStatusRepository:
    """Data access for calltrackers.ClassifierStatus lookup table.

    Status codes: 1 (results), 0 (processed/none), -1 (not suitable), -2 (data issue).
    """

    @staticmethod
    @handle_repository_errors
    def get_all() -> List[Dict[str, Any]]:
        """Return all rows ordered by id descending (1 first, -2 last)."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT id, description_brief, description "
                    "FROM calltrackers.ClassifierStatus ORDER BY id DESC"
                )
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_id_to_brief() -> Dict[int, str]:
        """Return {id: description_brief} for all status codes."""
        with get_session() as session:
            rows = session.execute(
                text(
                    "SELECT id, description_brief FROM calltrackers.ClassifierStatus"
                )
            ).mappings().all()
            return {row["id"]: row["description_brief"] for row in rows}

    @staticmethod
    @handle_repository_errors
    def get_id_to_description() -> Dict[int, str]:
        """Return {id: description} for all status codes (full descriptions)."""
        with get_session() as session:
            rows = session.execute(
                text("SELECT id, description FROM calltrackers.ClassifierStatus")
            ).mappings().all()
            return {row["id"]: row["description"] for row in rows}

    @staticmethod
    @handle_repository_errors
    def insert_status(id: int, description_brief: str, description: str) -> None:
        """Insert a new ClassifierStatus row.

        Raises :class:`~calltrackerslib.exceptions.ValidationError` if *id* already
        exists.  There is intentionally no update or delete method — status codes
        are permanent once written; changing or removing them would corrupt the
        interpretation of historical LocationLog entries.
        """
        with get_session() as session:
            existing = session.execute(
                text("SELECT id FROM calltrackers.ClassifierStatus WHERE id = :id"),
                {"id": id},
            ).mappings().first()
            if existing:
                raise ValidationError(
                    f"ClassifierStatus id={id} already exists. "
                    "Status codes are immutable — add a new code instead of modifying this one."
                )
            session.execute(
                text(
                    "INSERT INTO calltrackers.ClassifierStatus "
                    "(id, description_brief, description) VALUES (:id, :brief, :desc)"
                ),
                {"id": id, "brief": description_brief, "desc": description},
            )


class DataAvailabilityRepository:
    """Data access for calltrackers.DataAvailabilityStatus lookup table.

    Codes: 1 (Data present), 0 (Data not present), -1 (No data of this type recorded).
    """

    @staticmethod
    @handle_repository_errors
    def get_all() -> List[Dict[str, Any]]:
        """Return all rows ordered by id descending (1 first, -1 last)."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT id, description_brief, description "
                    "FROM calltrackers.DataAvailabilityStatus ORDER BY id DESC"
                )
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_id_to_brief() -> Dict[int, str]:
        """Return {id: description_brief} for all availability codes."""
        with get_session() as session:
            rows = session.execute(
                text(
                    "SELECT id, description_brief "
                    "FROM calltrackers.DataAvailabilityStatus"
                )
            ).mappings().all()
            return {row["id"]: row["description_brief"] for row in rows}


class ObservingProgramsRepository:
    """Data access for calltrackers.ObservingPrograms."""

    @staticmethod
    @handle_repository_errors
    def get_all() -> List[Dict[str, Any]]:
        """Return pk, Name, NameBrief, Comment, start_date, end_date, directory."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT pk, Name, NameBrief, Comment, start_date, end_date, directory "
                    "FROM calltrackers.ObservingPrograms"
                )
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_by_pk(pk: int) -> Dict[str, Any] | None:
        """Return a single program by pk, or None."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT pk, Name, NameBrief, Comment, start_date, end_date, directory "
                    "FROM calltrackers.ObservingPrograms WHERE pk = :pk"
                ),
                {"pk": pk},
            ).mappings().first()
