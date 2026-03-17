"""Repositories for read-only lookup / reference tables.

Covers:
    ClassifierStatus         Processing status codes for LocationLog processed_* columns
    DataAvailabilityStatus   Codes for acoustic_on_NAS / ultrasonic_on_NAS etc.
    ObservingPrograms        Program definitions with date ranges and directories
"""
from typing import Any, Dict, List

from sqlalchemy import text

from ...error_handlers import handle_repository_errors
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
