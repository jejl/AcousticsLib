"""Repository for ServiceNotes table operations."""
from datetime import date
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from ...error_handlers import handle_repository_errors
from ..connection import get_session


class ServiceNotesRepository:
    """Data access layer for calltrackers.ServiceNotes."""

    @staticmethod
    @handle_repository_errors
    def get_all() -> List[Dict[str, Any]]:
        """Return all service notes ordered by date descending."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT * FROM calltrackers.ServiceNotes ORDER BY service_date DESC"
                )
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_by_recorder_id(recorder_id: int) -> List[Dict[str, Any]]:
        """Return all service notes for a recorder ordered by date ascending."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT service_date, recorder_id, kit_id, notes, technician "
                    "FROM calltrackers.ServiceNotes "
                    "WHERE recorder_id = :rid ORDER BY service_date ASC"
                ),
                {"rid": recorder_id},
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_by_kit_id(kit_id: int) -> List[Dict[str, Any]]:
        """Return all service notes for a kit ordered by date ascending."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT service_date, recorder_id, kit_id, notes, technician "
                    "FROM calltrackers.ServiceNotes "
                    "WHERE kit_id = :kit_id ORDER BY service_date ASC"
                ),
                {"kit_id": kit_id},
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def create(
        recorder_id: Optional[int],
        kit_id: Optional[int],
        notes: str,
        technician: str,
        service_date: Optional[date] = None,
    ) -> int:
        """Insert a new service note and return its id.

        At least one of *recorder_id* or *kit_id* must be provided.
        """
        if not recorder_id and not kit_id:
            raise ValueError("At least one of recorder_id or kit_id must be provided")

        with get_session() as session:
            if service_date:
                result = session.execute(
                    text(
                        "INSERT INTO calltrackers.ServiceNotes "
                        "(recorder_id, kit_id, notes, technician, service_date) "
                        "VALUES (:rid, :kit_id, :notes, :tech, :sdate)"
                    ),
                    {
                        "rid": recorder_id, "kit_id": kit_id,
                        "notes": notes, "tech": technician, "sdate": service_date,
                    },
                )
            else:
                result = session.execute(
                    text(
                        "INSERT INTO calltrackers.ServiceNotes "
                        "(recorder_id, kit_id, notes, technician) "
                        "VALUES (:rid, :kit_id, :notes, :tech)"
                    ),
                    {
                        "rid": recorder_id, "kit_id": kit_id,
                        "notes": notes, "tech": technician,
                    },
                )
            return result.lastrowid

    @staticmethod
    @handle_repository_errors
    def update(
        note_id: int,
        notes: str,
        technician: str,
        service_date: Optional[date] = None,
    ) -> None:
        """Update an existing service note."""
        with get_session() as session:
            if service_date:
                session.execute(
                    text(
                        "UPDATE calltrackers.ServiceNotes "
                        "SET notes=:notes, technician=:tech, service_date=:sdate "
                        "WHERE id=:id"
                    ),
                    {"notes": notes, "tech": technician, "sdate": service_date, "id": note_id},
                )
            else:
                session.execute(
                    text(
                        "UPDATE calltrackers.ServiceNotes "
                        "SET notes=:notes, technician=:tech WHERE id=:id"
                    ),
                    {"notes": notes, "tech": technician, "id": note_id},
                )

    @staticmethod
    @handle_repository_errors
    def delete(note_id: int) -> None:
        """Delete a service note by id."""
        with get_session() as session:
            session.execute(
                text("DELETE FROM calltrackers.ServiceNotes WHERE id=:id"),
                {"id": note_id},
            )
