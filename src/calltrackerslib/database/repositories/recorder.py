"""Repository for Recorder table operations."""
from datetime import date
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from ...error_handlers import handle_repository_errors
from ...exceptions import NotFoundError
from ..connection import get_session


class RecorderRepository:
    """Data access layer for calltrackers.Recorder."""

    @staticmethod
    @handle_repository_errors
    def get_all() -> List[Dict[str, Any]]:
        """Return all recorders."""
        with get_session() as session:
            return session.execute(
                text("SELECT * FROM calltrackers.Recorder")
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_by_id(recorder_id: int) -> Optional[Dict[str, Any]]:
        """Return a single recorder by id, or None."""
        with get_session() as session:
            return session.execute(
                text("SELECT * FROM calltrackers.Recorder WHERE id = :id"),
                {"id": recorder_id},
            ).mappings().first()

    @staticmethod
    @handle_repository_errors
    def get_all_with_names() -> List[Dict[str, Any]]:
        """Return id, name, short_name for all recorders ordered by name."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT id, name, short_name "
                    "FROM calltrackers.Recorder ORDER BY name"
                )
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_serial_numbers() -> Dict[str, int]:
        """Return {serial_number: recorder_id} for all recorders with a serial."""
        with get_session() as session:
            rows = session.execute(
                text(
                    "SELECT serial_number, id "
                    "FROM calltrackers.Recorder WHERE serial_number IS NOT NULL"
                )
            ).mappings().all()
            return {row["serial_number"]: row["id"] for row in rows}

    @staticmethod
    @handle_repository_errors
    def exists_by_name_or_serial(name: str, serial: str) -> bool:
        """Return True if a recorder with the given name or serial already exists."""
        with get_session() as session:
            row = session.execute(
                text(
                    "SELECT COUNT(*) AS count FROM calltrackers.Recorder "
                    "WHERE name = :name OR serial_number = :serial"
                ),
                {"name": name, "serial": serial},
            ).mappings().first()
            return bool(row["count"])

    @staticmethod
    @handle_repository_errors
    def create(
        name: str,
        manufacturer: str,
        model: str,
        serial_number: str,
        owner_id: Optional[int] = None,
        custodian_id: Optional[int] = None,
        firmware_version: Optional[str] = None,
        purchased: Optional[date] = None,
    ) -> int:
        """Insert a new recorder and return its id."""
        with get_session() as session:
            result = session.execute(
                text(
                    "INSERT INTO calltrackers.Recorder "
                    "(name, Manufacturer, Model, serial_number, owner_id, custodian, "
                    "firmware_version, purchased) "
                    "VALUES (:name, :manufacturer, :model, :serial, :owner_id, "
                    ":custodian_id, :firmware, :purchased)"
                ),
                {
                    "name": name,
                    "manufacturer": manufacturer,
                    "model": model,
                    "serial": serial_number,
                    "owner_id": owner_id,
                    "custodian_id": custodian_id,
                    "firmware": firmware_version,
                    "purchased": purchased.strftime("%Y-%m-%d") if purchased else None,
                },
            )
            return result.lastrowid

    @staticmethod
    @handle_repository_errors
    def update(
        recorder_id: int,
        name: str,
        manufacturer: str,
        model: str,
        serial_number: str,
        owner_id: Optional[int] = None,
        custodian_id: Optional[int] = None,
        firmware_version: Optional[str] = None,
        purchased: Optional[date] = None,
    ) -> None:
        """Update an existing recorder."""
        with get_session() as session:
            session.execute(
                text(
                    "UPDATE calltrackers.Recorder SET "
                    "name=:name, Manufacturer=:manufacturer, Model=:model, "
                    "serial_number=:serial, owner_id=:owner_id, custodian=:custodian_id, "
                    "firmware_version=:firmware, purchased=:purchased "
                    "WHERE id=:id"
                ),
                {
                    "name": name,
                    "manufacturer": manufacturer,
                    "model": model,
                    "serial": serial_number,
                    "owner_id": owner_id,
                    "custodian_id": custodian_id,
                    "firmware": firmware_version,
                    "purchased": purchased.strftime("%Y-%m-%d") if purchased else None,
                    "id": recorder_id,
                },
            )

    @staticmethod
    @handle_repository_errors
    def delete(recorder_id: int) -> None:
        """Delete a recorder by id."""
        with get_session() as session:
            session.execute(
                text("DELETE FROM calltrackers.Recorder WHERE id = :id"),
                {"id": recorder_id},
            )
