"""Repository for SDCards and RecorderCard table operations."""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from ...error_handlers import handle_repository_errors
from ..connection import get_session


class SDCardRepository:
    """Data access layer for calltrackers.SDCards and calltrackers.RecorderCard."""

    @staticmethod
    @handle_repository_errors
    def get_all() -> List[Dict[str, Any]]:
        """All cards (one row each) with aggregated recorder assignments and owner name.

        Uses GROUP_CONCAT so a card with multiple RecorderCard entries (data anomaly)
        still appears as a single row.  recorder_ids and recorder_names are
        comma-separated strings when multiple assignments exist.
        """
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT sc.*, "
                    "  GROUP_CONCAT(rc.id_recorder ORDER BY rc.id) AS recorder_ids, "
                    "  GROUP_CONCAT(r.name ORDER BY rc.id SEPARATOR ', ') AS recorder_name, "
                    "  GROUP_CONCAT(r.short_name ORDER BY rc.id SEPARATOR ', ') AS recorder_short_name, "
                    "  p.PersName AS owner_name "
                    "FROM calltrackers.SDCards sc "
                    "LEFT JOIN calltrackers.RecorderCard rc ON rc.id_card = sc.id "
                    "LEFT JOIN calltrackers.Recorder r ON r.id = rc.id_recorder "
                    "LEFT JOIN calltrackers.People p ON p.id = sc.Owner_id "
                    "GROUP BY sc.id "
                    "ORDER BY sc.name"
                )
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_all_types() -> List[str]:
        """Distinct card type values ordered alphabetically."""
        with get_session() as session:
            rows = session.execute(
                text(
                    "SELECT DISTINCT type FROM calltrackers.SDCards "
                    "WHERE type IS NOT NULL ORDER BY type"
                )
            ).mappings().all()
            return [r["type"] for r in rows]

    @staticmethod
    @handle_repository_errors
    def get_all_capacities() -> List[float]:
        """Distinct CapacityGB values ordered numerically."""
        with get_session() as session:
            rows = session.execute(
                text(
                    "SELECT DISTINCT CapacityGB FROM calltrackers.SDCards "
                    "WHERE CapacityGB IS NOT NULL ORDER BY CapacityGB"
                )
            ).mappings().all()
            return [float(r["CapacityGB"]) for r in rows]

    @staticmethod
    @handle_repository_errors
    def get_next_nts_seq() -> int:
        """Return the next NTS sequence number (max existing + 1, minimum 1)."""
        with get_session() as session:
            row = session.execute(
                text(
                    "SELECT MAX(CAST(REGEXP_SUBSTR(name, '[0-9]+$') AS UNSIGNED)) AS max_seq "
                    "FROM calltrackers.SDCards WHERE name REGEXP '^NTS[0-9]+'"
                )
            ).mappings().first()
            max_seq = row["max_seq"] if row and row["max_seq"] is not None else 0
            return int(max_seq) + 1

    @staticmethod
    @handle_repository_errors
    def get_assignment(card_id: int) -> Optional[Dict[str, Any]]:
        """Return the RecorderCard row for this card, or None if unassigned."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT rc.*, r.name AS recorder_name "
                    "FROM calltrackers.RecorderCard rc "
                    "JOIN calltrackers.Recorder r ON r.id = rc.id_recorder "
                    "WHERE rc.id_card = :card_id"
                ),
                {"card_id": card_id},
            ).mappings().first()

    @staticmethod
    @handle_repository_errors
    def create(
        name: str,
        card_type: str,
        capacity_gb: float,
        owner_id: Optional[int],
        purchased: Optional[str],
        serial: Optional[str],
        date_added: Optional[date],
        comment: Optional[str],
    ) -> int:
        """Insert a new SD card and return its id."""
        with get_session() as session:
            result = session.execute(
                text(
                    "INSERT INTO calltrackers.SDCards "
                    "(name, type, CapacityGB, Owner_id, purchased, serial, date_added, comment, available) "
                    "VALUES (:name, :type, :capacity, :owner_id, :purchased, :serial, :date_added, :comment, 1)"
                ),
                {
                    "name": name,
                    "type": card_type,
                    "capacity": capacity_gb,
                    "owner_id": owner_id,
                    "purchased": purchased,
                    "serial": serial,
                    "date_added": date_added.isoformat() if date_added else None,
                    "comment": comment,
                },
            )
            return result.lastrowid

    @staticmethod
    @handle_repository_errors
    def update(
        card_id: int,
        name: str,
        card_type: str,
        capacity_gb: float,
        owner_id: Optional[int],
        purchased: Optional[str],
        serial: Optional[str],
        comment: Optional[str],
    ) -> None:
        """Update an existing SD card's details."""
        with get_session() as session:
            session.execute(
                text(
                    "UPDATE calltrackers.SDCards SET "
                    "name=:name, type=:type, CapacityGB=:capacity, Owner_id=:owner_id, "
                    "purchased=:purchased, serial=:serial, comment=:comment "
                    "WHERE id=:id"
                ),
                {
                    "name": name,
                    "type": card_type,
                    "capacity": capacity_gb,
                    "owner_id": owner_id,
                    "purchased": purchased,
                    "serial": serial,
                    "comment": comment,
                    "id": card_id,
                },
            )

    @staticmethod
    @handle_repository_errors
    def decommission(card_id: int, decommissioned_date: date) -> None:
        """Mark a card as unavailable and record the decommission date."""
        with get_session() as session:
            session.execute(
                text(
                    "UPDATE calltrackers.SDCards SET "
                    "available=0, decommissioned_date=:ddate "
                    "WHERE id=:id"
                ),
                {"ddate": decommissioned_date.isoformat(), "id": card_id},
            )

    @staticmethod
    @handle_repository_errors
    def reinstate(card_id: int) -> None:
        """Mark a previously decommissioned card as available again."""
        with get_session() as session:
            session.execute(
                text(
                    "UPDATE calltrackers.SDCards SET "
                    "available=1, decommissioned_date=NULL "
                    "WHERE id=:id"
                ),
                {"id": card_id},
            )

    @staticmethod
    @handle_repository_errors
    def assign_to_recorder(card_id: int, recorder_id: int) -> None:
        """Insert a RecorderCard link (does not check for existing — caller must verify)."""
        with get_session() as session:
            session.execute(
                text(
                    "INSERT INTO calltrackers.RecorderCard (id_recorder, id_card) "
                    "VALUES (:rid, :cid)"
                ),
                {"rid": recorder_id, "cid": card_id},
            )

    @staticmethod
    @handle_repository_errors
    def unassign_card(card_id: int) -> None:
        """Remove any RecorderCard link for this card."""
        with get_session() as session:
            session.execute(
                text("DELETE FROM calltrackers.RecorderCard WHERE id_card = :cid"),
                {"cid": card_id},
            )
