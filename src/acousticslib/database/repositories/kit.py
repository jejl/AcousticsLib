"""Repositories for Kit, KitRecorder, and KitCustodian tables."""
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from ...error_handlers import handle_repository_errors
from ..connection import get_session


class KitRepository:
    """Data access layer for calltrackers.Kit."""

    @staticmethod
    @handle_repository_errors
    def get_all() -> List[Dict[str, Any]]:
        """Return id, name, label for all kits ordered by name."""
        with get_session() as session:
            return session.execute(
                text("SELECT id, name, label, bolt_head_type FROM calltrackers.Kit ORDER BY name")
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_by_id(kit_id: int) -> Optional[Dict[str, Any]]:
        """Return a single kit by id, or None."""
        with get_session() as session:
            return session.execute(
                text("SELECT * FROM calltrackers.Kit WHERE id = :id"),
                {"id": kit_id},
            ).mappings().first()

    @staticmethod
    @handle_repository_errors
    def get_all_with_custodians() -> List[Dict[str, Any]]:
        """Return all kits with their current custodian information."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT "
                    "kit.id AS kit_id, kit.name AS kit_name, kit.label AS kit_label, "
                    "CONCAT("
                    "  CASE WHEN CUST.FirstName IS NOT NULL THEN CUST.FirstName ELSE '' END, "
                    "  CASE WHEN CUST.LastName  IS NOT NULL THEN CONCAT(' ', CUST.LastName) ELSE '' END"
                    ") AS custodian_name, "
                    "KC.custodian_id "
                    "FROM calltrackers.Kit kit "
                    "LEFT JOIN KitCustodian KC   ON kit.id = KC.kit_id "
                    "LEFT JOIN Custodian CUST    ON KC.custodian_id = CUST.id "
                    "ORDER BY kit.name"
                )
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_recorders_with_kits() -> List[Dict[str, Any]]:
        """Return all recorders with kit, custodian, and latest service note."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT recorder.name AS Recorder_Name, "
                    "CONCAT(recorder.Manufacturer, ' ', recorder.Model) AS Model, "
                    "CASE WHEN kit.name IS NOT NULL THEN kit.name ELSE 'Unassigned' END AS Kit_Name, "
                    "CASE WHEN kit.name IS NOT NULL THEN kit.label ELSE '- none -' END AS Kit_Label, "
                    "CASE "
                    "  WHEN kit.name IS NOT NULL THEN "
                    "    CONCAT("
                    "      CASE WHEN CUST.FirstName IS NOT NULL THEN CUST.FirstName ELSE '' END, "
                    "      CASE WHEN CUST.LastName  IS NOT NULL THEN CONCAT(' ', CUST.LastName) ELSE '' END"
                    "    ) "
                    "  ELSE "
                    "    CONCAT("
                    "      CASE WHEN CUST2.FirstName IS NOT NULL THEN CUST2.FirstName ELSE '' END, "
                    "      CASE WHEN CUST2.LastName  IS NOT NULL THEN CONCAT(' ', CUST2.LastName) ELSE '' END"
                    "    ) "
                    "END AS Custodian, "
                    "SN.service_date AS `Last Recorder Service`, "
                    "SN.notes AS Service_Notes "
                    "FROM Recorder recorder "
                    "LEFT JOIN Owner O           ON recorder.owner_id = O.id "
                    "LEFT JOIN KitRecorder kitrec ON kitrec.recorder_id = recorder.id "
                    "LEFT JOIN Kit kit            ON kitrec.kit_id = kit.id "
                    "LEFT JOIN KitCustodian KC    ON kitrec.kit_id = KC.kit_id "
                    "LEFT JOIN Custodian CUST     ON KC.custodian_id = CUST.id "
                    "LEFT JOIN Custodian CUST2    ON recorder.custodian = CUST2.id "
                    "LEFT JOIN ("
                    "  SELECT recorder_id, service_date, notes "
                    "  FROM calltrackers.ServiceNotes SN1 "
                    "  WHERE service_date = ("
                    "    SELECT MAX(service_date) FROM ServiceNotes SN2 "
                    "    WHERE SN2.recorder_id = SN1.recorder_id"
                    "  )"
                    ") SN ON recorder.id = SN.recorder_id "
                    "ORDER BY recorder.id"
                )
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def create(name: str, label: Optional[str] = None) -> int:
        """Insert a new kit and return its id."""
        with get_session() as session:
            result = session.execute(
                text("INSERT INTO calltrackers.Kit (name, label) VALUES (:name, :label)"),
                {"name": name, "label": label},
            )
            return result.lastrowid

    @staticmethod
    @handle_repository_errors
    def update(kit_id: int, name: str, label: Optional[str] = None) -> None:
        """Update kit name and label."""
        with get_session() as session:
            session.execute(
                text("UPDATE calltrackers.Kit SET name=:name, label=:label WHERE id=:id"),
                {"name": name, "label": label, "id": kit_id},
            )

    @staticmethod
    @handle_repository_errors
    def update_bolt_head_type(kit_id: int, bolt_head_type: Optional[str]) -> None:
        """Set the bolt head type (phillips / hex / None) for a kit."""
        with get_session() as session:
            session.execute(
                text("UPDATE calltrackers.Kit SET bolt_head_type=:bht WHERE id=:id"),
                {"bht": bolt_head_type, "id": kit_id},
            )

    @staticmethod
    @handle_repository_errors
    def delete(kit_id: int) -> None:
        """Delete a kit by id."""
        with get_session() as session:
            session.execute(
                text("DELETE FROM calltrackers.Kit WHERE id=:id"),
                {"id": kit_id},
            )


class KitRecorderRepository:
    """Data access layer for calltrackers.KitRecorder (recorder-to-kit assignment)."""

    @staticmethod
    @handle_repository_errors
    def get_recorder_for_kit(kit_id: int) -> Optional[int]:
        """Return the recorder_id currently assigned to a kit, or None."""
        with get_session() as session:
            row = session.execute(
                text("SELECT recorder_id FROM calltrackers.KitRecorder WHERE kit_id = :kit_id"),
                {"kit_id": kit_id},
            ).mappings().first()
        return row["recorder_id"] if row else None

    @staticmethod
    @handle_repository_errors
    def get_kit_for_recorder(recorder_id: int) -> Optional[int]:
        """Return the kit_id to which a recorder is assigned, or None."""
        with get_session() as session:
            row = session.execute(
                text("SELECT kit_id FROM calltrackers.KitRecorder WHERE recorder_id = :rid"),
                {"rid": recorder_id},
            ).mappings().first()
        return row["kit_id"] if row else None

    @staticmethod
    @handle_repository_errors
    def assign(kit_id: int, recorder_id: int) -> None:
        """Assign a recorder to a kit.

        Removes any existing assignment for this kit and this recorder before
        creating the new one.  All three operations are committed atomically.
        """
        with get_session() as session:
            session.execute(
                text("DELETE FROM calltrackers.KitRecorder WHERE kit_id = :kit_id"),
                {"kit_id": kit_id},
            )
            session.execute(
                text("DELETE FROM calltrackers.KitRecorder WHERE recorder_id = :rid"),
                {"rid": recorder_id},
            )
            session.execute(
                text(
                    "INSERT INTO calltrackers.KitRecorder (kit_id, recorder_id) "
                    "VALUES (:kit_id, :rid)"
                ),
                {"kit_id": kit_id, "rid": recorder_id},
            )

    @staticmethod
    @handle_repository_errors
    def unassign_by_kit(kit_id: int) -> None:
        """Remove any recorder assignment from a kit."""
        with get_session() as session:
            session.execute(
                text("DELETE FROM calltrackers.KitRecorder WHERE kit_id = :kit_id"),
                {"kit_id": kit_id},
            )


class KitCustodianRepository:
    """Data access layer for calltrackers.KitCustodian (kit-to-custodian assignment)."""

    @staticmethod
    @handle_repository_errors
    def get_by_kit_id(kit_id: int) -> Optional[Dict[str, Any]]:
        """Return the custodian assignment for a kit, or None."""
        with get_session() as session:
            return session.execute(
                text("SELECT * FROM calltrackers.KitCustodian WHERE kit_id = :kit_id"),
                {"kit_id": kit_id},
            ).mappings().first()

    @staticmethod
    @handle_repository_errors
    def get_kits_by_custodian(custodian_id: int) -> List[Dict[str, Any]]:
        """Return all kit_id rows assigned to *custodian_id*."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT kit_id FROM calltrackers.KitCustodian "
                    "WHERE custodian_id = :cid"
                ),
                {"cid": custodian_id},
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def assign_custodian(kit_id: int, custodian_id: int) -> None:
        """Assign a custodian to a kit (upsert)."""
        with get_session() as session:
            existing = session.execute(
                text("SELECT kit_id FROM calltrackers.KitCustodian WHERE kit_id = :kit_id"),
                {"kit_id": kit_id},
            ).mappings().first()

            if existing:
                session.execute(
                    text(
                        "UPDATE calltrackers.KitCustodian "
                        "SET custodian_id = :cid WHERE kit_id = :kit_id"
                    ),
                    {"cid": custodian_id, "kit_id": kit_id},
                )
            else:
                session.execute(
                    text(
                        "INSERT INTO calltrackers.KitCustodian (kit_id, custodian_id) "
                        "VALUES (:kit_id, :cid)"
                    ),
                    {"kit_id": kit_id, "cid": custodian_id},
                )

    @staticmethod
    @handle_repository_errors
    def unassign_custodian(kit_id: int) -> None:
        """Remove custodian assignment from a kit."""
        with get_session() as session:
            session.execute(
                text("DELETE FROM calltrackers.KitCustodian WHERE kit_id = :kit_id"),
                {"kit_id": kit_id},
            )
