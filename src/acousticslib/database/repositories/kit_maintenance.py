"""Repositories for the kit maintenance/servicing tables.

Tables: KitItemTemplate, KitItemTask, KitMaintenanceSession,
        KitItemCheck, KitItemTaskCheck.
"""
from __future__ import annotations

import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from ...error_handlers import handle_repository_errors
from ..connection import get_session


class KitMaintenanceRepository:
    """CRUD for KitMaintenanceSession, KitItemCheck, and KitItemTaskCheck."""

    # ── Sessions ──────────────────────────────────────────────────────────────

    @staticmethod
    @handle_repository_errors
    def get_all_kits_with_status() -> List[Dict[str, Any]]:
        """Return all kits with their most recent maintenance session (if any)."""
        with get_session() as session:
            return session.execute(text("""
                SELECT
                    k.id AS kit_id, k.name AS kit_name, k.label AS kit_label,
                    k.bolt_head_type,
                    s.id AS session_id, s.status, s.season,
                    s.started_at, s.completed_at, s.released_at, s.technician
                FROM calltrackers.Kit k
                LEFT JOIN calltrackers.KitMaintenanceSession s
                    ON s.kit_id = k.id
                    AND s.id = (
                        SELECT MAX(id)
                        FROM calltrackers.KitMaintenanceSession
                        WHERE kit_id = k.id
                    )
                ORDER BY k.name
            """)).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_session_by_id(session_id: int) -> Optional[Dict[str, Any]]:
        """Return full session data (including kit name and bolt_head_type)."""
        with get_session() as session:
            return session.execute(text("""
                SELECT s.*, k.name AS kit_name, k.label AS kit_label,
                       k.bolt_head_type
                FROM calltrackers.KitMaintenanceSession s
                JOIN calltrackers.Kit k ON k.id = s.kit_id
                WHERE s.id = :sid
            """), {"sid": session_id}).mappings().first()

    @staticmethod
    @handle_repository_errors
    def create_session(kit_id: int, season: str,
                       technician: Optional[str]) -> int:
        """Insert a new KitMaintenanceSession and return its id."""
        with get_session() as session:
            result = session.execute(text("""
                INSERT INTO calltrackers.KitMaintenanceSession
                    (kit_id, season, status, started_at, technician)
                VALUES (:kid, :season, 'in_maintenance', :now, :tech)
            """), {
                "kid": kit_id,
                "season": season,
                "now": datetime.datetime.utcnow(),
                "tech": technician,
            })
            new_id = result.lastrowid

        # Immediately initialise check rows for all active template items
        KitMaintenanceRepository.initialise_checks(new_id)
        return new_id

    @staticmethod
    @handle_repository_errors
    def initialise_checks(session_id: int) -> None:
        """Create KitItemCheck + KitItemTaskCheck rows for every active item.

        Idempotent: uses INSERT IGNORE so re-running is safe.
        """
        with get_session() as session:
            # One check row per active template item
            session.execute(text("""
                INSERT IGNORE INTO calltrackers.KitItemCheck
                    (session_id, item_id, quantity_needed, acquired)
                SELECT :sid, id, 0, 0
                FROM calltrackers.KitItemTemplate
                WHERE active = 1
            """), {"sid": session_id})

            # One task-check row per task for each item check
            session.execute(text("""
                INSERT IGNORE INTO calltrackers.KitItemTaskCheck
                    (item_check_id, task_id, completed)
                SELECT c.id, tk.id, 0
                FROM calltrackers.KitItemCheck c
                JOIN calltrackers.KitItemTask tk ON tk.item_id = c.item_id
                WHERE c.session_id = :sid
            """), {"sid": session_id})

    @staticmethod
    @handle_repository_errors
    def update_session(
        session_id: int,
        status: str,
        notes: Optional[str],
        technician: Optional[str],
        completed_at: Optional[datetime.datetime] = None,
        released_at: Optional[datetime.datetime] = None,
    ) -> None:
        """Update status, notes, technician and optional timestamps."""
        with get_session() as session:
            session.execute(text("""
                UPDATE calltrackers.KitMaintenanceSession
                SET status       = :status,
                    notes        = :notes,
                    technician   = :tech,
                    completed_at = :completed_at,
                    released_at  = :released_at
                WHERE id = :sid
            """), {
                "sid": session_id,
                "status": status,
                "notes": notes,
                "tech": technician,
                "completed_at": completed_at,
                "released_at": released_at,
            })

    # ── Item checks ───────────────────────────────────────────────────────────

    @staticmethod
    @handle_repository_errors
    def get_items_with_checks(session_id: int) -> List[Dict[str, Any]]:
        """Return all active template items joined to their check row for this session."""
        with get_session() as session:
            return session.execute(text("""
                SELECT
                    t.id AS item_id, t.item_name, t.quantity_type,
                    t.min_quantity, t.notes AS item_notes, t.sort_order,
                    c.id AS check_id, c.present, c.actual_quantity,
                    c.quantity_needed, c.acquired, c.notes AS check_notes
                FROM calltrackers.KitItemTemplate t
                LEFT JOIN calltrackers.KitItemCheck c
                    ON c.item_id = t.id AND c.session_id = :sid
                WHERE t.active = 1
                ORDER BY t.sort_order, t.id
            """), {"sid": session_id}).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_tasks_with_checks(session_id: int) -> List[Dict[str, Any]]:
        """Return all tasks for active items joined to their completion row."""
        with get_session() as session:
            return session.execute(text("""
                SELECT
                    tk.id AS task_id, tk.item_id, tk.task_description,
                    tk.tooltip, tk.sort_order,
                    c.id AS item_check_id,
                    tc.id AS task_check_id, tc.completed,
                    tc.notes AS task_check_notes
                FROM calltrackers.KitItemTemplate t
                JOIN calltrackers.KitItemTask tk ON tk.item_id = t.id
                LEFT JOIN calltrackers.KitItemCheck c
                    ON c.item_id = t.id AND c.session_id = :sid
                LEFT JOIN calltrackers.KitItemTaskCheck tc
                    ON tc.task_id = tk.id AND tc.item_check_id = c.id
                WHERE t.active = 1
                ORDER BY t.sort_order, tk.sort_order
            """), {"sid": session_id}).mappings().all()

    @staticmethod
    @handle_repository_errors
    def update_item_check(
        session_id: int,
        item_id: int,
        present: Optional[int],
        actual_quantity: Optional[int],
        quantity_needed: int,
        acquired: int,
        notes: Optional[str],
    ) -> None:
        """Upsert a KitItemCheck row (insert on first save, update thereafter)."""
        with get_session() as session:
            session.execute(text("""
                INSERT INTO calltrackers.KitItemCheck
                    (session_id, item_id, present, actual_quantity,
                     quantity_needed, acquired, notes)
                VALUES (:sid, :iid, :present, :actual_qty,
                        :qty_needed, :acquired, :notes)
                ON DUPLICATE KEY UPDATE
                    present         = VALUES(present),
                    actual_quantity = VALUES(actual_quantity),
                    quantity_needed = VALUES(quantity_needed),
                    acquired        = VALUES(acquired),
                    notes           = VALUES(notes)
            """), {
                "sid": session_id, "iid": item_id,
                "present": present, "actual_qty": actual_quantity,
                "qty_needed": quantity_needed, "acquired": acquired,
                "notes": notes,
            })

    @staticmethod
    @handle_repository_errors
    def update_task_check(
        item_check_id: int,
        task_id: int,
        completed: int,
        notes: Optional[str],
    ) -> None:
        """Upsert a KitItemTaskCheck row."""
        with get_session() as session:
            session.execute(text("""
                INSERT INTO calltrackers.KitItemTaskCheck
                    (item_check_id, task_id, completed, notes)
                VALUES (:icid, :tid, :completed, :notes)
                ON DUPLICATE KEY UPDATE
                    completed = VALUES(completed),
                    notes     = VALUES(notes)
            """), {
                "icid": item_check_id, "tid": task_id,
                "completed": completed, "notes": notes,
            })

    @staticmethod
    @handle_repository_errors
    def mark_acquired(check_id: int, acquired: int) -> None:
        """Toggle the acquired flag on a KitItemCheck row."""
        with get_session() as session:
            session.execute(text("""
                UPDATE calltrackers.KitItemCheck
                SET acquired = :acq WHERE id = :cid
            """), {"acq": acquired, "cid": check_id})

    # ── Shopping list ─────────────────────────────────────────────────────────

    @staticmethod
    @handle_repository_errors
    def get_shopping_list() -> List[Dict[str, Any]]:
        """Items needed across all non-released sessions.

        Returns one row per (session, item) pair where quantity_needed > 0.
        """
        with get_session() as session:
            return session.execute(text("""
                SELECT
                    t.item_name, t.sort_order,
                    k.name AS kit_name, k.bolt_head_type,
                    s.season, s.id AS session_id,
                    c.id AS check_id,
                    c.quantity_needed, c.acquired, c.notes AS check_notes
                FROM calltrackers.KitItemCheck c
                JOIN calltrackers.KitItemTemplate t ON t.id = c.item_id
                JOIN calltrackers.KitMaintenanceSession s ON s.id = c.session_id
                JOIN calltrackers.Kit k ON k.id = s.kit_id
                WHERE c.quantity_needed > 0
                  AND s.status != 'released'
                ORDER BY t.sort_order, k.name
            """)).mappings().all()

    # ── Template management ───────────────────────────────────────────────────

    @staticmethod
    @handle_repository_errors
    def get_all_template_items() -> List[Dict[str, Any]]:
        """All template items (active and inactive) ordered by sort_order."""
        with get_session() as session:
            return session.execute(text("""
                SELECT * FROM calltrackers.KitItemTemplate
                ORDER BY sort_order, id
            """)).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_all_template_tasks() -> List[Dict[str, Any]]:
        """All tasks across all items."""
        with get_session() as session:
            return session.execute(text("""
                SELECT * FROM calltrackers.KitItemTask
                ORDER BY item_id, sort_order
            """)).mappings().all()

    @staticmethod
    @handle_repository_errors
    def create_template_item(
        item_name: str,
        quantity_type: str,
        min_quantity: int,
        notes: Optional[str],
        sort_order: int,
    ) -> int:
        with get_session() as session:
            result = session.execute(text("""
                INSERT INTO calltrackers.KitItemTemplate
                    (item_name, quantity_type, min_quantity, notes, sort_order, active)
                VALUES (:name, :qtype, :minq, :notes, :sort, 1)
            """), {
                "name": item_name, "qtype": quantity_type,
                "minq": min_quantity, "notes": notes, "sort": sort_order,
            })
            return result.lastrowid

    @staticmethod
    @handle_repository_errors
    def update_template_item(
        item_id: int,
        item_name: str,
        quantity_type: str,
        min_quantity: int,
        notes: Optional[str],
        sort_order: int,
        active: bool,
    ) -> None:
        with get_session() as session:
            session.execute(text("""
                UPDATE calltrackers.KitItemTemplate
                SET item_name=:name, quantity_type=:qtype, min_quantity=:minq,
                    notes=:notes, sort_order=:sort, active=:active
                WHERE id=:iid
            """), {
                "name": item_name, "qtype": quantity_type, "minq": min_quantity,
                "notes": notes, "sort": sort_order,
                "active": int(active), "iid": item_id,
            })

    @staticmethod
    @handle_repository_errors
    def create_template_task(
        item_id: int,
        task_description: str,
        tooltip: Optional[str],
        sort_order: int,
    ) -> int:
        with get_session() as session:
            result = session.execute(text("""
                INSERT INTO calltrackers.KitItemTask
                    (item_id, task_description, tooltip, sort_order)
                VALUES (:iid, :desc, :tip, :sort)
            """), {
                "iid": item_id, "desc": task_description,
                "tip": tooltip, "sort": sort_order,
            })
            return result.lastrowid

    @staticmethod
    @handle_repository_errors
    def update_template_task(
        task_id: int,
        task_description: str,
        tooltip: Optional[str],
        sort_order: int,
    ) -> None:
        with get_session() as session:
            session.execute(text("""
                UPDATE calltrackers.KitItemTask
                SET task_description=:desc, tooltip=:tip, sort_order=:sort
                WHERE id=:tid
            """), {
                "desc": task_description, "tip": tooltip,
                "sort": sort_order, "tid": task_id,
            })

    @staticmethod
    @handle_repository_errors
    def delete_template_task(task_id: int) -> None:
        with get_session() as session:
            session.execute(text(
                "DELETE FROM calltrackers.KitItemTask WHERE id=:tid"
            ), {"tid": task_id})