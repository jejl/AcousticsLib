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
        """Return all kits with their most recent maintenance session (if any).

        Uses a CTE with ROW_NUMBER() to guarantee exactly one row per kit,
        avoiding MySQL 8 optimizer issues with correlated subqueries in
        LEFT JOIN ON clauses.
        """
        with get_session() as session:
            return session.execute(text("""
                WITH LatestSession AS (
                    SELECT id, kit_id, status, season,
                           started_at, completed_at, released_at, technician,
                           ROW_NUMBER() OVER (PARTITION BY kit_id ORDER BY id DESC) AS rn
                    FROM calltrackers.KitMaintenanceSession
                )
                SELECT
                    k.id AS kit_id, k.name AS kit_name, k.label AS kit_label,
                    k.bolt_head_type,
                    ls.id AS session_id, ls.status, ls.season,
                    ls.started_at, ls.completed_at, ls.released_at, ls.technician
                FROM calltrackers.Kit k
                LEFT JOIN LatestSession ls ON ls.kit_id = k.id AND ls.rn = 1
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
            # One check row per active template item, respecting conditions.
            # Conditional items (condition_key IS NOT NULL) are included only
            # when the kit's matching attribute equals condition_value.
            # To add a new condition type, add a new OR branch below and
            # the corresponding column to calltrackers.Kit.
            session.execute(text("""
                INSERT IGNORE INTO calltrackers.KitItemCheck
                    (session_id, item_id, quantity_needed, acquired)
                SELECT :sid, t.id, 0, 0
                FROM calltrackers.KitItemTemplate t
                JOIN calltrackers.KitMaintenanceSession s ON s.id = :sid
                JOIN calltrackers.Kit k ON k.id = s.kit_id
                WHERE t.active = 1
                  AND (
                    t.condition_key IS NULL
                    OR (t.condition_key = 'bolt_head_type'
                        AND t.condition_value = k.bolt_head_type)
                  )
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
        """Return template items joined to their check row for this session.

        Active items that match the kit's conditions are included.  Items that
        don't match the condition but already have a historical check row are
        also included (so mid-session config changes don't drop recorded data).
        Retired items follow the same historical-preservation rule.
        """
        with get_session() as session:
            return session.execute(text("""
                SELECT
                    t.id AS item_id, t.item_name, t.quantity_type,
                    t.min_quantity, t.notes AS item_notes, t.sort_order,
                    t.active, t.condition_key, t.condition_value,
                    c.id AS check_id, c.present, c.actual_quantity,
                    c.quantity_needed, c.acquired, c.notes AS check_notes
                FROM calltrackers.KitItemTemplate t
                JOIN calltrackers.KitMaintenanceSession s ON s.id = :sid
                JOIN calltrackers.Kit k ON k.id = s.kit_id
                LEFT JOIN calltrackers.KitItemCheck c
                    ON c.item_id = t.id AND c.session_id = :sid
                WHERE (
                    t.active = 1
                    AND (
                        t.condition_key IS NULL
                        OR (t.condition_key = 'bolt_head_type'
                            AND t.condition_value = k.bolt_head_type)
                    )
                )
                OR c.id IS NOT NULL
                ORDER BY t.sort_order, t.id
            """), {"sid": session_id}).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_tasks_with_checks(session_id: int) -> List[Dict[str, Any]]:
        """Return tasks joined to their completion row for this session.

        Mirrors the same active/condition/historical logic as get_items_with_checks.
        """
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
                JOIN calltrackers.KitMaintenanceSession s ON s.id = :sid
                JOIN calltrackers.Kit k ON k.id = s.kit_id
                LEFT JOIN calltrackers.KitItemCheck c
                    ON c.item_id = t.id AND c.session_id = :sid
                LEFT JOIN calltrackers.KitItemTaskCheck tc
                    ON tc.task_id = tk.id AND tc.item_check_id = c.id
                WHERE (
                    t.active = 1
                    AND (
                        t.condition_key IS NULL
                        OR (t.condition_key = 'bolt_head_type'
                            AND t.condition_value = k.bolt_head_type)
                    )
                )
                OR c.id IS NOT NULL
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
        Includes spare stock, order-tracking fields, and chosen supplier name.
        """
        with get_session() as session:
            return session.execute(text("""
                SELECT
                    t.id AS item_id,
                    t.item_name, t.sort_order,
                    k.name AS kit_name, k.bolt_head_type,
                    s.season, s.id AS session_id,
                    c.id AS check_id,
                    c.quantity_needed, c.acquired, c.notes AS check_notes,
                    c.ordered_at, c.received_at, c.supplier_id,
                    COALESCE(ss.quantity, 0) AS spare_stock,
                    sup.name AS supplier_name
                FROM calltrackers.KitItemCheck c
                JOIN calltrackers.KitItemTemplate t ON t.id = c.item_id
                JOIN calltrackers.KitMaintenanceSession s ON s.id = c.session_id
                JOIN calltrackers.Kit k ON k.id = s.kit_id
                LEFT JOIN calltrackers.KitSpareStock ss ON ss.item_id = t.id
                LEFT JOIN calltrackers.KitSupplier sup ON sup.id = c.supplier_id
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
        condition_key: Optional[str] = None,
        condition_value: Optional[str] = None,
    ) -> int:
        with get_session() as session:
            result = session.execute(text("""
                INSERT INTO calltrackers.KitItemTemplate
                    (item_name, quantity_type, min_quantity, notes, sort_order,
                     active, condition_key, condition_value)
                VALUES (:name, :qtype, :minq, :notes, :sort,
                        1, :ckey, :cval)
            """), {
                "name": item_name, "qtype": quantity_type,
                "minq": min_quantity, "notes": notes, "sort": sort_order,
                "ckey": condition_key, "cval": condition_value,
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
        condition_key: Optional[str] = None,
        condition_value: Optional[str] = None,
    ) -> None:
        with get_session() as session:
            session.execute(text("""
                UPDATE calltrackers.KitItemTemplate
                SET item_name=:name, quantity_type=:qtype, min_quantity=:minq,
                    notes=:notes, sort_order=:sort, active=:active,
                    condition_key=:ckey, condition_value=:cval
                WHERE id=:iid
            """), {
                "name": item_name, "qtype": quantity_type, "minq": min_quantity,
                "notes": notes, "sort": sort_order,
                "active": int(active), "iid": item_id,
                "ckey": condition_key, "cval": condition_value,
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

    @staticmethod
    @handle_repository_errors
    def delete_template_item(item_id: int) -> None:
        """Permanently delete a template item and all associated records.

        Removes KitItemTaskCheck, KitItemTask, KitItemCheck, and the
        KitItemTemplate row itself.  Only safe to call on retired (active=0)
        items; caller is responsible for enforcing that guard.
        """
        with get_session() as session:
            # Remove task-check rows that belong to tasks or checks of this item
            session.execute(text("""
                DELETE tc
                FROM calltrackers.KitItemTaskCheck tc
                LEFT JOIN calltrackers.KitItemTask   tk ON tk.id = tc.task_id
                LEFT JOIN calltrackers.KitItemCheck  ic ON ic.id = tc.item_check_id
                WHERE tk.item_id = :iid OR ic.item_id = :iid
            """), {"iid": item_id})
            session.execute(text(
                "DELETE FROM calltrackers.KitItemTask WHERE item_id = :iid"
            ), {"iid": item_id})
            session.execute(text(
                "DELETE FROM calltrackers.KitItemCheck WHERE item_id = :iid"
            ), {"iid": item_id})
            session.execute(text(
                "DELETE FROM calltrackers.KitItemTemplate WHERE id = :iid"
            ), {"iid": item_id})

    # ── Spare stock ───────────────────────────────────────────────────────────

    @staticmethod
    @handle_repository_errors
    def get_all_spare_stock() -> List[Dict[str, Any]]:
        """All active template items with their current spare stock (0 if none)."""
        with get_session() as session:
            return session.execute(text("""
                SELECT
                    t.id AS item_id, t.item_name, t.quantity_type,
                    t.min_quantity, t.sort_order,
                    COALESCE(ss.id, NULL)       AS stock_id,
                    COALESCE(ss.quantity, 0)    AS quantity,
                    ss.notes                    AS stock_notes,
                    ss.updated_at
                FROM calltrackers.KitItemTemplate t
                LEFT JOIN calltrackers.KitSpareStock ss ON ss.item_id = t.id
                WHERE t.active = 1
                ORDER BY t.sort_order, t.id
            """)).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_spare_stock_for_item(item_id: int) -> Optional[Dict[str, Any]]:
        """Return the KitSpareStock row for a single item, or None."""
        with get_session() as session:
            return session.execute(text("""
                SELECT id, item_id, quantity, notes, updated_at
                FROM calltrackers.KitSpareStock
                WHERE item_id = :iid
            """), {"iid": item_id}).mappings().first()

    @staticmethod
    @handle_repository_errors
    def upsert_spare_stock(
        item_id: int,
        quantity: int,
        notes: Optional[str],
    ) -> None:
        """Set spare stock for an item (insert on first call, update thereafter)."""
        with get_session() as session:
            session.execute(text("""
                INSERT INTO calltrackers.KitSpareStock
                    (item_id, quantity, notes, updated_at)
                VALUES (:iid, :qty, :notes, NOW())
                ON DUPLICATE KEY UPDATE
                    quantity   = VALUES(quantity),
                    notes      = VALUES(notes),
                    updated_at = NOW()
            """), {"iid": item_id, "qty": quantity, "notes": notes})

    @staticmethod
    @handle_repository_errors
    def decrement_spare_stock(item_id: int, quantity: int) -> None:
        """Reduce spare stock by *quantity*, floored at 0."""
        with get_session() as session:
            session.execute(text("""
                UPDATE calltrackers.KitSpareStock
                SET quantity   = GREATEST(0, quantity - :qty),
                    updated_at = NOW()
                WHERE item_id = :iid
            """), {"iid": item_id, "qty": quantity})

    @staticmethod
    @handle_repository_errors
    def get_spares_allocation_plan() -> List[Dict[str, Any]]:
        """Items where spares are available AND at least one active session needs them.

        Returns one row per (item, session/kit) pair.
        """
        with get_session() as session:
            return session.execute(text("""
                SELECT
                    t.id AS item_id, t.item_name, t.sort_order,
                    ss.quantity AS spare_stock,
                    k.id AS kit_id, k.name AS kit_name,
                    s.id AS session_id, s.season,
                    c.id AS check_id, c.quantity_needed
                FROM calltrackers.KitSpareStock ss
                JOIN calltrackers.KitItemTemplate t ON t.id = ss.item_id
                JOIN calltrackers.KitItemCheck c ON c.item_id = t.id
                JOIN calltrackers.KitMaintenanceSession s ON s.id = c.session_id
                JOIN calltrackers.Kit k ON k.id = s.kit_id
                WHERE ss.quantity > 0
                  AND c.quantity_needed > 0
                  AND c.acquired = 0
                  AND s.status != 'released'
                ORDER BY t.sort_order, k.name
            """)).mappings().all()

    @staticmethod
    @handle_repository_errors
    def mark_check_acquired(
        session_id: int,
        item_id: int,
        note: Optional[str],
    ) -> None:
        """Set acquired=1 on a KitItemCheck row and append an optional note."""
        with get_session() as session:
            session.execute(text("""
                UPDATE calltrackers.KitItemCheck
                SET acquired = 1,
                    notes = CASE
                        WHEN :note IS NULL THEN notes
                        WHEN notes IS NULL OR notes = '' THEN :note
                        ELSE CONCAT(notes, '; ', :note)
                    END
                WHERE session_id = :sid AND item_id = :iid
            """), {"sid": session_id, "iid": item_id, "note": note})

    # ── Supplier directory ────────────────────────────────────────────────────

    @staticmethod
    @handle_repository_errors
    def get_all_suppliers(active_only: bool = True) -> List[Dict[str, Any]]:
        with get_session() as session:
            sql = "SELECT * FROM calltrackers.KitSupplier"
            if active_only:
                sql += " WHERE active = 1"
            sql += " ORDER BY name"
            return session.execute(text(sql)).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_supplier_by_id(supplier_id: int) -> Optional[Dict[str, Any]]:
        with get_session() as session:
            return session.execute(text("""
                SELECT * FROM calltrackers.KitSupplier WHERE id = :sid
            """), {"sid": supplier_id}).mappings().first()

    @staticmethod
    @handle_repository_errors
    def create_supplier(
        name: str,
        location: Optional[str],
        url: Optional[str],
        notes: Optional[str],
    ) -> int:
        with get_session() as session:
            result = session.execute(text("""
                INSERT INTO calltrackers.KitSupplier
                    (name, location, url, notes, active)
                VALUES (:name, :loc, :url, :notes, 1)
            """), {"name": name, "loc": location, "url": url, "notes": notes})
            return result.lastrowid

    @staticmethod
    @handle_repository_errors
    def update_supplier(
        supplier_id: int,
        name: str,
        location: Optional[str],
        url: Optional[str],
        notes: Optional[str],
        active: bool,
    ) -> None:
        with get_session() as session:
            session.execute(text("""
                UPDATE calltrackers.KitSupplier
                SET name=:name, location=:loc, url=:url,
                    notes=:notes, active=:active
                WHERE id=:sid
            """), {
                "sid": supplier_id, "name": name, "loc": location,
                "url": url, "notes": notes, "active": int(active),
            })

    # ── Item supplier pricing ─────────────────────────────────────────────────

    @staticmethod
    @handle_repository_errors
    def get_prices_for_item(item_id: int) -> List[Dict[str, Any]]:
        """All active pricing rows for one item, joined to supplier name."""
        with get_session() as session:
            return session.execute(text("""
                SELECT
                    p.id, p.item_id, p.supplier_id,
                    s.name AS supplier_name,
                    p.pack_qty, p.price_per_pack, p.min_order_packs,
                    p.postage, p.postage_free_threshold,
                    p.notes, p.active
                FROM calltrackers.KitItemSupplierPrice p
                JOIN calltrackers.KitSupplier s ON s.id = p.supplier_id
                WHERE p.item_id = :iid
                ORDER BY s.name, p.pack_qty
            """), {"iid": item_id}).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_all_item_prices() -> List[Dict[str, Any]]:
        """All pricing rows across all items, joined to item and supplier names."""
        with get_session() as session:
            return session.execute(text("""
                SELECT
                    p.id, p.item_id, p.supplier_id,
                    t.item_name,
                    s.name AS supplier_name,
                    p.pack_qty, p.price_per_pack, p.min_order_packs,
                    p.postage, p.postage_free_threshold,
                    p.notes, p.active
                FROM calltrackers.KitItemSupplierPrice p
                JOIN calltrackers.KitItemTemplate t ON t.id = p.item_id
                JOIN calltrackers.KitSupplier s     ON s.id = p.supplier_id
                ORDER BY t.sort_order, s.name, p.pack_qty
            """)).mappings().all()

    @staticmethod
    @handle_repository_errors
    def create_item_price(
        item_id: int,
        supplier_id: int,
        pack_qty: int,
        price_per_pack: Optional[float],
        min_order_packs: int,
        postage: float,
        postage_free_threshold: Optional[float],
        notes: Optional[str],
    ) -> int:
        with get_session() as session:
            result = session.execute(text("""
                INSERT INTO calltrackers.KitItemSupplierPrice
                    (item_id, supplier_id, pack_qty, price_per_pack,
                     min_order_packs, postage, postage_free_threshold,
                     notes, active)
                VALUES (:iid, :sup, :pqty, :ppp,
                        :mop, :post, :pft,
                        :notes, 1)
            """), {
                "iid": item_id, "sup": supplier_id,
                "pqty": pack_qty, "ppp": price_per_pack,
                "mop": min_order_packs, "post": postage,
                "pft": postage_free_threshold, "notes": notes,
            })
            return result.lastrowid

    @staticmethod
    @handle_repository_errors
    def update_item_price(
        price_id: int,
        pack_qty: int,
        price_per_pack: Optional[float],
        min_order_packs: int,
        postage: float,
        postage_free_threshold: Optional[float],
        notes: Optional[str],
        active: bool,
    ) -> None:
        with get_session() as session:
            session.execute(text("""
                UPDATE calltrackers.KitItemSupplierPrice
                SET pack_qty               = :pqty,
                    price_per_pack         = :ppp,
                    min_order_packs        = :mop,
                    postage                = :post,
                    postage_free_threshold = :pft,
                    notes                  = :notes,
                    active                 = :active
                WHERE id = :pid
            """), {
                "pid": price_id, "pqty": pack_qty, "ppp": price_per_pack,
                "mop": min_order_packs, "post": postage,
                "pft": postage_free_threshold, "notes": notes,
                "active": int(active),
            })

    @staticmethod
    @handle_repository_errors
    def delete_item_price(price_id: int) -> None:
        with get_session() as session:
            session.execute(text(
                "DELETE FROM calltrackers.KitItemSupplierPrice WHERE id = :pid"
            ), {"pid": price_id})

    # ── Order tracking ────────────────────────────────────────────────────────

    @staticmethod
    @handle_repository_errors
    def update_order_status(
        check_id: int,
        ordered_at: Optional[datetime.date],
        received_at: Optional[datetime.date],
        supplier_id: Optional[int],
    ) -> None:
        """Update order-tracking fields on a KitItemCheck row."""
        with get_session() as session:
            session.execute(text("""
                UPDATE calltrackers.KitItemCheck
                SET ordered_at  = :ordered_at,
                    received_at = :received_at,
                    supplier_id = :supplier_id
                WHERE id = :cid
            """), {
                "cid": check_id,
                "ordered_at": ordered_at,
                "received_at": received_at,
                "supplier_id": supplier_id,
            })