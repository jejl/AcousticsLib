"""Repository for in-app documentation pages (calltrackers.DocPage / DocRecorderType)."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from ...error_handlers import handle_repository_errors
from ..connection import get_session


class DocsRepository:
    """CRUD operations for calltrackers.DocPage and DocRecorderType."""

    # ── DocPage ────────────────────────────────────────────────────────────────

    @staticmethod
    @handle_repository_errors
    def get_all_pages() -> List[Dict[str, Any]]:
        """Return all top-level pages (variant_of IS NULL), ordered by sort_order."""
        with get_session() as session:
            rows = session.execute(text("""
                SELECT id, title, content, sort_order, updated_at, updated_by,
                       COALESCE(section_type, 'internal') AS section_type
                FROM calltrackers.DocPage
                WHERE variant_of IS NULL
                ORDER BY sort_order, id
            """)).mappings().all()
            return [dict(r) for r in rows]

    @staticmethod
    @handle_repository_errors
    def get_public_pages() -> List[Dict[str, Any]]:
        """Return only public-guide pages (section_type='public'), ordered by sort_order."""
        with get_session() as session:
            rows = session.execute(text("""
                SELECT id, title, content, sort_order, updated_at, updated_by,
                       COALESCE(section_type, 'internal') AS section_type
                FROM calltrackers.DocPage
                WHERE variant_of IS NULL
                  AND COALESCE(section_type, 'internal') = 'public'
                ORDER BY sort_order, id
            """)).mappings().all()
            return [dict(r) for r in rows]

    @staticmethod
    @handle_repository_errors
    def get_variants(parent_id: int) -> List[Dict[str, Any]]:
        """Return recorder-type variant pages for a given parent page."""
        with get_session() as session:
            rows = session.execute(text("""
                SELECT id, title, content, sort_order, updated_at, updated_by, variant_of
                FROM calltrackers.DocPage
                WHERE variant_of = :pid
                ORDER BY sort_order, id
            """), {"pid": parent_id}).mappings().all()
            return [dict(r) for r in rows]

    @staticmethod
    @handle_repository_errors
    def save_page(
        page_id: Optional[int],
        title: str,
        content: str,
        sort_order: int,
        updated_by: str,
        variant_of: Optional[int] = None,
        section_type: str = "internal",
    ) -> int:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        with get_session() as session:
            if page_id is None:
                result = session.execute(text("""
                    INSERT INTO calltrackers.DocPage
                        (title, content, sort_order, updated_at, updated_by,
                         variant_of, section_type)
                    VALUES (:title, :content, :sort_order, :now, :by, :vof, :st)
                """), {"title": title, "content": content, "sort_order": sort_order,
                       "now": now, "by": updated_by, "vof": variant_of,
                       "st": section_type})
                return result.lastrowid
            else:
                session.execute(text("""
                    UPDATE calltrackers.DocPage
                    SET title        = :title,
                        content      = :content,
                        sort_order   = :sort_order,
                        updated_at   = :now,
                        updated_by   = :by,
                        section_type = :st
                    WHERE id = :id
                """), {"title": title, "content": content, "sort_order": sort_order,
                       "now": now, "by": updated_by, "id": page_id,
                       "st": section_type})
                return page_id

    @staticmethod
    @handle_repository_errors
    def delete_page(page_id: int) -> None:
        """Delete a page and all its variant children."""
        with get_session() as session:
            session.execute(text(
                "DELETE FROM calltrackers.DocPage WHERE variant_of = :id"
            ), {"id": page_id})
            session.execute(text(
                "DELETE FROM calltrackers.DocPage WHERE id = :id"
            ), {"id": page_id})

    @staticmethod
    @handle_repository_errors
    def reorder_pages(ordered_ids: List[int]) -> None:
        """Re-assign sort_order (×10 spacing) to match the given page-id order."""
        with get_session() as session:
            for rank, page_id in enumerate(ordered_ids):
                session.execute(text("""
                    UPDATE calltrackers.DocPage
                    SET sort_order = :so
                    WHERE id = :id AND variant_of IS NULL
                """), {"so": (rank + 1) * 10, "id": page_id})

    # ── DocRecorderType ────────────────────────────────────────────────────────

    @staticmethod
    @handle_repository_errors
    def get_recorder_types() -> List[Dict[str, Any]]:
        """Return all registered documentation recorder types."""
        with get_session() as session:
            rows = session.execute(text("""
                SELECT id, display_name, marker_key, sort_order
                FROM calltrackers.DocRecorderType
                ORDER BY sort_order, id
            """)).mappings().all()
            return [dict(r) for r in rows]

    @staticmethod
    @handle_repository_errors
    def save_recorder_type(
        type_id: Optional[int],
        display_name: str,
        marker_key: str,
        sort_order: int,
    ) -> int:
        with get_session() as session:
            if type_id is None:
                result = session.execute(text("""
                    INSERT INTO calltrackers.DocRecorderType
                        (display_name, marker_key, sort_order)
                    VALUES (:dn, :mk, :so)
                """), {"dn": display_name, "mk": marker_key, "so": sort_order})
                return result.lastrowid
            else:
                session.execute(text("""
                    UPDATE calltrackers.DocRecorderType
                    SET display_name = :dn,
                        marker_key   = :mk,
                        sort_order   = :so
                    WHERE id = :id
                """), {"dn": display_name, "mk": marker_key, "so": sort_order, "id": type_id})
                return type_id

    @staticmethod
    @handle_repository_errors
    def delete_recorder_type(type_id: int) -> None:
        with get_session() as session:
            session.execute(text(
                "DELETE FROM calltrackers.DocRecorderType WHERE id = :id"
            ), {"id": type_id})

    @staticmethod
    @handle_repository_errors
    def get_available_recorders() -> List[str]:
        """Return distinct 'Manufacturer Model' labels from the Recorder table."""
        with get_session() as session:
            rows = session.execute(text("""
                SELECT DISTINCT CONCAT(Manufacturer, ' ', Model) AS label
                FROM calltrackers.Recorder
                WHERE Manufacturer IS NOT NULL AND Manufacturer != ''
                  AND Model        IS NOT NULL AND Model        != ''
                ORDER BY label
            """)).mappings().all()
            return [r["label"] for r in rows]
