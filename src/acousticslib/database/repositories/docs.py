"""Repository for in-app documentation pages (calltrackers.DocPage)."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from ...error_handlers import handle_repository_errors
from ..connection import get_session


class DocsRepository:
    """CRUD operations for calltrackers.DocPage."""

    @staticmethod
    @handle_repository_errors
    def get_all_pages() -> List[Dict[str, Any]]:
        with get_session() as session:
            rows = session.execute(text("""
                SELECT id, title, content, sort_order, updated_at, updated_by
                FROM calltrackers.DocPage
                ORDER BY sort_order, id
            """)).mappings().all()
            return [dict(r) for r in rows]

    @staticmethod
    @handle_repository_errors
    def save_page(
        page_id: Optional[int],
        title: str,
        content: str,
        sort_order: int,
        updated_by: str,
    ) -> int:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        with get_session() as session:
            if page_id is None:
                result = session.execute(text("""
                    INSERT INTO calltrackers.DocPage
                        (title, content, sort_order, updated_at, updated_by)
                    VALUES (:title, :content, :sort_order, :now, :by)
                """), {"title": title, "content": content,
                       "sort_order": sort_order, "now": now, "by": updated_by})
                return result.lastrowid
            else:
                session.execute(text("""
                    UPDATE calltrackers.DocPage
                    SET title      = :title,
                        content    = :content,
                        sort_order = :sort_order,
                        updated_at = :now,
                        updated_by = :by
                    WHERE id = :id
                """), {"title": title, "content": content,
                       "sort_order": sort_order, "now": now, "by": updated_by,
                       "id": page_id})
                return page_id

    @staticmethod
    @handle_repository_errors
    def delete_page(page_id: int) -> None:
        with get_session() as session:
            session.execute(text(
                "DELETE FROM calltrackers.DocPage WHERE id = :id"
            ), {"id": page_id})
