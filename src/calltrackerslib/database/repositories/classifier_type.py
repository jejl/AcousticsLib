"""Repository for the ClassifierType table.

Defines every BTO acoustic classifier known to the system — its canonical key,
human-readable label, the substring used to detect it in CSV Classifier_Name
values, the Results table it writes to, and the LocationLog columns it updates.

The table is created automatically on first read if it does not already exist
(``CREATE TABLE IF NOT EXISTS``) and seeded with the three original classifiers
(bat / bittern / curlew), so no manual migration is required.

A module-level cache avoids repeated DB round-trips.  Call
``ClassifierTypeRepository.invalidate_cache()`` after inserting a new row so
that the rest of the process picks up the change immediately.
"""
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from ...error_handlers import handle_repository_errors
from ..connection import get_session

_CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS calltrackers.ClassifierType (
        classifier_key   VARCHAR(50)   NOT NULL,
        display_name     VARCHAR(100)  NOT NULL,
        csv_identifier   VARCHAR(200)  NOT NULL,
        results_table    VARCHAR(100)  NOT NULL,
        loc_log_column   VARCHAR(100)  NOT NULL,
        loc_log_time_col VARCHAR(100)  NOT NULL,
        active           TINYINT(1)    NOT NULL DEFAULT 1,
        sort_order       INT           NOT NULL DEFAULT 0,
        PRIMARY KEY (classifier_key)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

_SEED_ROWS = [
    {
        "classifier_key":   "bat",
        "display_name":     "Bats (ultrasonic)",
        "csv_identifier":   "classifespAUS",
        "results_table":    "ResultsBats",
        "loc_log_column":   "processed_ultra",
        "loc_log_time_col": "processed_ultra_time",
        "active":           1,
        "sort_order":       0,
    },
    {
        "classifier_key":   "bittern",
        "display_name":     "Bittern",
        "csv_identifier":   "AustralasianBittern",
        "results_table":    "ResultsBitterns",
        "loc_log_column":   "processed_acoustic",
        "loc_log_time_col": "processed_acoustic_time",
        "active":           1,
        "sort_order":       1,
    },
    {
        "classifier_key":   "curlew",
        "display_name":     "Curlew",
        "csv_identifier":   "FarEasternCurlew",
        "results_table":    "ResultsCurlews",
        "loc_log_column":   "processed_curlew",
        "loc_log_time_col": "processed_curlew_time",
        "active":           1,
        "sort_order":       2,
    },
]

# Module-level cache — populated on first DB read, cleared by invalidate_cache().
_cache: Optional[List[Dict[str, Any]]] = None


def _ensure_table(session) -> None:
    """Create table if absent and insert seed rows (INSERT IGNORE)."""
    session.execute(text(_CREATE_TABLE_SQL))
    for row in _SEED_ROWS:
        session.execute(
            text(
                "INSERT IGNORE INTO calltrackers.ClassifierType "
                "(classifier_key, display_name, csv_identifier, results_table, "
                " loc_log_column, loc_log_time_col, active, sort_order) "
                "VALUES (:classifier_key, :display_name, :csv_identifier, "
                "        :results_table, :loc_log_column, :loc_log_time_col, "
                "        :active, :sort_order)"
            ),
            row,
        )


class ClassifierTypeRepository:
    """Data access for calltrackers.ClassifierType."""

    @staticmethod
    def invalidate_cache() -> None:
        """Clear the in-process cache so the next read hits the database."""
        global _cache
        _cache = None

    @staticmethod
    @handle_repository_errors
    def get_all(active_only: bool = True) -> List[Dict[str, Any]]:
        """Return all classifier type rows, sorted by sort_order.

        Results are cached in-process after the first call.  Pass
        ``active_only=False`` to include inactive rows.
        """
        global _cache
        if _cache is None:
            with get_session() as session:
                _ensure_table(session)
                rows = session.execute(
                    text(
                        "SELECT classifier_key, display_name, csv_identifier, "
                        "       results_table, loc_log_column, loc_log_time_col, "
                        "       active, sort_order "
                        "FROM calltrackers.ClassifierType "
                        "ORDER BY sort_order, classifier_key"
                    )
                ).mappings().all()
                _cache = [dict(r) for r in rows]

        if active_only:
            return [r for r in _cache if r["active"]]
        return list(_cache)

    @staticmethod
    @handle_repository_errors
    def get_by_key(classifier_key: str) -> Optional[Dict[str, Any]]:
        """Return a single row by primary key, or None if not found."""
        rows = ClassifierTypeRepository.get_all(active_only=False)
        for r in rows:
            if r["classifier_key"] == classifier_key:
                return r
        return None

    @staticmethod
    @handle_repository_errors
    def insert(
        classifier_key: str,
        display_name: str,
        csv_identifier: str,
        results_table: str,
        loc_log_column: str,
        loc_log_time_col: str,
        sort_order: int = 0,
    ) -> None:
        """Insert a new classifier type row and invalidate the cache."""
        with get_session() as session:
            _ensure_table(session)
            session.execute(
                text(
                    "INSERT INTO calltrackers.ClassifierType "
                    "(classifier_key, display_name, csv_identifier, results_table, "
                    " loc_log_column, loc_log_time_col, active, sort_order) "
                    "VALUES (:key, :name, :csv_id, :table, :col, :time_col, 1, :order)"
                ),
                {
                    "key":     classifier_key,
                    "name":    display_name,
                    "csv_id":  csv_identifier,
                    "table":   results_table,
                    "col":     loc_log_column,
                    "time_col": loc_log_time_col,
                    "order":   sort_order,
                },
            )
        ClassifierTypeRepository.invalidate_cache()
