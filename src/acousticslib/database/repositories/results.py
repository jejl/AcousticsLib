"""Repository for BTO acoustic classifier results tables.

Security note:
    ``add_results()`` and ``delete_stale_results()`` accept a *table_name*
    parameter that is interpolated directly into SQL.  They validate it against
    ``_TABLE_ALLOWLIST`` before use.  Do not add dynamic-table methods without
    this guard.
"""
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from ...error_handlers import handle_repository_errors
from ..connection import get_session

# Only these table names may be used in dynamic SQL
_TABLE_ALLOWLIST = frozenset({"ResultsBats", "ResultsBitterns", "ResultsCurlews"})


class ResultsRepository:
    """Data access layer for classifier results tables."""

    # ------------------------------------------------------------------ #
    # Read                                                                 #
    # ------------------------------------------------------------------ #

    @staticmethod
    @handle_repository_errors
    def get_bats(obs_id: int, min_probability: float = 0.9) -> List[Dict[str, Any]]:
        """Return bat classifier results for an observation above *min_probability*."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT Scientific_Name, English_Name, Probability, Actual_Date "
                    "FROM calltrackers.ResultsBats "
                    "WHERE observation_id = :obs_id AND Probability >= :prob"
                ),
                {"obs_id": obs_id, "prob": min_probability},
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_bitterns(obs_id: int, min_score: float = 0.9) -> List[Dict[str, Any]]:
        """Return Australasian Bittern classifier results above *min_score*."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT Scientific_Name, English_Name, Score, Actual_Date "
                    "FROM calltrackers.ResultsBitterns "
                    "WHERE observation_id = :obs_id AND Score >= :score"
                ),
                {"obs_id": obs_id, "score": min_score},
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_curlews(obs_id: int, min_score: float = 0.9) -> List[Dict[str, Any]]:
        """Return Far Eastern Curlew classifier results above *min_score*."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT Scientific_Name, English_Name, Score, Actual_Date "
                    "FROM calltrackers.ResultsCurlews "
                    "WHERE observation_id = :obs_id AND Score >= :score"
                ),
                {"obs_id": obs_id, "score": min_score},
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_all_for_observation(obs_id: int, table_name: str) -> List[Dict[str, Any]]:
        """Return all result rows for an observation with file-level detail columns.

        Returns ``Original_File_Name``, ``Actual_Datetime``, and
        ``observation_id`` — the columns needed by the event-finder tool to
        locate detections within individual WAV files.

        Args:
            obs_id: LocationLog.id
            table_name: Must be in ``_TABLE_ALLOWLIST``.
        """
        if table_name not in _TABLE_ALLOWLIST:
            raise ValueError(
                f"Table '{table_name}' is not in the allowed list: {_TABLE_ALLOWLIST}"
            )
        with get_session() as session:
            return session.execute(
                text(
                    f"SELECT Original_File_Name, Actual_Datetime, observation_id "
                    f"FROM calltrackers.`{table_name}` "
                    f"WHERE observation_id = :obs_id "
                    f"ORDER BY Actual_Datetime"
                ),
                {"obs_id": obs_id},
            ).mappings().all()

    # ------------------------------------------------------------------ #
    # Write                                                                #
    # ------------------------------------------------------------------ #

    @staticmethod
    @handle_repository_errors
    def add_results(table_name: str, rows: List[Dict[str, Any]]) -> int:
        """Bulk-insert classifier result rows. Returns the number of rows inserted.

        Args:
            table_name: Must be in ``_TABLE_ALLOWLIST``.
            rows: List of dicts; all dicts must share the same keys.
        """
        if table_name not in _TABLE_ALLOWLIST:
            raise ValueError(
                f"Table '{table_name}' is not in the allowed list: {_TABLE_ALLOWLIST}"
            )
        if not rows:
            return 0

        columns = list(rows[0].keys())
        col_str    = ", ".join(f"`{c}`" for c in columns)
        params_str = ", ".join(f":{c}" for c in columns)
        sql = f"INSERT INTO calltrackers.`{table_name}` ({col_str}) VALUES ({params_str})"

        with get_session() as session:
            result = session.execute(text(sql), rows)
            return result.rowcount

    @staticmethod
    @handle_repository_errors
    def get_scored_with_metadata(
        obs_id: int,
        table_name: str,
        min_score: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """Return result rows joined with Metadata for an observation.

        Returns ``Original_File_Name``, ``start_time``, ``Actual_Datetime``,
        ``Score``.  When *min_score* is provided and > 0 only rows at or above
        the threshold are returned.

        Args:
            obs_id: LocationLog.id
            table_name: Must be in ``_TABLE_ALLOWLIST``.
            min_score: Optional score floor; None means no filtering.
        """
        if table_name not in _TABLE_ALLOWLIST:
            raise ValueError(
                f"Table '{table_name}' is not in the allowed list: {_TABLE_ALLOWLIST}"
            )
        if obs_id is None:
            return []
        apply_filter = min_score is not None and min_score > 0.0
        params: dict = {"obs_id": obs_id}
        if apply_filter:
            params["min_score"] = min_score
        with get_session() as session:
            rows = session.execute(
                text(
                    f"SELECT r.Original_File_Name, m.start_time, r.Actual_Datetime, r.Score "
                    f"FROM calltrackers.`{table_name}` r "
                    f"JOIN calltrackers.Metadata m "
                    f"  ON m.file_name = r.Original_File_Name "
                    f"  AND m.observation_id = :obs_id "
                    f"WHERE r.observation_id = :obs_id"
                    + (" AND r.Score >= :min_score" if apply_filter else "")
                ),
                params,
            ).mappings().all()
        return [dict(r) for r in rows]

    @staticmethod
    @handle_repository_errors
    def delete_stale_results(obs_id: int, table_name: str) -> int:
        """Delete all results for *obs_id* from *table_name*. Returns rows deleted.

        Args:
            obs_id: The LocationLog.id.
            table_name: Must be in ``_TABLE_ALLOWLIST``.
        """
        if table_name not in _TABLE_ALLOWLIST:
            raise ValueError(
                f"Table '{table_name}' is not in the allowed list: {_TABLE_ALLOWLIST}"
            )
        with get_session() as session:
            result = session.execute(
                text(
                    f"DELETE FROM calltrackers.`{table_name}` "
                    f"WHERE observation_id = :obs_id"
                ),
                {"obs_id": obs_id},
            )
            return result.rowcount
