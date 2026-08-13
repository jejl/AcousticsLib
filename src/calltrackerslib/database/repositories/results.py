"""Repository for BTO acoustic classifier results tables.

Security note:
    ``add_results()`` and ``delete_stale_results()`` accept a *table_name*
    parameter that is interpolated directly into SQL.  They validate it against
    ``_get_table_allowlist()`` before use.  Do not add dynamic-table methods
    without this guard.
"""
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from ...error_handlers import handle_repository_errors
from ..connection import get_session

def _get_table_allowlist() -> frozenset:
    from .classifier_type import ClassifierTypeRepository
    return frozenset(r["results_table"] for r in ClassifierTypeRepository.get_all())


# Column-name asymmetry across the results tables: ResultsBats scores its rows
# in ``Probability``, every other table uses ``Score``.  These values ARE
# interpolated into SQL, so this mapping is the only permitted source for them
# and it is only ever keyed by an already-allowlisted table name.
#
# The eventual fix is a ``score_column`` on the ClassifierType table, which
# would make this data rather than code — but that is a schema migration for a
# cosmetic gain, so it has not been done.
_SCORE_COLUMN: Dict[str, str] = {"ResultsBats": "Probability"}
_DEFAULT_SCORE_COLUMN = "Score"
_SPECIES_COLUMN = "English_Name"
_NO_ID_SENTINEL = "No ID"

# An unidentified detection is recorded inconsistently: ResultsBats writes
# ``Species = 'No ID'`` but leaves ``English_Name`` as an EMPTY STRING, not
# 'No ID'.  Filtering on the sentinel alone therefore lets 242k unidentified
# bat rows through as a species whose name is ''.  Both forms must be excluded.
_IDENTIFIED_SQL = (
    f"r.`{_SPECIES_COLUMN}` IS NOT NULL "
    f"AND TRIM(r.`{_SPECIES_COLUMN}`) <> '' "
    f"AND r.`{_SPECIES_COLUMN}` <> :no_id "
    f"AND (r.`Species` IS NULL OR r.`Species` <> :no_id)"
)


def _score_column(table_name: str) -> str:
    """Return the score column for an ALREADY-ALLOWLISTED table name."""
    return _SCORE_COLUMN.get(table_name, _DEFAULT_SCORE_COLUMN)


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
            table_name: Must be a registered results table (from ClassifierType).
        """
        allowlist = _get_table_allowlist()
        if table_name not in allowlist:
            raise ValueError(
                f"Table '{table_name}' is not in the allowed list: {allowlist}"
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

    @staticmethod
    @handle_repository_errors
    def get_observation_ids_with_detections(
        table_name: str,
        min_score: Optional[float] = None,
        species: Optional[str] = None,
        exclude_no_id: bool = True,
    ) -> List[Dict[str, Any]]:
        """Return per-observation detection counts across the whole table.

        This is the only cross-observation read in this repository — every
        other method is scoped to a single ``observation_id``.  It exists so a
        caller can ask "which observations have a qualifying detection?" in one
        query rather than one query per observation.

        Returns ``[{"observation_id": int, "n": int}, ...]`` for every
        observation with at least one qualifying row.  Rows an import could not
        match to a deployment carry ``observation_id = -1``; they are returned
        like any other and simply fail to match a real LocationLog id.

        Args:
            table_name: Must be a registered results table (from ClassifierType).
            min_score: Applied to ``Probability`` (ResultsBats) or ``Score``
                (all others).  None means no score filtering.
            species: Exact ``English_Name`` match, bound as a parameter.
            exclude_no_id: Drop rows with no species identification — a blank
                English_Name as well as the 'No ID' sentinel.  Ignored when
                *species* is given, since that is already specific.
        """
        allowlist = _get_table_allowlist()
        if table_name not in allowlist:
            raise ValueError(
                f"Table '{table_name}' is not in the allowed list: {allowlist}"
            )

        score_col = _score_column(table_name)   # literal, from a constant
        clauses: List[str] = []
        params: Dict[str, Any] = {}
        if min_score is not None:
            clauses.append(f"r.`{score_col}` >= :min_score")
            params["min_score"] = min_score
        if species:
            clauses.append(f"r.`{_SPECIES_COLUMN}` = :species")
            params["species"] = species
        elif exclude_no_id:
            clauses.append(f"({_IDENTIFIED_SQL})")
            params["no_id"] = _NO_ID_SENTINEL

        where = f"WHERE {' AND '.join(clauses)} " if clauses else ""
        with get_session() as session:
            rows = session.execute(
                text(
                    f"SELECT r.observation_id AS observation_id, COUNT(*) AS n "
                    f"FROM calltrackers.`{table_name}` r "
                    f"{where}"
                    f"GROUP BY r.observation_id"
                ),
                params,
            ).mappings().all()
        return [dict(r) for r in rows]

    @staticmethod
    @handle_repository_errors
    def get_distinct_species(table_name: str) -> List[Dict[str, Any]]:
        """Return ``[{"species": str, "n": int}]`` ordered by frequency.

        There is no species reference table anywhere in the schema — species
        are free text written straight from the BTO CSV — so a species picker
        has to be built from the data itself.  Unidentified rows are excluded.

        Args:
            table_name: Must be a registered results table (from ClassifierType).
        """
        allowlist = _get_table_allowlist()
        if table_name not in allowlist:
            raise ValueError(
                f"Table '{table_name}' is not in the allowed list: {allowlist}"
            )
        with get_session() as session:
            rows = session.execute(
                text(
                    f"SELECT r.`{_SPECIES_COLUMN}` AS species, COUNT(*) AS n "
                    f"FROM calltrackers.`{table_name}` r "
                    f"WHERE {_IDENTIFIED_SQL} "
                    f"GROUP BY r.`{_SPECIES_COLUMN}` "
                    f"ORDER BY n DESC"
                ),
                {"no_id": _NO_ID_SENTINEL},
            ).mappings().all()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ #
    # Write                                                                #
    # ------------------------------------------------------------------ #

    @staticmethod
    @handle_repository_errors
    def add_results(table_name: str, rows: List[Dict[str, Any]]) -> int:
        """Bulk-insert classifier result rows. Returns the number of rows inserted.

        Args:
            table_name: Must be a registered results table (from ClassifierType).
            rows: List of dicts; all dicts must share the same keys.
        """
        allowlist = _get_table_allowlist()
        if table_name not in allowlist:
            raise ValueError(
                f"Table '{table_name}' is not in the allowed list: {allowlist}"
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
            table_name: Must be a registered results table (from ClassifierType).
            min_score: Optional score floor; None means no filtering.
        """
        allowlist = _get_table_allowlist()
        if table_name not in allowlist:
            raise ValueError(
                f"Table '{table_name}' is not in the allowed list: {allowlist}"
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
            table_name: Must be a registered results table (from ClassifierType).
        """
        allowlist = _get_table_allowlist()
        if table_name not in allowlist:
            raise ValueError(
                f"Table '{table_name}' is not in the allowed list: {allowlist}"
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
