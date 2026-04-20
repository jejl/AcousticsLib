"""Repository for LocationLog (Observation) operations.

All start_time / end_time values returned by :meth:`get_all_for_recorder` are
tz-aware ``datetime`` objects in Australia/Hobart.  Missing DB values default to
``2020-01-01`` and ``2100-01-01`` respectively, matching the behaviour of the
original CTDatabase implementation.
"""
import datetime as dt_module
from typing import Any, Dict, FrozenSet, List, Optional

from loguru import logger
from sqlalchemy import text

from ...error_handlers import handle_repository_errors
from ...exceptions import ValidationError
from ...time_utils import localize_hobart
from ..connection import get_session

# Classifier type → (processed_col, time_col)
_CLASSIFIER_COLUMN_MAP: Dict[str, tuple] = {
    "bat":     ("processed_ultra",    "processed_ultra_time"),
    "bittern": ("processed_acoustic", "processed_acoustic_time"),
    "curlew":  ("processed_curlew",   "processed_curlew_time"),
}

_DEFAULT_START = "2020-01-01 00:00:00"
_DEFAULT_END   = "2100-01-01 00:00:00"


def _localize(val: Any, default_str: str) -> dt_module.datetime:
    """Convert a raw DB datetime value to a tz-aware Australia/Hobart datetime."""
    if isinstance(val, dt_module.datetime):
        d = val
    else:
        s = str(val) if val else default_str
        try:
            d = dt_module.datetime.fromisoformat(s)
        except Exception:
            d = dt_module.datetime.fromisoformat(default_str)
    return localize_hobart(d)


class ObservationRepository:
    """Data access layer for calltrackers.LocationLog."""

    # Columns the UI is permitted to write back to LocationLog
    EDITABLE_COLUMNS: FrozenSet[str] = frozenset({
        "Location_Description", "Comment", "lat", "lon",
        "square", "sub_square", "person",
        "processed_ultra", "processed_acoustic", "processed_curlew",
        "processed_ultra_time", "processed_acoustic_time", "processed_curlew_time",
        "acoustic_on_NAS", "ultrasonic_on_NAS",
        "acoustic_on_object_storage", "ultrasonic_on_object_storage",
    })

    @staticmethod
    @handle_repository_errors
    def get_by_obscode(obscode: str) -> Optional[Dict[str, Any]]:
        """Return the LocationLog entry for *obscode*, or None."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT id, obscode, start_time, end_time, recorder_id, program_id, "
                    "lat, lon, square, sub_square, Location_Description, Comment "
                    "FROM calltrackers.LocationLog WHERE obscode = :obscode"
                ),
                {"obscode": obscode},
            ).mappings().first()

    @staticmethod
    @handle_repository_errors
    def get_by_id(obs_id: int) -> Optional[Dict[str, Any]]:
        """Return a single observation joined with recorder and program info, or None."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT LL.id, LL.obscode, LL.recorder_id, LL.program_id, "
                    "LL.start_time, LL.end_time, LL.square, "
                    "OP.directory AS program_directory, "
                    "R.name AS recorder_name, "
                    "R.short_name AS recorder_short_name "
                    "FROM calltrackers.LocationLog LL "
                    "JOIN calltrackers.ObservingPrograms OP ON LL.program_id = OP.pk "
                    "JOIN calltrackers.Recorder R ON LL.recorder_id = R.id "
                    "WHERE LL.id = :id"
                ),
                {"id": obs_id},
            ).mappings().first()

    @staticmethod
    @handle_repository_errors
    def get_all_for_recorder(recorder_id: int) -> List[Dict[str, Any]]:
        """Return all observations for a recorder with tz-aware start/end times.

        If *recorder_id* is 0 or None, returns all observations across all recorders.
        """
        with get_session() as session:
            if recorder_id:
                rows = session.execute(
                    text(
                        "SELECT id, start_time, end_time, recorder_id, "
                        "square, lat, lon, program_id "
                        "FROM calltrackers.LocationLog WHERE recorder_id = :rid"
                    ),
                    {"rid": recorder_id},
                ).mappings().all()
            else:
                rows = session.execute(
                    text(
                        "SELECT id, start_time, end_time, recorder_id, "
                        "square, lat, lon, program_id "
                        "FROM calltrackers.LocationLog"
                    )
                ).mappings().all()

        result = []
        for row in rows:
            row = dict(row)
            row["start_time"] = _localize(row["start_time"], _DEFAULT_START)
            row["end_time"]   = _localize(row["end_time"],   _DEFAULT_END)
            result.append(row)
        return result

    @staticmethod
    @handle_repository_errors
    def get_all_summary() -> List[Dict[str, Any]]:
        """Return all LocationLog rows joined with recorder, program, and observer info."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT "
                    "LL.id, LL.obscode, LL.sequence_num, LL.start_time, LL.end_time, "
                    "R.name        AS recorder_name, "
                    "R.short_name  AS recorder_short_name, "
                    "OP.Name       AS program_name, "
                    "OP.NameBrief  AS program_brief, "
                    "OP.directory  AS program_directory, "
                    "LL.square, LL.sub_square, LL.lat, LL.lon, "
                    "LL.Location_Description, LL.Comment, "
                    "P.PersName    AS observer_name, "
                    "LL.processed_ultra, LL.processed_acoustic, LL.processed_curlew, "
                    "LL.processed_ultra_time, LL.processed_acoustic_time, "
                    "LL.processed_curlew_time, "
                    "LL.acoustic_on_NAS, LL.ultrasonic_on_NAS, "
                    "LL.acoustic_on_object_storage, LL.ultrasonic_on_object_storage "
                    "FROM calltrackers.LocationLog LL "
                    "JOIN  calltrackers.Recorder R           ON LL.recorder_id = R.id "
                    "JOIN  calltrackers.ObservingPrograms OP ON LL.program_id  = OP.pk "
                    "LEFT JOIN calltrackers.People P         ON LL.person      = P.id "
                    "ORDER BY LL.start_time DESC"
                )
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def check_overlaps(
        recorder_id: int,
        start_time: dt_module.datetime,
        end_time: dt_module.datetime,
    ) -> List[Dict[str, Any]]:
        """Return LocationLog rows that overlap the given time window for a recorder."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT * FROM calltrackers.LocationLog "
                    "WHERE recorder_id = :rid "
                    "AND (start_time < :end_time AND end_time > :start_time)"
                ),
                {"rid": recorder_id, "start_time": start_time, "end_time": end_time},
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def update_fields(obs_id: int, fields: Dict[str, Any]) -> None:
        """Update a subset of editable fields on a LocationLog row.

        Only columns in :attr:`EDITABLE_COLUMNS` are permitted; any others
        raise :class:`~acousticslib.exceptions.ValidationError`.
        """
        if not fields:
            return
        disallowed = set(fields) - ObservationRepository.EDITABLE_COLUMNS
        if disallowed:
            raise ValidationError(
                f"Column(s) {disallowed} are not editable via this method."
            )
        set_clause = ", ".join(f"`{col}` = :{col}" for col in fields)
        with get_session() as session:
            session.execute(
                text(
                    f"UPDATE calltrackers.LocationLog "
                    f"SET {set_clause} WHERE id = :obs_id"
                ),
                {**fields, "obs_id": obs_id},
            )

    @staticmethod
    @handle_repository_errors
    def update_processing_status(
        observation_id: int,
        classifier_type: str,
        status: Optional[int],
        processing_time: Optional[dt_module.datetime] = None,
    ) -> None:
        """Update the processed_* flag and timestamp for one classifier type.

        Args:
            observation_id: LocationLog.id
            classifier_type: 'bat', 'bittern', or 'curlew'
            status: 1 (results), 0 (none), -1 (not suitable), -2 (data issue), None (clear)
            processing_time: Datetime of processing; None leaves the column as NULL.
        """
        if classifier_type not in _CLASSIFIER_COLUMN_MAP:
            raise ValueError(
                f"Unknown classifier_type: {classifier_type!r}. "
                f"Must be one of: {list(_CLASSIFIER_COLUMN_MAP)}"
            )
        processed_col, time_col = _CLASSIFIER_COLUMN_MAP[classifier_type]
        with get_session() as session:
            session.execute(
                text(
                    f"UPDATE calltrackers.LocationLog "
                    f"SET `{processed_col}` = :status, `{time_col}` = :proc_time "
                    f"WHERE id = :obs_id"
                ),
                {"status": status, "proc_time": processing_time, "obs_id": observation_id},
            )

    @staticmethod
    @handle_repository_errors
    def get_next_sequence_num() -> int:
        """Return MAX(sequence_num) + 1, or 1 if the table is empty."""
        with get_session() as session:
            row = session.execute(
                text("SELECT MAX(sequence_num) AS maxid FROM calltrackers.LocationLog")
            ).mappings().first()
            return (row["maxid"] or 0) + 1

    @staticmethod
    @handle_repository_errors
    def insert_locationlog(entries: List[Dict[str, Any]]) -> int:
        """Bulk-insert LocationLog entries. Returns the number of rows inserted."""
        if not entries:
            return 0
        insert_sql = text(
            "INSERT INTO calltrackers.LocationLog "
            "(sequence_num, start_time, end_time, start_file, end_file, recorder_id, "
            " program_id, lat, lon, square, correct_position, Location_Description, "
            " Comment, person) "
            "VALUES (:sequence_num, :start_time, :end_time, :start_file, :end_file, "
            "        :recorder_id, :program_id, :lat, :lon, :square, :correct_position, "
            "        :Location_Description, :Comment, :person)"
        )
        with get_session() as session:
            for entry in entries:
                session.execute(insert_sql, entry)
        return len(entries)

    @staticmethod
    @handle_repository_errors
    def delete(obs_id: int) -> None:
        """Delete a LocationLog row by *obs_id*."""
        with get_session() as session:
            session.execute(
                text("DELETE FROM calltrackers.LocationLog WHERE id = :id"),
                {"id": obs_id},
            )

    @staticmethod
    @handle_repository_errors
    def get_start_file_lookup() -> Dict[str, int]:
        """Return {start_file: id} for all LocationLog rows."""
        with get_session() as session:
            rows = session.execute(
                text("SELECT id, start_file FROM calltrackers.LocationLog")
            ).mappings().all()
        return {r["start_file"]: r["id"] for r in rows}

    @staticmethod
    @handle_repository_errors
    def get_all_obscode_program_ids() -> List[Dict[str, Any]]:
        """Return [{obscode, program_id}] for every LocationLog row with a non-null obscode."""
        with get_session() as session:
            rows = session.execute(
                text(
                    "SELECT obscode, program_id "
                    "FROM calltrackers.LocationLog "
                    "WHERE obscode IS NOT NULL"
                )
            ).mappings().all()
        return [{"obscode": r["obscode"], "program_id": r["program_id"]} for r in rows]

    @staticmethod
    @handle_repository_errors
    def get_by_sequence_num(seq_num: int) -> Optional[Dict[str, Any]]:
        """Return the LocationLog row for *seq_num*, or None."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT id, obscode, sequence_num "
                    "FROM calltrackers.LocationLog WHERE sequence_num = :seq"
                ),
                {"seq": seq_num},
            ).mappings().first()

    @staticmethod
    @handle_repository_errors
    def update_fields_by_sequence_num(seq_num: int, fields: Dict[str, Any]) -> None:
        """Update editable fields on the LocationLog row matching *seq_num*.

        Only columns in :attr:`EDITABLE_COLUMNS` are permitted.
        """
        if not fields:
            return
        disallowed = set(fields) - ObservationRepository.EDITABLE_COLUMNS
        if disallowed:
            raise ValidationError(
                f"Column(s) {disallowed} are not editable via this method."
            )
        set_clause = ", ".join(f"`{col}` = :{col}" for col in fields)
        with get_session() as session:
            session.execute(
                text(
                    f"UPDATE calltrackers.LocationLog "
                    f"SET {set_clause} WHERE sequence_num = :seq_num"
                ),
                {**fields, "seq_num": seq_num},
            )

    @staticmethod
    @handle_repository_errors
    def get_recorder_observer(obs_id: int) -> Dict[str, Any]:
        """Return recorder_name and observer_name for *obs_id*, or an empty dict."""
        with get_session() as session:
            row = session.execute(
                text(
                    "SELECT R.name AS recorder_name, P.PersName AS observer_name "
                    "FROM calltrackers.LocationLog LL "
                    "JOIN calltrackers.Recorder R ON LL.recorder_id = R.id "
                    "LEFT JOIN calltrackers.People P ON LL.person = P.id "
                    "WHERE LL.id = :id"
                ),
                {"id": obs_id},
            ).mappings().first()
        return dict(row) if row else {}
