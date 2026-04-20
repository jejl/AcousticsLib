"""Repository for the calltrackers.Metadata table."""
from typing import Any, Callable, Dict, List, Optional

from sqlalchemy import bindparam, text

from ...error_handlers import handle_repository_errors
from ..connection import get_session


class MetadataRepository:
    """Data access layer for WAV file metadata records."""

    @staticmethod
    @handle_repository_errors
    def delete_by_filenames(file_names: List[str], batch_size: int = 500) -> int:
        """Delete Metadata rows matching *file_names* (batched). Returns rows deleted."""
        if not file_names:
            return 0
        delete_sql = text(
            "DELETE FROM calltrackers.Metadata WHERE file_name IN :names"
        ).bindparams(bindparam("names", expanding=True))
        total = 0
        with get_session() as session:
            for i in range(0, len(file_names), batch_size):
                batch = file_names[i : i + batch_size]
                if batch:
                    result = session.execute(delete_sql, {"names": batch})
                    total += result.rowcount
        return total

    @staticmethod
    @handle_repository_errors
    def insert_bulk(
        rows: List[Dict[str, Any]],
        batch_size: int = 100,
        progress_callback: Optional[Callable[[float, str], None]] = None,
    ) -> int:
        """Bulk-insert Metadata rows. Returns total rows inserted."""
        if not rows:
            return 0
        insert_sql = text(
            "INSERT INTO calltrackers.Metadata "
            "(file_name, recorder_id, observation_id, start_time, data_length_s, "
            " n_channels, n_frames, frame_rate, sample_width, compression, "
            " data_length_bytes, internal_temp) "
            "VALUES (:file_name, :recorder_id, :observation_id, :start_time, "
            "        :data_length_s, :n_channels, :n_frames, :frame_rate, "
            "        :sample_width, :compression, :data_length_bytes, :internal_temp)"
        )
        with get_session() as session:
            for i in range(0, len(rows), batch_size):
                session.execute(insert_sql, rows[i : i + batch_size])
                if progress_callback:
                    done = min(i + batch_size, len(rows))
                    progress_callback(
                        done / len(rows),
                        f"Saving WAV file metadata... ({done}/{len(rows)})",
                    )
        return len(rows)

    @staticmethod
    @handle_repository_errors
    def delete_by_observation(obs_id: int) -> int:
        """Delete all Metadata rows for *obs_id*. Returns rows deleted."""
        with get_session() as session:
            result = session.execute(
                text("DELETE FROM calltrackers.Metadata WHERE observation_id = :id"),
                {"id": obs_id},
            )
            return result.rowcount
