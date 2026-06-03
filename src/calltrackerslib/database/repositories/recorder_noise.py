"""Repository for calltrackers.RecorderNoiseFloor.

One row per (recorder_id, sample_rate, measured_at) where measured_at is
truncated to the hour by the service layer before writing.
"""
from __future__ import annotations

import datetime as dt_module
import json
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from ...error_handlers import handle_repository_errors
from ..connection import get_session

_UPSERT_SQL = text(
    "INSERT INTO calltrackers.RecorderNoiseFloor "
    "(recorder_id, measured_at, sample_rate, gain, integration_time_s, "
    " noise_floor_dbfs, snr_db, spectrum_json, source_file, source) "
    "VALUES (:recorder_id, :measured_at, :sample_rate, :gain, :integration_time_s, "
    "        :noise_floor_dbfs, :snr_db, :spectrum_json, :source_file, :source) "
    "ON DUPLICATE KEY UPDATE "
    "    gain               = VALUES(gain), "
    "    integration_time_s = VALUES(integration_time_s), "
    "    noise_floor_dbfs   = VALUES(noise_floor_dbfs), "
    "    snr_db             = VALUES(snr_db), "
    "    spectrum_json      = VALUES(spectrum_json), "
    "    source_file        = VALUES(source_file), "
    "    source             = VALUES(source)"
)


class RecorderNoiseRepository:
    """Data access layer for calltrackers.RecorderNoiseFloor."""

    @staticmethod
    @handle_repository_errors
    def upsert(row: Dict[str, Any]) -> None:
        """Insert or update one noise measurement.

        ``row`` must contain: recorder_id, measured_at (truncated to hour),
        sample_rate, gain (or None), integration_time_s, noise_floor_dbfs,
        snr_db (or None), spectrum_json (list or JSON string), source_file,
        source.
        """
        spec = row.get("spectrum_json")
        if not isinstance(spec, str):
            spec = json.dumps(spec)
        with get_session() as session:
            session.execute(_UPSERT_SQL, {**row, "spectrum_json": spec})

    @staticmethod
    @handle_repository_errors
    def get_for_recorder(
        recorder_id: int,
        sample_rate: Optional[int] = None,
        since: Optional[dt_module.datetime] = None,
    ) -> List[Dict[str, Any]]:
        """Return measurements for *recorder_id*, ordered by measured_at.

        Optionally filter by *sample_rate* and/or *since* (inclusive lower bound).
        spectrum_json is decoded back to a Python list.
        """
        clauses = ["recorder_id = :recorder_id"]
        params: Dict[str, Any] = {"recorder_id": recorder_id}
        if sample_rate is not None:
            clauses.append("sample_rate = :sample_rate")
            params["sample_rate"] = sample_rate
        if since is not None:
            clauses.append("measured_at >= :since")
            params["since"] = since
        where = " AND ".join(clauses)
        with get_session() as session:
            rows = session.execute(
                text(
                    "SELECT id, recorder_id, measured_at, sample_rate, gain, "
                    "       integration_time_s, noise_floor_dbfs, snr_db, "
                    "       spectrum_json, source_file, source "
                    f"FROM calltrackers.RecorderNoiseFloor "
                    f"WHERE {where} "
                    "ORDER BY measured_at"
                ),
                params,
            ).mappings().all()
        result = []
        for r in rows:
            d = dict(r)
            if isinstance(d.get("spectrum_json"), str):
                d["spectrum_json"] = json.loads(d["spectrum_json"])
            result.append(d)
        return result

    @staticmethod
    @handle_repository_errors
    def get_latest_before(
        recorder_id: int,
        sample_rate: int,
        before_dt: dt_module.datetime,
    ) -> Optional[dt_module.datetime]:
        """Return the most recent measured_at < *before_dt* for this recorder/rate."""
        with get_session() as session:
            row = session.execute(
                text(
                    "SELECT MAX(measured_at) AS ts "
                    "FROM calltrackers.RecorderNoiseFloor "
                    "WHERE recorder_id = :rid AND sample_rate = :sr "
                    "  AND measured_at < :before_dt"
                ),
                {"rid": recorder_id, "sr": sample_rate, "before_dt": before_dt},
            ).mappings().first()
        return row["ts"] if row else None

    @staticmethod
    @handle_repository_errors
    def get_earliest_after(
        recorder_id: int,
        sample_rate: int,
        after_dt: dt_module.datetime,
    ) -> Optional[dt_module.datetime]:
        """Return the earliest measured_at > *after_dt* for this recorder/rate."""
        with get_session() as session:
            row = session.execute(
                text(
                    "SELECT MIN(measured_at) AS ts "
                    "FROM calltrackers.RecorderNoiseFloor "
                    "WHERE recorder_id = :rid AND sample_rate = :sr "
                    "  AND measured_at > :after_dt"
                ),
                {"rid": recorder_id, "sr": sample_rate, "after_dt": after_dt},
            ).mappings().first()
        return row["ts"] if row else None

    @staticmethod
    @handle_repository_errors
    def get_all_recorders_with_data() -> List[Dict[str, Any]]:
        """Return distinct recorders that have at least one noise measurement.

        Returns dicts with keys: recorder_id, recorder_name, sample_rates (list),
        first_measured_at, last_measured_at, measurement_count.
        """
        with get_session() as session:
            rows = session.execute(
                text(
                    "SELECT rnf.recorder_id, "
                    "       r.name AS recorder_name, "
                    "       GROUP_CONCAT(DISTINCT rnf.sample_rate ORDER BY rnf.sample_rate) "
                    "           AS sample_rates_csv, "
                    "       MIN(rnf.measured_at) AS first_measured_at, "
                    "       MAX(rnf.measured_at) AS last_measured_at, "
                    "       COUNT(*) AS measurement_count "
                    "FROM calltrackers.RecorderNoiseFloor rnf "
                    "JOIN calltrackers.Recorder r ON r.id = rnf.recorder_id "
                    "GROUP BY rnf.recorder_id, r.name "
                    "ORDER BY r.name"
                )
            ).mappings().all()
        result = []
        for r in rows:
            d = dict(r)
            csv = d.pop("sample_rates_csv", "") or ""
            d["sample_rates"] = [int(x) for x in csv.split(",") if x]
            result.append(d)
        return result
