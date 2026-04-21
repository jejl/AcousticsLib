"""Repository for WeatherHourly operations.

Stores hourly meteorological data (Open-Meteo ERA5-Land) keyed by observation_id.
All datetime values are stored and returned as UTC-naive DATETIME values; callers
are responsible for timezone conversion if needed.
"""
import datetime as dt_module
from typing import Any, Dict, List, Optional

from loguru import logger
from sqlalchemy import text

from ...error_handlers import handle_repository_errors
from ..connection import get_session

_UPSERT_SQL = text(
    "INSERT INTO calltrackers.WeatherHourly "
    "(observation_id, `datetime`, temperature_2m, relative_humidity_2m, "
    " surface_pressure, wind_speed_10m, wind_direction_10m, "
    " precipitation, weather_code, fetched_at) "
    "VALUES (:observation_id, :datetime, :temperature_2m, :relative_humidity_2m, "
    "        :surface_pressure, :wind_speed_10m, :wind_direction_10m, "
    "        :precipitation, :weather_code, :fetched_at) "
    "ON DUPLICATE KEY UPDATE "
    "    temperature_2m       = VALUES(temperature_2m), "
    "    relative_humidity_2m = VALUES(relative_humidity_2m), "
    "    surface_pressure     = VALUES(surface_pressure), "
    "    wind_speed_10m       = VALUES(wind_speed_10m), "
    "    wind_direction_10m   = VALUES(wind_direction_10m), "
    "    precipitation        = VALUES(precipitation), "
    "    weather_code         = VALUES(weather_code), "
    "    fetched_at           = VALUES(fetched_at)"
)


class WeatherRepository:
    """Data access layer for calltrackers.WeatherHourly."""

    @staticmethod
    @handle_repository_errors
    def get_for_observation(obs_id: int) -> List[Dict[str, Any]]:
        """Return all hourly weather rows for *obs_id*, ordered by datetime."""
        with get_session() as session:
            rows = session.execute(
                text(
                    "SELECT id, observation_id, `datetime`, temperature_2m, "
                    "relative_humidity_2m, surface_pressure, "
                    "wind_speed_10m, wind_direction_10m, "
                    "precipitation, weather_code, fetched_at "
                    "FROM calltrackers.WeatherHourly "
                    "WHERE observation_id = :obs_id "
                    "ORDER BY `datetime`"
                ),
                {"obs_id": obs_id},
            ).mappings().all()
        return [dict(r) for r in rows]

    @staticmethod
    @handle_repository_errors
    def has_data(obs_id: int) -> bool:
        """Return True if at least one row exists for *obs_id*."""
        with get_session() as session:
            row = session.execute(
                text(
                    "SELECT 1 FROM calltrackers.WeatherHourly "
                    "WHERE observation_id = :obs_id LIMIT 1"
                ),
                {"obs_id": obs_id},
            ).first()
        return row is not None

    @staticmethod
    @handle_repository_errors
    def get_fetched_at(obs_id: int) -> Optional[dt_module.datetime]:
        """Return the most recent fetched_at timestamp for *obs_id*, or None."""
        with get_session() as session:
            row = session.execute(
                text(
                    "SELECT MAX(fetched_at) AS fetched_at "
                    "FROM calltrackers.WeatherHourly "
                    "WHERE observation_id = :obs_id"
                ),
                {"obs_id": obs_id},
            ).mappings().first()
        return row["fetched_at"] if row else None

    @staticmethod
    @handle_repository_errors
    def upsert_hourly(rows: List[Dict[str, Any]]) -> int:
        """Insert or update hourly weather rows.

        Each row dict must contain: ``observation_id``, ``datetime`` (UTC naive),
        and all meteorological columns.  Returns the number of rows processed.
        """
        if not rows:
            return 0
        now = dt_module.datetime.utcnow()
        with get_session() as session:
            for row in rows:
                session.execute(
                    _UPSERT_SQL,
                    {**row, "fetched_at": now},
                )
        logger.debug(f"Upserted {len(rows)} WeatherHourly rows")
        return len(rows)

    @staticmethod
    @handle_repository_errors
    def delete_for_observation(obs_id: int) -> int:
        """Delete all weather rows for *obs_id*. Returns the number of rows deleted."""
        with get_session() as session:
            result = session.execute(
                text(
                    "DELETE FROM calltrackers.WeatherHourly "
                    "WHERE observation_id = :obs_id"
                ),
                {"obs_id": obs_id},
            )
        deleted = result.rowcount
        logger.debug(f"Deleted {deleted} WeatherHourly rows for obs_id={obs_id}")
        return deleted

    @staticmethod
    @handle_repository_errors
    def get_obs_ids_without_weather() -> List[int]:
        """Return LocationLog ids that have no rows in WeatherHourly."""
        with get_session() as session:
            rows = session.execute(
                text(
                    "SELECT LL.id "
                    "FROM calltrackers.LocationLog LL "
                    "LEFT JOIN calltrackers.WeatherHourly WH "
                    "    ON LL.id = WH.observation_id "
                    "WHERE WH.observation_id IS NULL "
                    "  AND LL.lat IS NOT NULL "
                    "  AND LL.lon IS NOT NULL "
                    "ORDER BY LL.start_time DESC"
                )
            ).mappings().all()
        return [r["id"] for r in rows]

    @staticmethod
    @handle_repository_errors
    def get_coverage_summary() -> List[Dict[str, Any]]:
        """Return per-observation weather coverage counts for admin/backfill views."""
        with get_session() as session:
            rows = session.execute(
                text(
                    "SELECT LL.id, LL.obscode, LL.start_time, LL.end_time, "
                    "       LL.lat, LL.lon, "
                    "       COUNT(WH.id) AS weather_rows, "
                    "       MAX(WH.fetched_at) AS last_fetched "
                    "FROM calltrackers.LocationLog LL "
                    "LEFT JOIN calltrackers.WeatherHourly WH "
                    "    ON LL.id = WH.observation_id "
                    "WHERE LL.lat IS NOT NULL AND LL.lon IS NOT NULL "
                    "GROUP BY LL.id, LL.obscode, LL.start_time, LL.end_time, LL.lat, LL.lon "
                    "ORDER BY LL.start_time DESC"
                )
            ).mappings().all()
        return [dict(r) for r in rows]
