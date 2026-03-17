"""Timezone-aware datetime utilities for acoustic data management.

All timestamps in this library are normalised to Australia/Hobart (AEST/AEDT).
The key design principle, matching field-recorder behaviour, is that ``localize_hobart``
*replaces* the timezone without converting the time value.  Recorders embed a UTC
offset in their GUANO headers that may not accurately reflect DST transitions;
the recorded time value itself is always the correct local (Hobart) clock time.

Public API:
    HOBART                     ZoneInfo for Australia/Hobart
    localize_hobart            Attach Hobart tzinfo to any datetime (replace, not convert)
    parse_guano_timestamp      Parse a raw GUANO Timestamp field to a Hobart-aware datetime
    match_observation_window   Match a timestamp to a single LocationLog observation window
"""
import datetime
import re
from typing import Any, Dict, Optional, Sequence
from zoneinfo import ZoneInfo

from loguru import logger

from .exceptions import AmbiguousObservationError

HOBART = ZoneInfo("Australia/Hobart")


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------

def localize_hobart(dt: datetime.datetime) -> datetime.datetime:
    """Return *dt* with its tzinfo replaced by Australia/Hobart.

    This is a *replace*, not a conversion: the clock time is preserved regardless
    of any tzinfo already attached.  This matches the behaviour of all existing
    code in WAVManager, CTDatabase, and ClassifierResultsService, where recorder
    timestamps are assumed to be in local Hobart time even when the embedded UTC
    offset is incorrect (e.g. a recorder that was not updated for DST).
    """
    return dt.replace(tzinfo=HOBART)


def parse_guano_timestamp(ts: Any) -> datetime.datetime:
    """Parse a GUANO Timestamp field to a tz-aware Australia/Hobart datetime.

    The guano library may return either a ``datetime.datetime`` object (when it
    can parse the value itself) or a raw string.  This function handles both and
    also fixes the two known malformed string formats produced by field recorders:

    1. Compact ISO 8601 without separators: ``20220827T050000+1000``
       (produced by some Wildlife Acoustics firmware versions)
    2. Malformed fractional seconds: ``2023-10-10T17:40:02.-31003+10:00``
       (produced by some Titley/Anabat firmware versions)

    The returned datetime always has tzinfo=HOBART (via :func:`localize_hobart`).
    Raises:
        ValueError: If the timestamp string cannot be parsed by any strategy.
    """
    if isinstance(ts, datetime.datetime):
        return localize_hobart(ts)

    ts_str = str(ts).strip()

    # Strategy 1: well-formed ISO 8601 (handles the common case first)
    try:
        return localize_hobart(datetime.datetime.fromisoformat(ts_str))
    except ValueError:
        pass

    # Strategy 2: compact format — YYYYMMDDTHHmmss+HHMM (no separators)
    # Example: 20220827T050000+1000
    compact = re.match(
        r"^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})([+-]\d{4})$", ts_str
    )
    if compact:
        yr, mo, dy, hr, mi, se, off = compact.groups()
        off_fmt = f"{off[:3]}:{off[3:]}"  # +1000 → +10:00
        try:
            return localize_hobart(
                datetime.datetime.fromisoformat(
                    f"{yr}-{mo}-{dy}T{hr}:{mi}:{se}{off_fmt}"
                )
            )
        except ValueError:
            pass

    # Strategy 3: malformed fractional seconds — fix non-digit chars and limit to 6
    # Example: 2023-10-10T17:40:02.-31003+10:00
    malformed = re.match(
        r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(.*?)([+-]\d{2}:\d{2})$", ts_str
    )
    if malformed:
        dt_part, frac, offset = malformed.groups()
        frac_clean = re.sub(r"\D", "", frac)[:6]  # keep only digits, max 6
        try:
            return localize_hobart(
                datetime.datetime.fromisoformat(
                    f"{dt_part}.{frac_clean}{offset}"
                )
            )
        except ValueError:
            pass

    raise ValueError(f"Cannot parse GUANO timestamp: {ts_str!r}")


# ---------------------------------------------------------------------------
# Observation window matching
# ---------------------------------------------------------------------------

def match_observation_window(
    timestamp: datetime.datetime,
    observations: Sequence[Dict[str, Any]],
    buffer_minutes: int = 60,
) -> Optional[Dict[str, Any]]:
    """Match *timestamp* to a single LocationLog observation window.

    Each entry in *observations* must have tz-aware ``start_time`` and
    ``end_time`` keys (as returned by ``ObservationRepository.get_all_for_recorder``).

    The match window is::

        (start_time - buffer) <= timestamp <= (end_time + buffer)

    Args:
        timestamp:       The tz-aware datetime to match.  If naive it is first
                         localised to Hobart via :func:`localize_hobart`.
        observations:    Sequence of observation dicts with ``start_time``,
                         ``end_time``, and ``id`` keys.
        buffer_minutes:  Tolerance applied symmetrically around the window.
                         Defaults to 60 minutes (matching ClassifierResultsService).

    Returns:
        The single matching observation dict, or ``None`` if no match was found.

    Raises:
        AmbiguousObservationError: If two or more windows match the timestamp.
    """
    if timestamp.tzinfo is None:
        timestamp = localize_hobart(timestamp)

    buffer = datetime.timedelta(minutes=buffer_minutes)

    matches = [
        obs for obs in observations
        if (obs["start_time"] - buffer) <= timestamp <= (obs["end_time"] + buffer)
    ]

    if len(matches) == 1:
        return matches[0]
    if len(matches) == 0:
        logger.debug(f"No observation window found for {timestamp}")
        return None

    ids = [obs["id"] for obs in matches]
    raise AmbiguousObservationError(
        f"Timestamp {timestamp} matched {len(matches)} observation windows "
        f"(ids: {ids}). Cannot determine a unique match."
    )
