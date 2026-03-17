"""WAV file metadata extraction.

Reads recorder identity, timestamp, location and audio parameters from WAV files
using three strategies in priority order:

1. GUANO header (guano.GuanoFile) — preferred; covers all modern recorders.
2. Filename pattern matching — covers Wildlife Acoustics (NT01-NT08), Titley
   Chorus (NT11-NT15), BAR-LT (JEJL), and a generic YYYYMMDD_HHMMSS suffix.
3. Hardcoded serial-number map — for known recorders that produce no GUANO
   metadata and whose filenames do not embed a serial number.

All timestamps are returned as tz-aware ``datetime`` objects localised to
Australia/Hobart (via :func:`acousticslib.time_utils.localize_hobart`).

Usage::

    from acousticslib.audio.metadata import read_wav_metadata

    meta = read_wav_metadata("/data/NT03-WEDGE_20240321_193402.wav")
    print(meta.timestamp, meta.serial_no, meta.duration_sec)
"""
import re
import wave
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from loguru import logger

from ..exceptions import WavMetadataError
from ..time_utils import localize_hobart, parse_guano_timestamp

# ---------------------------------------------------------------------------
# Hardcoded serial numbers for recorders that produce no GUANO Serial field.
# Keys are substrings matched against the WAV *filename* (basename only).
# Order matters for GSHANNON / GSHANNON2: the more-specific key must be first.
# ---------------------------------------------------------------------------
_FILENAME_TO_SERIAL: dict[str, str] = {
    "GSHANNON2": "751165",
    "GSHANNON":  "697403",
    "GSHAN":     "697403",
    "NT11":      "697404",
    "NT12":      "711343",
    "NT13":      "796919",
    "JEJL":      "00023696",
}

# Recorder name embedded in GUANO for Wildlife Acoustics and Titley recorders
_WA_RECORDER_KEY   = "WA|Song Meter|Prefix"
_ANABAT_ASSET_KEY  = "Anabat|Asset ID"

# Generic YYYYMMDD_HHMMSS suffix pattern (e.g. UNIT-01__1__20251122_192000.wav)
_YYYYMMDD_HHMMSS = re.compile(r".*(\d{8})_(\d{6})\.wav$", re.IGNORECASE)


@dataclass
class WavMetadata:
    """All metadata extracted from a single WAV file.

    Audio parameters (``nchannels``, ``sample_rate``, ``duration_sec``) are
    populated from the WAV header and are always present for valid WAV files.

    All other fields are ``None`` when the information could not be determined.
    """
    path: Path

    # Location
    lat: Optional[float] = None
    lon: Optional[float] = None
    elevation: Optional[float] = None

    # Recorder identity
    serial_no: Optional[str] = None
    recorder_name: Optional[str] = None

    # Timestamp — always tz-aware Australia/Hobart when not None
    timestamp: Optional[datetime] = None

    # Environmental
    temperature: Optional[float] = None

    # Audio parameters (populated from WAV header)
    nchannels: Optional[int] = None
    sample_rate: Optional[int] = None
    duration_sec: Optional[float] = None

    @property
    def date_path(self) -> Optional[str]:
        """Return a ``YYYY_MM_DD`` directory string derived from the timestamp."""
        if self.timestamp is None:
            return None
        return self.timestamp.strftime("%Y_%m_%d")

    @property
    def recorder_date_path(self) -> Optional[str]:
        """Return a ``recorder_name/YYYY_MM_DD`` path string."""
        if self.recorder_name is None or self.timestamp is None:
            return None
        return f"{self.recorder_name}/{self.date_path}"


def read_wav_metadata(path: str | Path) -> WavMetadata:
    """Extract metadata from a WAV file using GUANO, filename, and hardcoded fallbacks.

    Args:
        path: Full path to the WAV file.

    Returns:
        :class:`WavMetadata` populated with all available information.

    Raises:
        WavMetadataError: If the file cannot be opened as a WAV file.
    """
    path = Path(path)
    meta = WavMetadata(path=path)

    # ------------------------------------------------------------------
    # Step 1 — WAV header (audio parameters; always attempted)
    # ------------------------------------------------------------------
    try:
        with wave.open(str(path), "rb") as wf:
            meta.nchannels = wf.getnchannels()
            meta.sample_rate = wf.getframerate()
            nframes = wf.getnframes()
            meta.duration_sec = nframes / meta.sample_rate if meta.sample_rate else None
    except Exception as exc:
        raise WavMetadataError(f"Cannot open WAV file '{path}': {exc}") from exc

    # ------------------------------------------------------------------
    # Step 2 — GUANO header (location, timestamp, recorder identity, temp)
    # ------------------------------------------------------------------
    try:
        import guano
        gfile = guano.GuanoFile(str(path), strict=False)
        _apply_guano(meta, gfile)
    except WavMetadataError:
        raise
    except Exception as exc:
        logger.debug(f"GUANO read failed for '{path.name}': {exc}")

    # ------------------------------------------------------------------
    # Step 3 — Filename timestamp fallback (when GUANO gave no timestamp)
    # ------------------------------------------------------------------
    if meta.timestamp is None:
        _apply_filename_timestamp(meta)

    # ------------------------------------------------------------------
    # Step 4 — Serial number fallback (when GUANO gave no serial)
    # ------------------------------------------------------------------
    if meta.serial_no is None:
        _apply_hardcoded_serial(meta)

    return meta


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _apply_guano(meta: WavMetadata, gfile) -> None:
    """Populate *meta* from an open GuanoFile object."""
    if "Loc Position" in gfile:
        meta.lat, meta.lon = gfile["Loc Position"]
    if "Loc Elevation" in gfile:
        meta.elevation = float(gfile["Loc Elevation"])
    if _WA_RECORDER_KEY in gfile:
        meta.recorder_name = gfile[_WA_RECORDER_KEY]
    if _ANABAT_ASSET_KEY in gfile:
        meta.recorder_name = gfile[_ANABAT_ASSET_KEY]
    if "Serial" in gfile:
        meta.serial_no = str(gfile["Serial"])
    if "Temperature Int" in gfile:
        try:
            meta.temperature = float(gfile["Temperature Int"])
        except (ValueError, TypeError) as exc:
            logger.warning(
                f"Invalid Temperature Int value {gfile['Temperature Int']!r} "
                f"in '{meta.path.name}': {exc}"
            )
    if "Timestamp" in gfile:
        try:
            meta.timestamp = parse_guano_timestamp(gfile["Timestamp"])
        except ValueError as exc:
            logger.warning(
                f"Cannot parse GUANO Timestamp in '{meta.path.name}': {exc}"
            )
    else:
        logger.debug(f"No GUANO Timestamp in '{meta.path.name}'")


def _apply_filename_timestamp(meta: WavMetadata) -> None:
    """Attempt to extract a timestamp from the WAV filename.

    Patterns recognised (matched against the basename without extension):

    NT01–NT08  ``<RECID>_<YYYYMMDD>_<HHMMSS>``
    NT11–NT15  ``<NAME>_<NAME>_<YYYY-MM-DD>_<HH-MM-SS>``
    ac_/us_    ``<prefix>_<YYYY-MM-DD>_<HH-MM-SS>``
    JEJL       ``JEJL_<ISO8601>_<description>``
    Generic    anything ending with ``_<YYYYMMDD>_<HHMMSS>.wav``
    """
    name = meta.path.name
    stem = meta.path.stem  # without .wav

    try:
        if any(f"NT{n:02d}" in name for n in range(1, 9)):
            # NT01-SITENAME_20231115_194345
            parts = stem.split("_")
            meta.recorder_name = meta.recorder_name or parts[0]
            meta.timestamp = localize_hobart(
                datetime.strptime(f"{parts[1]}_{parts[2]}", "%Y%m%d_%H%M%S")
            )
            return

        if any(f"NT{n}" in name for n in range(11, 99)):
            # NT11_DUNNART_2024-02-21_06-14-29
            parts = stem.split("_")
            meta.recorder_name = meta.recorder_name or parts[0]
            meta.timestamp = localize_hobart(
                datetime.strptime(f"{parts[-2]}_{parts[-1]}", "%Y-%m-%d_%H-%M-%S")
            )
            return

        if name.startswith("ac_") or name.startswith("us_"):
            # ac_2024-03-01_12-00-00 or us_...
            parts = stem.split("_")
            meta.timestamp = localize_hobart(
                datetime.strptime(f"{parts[-2]}_{parts[-1]}", "%Y-%m-%d_%H-%M-%S")
            )
            return

        if "JEJL" in name:
            # JEJL_20231010T203800+1100_Sunset
            parts = stem.split("_", 2)
            meta.recorder_name = meta.recorder_name or "JEJL"
            meta.timestamp = parse_guano_timestamp(parts[1])
            return

        m = _YYYYMMDD_HHMMSS.match(name)
        if m:
            date_str, time_str = m.groups()
            meta.timestamp = localize_hobart(
                datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S")
            )
            return

    except (ValueError, IndexError) as exc:
        logger.warning(f"Filename timestamp parse failed for '{name}': {exc}")

    logger.warning(f"No timestamp pattern matched for '{name}'")


def _apply_hardcoded_serial(meta: WavMetadata) -> None:
    """Look up a serial number from the hardcoded map using the filename as key.

    Entries are checked in definition order; the map is ordered so that the
    more-specific key (GSHANNON2) is checked before its prefix (GSHANNON).
    """
    name = meta.path.name
    for key, serial in _FILENAME_TO_SERIAL.items():
        if key in name:
            meta.serial_no = serial
            logger.debug(
                f"Assigned hardcoded serial {serial!r} to '{name}' via key {key!r}"
            )
            return
