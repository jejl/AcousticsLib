"""WAV file metadata extraction.

Reads recorder identity, timestamp, location and audio parameters from WAV files
using four strategies in priority order:

1. GUANO header (guano.GuanoFile) — preferred; covers all modern recorders.
2. wavinfo INFO chunk — covers FrontierLabs BAR-LT recorders that embed
   start/end/location in the WAV title tag and the serial number in the album tag.
3. Filename pattern matching — covers Wildlife Acoustics (NT01-NT08), Titley
   Chorus (NT11-NT15), BAR-LT (JEJL, serial_YYYYMMDD, name_YYYYMMDD), and a
   generic YYYYMMDD_HHMMSS suffix.
4. Hardcoded serial-number map — for known recorders that produce no GUANO
   metadata and whose filenames do not embed a serial number.

All timestamps are returned as tz-aware ``datetime`` objects localised to
Australia/Hobart (via :func:`acousticslib.time_utils.localize_hobart`).

Public parsing utilities (may also be called directly):
    parse_bar_title_long   Parse a BAR-LT long-format WAV title tag
    parse_bar_title_short  Parse a BAR-LT short-format filename

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


def parse_bar_title_long(
    title: str,
) -> tuple[Optional[datetime], Optional[datetime], Optional[float], Optional[float]]:
    """Parse a FrontierLabs BAR-LT long-format WAV title tag.

    BAR-LT recorders write the recording window and GPS fix into the WAV INFO
    title field, e.g.::

        S20250403T190158.327009+1100_E20250403T194658.320872+1100_-41.09533+146.65492

    The ``.wav`` extension is stripped if present before matching.

    Args:
        title: Raw title string from the WAV INFO chunk (or filename stem).

    Returns:
        ``(start, end, lat, lon)`` as tz-aware datetimes and floats, or
        ``(None, None, None, None)`` if the string does not match.
    """
    pattern = (
        r"S(?P<start>\d{8}T\d{6}\.\d{6}[+-]\d{4})_"
        r"E(?P<end>\d{8}T\d{6}\.\d{6}[+-]\d{4})_"
        r"(?P<lat>[+-]?\d+\.\d+)"
        r"(?P<lon>[+-]?\d+\.\d+)"
    )
    m = re.match(pattern, title.replace(".wav", ""))
    if not m:
        return None, None, None, None

    def _parse(dtstr: str) -> datetime:
        # '20250403T190158.327009+1100' → '2025-04-03T19:01:58.327009+11:00'
        date = dtstr[:8]
        time_part = dtstr[9:15]
        micro = dtstr[16:22]
        offset = dtstr[22:]
        return datetime.fromisoformat(
            f"{date[:4]}-{date[4:6]}-{date[6:8]}T"
            f"{time_part[:2]}:{time_part[2:4]}:{time_part[4:6]}.{micro}"
            f"{offset[:3]}:{offset[3:]}"
        )

    try:
        start = _parse(m.group("start"))
        end = _parse(m.group("end"))
        lat = float(m.group("lat"))
        lon = float(m.group("lon"))
        return start, end, lat, lon
    except (ValueError, IndexError):
        return None, None, None, None


def parse_bar_title_short(
    title: str,
) -> tuple[Optional[str], Optional[str], Optional[datetime]]:
    """Parse a FrontierLabs BAR-LT short-format filename.

    Handles three filename formats (with or without ``.wav`` extension):

    1. **Serial + local time**: ``00014281_20260124_000500[.wav]``
       All-digit prefix is treated as the recorder serial number.
       Returns ``(None, serial, start_time)``.

    2. **Name + local time**: ``B8_20260122_043600[.wav]``
       Letter-prefixed token is the recorder name (caller must resolve to
       serial via a recorder table lookup).
       Returns ``(rec_name, None, start_time)``.

    3. **Name + offset time**: ``JEJL_20251215T044000+1100_suffix[.wav]``
       Recorder name followed by an explicit UTC-offset timestamp.
       Returns ``(rec_name, None, start_time)``.

    Local-time formats (1 & 2) are localised to Australia/Hobart.

    Returns:
        ``(rec_name, serial, start_time)`` — whichever fields were found;
        unresolved fields are ``None``.  Returns ``(None, None, None)`` if no
        format matches.
    """
    stem = Path(title).stem  # strip .wav if present

    # Format 1: <serial>_YYYYMMDD_hhmmss (all-digit prefix)
    m = re.match(
        r"^(?P<serial>\d+)_(?P<date>\d{8})_(?P<time>\d{6})$",
        stem,
        re.IGNORECASE,
    )
    if m:
        start = localize_hobart(
            datetime.strptime(f"{m.group('date')}{m.group('time')}", "%Y%m%d%H%M%S")
        )
        return None, m.group("serial"), start

    # Format 2: <rec_name>_YYYYMMDD_hhmmss (letter-prefixed name, local time)
    m = re.match(
        r"^(?P<rec_name>[A-Za-z]\w*)_(?P<date>\d{8})_(?P<time>\d{6})$",
        stem,
        re.IGNORECASE,
    )
    if m:
        start = localize_hobart(
            datetime.strptime(f"{m.group('date')}{m.group('time')}", "%Y%m%d%H%M%S")
        )
        return m.group("rec_name"), None, start

    # Format 3: <rec_name>_YYYYMMDDThhmmss±hhmm[_suffix] (name + UTC-offset)
    m = re.match(
        r"^(?P<rec_name>\w+)_(?P<ts>\d{8}T\d{6}[+-]\d{4})",
        stem,
    )
    if m:
        try:
            start = parse_guano_timestamp(m.group("ts"))
            return m.group("rec_name"), None, start
        except ValueError:
            pass

    return None, None, None


def read_wav_metadata(path: str | Path) -> WavMetadata:
    """Extract metadata from a WAV file using GUANO, wavinfo, filename, and hardcoded fallbacks.

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
    # Step 2.5 — wavinfo INFO chunk (FrontierLabs BAR-LT recorders)
    # Only attempted when GUANO left fields unpopulated.
    # ------------------------------------------------------------------
    if meta.timestamp is None or meta.lat is None or meta.serial_no is None:
        try:
            import wavinfo
            wi = wavinfo.WavInfoReader(str(path))
            if wi.info is not None:
                if wi.info.album and meta.serial_no is None:
                    meta.serial_no = str(wi.info.album)
                if wi.info.title:
                    _start, _end, _lat, _lon = parse_bar_title_long(wi.info.title)
                    if _start is not None:
                        if meta.timestamp is None:
                            meta.timestamp = localize_hobart(_start)
                        if meta.lat is None and _lat is not None:
                            meta.lat = _lat
                            meta.lon = _lon
        except Exception as exc:
            logger.debug(f"wavinfo read failed for '{path.name}': {exc}")

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
        # BAR-LT serial_YYYYMMDD_hhmmss: '00014281_20260124_000500.wav'
        # (All-digit prefix — must be checked before the generic pattern below)
        m = re.match(r"^(?P<serial>\d+)_(?P<date>\d{8})_(?P<time>\d{6})$", stem)
        if m:
            meta.serial_no = meta.serial_no or m.group("serial")
            meta.timestamp = localize_hobart(
                datetime.strptime(f"{m.group('date')}{m.group('time')}", "%Y%m%d%H%M%S")
            )
            return

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

        # BAR-LT name_YYYYMMDDThhmmss±hhmm[_suffix]: 'B8_20251215T044000+1100.wav'
        m = re.match(r"^(?P<rec_name>[A-Za-z]\w*)_(?P<ts>\d{8}T\d{6}[+-]\d{4})", stem)
        if m:
            try:
                meta.recorder_name = meta.recorder_name or m.group("rec_name")
                meta.timestamp = parse_guano_timestamp(m.group("ts"))
                return
            except ValueError:
                pass

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
