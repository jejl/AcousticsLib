"""Audio file discovery and indexing utilities.

Ported and consolidated from:
    CallTrackersProcessing/db_utils.py  — get_audio_file_names, build_file_index
    CallTrackersAdmin ClassifierResultsService.build_file_index

These utilities are intentionally simple filesystem walkers with no database
or audio-library dependencies so they can be imported cheaply anywhere.
"""
import os
from pathlib import Path


def get_audio_file_names(data_dir: str | Path) -> list[Path]:
    """Return paths to all audio files under *data_dir*.

    Includes ``.wav`` and ``.mp3`` files (case-insensitive).
    Excludes:
        - Files whose name ends with ``lowpass.wav`` / ``lowpass.WAV``
        - Files containing more than one ``.`` (e.g. ``file.backup.wav``)
        - Dotfiles

    Args:
        data_dir: Root directory to search (walked recursively).

    Returns:
        List of :class:`~pathlib.Path` objects for matching files.
    """
    audio_exts = {".wav", ".mp3"}
    result: list[Path] = []

    for dirpath, _, filenames in os.walk(data_dir):
        for name in filenames:
            if name.startswith("."):
                continue
            p = Path(dirpath) / name
            if p.suffix.lower() not in audio_exts:
                continue
            if name.count(".") != 1:
                continue
            lower = name.lower()
            if lower.endswith("lowpass.wav") or lower.endswith("lowpass.mp3"):
                continue
            result.append(p)

    return result


def build_file_index(data_dir: str | Path) -> dict[str, Path]:
    """Return a ``{filename: full_path}`` index for all WAV files under *data_dir*.

    Dotfiles are excluded.  If the same filename appears in more than one
    sub-directory, the last one encountered wins (with a warning logged).

    Args:
        data_dir: Root directory to search (walked recursively).

    Returns:
        Dictionary mapping bare filename (e.g. ``"NT03-WEDGE_20240321_193402.wav"``)
        to its absolute :class:`~pathlib.Path`.
    """
    from loguru import logger

    index: dict[str, Path] = {}
    for dirpath, _, filenames in os.walk(data_dir):
        for name in filenames:
            if name.startswith(".") or not name.lower().endswith(".wav"):
                continue
            full = Path(dirpath) / name
            if name in index:
                logger.warning(
                    f"Duplicate filename '{name}' found; "
                    f"replacing '{index[name]}' with '{full}'"
                )
            index[name] = full
    return index
