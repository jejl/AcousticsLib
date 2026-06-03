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


def build_file_index(data_dir: str | Path) -> dict[str, list[Path]]:
    """Return a ``{filename: [full_path, ...]}`` index for all WAV files under *data_dir*.

    Each filename maps to a list of all paths where that filename appears — the
    same filename can legitimately exist under multiple recorder sub-directories.
    Dotfiles and files inside any directory named ``staging`` are excluded.

    Args:
        data_dir: Root directory to search (walked recursively).

    Returns:
        Dictionary mapping bare filename (e.g. ``"NT03-WEDGE_20240321_193402.wav"``)
        to a list of absolute :class:`~pathlib.Path` objects.
    """
    index: dict[str, list[Path]] = {}
    for dirpath, dirs, filenames in os.walk(data_dir, topdown=True):
        dirs[:] = [d for d in dirs if d != "staging"]
        for name in filenames:
            if name.startswith(".") or not name.lower().endswith(".wav"):
                continue
            index.setdefault(name, []).append(Path(dirpath) / name)
    return index
