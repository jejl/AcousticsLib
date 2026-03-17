"""Higher-level file management utilities.

Provides:
    find_duplicate_wav_files    Detect WAV files with the same name in multiple dirs
    organize_by_date_recorder   Move/copy WAVs into recorder_name/YYYY_MM_DD hierarchy

Ported and generalised from:
    CallTrackersProcessing/find_dupes.py
    CallTrackersProcessing/move_solutions.py
"""
import os
import shutil
from pathlib import Path

from loguru import logger


def find_duplicate_wav_files(data_dir: str | Path) -> list[list[Path]]:
    """Find WAV files that share the same filename across different directories.

    Walks *data_dir* recursively and groups files by basename.  Returns only
    groups where the same name appears in two or more locations.

    This matches the original ``find_dupes.py`` approach (name-based, not
    content-hash-based) which is appropriate for acoustic datasets where the
    same recording should never legitimately appear twice.

    Args:
        data_dir: Root directory to search.

    Returns:
        List of groups.  Each group is a list of :class:`~pathlib.Path` objects
        that all share the same filename.  Singletons are not included.
    """
    by_name: dict[str, list[Path]] = {}
    for dirpath, _, filenames in os.walk(data_dir):
        for name in filenames:
            if not name.lower().endswith(".wav") or name.startswith("."):
                continue
            by_name.setdefault(name, []).append(Path(dirpath) / name)

    return [paths for paths in by_name.values() if len(paths) > 1]


def organize_by_date_recorder(
    src_dir: str | Path,
    dest_dir: str | Path,
    dry_run: bool = False,
    copy: bool = False,
) -> list[tuple[Path, Path]]:
    """Move (or copy) WAV files into a ``recorder_name/YYYY_MM_DD`` hierarchy.

    Reads metadata from each WAV file using
    :func:`acousticslib.audio.metadata.read_wav_metadata` to determine the
    recorder name and recording date.  Files for which metadata cannot be
    extracted are skipped with a warning.

    Args:
        src_dir:  Source directory (walked recursively for WAV files).
        dest_dir: Destination root directory.  Sub-directories are created
                  as needed.
        dry_run:  If True, log intended moves but do not touch the filesystem.
        copy:     If True, copy files instead of moving them.

    Returns:
        List of ``(src_path, dest_path)`` pairs for every file that was
        processed (or would have been, in dry-run mode).
    """
    from .audio.metadata import read_wav_metadata
    from .exceptions import WavMetadataError
    from .audio.io import get_audio_file_names

    src_dir  = Path(src_dir)
    dest_dir = Path(dest_dir)
    results: list[tuple[Path, Path]] = []

    for src_path in get_audio_file_names(src_dir):
        try:
            meta = read_wav_metadata(src_path)
        except WavMetadataError as exc:
            logger.warning(f"Skipping '{src_path.name}': {exc}")
            continue

        if meta.recorder_date_path is None:
            logger.warning(
                f"Skipping '{src_path.name}': recorder name or timestamp unavailable"
            )
            continue

        dest_path = dest_dir / meta.recorder_date_path / src_path.name
        results.append((src_path, dest_path))

        if dry_run:
            logger.info(f"[dry-run] {'copy' if copy else 'move'}: {src_path} → {dest_path}")
            continue

        dest_path.parent.mkdir(parents=True, exist_ok=True)
        if copy:
            shutil.copy2(src_path, dest_path)
            logger.info(f"Copied: {src_path.name} → {dest_path}")
        else:
            shutil.move(str(src_path), dest_path)
            logger.info(f"Moved: {src_path.name} → {dest_path}")

    return results
