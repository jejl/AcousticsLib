"""Repositories for SoundClass tables in the calltrackers database.

These tables were migrated from the legacy ``sound_classifier`` database.
All tables live in the ``calltrackers`` schema after migration.

Table mapping (sound_classifier → calltrackers):
    config                    → SoundClassConfig
    user_config               → SoundClassUserConfig
    classification_categories → SoundClassCategory
    classifications           → SoundClassification
    call_library              → CallLibrary

Users are shared with the calltrackers.users table (see UserRepository).
"""
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from ...error_handlers import handle_repository_errors
from ..connection import get_session


class SoundClassConfigRepository:
    """Key/value configuration store for SoundClass global settings."""

    @staticmethod
    @handle_repository_errors
    def get(name: str) -> Optional[str]:
        """Return config value for *name*, or None."""
        with get_session() as session:
            row = session.execute(
                text("SELECT value FROM calltrackers.SoundClassConfig WHERE name = :name"),
                {"name": name},
            ).mappings().first()
            return str(row["value"]) if row else None

    @staticmethod
    @handle_repository_errors
    def set(name: str, value: str) -> None:
        """Insert or update a config value."""
        with get_session() as session:
            session.execute(
                text(
                    "INSERT INTO calltrackers.SoundClassConfig (name, value) "
                    "VALUES (:name, :value) "
                    "ON DUPLICATE KEY UPDATE value = VALUES(value)"
                ),
                {"name": name, "value": str(value)},
            )


class SoundClassUserConfigRepository:
    """Per-user spectrogram and UI preferences."""

    @staticmethod
    @handle_repository_errors
    def get(user_id: int) -> Optional[Dict[str, Any]]:
        """Return config row for *user_id*, or None."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT freq_min, freq_max, colormap, last_location, last_date "
                    "FROM calltrackers.SoundClassUserConfig WHERE user_id = :uid"
                ),
                {"uid": user_id},
            ).mappings().first()

    @staticmethod
    @handle_repository_errors
    def set(
        user_id: int,
        freq_min: int,
        freq_max: int,
        colormap: str,
        last_location: Optional[str] = None,
        last_date: Optional[str] = None,
    ) -> None:
        """Insert or update preferences for *user_id*."""
        with get_session() as session:
            session.execute(
                text(
                    "INSERT INTO calltrackers.SoundClassUserConfig "
                    "(user_id, freq_min, freq_max, colormap, last_location, last_date) "
                    "VALUES (:uid, :fmin, :fmax, :cmap, :loc, :dt) "
                    "ON DUPLICATE KEY UPDATE "
                    "freq_min=:fmin, freq_max=:fmax, colormap=:cmap, "
                    "last_location=:loc, last_date=:dt"
                ),
                {
                    "uid": user_id,
                    "fmin": freq_min,
                    "fmax": freq_max,
                    "cmap": colormap,
                    "loc": last_location,
                    "dt": last_date,
                },
            )


class SoundClassCategoryRepository:
    """Classification categories (e.g. Bittern, Not a bittern, Unsure)."""

    @staticmethod
    @handle_repository_errors
    def get_all() -> List[Dict[str, Any]]:
        """Return all categories ordered by id."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT id, category_name "
                    "FROM calltrackers.SoundClassCategory ORDER BY id ASC"
                )
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def add(category_name: str) -> int:
        """Insert a new category and return its id."""
        with get_session() as session:
            result = session.execute(
                text(
                    "INSERT INTO calltrackers.SoundClassCategory (category_name) "
                    "VALUES (:name)"
                ),
                {"name": category_name},
            )
            return result.lastrowid

    @staticmethod
    @handle_repository_errors
    def rename(cat_id: int, new_name: str) -> int:
        """Rename a category; return rows affected."""
        with get_session() as session:
            result = session.execute(
                text(
                    "UPDATE calltrackers.SoundClassCategory "
                    "SET category_name = :name WHERE id = :id"
                ),
                {"name": new_name.strip(), "id": cat_id},
            )
            return result.rowcount

    @staticmethod
    @handle_repository_errors
    def delete(cat_id: int) -> None:
        """Delete a category by id."""
        with get_session() as session:
            session.execute(
                text("DELETE FROM calltrackers.SoundClassCategory WHERE id = :id"),
                {"id": cat_id},
            )

    @staticmethod
    @handle_repository_errors
    def in_use(cat_id: int) -> bool:
        """Return True if any classification references this category."""
        with get_session() as session:
            row = session.execute(
                text(
                    "SELECT COUNT(*) AS n FROM calltrackers.SoundClassification "
                    "WHERE classification_category_id = :id"
                ),
                {"id": cat_id},
            ).mappings().first()
            return bool(row["n"])


class SoundClassificationRepository:
    """Human classifications of acoustic WAV files."""

    @staticmethod
    @handle_repository_errors
    def get_by_user_and_location(user_id: int, location: str) -> List[str]:
        """Return WAV filenames classified by *user_id* at *location*."""
        with get_session() as session:
            rows = session.execute(
                text(
                    "SELECT wav_file FROM calltrackers.SoundClassification "
                    "WHERE user_id = :uid AND location = :loc"
                ),
                {"uid": user_id, "loc": location},
            ).mappings().all()
            return [r["wav_file"] for r in rows]

    @staticmethod
    @handle_repository_errors
    def get_previous(user_id: int, wav_file: str, location: str) -> Optional[Dict[str, Any]]:
        """Return the most recent classification for a given user/file/location."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT sc.id, sc.comments, scc.category_name "
                    "FROM calltrackers.SoundClassification sc "
                    "JOIN calltrackers.SoundClassCategory scc "
                    "  ON sc.classification_category_id = scc.id "
                    "WHERE sc.user_id = :uid AND sc.wav_file = :wav AND sc.location = :loc"
                ),
                {"uid": user_id, "wav": wav_file, "loc": location},
            ).mappings().first()

    @staticmethod
    @handle_repository_errors
    def save(
        user_id: int,
        wav_file: str,
        category_name: str,
        comments: str,
        location: str,
        needs_discussion: bool = False,
    ) -> None:
        """Insert a classification, resolving *category_name* to its id."""
        with get_session() as session:
            row = session.execute(
                text(
                    "SELECT id FROM calltrackers.SoundClassCategory "
                    "WHERE category_name = :name"
                ),
                {"name": category_name},
            ).mappings().first()
            if row is None:
                raise ValueError(
                    f"Classification category '{category_name}' does not exist."
                )
            session.execute(
                text(
                    "INSERT INTO calltrackers.SoundClassification "
                    "(user_id, wav_file, comments, location, "
                    " classification_category_id, needs_discussion) "
                    "VALUES (:uid, :wav, :cmt, :loc, :cat_id, :nd)"
                ),
                {
                    "uid": user_id,
                    "wav": wav_file,
                    "cmt": comments,
                    "loc": location,
                    "cat_id": row["id"],
                    "nd": int(needs_discussion),
                },
            )

    @staticmethod
    @handle_repository_errors
    def delete(user_id: int, wav_file: str, location: str) -> None:
        """Delete all classifications for a given user/file/location."""
        with get_session() as session:
            session.execute(
                text(
                    "DELETE FROM calltrackers.SoundClassification "
                    "WHERE user_id = :uid AND wav_file = :wav AND location = :loc"
                ),
                {"uid": user_id, "wav": wav_file, "loc": location},
            )

    @staticmethod
    @handle_repository_errors
    def files_with_n_or_more_users(location: str, n: int) -> set:
        """Return WAV filenames at *location* classified by ≥ *n* distinct users."""
        with get_session() as session:
            rows = session.execute(
                text(
                    "SELECT wav_file FROM calltrackers.SoundClassification "
                    "WHERE location = :loc "
                    "GROUP BY wav_file "
                    "HAVING COUNT(DISTINCT user_id) >= :n"
                ),
                {"loc": location, "n": n},
            ).mappings().all()
            return {r["wav_file"] for r in rows}

    @staticmethod
    @handle_repository_errors
    def get_dashboard_stats(user_id: int) -> Dict[str, Any]:
        """Return per-user classification count and per-category breakdown."""
        with get_session() as session:
            count_row = session.execute(
                text(
                    "SELECT COUNT(*) AS n FROM calltrackers.SoundClassification "
                    "WHERE user_id = :uid"
                ),
                {"uid": user_id},
            ).mappings().first()
            cat_rows = session.execute(
                text(
                    "SELECT scc.category_name, COUNT(*) AS n "
                    "FROM calltrackers.SoundClassification sc "
                    "JOIN calltrackers.SoundClassCategory scc "
                    "  ON sc.classification_category_id = scc.id "
                    "WHERE sc.user_id = :uid "
                    "GROUP BY scc.category_name"
                ),
                {"uid": user_id},
            ).mappings().all()
            user_counts_rows = session.execute(
                text(
                    "SELECT location, wav_file, COUNT(DISTINCT user_id) AS n_users "
                    "FROM calltrackers.SoundClassification "
                    "GROUP BY location, wav_file"
                )
            ).mappings().all()
        return {
            "total_classified": count_row["n"],
            "cat_counts": [(r["category_name"], r["n"]) for r in cat_rows],
            "user_counts": {(r["location"], r["wav_file"]): r["n_users"] for r in user_counts_rows},
        }

    @staticmethod
    @handle_repository_errors
    def get_categories_for_file(wav_file: str) -> List[str]:
        """Return all category names applied to *wav_file* across all users."""
        with get_session() as session:
            rows = session.execute(
                text(
                    "SELECT scc.category_name "
                    "FROM calltrackers.SoundClassification sc "
                    "JOIN calltrackers.SoundClassCategory scc "
                    "  ON sc.classification_category_id = scc.id "
                    "WHERE sc.wav_file = :wav"
                ),
                {"wav": wav_file},
            ).mappings().all()
            return [r["category_name"] for r in rows]

    @staticmethod
    @handle_repository_errors
    def get_audited_files(audit_threshold: int) -> List[Dict[str, Any]]:
        """Return files classified by ≥ *audit_threshold* users, with observation metadata."""
        with get_session() as session:
            return session.execute(
                text(
                    """
                    WITH processed AS (
                        SELECT
                            location, wav_file, user_id,
                            REGEXP_REPLACE(wav_file, '_[0-9.]+_[0-9.]+\\.wav$', '.wav')
                                AS parent_wav
                        FROM calltrackers.SoundClassification
                    )
                    SELECT
                        p.location, p.wav_file, p.parent_wav,
                        COUNT(DISTINCT p.user_id) AS n_users,
                        M.observation_id,
                        LL.lat, LL.lon
                    FROM processed p
                    JOIN calltrackers.Metadata M ON M.file_name = p.parent_wav
                    JOIN calltrackers.LocationLog LL ON M.observation_id = LL.id
                    GROUP BY p.location, p.wav_file, M.observation_id, LL.lat, LL.lon
                    HAVING n_users >= :n
                    """
                ),
                {"n": audit_threshold},
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_users_with_discussion_flags() -> List[Dict[str, Any]]:
        """Return users who have ≥1 classification flagged for discussion."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT DISTINCT u.id, u.username "
                    "FROM calltrackers.users u "
                    "JOIN calltrackers.SoundClassification sc ON u.id = sc.user_id "
                    "WHERE sc.needs_discussion = 1"
                )
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_flagged_for_user(user_id: int) -> List[Dict[str, Any]]:
        """Return wav_file/location pairs flagged for discussion for *user_id*."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT wav_file, location "
                    "FROM calltrackers.SoundClassification "
                    "WHERE user_id = :uid AND needs_discussion = 1"
                ),
                {"uid": user_id},
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def clear_discussion_flag(user_id: int, wav_file: str, location: str) -> None:
        """Set needs_discussion = 0 for a specific classification."""
        with get_session() as session:
            session.execute(
                text(
                    "UPDATE calltrackers.SoundClassification "
                    "SET needs_discussion = 0 "
                    "WHERE user_id = :uid AND wav_file = :wav AND location = :loc"
                ),
                {"uid": user_id, "wav": wav_file, "loc": location},
            )


class CallLibraryRepository:
    """Reference call library entries."""

    @staticmethod
    @handle_repository_errors
    def get_all() -> List[Dict[str, Any]]:
        """Return all call library rows."""
        with get_session() as session:
            return session.execute(
                text("SELECT * FROM calltrackers.CallLibrary")
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def file_exists(filename: str) -> bool:
        """Return True if *filename* is already in the call library."""
        with get_session() as session:
            row = session.execute(
                text("SELECT id FROM calltrackers.CallLibrary WHERE file_name = :fn"),
                {"fn": filename},
            ).mappings().first()
            return row is not None

    @staticmethod
    @handle_repository_errors
    def insert(filename: str) -> None:
        """Insert a new call library entry, linking to Metadata if possible."""
        with get_session() as session:
            meta_row = session.execute(
                text(
                    "SELECT id FROM calltrackers.Metadata "
                    "WHERE :fn LIKE CONCAT('%', REPLACE(file_name, '.wav', ''), '%') "
                    "LIMIT 1"
                ),
                {"fn": filename},
            ).mappings().first()
            meta_id = meta_row["id"] if meta_row else None
            session.execute(
                text(
                    "INSERT INTO calltrackers.CallLibrary "
                    "(file_name, title, description, scientific_name, english_name, meta_id) "
                    "VALUES (:fn, NULL, NULL, NULL, NULL, :meta_id)"
                ),
                {"fn": filename, "meta_id": meta_id},
            )

    @staticmethod
    @handle_repository_errors
    def update(
        filename: str,
        title: Optional[str],
        description: Optional[str],
        scientific_name: Optional[str],
        english_name: Optional[str],
    ) -> None:
        """Update metadata fields for an existing call library entry."""
        with get_session() as session:
            session.execute(
                text(
                    "UPDATE calltrackers.CallLibrary "
                    "SET title=:title, description=:desc, "
                    "    scientific_name=:sci, english_name=:eng "
                    "WHERE file_name = :fn"
                ),
                {
                    "title": title,
                    "desc": description,
                    "sci": scientific_name,
                    "eng": english_name,
                    "fn": filename,
                },
            )
