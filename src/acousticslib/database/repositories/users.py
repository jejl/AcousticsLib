"""Repository for the users table (bcrypt authentication)."""
import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from ...error_handlers import handle_repository_errors
from ..connection import get_session


class UserRepository:
    """Data access layer for calltrackers.users."""

    @staticmethod
    @handle_repository_errors
    def get_all() -> List[Dict[str, Any]]:
        """Return all users (excluding password hashes) ordered by username."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT id, username, full_name, email, phone, last_login, is_admin "
                    "FROM calltrackers.users ORDER BY username"
                )
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_by_id(user_id: int) -> Optional[Dict[str, Any]]:
        """Return a user by id (excluding password hash), or None."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT id, username, full_name, email, phone, last_login, is_admin "
                    "FROM calltrackers.users WHERE id = :id"
                ),
                {"id": user_id},
            ).mappings().first()

    @staticmethod
    @handle_repository_errors
    def get_by_username(username: str) -> Optional[Dict[str, Any]]:
        """Return a full user row (including password_hash) for authentication, or None."""
        with get_session() as session:
            return session.execute(
                text("SELECT * FROM calltrackers.users WHERE username = :uname"),
                {"uname": username},
            ).mappings().first()

    @staticmethod
    @handle_repository_errors
    def username_exists(username: str) -> bool:
        """Return True if a user with *username* already exists."""
        with get_session() as session:
            row = session.execute(
                text(
                    "SELECT COUNT(*) AS count FROM calltrackers.users "
                    "WHERE username = :uname"
                ),
                {"uname": username},
            ).mappings().first()
            return bool(row["count"])

    @staticmethod
    @handle_repository_errors
    def create(
        username: str,
        password_hash: str,
        full_name: str,
        email: str,
        phone: str,
        is_admin: bool = False,
    ) -> int:
        """Insert a new user and return their id.

        The caller is responsible for hashing the password before passing it.
        """
        with get_session() as session:
            result = session.execute(
                text(
                    "INSERT INTO calltrackers.users "
                    "(username, password_hash, full_name, email, phone, is_admin) "
                    "VALUES (:uname, :pw_hash, :full_name, :email, :phone, :is_admin)"
                ),
                {
                    "uname": username,
                    "pw_hash": password_hash,
                    "full_name": full_name,
                    "email": email,
                    "phone": phone,
                    "is_admin": int(is_admin),
                },
            )
            return result.lastrowid

    @staticmethod
    @handle_repository_errors
    def update(
        user_id: int,
        full_name: str,
        email: str,
        phone: str,
        is_admin: bool,
    ) -> None:
        """Update user details (excluding password)."""
        with get_session() as session:
            session.execute(
                text(
                    "UPDATE calltrackers.users "
                    "SET full_name=:full_name, email=:email, phone=:phone, "
                    "is_admin=:is_admin WHERE id=:id"
                ),
                {
                    "full_name": full_name,
                    "email": email,
                    "phone": phone,
                    "is_admin": int(is_admin),
                    "id": user_id,
                },
            )

    @staticmethod
    @handle_repository_errors
    def update_password(user_id: int, password_hash: str) -> None:
        """Replace the stored password hash for *user_id*."""
        with get_session() as session:
            session.execute(
                text(
                    "UPDATE calltrackers.users SET password_hash=:pw_hash WHERE id=:id"
                ),
                {"pw_hash": password_hash, "id": user_id},
            )

    @staticmethod
    @handle_repository_errors
    def update_last_login(user_id: int) -> None:
        """Set last_login to the current UTC time for *user_id*."""
        with get_session() as session:
            session.execute(
                text(
                    "UPDATE calltrackers.users SET last_login = :now WHERE id = :id"
                ),
                {"now": datetime.datetime.now(), "id": user_id},
            )

    @staticmethod
    @handle_repository_errors
    def delete(user_id: int) -> None:
        """Delete a user by id."""
        with get_session() as session:
            session.execute(
                text("DELETE FROM calltrackers.users WHERE id=:id"),
                {"id": user_id},
            )
