"""Repository for the users table (bcrypt authentication)."""
import datetime
from typing import Any, Dict, List, Optional, Tuple

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
                    "SELECT id, username, full_name, email, phone, last_login, "
                    "is_admin, disabled "
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
                    "SELECT id, username, full_name, email, phone, last_login, "
                    "is_admin, disabled "
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
    def set_disabled(user_id: int, disabled: bool) -> None:
        """Set the disabled flag for a user."""
        with get_session() as session:
            session.execute(
                text(
                    "UPDATE calltrackers.users SET disabled=:disabled WHERE id=:id"
                ),
                {"disabled": int(disabled), "id": user_id},
            )

    @staticmethod
    @handle_repository_errors
    def get_by_email(email: str) -> Optional[Dict[str, Any]]:
        """Return a user row (including reset token fields) by email, or None.

        The lookup is case-insensitive.
        """
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT id, username, full_name, email, "
                    "reset_token_hash, reset_token_expires_at, reset_requested_at "
                    "FROM calltrackers.users WHERE LOWER(email) = LOWER(:email)"
                ),
                {"email": email},
            ).mappings().first()

    @staticmethod
    @handle_repository_errors
    def set_reset_token(
        user_id: int,
        token_hash: str,
        expires_at: datetime.datetime,
    ) -> None:
        """Store a password-reset token hash and expiry for *user_id*.

        Also records the request time so callers can rate-limit repeat requests.
        Any previous token for this user is overwritten.
        """
        with get_session() as session:
            session.execute(
                text(
                    "UPDATE calltrackers.users "
                    "SET reset_token_hash=:hash, "
                    "    reset_token_expires_at=:expires, "
                    "    reset_requested_at=:now "
                    "WHERE id=:id"
                ),
                {
                    "hash":    token_hash,
                    "expires": expires_at,
                    "now":     datetime.datetime.utcnow(),
                    "id":      user_id,
                },
            )

    @staticmethod
    @handle_repository_errors
    def get_by_reset_token_hash(token_hash: str) -> Optional[Dict[str, Any]]:
        """Return the user row for *token_hash* if it exists (expired or not).

        The caller is responsible for checking the expiry timestamp.
        """
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT id, username, full_name, email, "
                    "reset_token_expires_at "
                    "FROM calltrackers.users "
                    "WHERE reset_token_hash = :hash"
                ),
                {"hash": token_hash},
            ).mappings().first()

    @staticmethod
    @handle_repository_errors
    def clear_reset_token(user_id: int) -> None:
        """Remove the password-reset token for *user_id* (single-use enforcement)."""
        with get_session() as session:
            session.execute(
                text(
                    "UPDATE calltrackers.users "
                    "SET reset_token_hash=NULL, reset_token_expires_at=NULL "
                    "WHERE id=:id"
                ),
                {"id": user_id},
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
