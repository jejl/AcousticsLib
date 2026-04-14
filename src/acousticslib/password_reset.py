"""Secure password-reset service shared across AstroAcoustics applications.

Typical usage
-------------
Step 1 — user submits their email on the "forgot password" form::

    from acousticslib.password_reset import PasswordResetService

    ok, msg = PasswordResetService.request_reset(
        email="user@example.com",
        app_url="https://calltrackers.example.com",
    )
    # Always show msg to the user (generic text regardless of whether the
    # address exists — prevents account enumeration).

Step 2 — user arrives via the link in the email (``?reset_token=<token>``)::

    ok, msg, user_id = PasswordResetService.validate_token(token)
    if not ok:
        show_error(msg)

Step 3 — user submits the new-password form::

    ok, msg = PasswordResetService.complete_reset(token, new_password)

Security properties
-------------------
- Tokens are 32 bytes of OS randomness (``secrets.token_urlsafe``).
- Only the SHA-256 digest is stored in the database; the raw token travels
  only in the email link and is never persisted.
- Tokens expire after 1 hour.
- Tokens are single-use: cleared immediately on successful password reset.
- Reset requests are rate-limited to one per 5 minutes per account.
- The response to a reset request is always identical regardless of whether
  the email address exists (prevents user enumeration).
"""
from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

import bcrypt
from loguru import logger

from .database.repositories.users import UserRepository
from .email import send_email

# ── Constants ─────────────────────────────────────────────────────────────────
_TOKEN_BYTES        = 32   # 32 raw bytes → 43-char url-safe base64 string
_EXPIRY_HOURS       = 1
_RATE_LIMIT_MINUTES = 5    # minimum gap between reset requests per account

# Message shown regardless of whether the email exists (avoids enumeration).
_GENERIC_OK_MSG = (
    "If that email address is registered you will receive a reset link shortly. "
    "Please check your inbox and spam folder."
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _hash_token(token: str) -> str:
    """Return the SHA-256 hex digest of *token* (stored in DB instead of raw)."""
    return hashlib.sha256(token.encode()).hexdigest()


def _utcnow() -> datetime:
    """Naive UTC datetime suitable for comparison with DB DATETIME values."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ── Service ───────────────────────────────────────────────────────────────────

class PasswordResetService:
    """Shared password-reset logic for AstroAcoustics applications."""

    @staticmethod
    def request_reset(email: str, app_url: str) -> Tuple[bool, str]:
        """Initiate a password reset for *email*.

        If the address is registered, a time-limited reset link is emailed.
        The response is always the same generic message to prevent account
        enumeration.

        Args:
            email:   The address submitted by the user.
            app_url: Base URL of the calling application.  The reset link will
                     be ``{app_url}?reset_token=<token>``.

        Returns:
            ``(True, message)`` — always, unless an unexpected exception escapes.
        """
        email = email.strip().lower()

        try:
            user = UserRepository.get_by_email(email)
        except Exception as exc:
            logger.error("Password reset DB lookup error: %s", exc)
            return True, _GENERIC_OK_MSG

        if not user:
            logger.info("Password reset requested for unknown email: %s", email)
            return True, _GENERIC_OK_MSG

        # Rate-limit: reject if a request was made less than 5 minutes ago.
        last_req = user.get("reset_requested_at")
        if last_req is not None:
            elapsed = (_utcnow() - last_req).total_seconds()
            if elapsed < _RATE_LIMIT_MINUTES * 60:
                logger.info(
                    "Reset rate-limited for user %s (%.0fs ago)", user["id"], elapsed
                )
                return True, _GENERIC_OK_MSG

        # Generate token, store hash.
        token      = secrets.token_urlsafe(_TOKEN_BYTES)
        token_hash = _hash_token(token)
        expires_at = _utcnow() + timedelta(hours=_EXPIRY_HOURS)

        try:
            UserRepository.set_reset_token(user["id"], token_hash, expires_at)
        except Exception as exc:
            logger.error("Failed to store reset token for user %s: %s", user["id"], exc)
            return True, _GENERIC_OK_MSG

        reset_url = f"{app_url.rstrip('/')}?reset_token={token}"
        name      = user.get("full_name") or user.get("username", "")
        body = (
            f"Hi {name},\n\n"
            f"A password reset was requested for your account.\n\n"
            f"Click the link below to set a new password. "
            f"This link expires in {_EXPIRY_HOURS} hour(s):\n\n"
            f"    {reset_url}\n\n"
            f"If you did not request this you can safely ignore this email — "
            f"your password will not change.\n\n"
            f"— AstroAcoustics / NatureTrackers"
        )
        if not send_email(to=email, subject="Password reset request", body_text=body):
            # Email failure is logged inside send_email; still return generic OK
            # so the UI doesn't reveal whether the address exists.
            logger.warning("Reset email failed to deliver to %s", email)

        return True, _GENERIC_OK_MSG

    @staticmethod
    def validate_token(token: str) -> Tuple[bool, str, Optional[int]]:
        """Check that *token* is valid and unexpired.

        Returns:
            ``(True, "OK", user_id)``  — token is valid.
            ``(False, message, None)`` — token is invalid or expired.
        """
        if not token:
            return False, "No reset token provided.", None

        token_hash = _hash_token(token)
        try:
            user = UserRepository.get_by_reset_token_hash(token_hash)
        except Exception as exc:
            logger.error("Token validation error: %s", exc)
            return False, "An error occurred. Please try again.", None

        if not user:
            return (
                False,
                "This reset link is invalid or has already been used.",
                None,
            )

        expires_at = user.get("reset_token_expires_at")
        if expires_at and _utcnow() > expires_at:
            return (
                False,
                f"This reset link expired after {_EXPIRY_HOURS} hour(s). "
                "Please request a new one.",
                None,
            )

        return True, "OK", int(user["id"])

    @staticmethod
    def complete_reset(token: str, new_password: str) -> Tuple[bool, str]:
        """Apply *new_password* to the account identified by *token*.

        The token is consumed (cleared from the DB) on success so that it
        cannot be reused.

        Returns:
            ``(True, message)``  — password updated.
            ``(False, message)`` — validation or DB failure.
        """
        if len(new_password) < 8:
            return False, "Password must be at least 8 characters."

        valid, msg, user_id = PasswordResetService.validate_token(token)
        if not valid:
            return False, msg

        new_hash = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()
        try:
            UserRepository.update_password(user_id, new_hash)
            UserRepository.clear_reset_token(user_id)
        except Exception as exc:
            logger.error(
                "Failed to complete password reset for user %s: %s", user_id, exc
            )
            return False, "An error occurred while resetting your password. Please try again."

        logger.info("Password reset completed for user %s", user_id)
        return True, "Your password has been reset successfully. You can now log in."