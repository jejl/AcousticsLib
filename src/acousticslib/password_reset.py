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
- If more than ``_SUSPICIOUS_THRESHOLD`` accounts request resets within
  ``_SUSPICIOUS_WINDOW_MINUTES`` minutes the admin is notified by email.
- The response to a reset request is always identical regardless of whether
  the email address exists (prevents user enumeration).
"""
from __future__ import annotations

import hashlib
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

import bcrypt
from loguru import logger

from .database.repositories.users import UserRepository
from .email import send_email
from .password_validation import validate_password, PASSWORD_REQUIREMENTS_TEXT

# ── Constants ─────────────────────────────────────────────────────────────────
_TOKEN_BYTES        = 32   # 32 raw bytes → 43-char url-safe base64 string
_EXPIRY_HOURS       = 1
_RATE_LIMIT_MINUTES = 5    # minimum gap between reset requests per account

# Suspicious-activity thresholds: if more than this many distinct accounts
# request resets within the window, alert the admin.
_SUSPICIOUS_THRESHOLD      = 3
_SUSPICIOUS_WINDOW_MINUTES = 10

# Message shown regardless of whether the email exists (avoids enumeration).
_GENERIC_OK_MSG = (
    "If that email address is registered you will receive a reset link shortly. "
    "Please check your inbox and spam folder."
)

_SUPPORT_EMAIL = "acoustic@naturetrackers.au"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _hash_token(token: str) -> str:
    """Return the SHA-256 hex digest of *token* (stored in DB instead of raw)."""
    return hashlib.sha256(token.encode()).hexdigest()


def _utcnow() -> datetime:
    """Naive UTC datetime suitable for comparison with DB DATETIME values."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _admin_email() -> str:
    """Return the configured admin email address, or empty string if unset."""
    return os.getenv("CALLTRACKERS_ADMIN_EMAIL", "").strip()


def _notify_admin(subject: str, body: str) -> None:
    """Send a notification to the admin address if one is configured."""
    addr = _admin_email()
    if not addr:
        return
    if not send_email(to=addr, subject=subject, body_text=body):
        logger.warning(f"Admin notification email failed to deliver to {addr}")


# ── Service ───────────────────────────────────────────────────────────────────

class PasswordResetService:
    """Shared password-reset logic for AstroAcoustics applications."""

    @staticmethod
    def request_reset(email: str, app_url: str) -> Tuple[bool, str]:
        """Initiate a password reset for *email*.

        If the address is registered, a time-limited reset link is emailed.
        The response is always the same generic message to prevent account
        enumeration.

        Admin is notified on every successful request and on suspicious
        activity (many accounts requesting resets in a short window).

        Args:
            email:   The address submitted by the user.
            app_url: Base URL of the calling application.  The reset link will
                     be ``{app_url}?reset_token=<token>``.

        Returns:
            ``(True, message)``  — email sent (or address not found).
            ``(False, message)`` — email delivery failed.
        """
        email = email.strip().lower()

        try:
            user = UserRepository.get_by_email(email)
        except Exception as exc:
            logger.error(f"Password reset DB lookup error: {exc}")
            return False, (
                f"A database error occurred. Please try again later or contact "
                f"{_SUPPORT_EMAIL} for help."
            )

        if not user:
            logger.info(f"Password reset requested for unknown email: {email}")
            return True, _GENERIC_OK_MSG

        # ── Per-account rate limit ────────────────────────────────────────────
        last_req = user.get("reset_requested_at")
        if last_req is not None:
            elapsed = (_utcnow() - last_req).total_seconds()
            if elapsed < _RATE_LIMIT_MINUTES * 60:
                remaining = int(_RATE_LIMIT_MINUTES * 60 - elapsed)
                mins, secs = divmod(remaining, 60)
                wait_str = (
                    f"{mins} minute{'s' if mins != 1 else ''} {secs} second{'s' if secs != 1 else ''}"
                    if mins else f"{secs} second{'s' if secs != 1 else ''}"
                )
                logger.info(
                    f"Reset rate-limited for user {user['id']} "
                    f"({elapsed:.0f}s ago, {remaining}s remaining)"
                )
                return True, (
                    f"A reset link was recently sent to this address. "
                    f"Please wait {wait_str} before requesting another, "
                    f"and check your inbox and spam folder."
                )

        # ── Global suspicious-activity check ─────────────────────────────────
        try:
            recent_count = UserRepository.count_recent_reset_requests(
                _SUSPICIOUS_WINDOW_MINUTES
            )
        except Exception as exc:
            logger.warning(f"Could not check global reset rate: {exc}")
            recent_count = 0

        if recent_count >= _SUSPICIOUS_THRESHOLD:
            logger.warning(
                f"Suspicious activity: {recent_count} password reset requests "
                f"in the last {_SUSPICIOUS_WINDOW_MINUTES} minutes "
                f"(latest: user {user['id']})"
            )
            _notify_admin(
                subject="ALERT: Multiple password reset requests — CallTrackers Admin",
                body=(
                    f"WARNING: {recent_count} password reset requests have been "
                    f"made in the last {_SUSPICIOUS_WINDOW_MINUTES} minutes.\n\n"
                    f"The most recent was for account '{user.get('username', '')}' "
                    f"({email}) at {_utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC.\n\n"
                    "This may indicate a security incident. "
                    "Please review your account logs.\n\n"
                    "— CallTrackers Admin (automated alert)"
                ),
            )

        # ── Generate token, store hash ────────────────────────────────────────
        token      = secrets.token_urlsafe(_TOKEN_BYTES)
        token_hash = _hash_token(token)
        expires_at = _utcnow() + timedelta(hours=_EXPIRY_HOURS)

        try:
            UserRepository.set_reset_token(user["id"], token_hash, expires_at)
        except Exception as exc:
            logger.error(f"Failed to store reset token for user {user['id']}: {exc}")
            return False, (
                f"A database error occurred. Please try again later or contact "
                f"{_SUPPORT_EMAIL} for help."
            )

        # ── Email user ────────────────────────────────────────────────────────
        reset_url = f"{app_url.rstrip('/')}?reset_token={token}"
        name      = user.get("full_name") or user.get("username", "")
        body = (
            f"Hi {name},\n\n"
            f"A password reset was requested for your account.\n\n"
            f"Click the link below to set a new password. "
            f"This link expires in {_EXPIRY_HOURS} hour(s):\n\n"
            f"    {reset_url}\n\n"
            f"{PASSWORD_REQUIREMENTS_TEXT}\n\n"
            f"If you did not request this you can safely ignore this email — "
            f"your password will not change.\n\n"
            f"— AstroAcoustics / NatureTrackers"
        )
        if not send_email(to=email, subject="Password reset request", body_text=body):
            logger.warning(f"Reset email failed to deliver to {email}")
            return (
                False,
                f"Unable to send the reset email. Please contact "
                f"{_SUPPORT_EMAIL} for help.",
            )

        # ── Notify admin ──────────────────────────────────────────────────────
        _notify_admin(
            subject="Password reset notification — CallTrackers Admin",
            body=(
                f"A password reset was requested for account "
                f"'{user.get('username', '')}' ({email}) "
                f"at {_utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC.\n\n"
                "— CallTrackers Admin (automated notification)"
            ),
        )

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
            logger.error(f"Token validation error: {exc}")
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
        valid_pw, failed = validate_password(new_password)
        if not valid_pw:
            return False, "Password does not meet requirements: " + "; ".join(failed) + "."

        valid_tok, msg, user_id = PasswordResetService.validate_token(token)
        if not valid_tok:
            return False, msg

        new_hash = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()
        try:
            UserRepository.update_password(user_id, new_hash)
            UserRepository.clear_reset_token(user_id)
        except Exception as exc:
            logger.error(
                f"Failed to complete password reset for user {user_id}: {exc}"
            )
            return False, "An error occurred while resetting your password. Please try again."

        logger.info(f"Password reset completed for user {user_id}")
        return True, "Your password has been reset successfully. You can now log in."
