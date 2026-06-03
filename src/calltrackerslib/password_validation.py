"""Password strength validation shared across AstroAcoustics applications.

All password entry points (registration, admin reset, self-service change,
email-based reset) should call ``validate_password`` before hashing.

UI modules can display ``PASSWORD_REQUIREMENTS_TEXT`` as a hint.
"""
from __future__ import annotations

import re
from typing import Tuple

# ── Policy constants ──────────────────────────────────────────────────────────
_MIN_LENGTH = 10

# Each entry is (test_func, human-readable description of the requirement).
_CHECKS: list[tuple] = [
    (lambda p: len(p) >= _MIN_LENGTH,
     f"At least {_MIN_LENGTH} characters"),
    (lambda p: bool(re.search(r"[A-Z]", p)),
     "At least one uppercase letter (A–Z)"),
    (lambda p: bool(re.search(r"[a-z]", p)),
     "At least one lowercase letter (a–z)"),
    (lambda p: bool(re.search(r"\d", p)),
     "At least one digit (0–9)"),
    (lambda p: bool(re.search(r"[^A-Za-z0-9]", p)),
     "At least one special character (e.g. !@#$%^&*)"),
]

# Short summary suitable for display in a UI caption or email body.
PASSWORD_REQUIREMENTS_TEXT: str = (
    f"Passwords must be at least {_MIN_LENGTH} characters and contain "
    "an uppercase letter, a lowercase letter, a digit, and a special "
    "character (e.g. !@#$%^&*)."
)


def validate_password(password: str) -> Tuple[bool, list[str]]:
    """Check *password* against the shared security policy.

    Args:
        password: The plain-text candidate password.

    Returns:
        ``(True, [])`` when all requirements are met.
        ``(False, ["unmet requirement", ...])`` listing every failed check.
    """
    failed = [desc for check, desc in _CHECKS if not check(password)]
    return (len(failed) == 0, failed)
