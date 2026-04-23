"""Tests for acousticslib.password_validation.validate_password."""
import pytest

from acousticslib.password_validation import (
    PASSWORD_REQUIREMENTS_TEXT,
    validate_password,
)


class TestValidatePasswordPasses:
    def test_strong_password(self):
        ok, failed = validate_password("Secure1!Pass")
        assert ok is True
        assert failed == []

    def test_exactly_minimum_length(self):
        ok, _ = validate_password("Abcdefg1!x")   # 10 chars
        assert ok is True

    def test_long_password(self):
        ok, _ = validate_password("MyVeryLong$ecurePassw0rd!")
        assert ok is True

    def test_various_special_chars(self):
        for sp in "!@#$%^&*()-_=+[]{}|;:',.<>?/`~":
            ok, _ = validate_password(f"Abcdefgh1{sp}")
            assert ok is True, f"Special char {sp!r} should be accepted"


class TestValidatePasswordFails:
    def test_too_short(self):
        ok, failed = validate_password("Ab1!")
        assert ok is False
        assert any("10" in f for f in failed)

    def test_no_uppercase(self):
        ok, failed = validate_password("lowercase1!")
        assert ok is False
        assert any("uppercase" in f.lower() for f in failed)

    def test_no_lowercase(self):
        ok, failed = validate_password("UPPERCASE1!")
        assert ok is False
        assert any("lowercase" in f.lower() for f in failed)

    def test_no_digit(self):
        ok, failed = validate_password("NoDigits!!")
        assert ok is False
        assert any("digit" in f.lower() for f in failed)

    def test_no_special_char(self):
        ok, failed = validate_password("NoSpecial1A")
        assert ok is False
        assert any("special" in f.lower() for f in failed)

    def test_empty_string(self):
        ok, failed = validate_password("")
        assert ok is False
        assert len(failed) > 0

    def test_all_failures_reported(self):
        ok, failed = validate_password("a")   # short, no upper, no digit, no special
        assert ok is False
        assert len(failed) >= 3

    def test_space_is_a_special_char(self):
        ok, _ = validate_password("HasSpace 1A")
        assert ok is True


class TestPasswordRequirementsText:
    def test_is_nonempty_string(self):
        assert isinstance(PASSWORD_REQUIREMENTS_TEXT, str)
        assert len(PASSWORD_REQUIREMENTS_TEXT) > 0

    def test_mentions_minimum_length(self):
        assert "10" in PASSWORD_REQUIREMENTS_TEXT
