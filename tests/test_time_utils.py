"""Tests for acousticslib.time_utils."""
import datetime
from zoneinfo import ZoneInfo

import pytest

from acousticslib.exceptions import AmbiguousObservationError
from acousticslib.time_utils import (
    HOBART,
    localize_hobart,
    match_observation_window,
    parse_guano_timestamp,
)

_HOBART = ZoneInfo("Australia/Hobart")


# ---------------------------------------------------------------------------
# localize_hobart
# ---------------------------------------------------------------------------

class TestLocalizeHobart:
    def test_attaches_hobart_to_naive(self):
        dt = datetime.datetime(2024, 3, 21, 19, 34, 2)
        result = localize_hobart(dt)
        assert result.tzinfo == _HOBART

    def test_preserves_clock_time(self):
        dt = datetime.datetime(2024, 3, 21, 19, 34, 2)
        result = localize_hobart(dt)
        assert result.hour == 19
        assert result.minute == 34
        assert result.second == 2

    def test_replaces_existing_tzinfo(self):
        utc = ZoneInfo("UTC")
        dt = datetime.datetime(2024, 3, 21, 9, 34, 2, tzinfo=utc)
        result = localize_hobart(dt)
        assert result.tzinfo == _HOBART
        assert result.hour == 9   # clock time preserved, NOT converted

    def test_hobart_constant_is_correct_zone(self):
        assert str(HOBART) == "Australia/Hobart"


# ---------------------------------------------------------------------------
# parse_guano_timestamp
# ---------------------------------------------------------------------------

class TestParseGuanoTimestamp:
    def test_datetime_input_returns_localized(self):
        dt = datetime.datetime(2024, 3, 21, 19, 0, 0)
        result = parse_guano_timestamp(dt)
        assert result.tzinfo == _HOBART
        assert result.hour == 19

    def test_well_formed_iso8601(self):
        result = parse_guano_timestamp("2024-03-21T19:34:02+10:00")
        assert result.tzinfo == _HOBART
        assert result.year == 2024
        assert result.month == 3
        assert result.day == 21
        assert result.hour == 19

    def test_compact_no_separators(self):
        # Strategy 2: YYYYMMDDTHHmmss+HHMM
        result = parse_guano_timestamp("20220827T050000+1000")
        assert result.tzinfo == _HOBART
        assert result.year == 2022
        assert result.month == 8
        assert result.day == 27
        assert result.hour == 5

    def test_malformed_fractional_seconds(self):
        # Strategy 3: non-digit chars and negative fractional part
        result = parse_guano_timestamp("2023-10-10T17:40:02.-31003+10:00")
        assert result.tzinfo == _HOBART
        assert result.year == 2023
        assert result.hour == 17
        assert result.minute == 40
        assert result.second == 2

    def test_unparseable_raises_value_error(self):
        with pytest.raises(ValueError, match="Cannot parse GUANO timestamp"):
            parse_guano_timestamp("not-a-timestamp")

    def test_strips_whitespace(self):
        result = parse_guano_timestamp("  2024-03-21T19:34:02+10:00  ")
        assert result.year == 2024


# ---------------------------------------------------------------------------
# match_observation_window
# ---------------------------------------------------------------------------

def _make_obs(obs_id, start_h, end_h):
    base = datetime.date(2024, 3, 21)
    return {
        "id": obs_id,
        "start_time": datetime.datetime.combine(base, datetime.time(start_h), tzinfo=_HOBART),
        "end_time":   datetime.datetime.combine(base, datetime.time(end_h),   tzinfo=_HOBART),
    }


class TestMatchObservationWindow:
    _obs = [_make_obs(1, 18, 23), _make_obs(2, 10, 14)]

    def test_returns_matching_observation(self):
        ts = datetime.datetime(2024, 3, 21, 20, 0, tzinfo=_HOBART)
        result = match_observation_window(ts, self._obs)
        assert result["id"] == 1

    def test_returns_none_when_no_match(self):
        ts = datetime.datetime(2024, 3, 21, 16, 0, tzinfo=_HOBART)
        result = match_observation_window(ts, self._obs)
        assert result is None

    def test_buffer_extends_window(self):
        # 30 min before start of obs 1 (18:00) — within default 60-min buffer
        ts = datetime.datetime(2024, 3, 21, 17, 35, tzinfo=_HOBART)
        result = match_observation_window(ts, self._obs, buffer_minutes=60)
        assert result["id"] == 1

    def test_raises_on_ambiguous_match(self):
        # Two overlapping windows
        obs = [_make_obs(1, 10, 14), _make_obs(2, 10, 14)]
        ts = datetime.datetime(2024, 3, 21, 12, 0, tzinfo=_HOBART)
        with pytest.raises(AmbiguousObservationError):
            match_observation_window(ts, obs)

    def test_localizes_naive_timestamp(self):
        naive_ts = datetime.datetime(2024, 3, 21, 20, 0)  # no tzinfo
        result = match_observation_window(naive_ts, self._obs)
        assert result["id"] == 1

    def test_empty_observations_returns_none(self):
        ts = datetime.datetime(2024, 3, 21, 20, 0, tzinfo=_HOBART)
        assert match_observation_window(ts, []) is None

    def test_zero_buffer(self):
        # timestamp exactly at start boundary with zero buffer
        ts = datetime.datetime(2024, 3, 21, 18, 0, tzinfo=_HOBART)
        result = match_observation_window(ts, self._obs, buffer_minutes=0)
        assert result["id"] == 1
