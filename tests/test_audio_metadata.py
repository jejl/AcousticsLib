"""Tests for acousticslib.audio.metadata parsing functions.

Only the pure parsing functions (parse_bar_title_long, parse_bar_title_short,
WavMetadata properties) are tested here — read_wav_metadata requires a real
WAV file and is integration-tested separately.
"""
import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from acousticslib.audio.metadata import (
    WavMetadata,
    parse_bar_title_long,
    parse_bar_title_short,
)

_HOBART = ZoneInfo("Australia/Hobart")


# ---------------------------------------------------------------------------
# parse_bar_title_long
# ---------------------------------------------------------------------------

class TestParseBarTitleLong:
    _VALID = "S20250403T190158.327009+1100_E20250403T194658.320872+1100_-41.09533+146.65492"

    def test_parses_valid_title(self):
        start, end, lat, lon = parse_bar_title_long(self._VALID)
        assert start is not None
        assert end is not None
        assert lat == pytest.approx(-41.09533)
        assert lon == pytest.approx(146.65492)

    def test_start_timestamp_correct(self):
        start, *_ = parse_bar_title_long(self._VALID)
        assert start.year == 2025
        assert start.month == 4
        assert start.day == 3
        assert start.hour == 19
        assert start.minute == 1
        assert start.second == 58

    def test_end_timestamp_correct(self):
        _, end, _, _ = parse_bar_title_long(self._VALID)
        assert end.hour == 19
        assert end.minute == 46
        assert end.second == 58

    def test_strips_wav_extension(self):
        with_ext = self._VALID + ".wav"
        start, end, lat, lon = parse_bar_title_long(with_ext)
        assert start is not None
        assert lat == pytest.approx(-41.09533)

    def test_returns_none_tuple_on_no_match(self):
        result = parse_bar_title_long("not_a_bar_title.wav")
        assert result == (None, None, None, None)

    def test_returns_none_tuple_on_empty_string(self):
        assert parse_bar_title_long("") == (None, None, None, None)

    def test_positive_lat_lon(self):
        title = "S20250403T190158.327009+1100_E20250403T194658.320872+1100_+51.50000+000.12500"
        start, end, lat, lon = parse_bar_title_long(title)
        assert lat == pytest.approx(51.5)
        assert lon == pytest.approx(0.125)


# ---------------------------------------------------------------------------
# parse_bar_title_short
# ---------------------------------------------------------------------------

class TestParseBarTitleShort:
    def test_format1_serial_date(self):
        rec_name, serial, ts = parse_bar_title_short("00014281_20260124_000500")
        assert rec_name is None
        assert serial == "00014281"
        assert ts is not None
        assert ts.year == 2026
        assert ts.month == 1
        assert ts.day == 24
        assert ts.hour == 0
        assert ts.minute == 5
        assert ts.second == 0
        assert ts.tzinfo == _HOBART

    def test_format1_with_wav_extension(self):
        _, serial, ts = parse_bar_title_short("00014281_20260124_000500.wav")
        assert serial == "00014281"
        assert ts is not None

    def test_format2_name_date(self):
        rec_name, serial, ts = parse_bar_title_short("B8_20260122_043600")
        assert rec_name == "B8"
        assert serial is None
        assert ts.hour == 4
        assert ts.minute == 36
        assert ts.tzinfo == _HOBART

    def test_format3_name_offset_timestamp(self):
        rec_name, serial, ts = parse_bar_title_short("JEJL_20251215T044000+1100_suffix")
        assert rec_name == "JEJL"
        assert serial is None
        assert ts is not None
        assert ts.year == 2025
        assert ts.hour == 4

    def test_no_match_returns_none_triple(self):
        result = parse_bar_title_short("random_filename_no_pattern")
        assert result == (None, None, None)

    def test_empty_string_returns_none_triple(self):
        assert parse_bar_title_short("") == (None, None, None)


# ---------------------------------------------------------------------------
# WavMetadata properties
# ---------------------------------------------------------------------------

class TestWavMetadataProperties:
    def _make(self, timestamp=None, recorder_name=None):
        return WavMetadata(
            path=Path("/data/test.wav"),
            timestamp=timestamp,
            recorder_name=recorder_name,
        )

    def test_date_path_none_when_no_timestamp(self):
        meta = self._make()
        assert meta.date_path is None

    def test_date_path_format(self):
        ts = datetime.datetime(2024, 3, 21, 19, 0, tzinfo=_HOBART)
        meta = self._make(timestamp=ts)
        assert meta.date_path == "2024_03_21"

    def test_recorder_date_path_none_when_no_recorder(self):
        ts = datetime.datetime(2024, 3, 21, tzinfo=_HOBART)
        meta = self._make(timestamp=ts)
        assert meta.recorder_date_path is None

    def test_recorder_date_path_none_when_no_timestamp(self):
        meta = self._make(recorder_name="NT03")
        assert meta.recorder_date_path is None

    def test_recorder_date_path_combined(self):
        ts = datetime.datetime(2024, 3, 21, 19, 0, tzinfo=_HOBART)
        meta = self._make(timestamp=ts, recorder_name="NT03")
        assert meta.recorder_date_path == "NT03/2024_03_21"
