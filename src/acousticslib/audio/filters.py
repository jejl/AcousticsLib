"""DSP filter utilities for audio files.

Ported from SoundClass/src/audio_utils.py.

Functions here operate on files (read → filter → return array) rather than
on in-memory arrays, to match the calling pattern used throughout the
CallTrackers pipeline.  For in-memory signal processing see
``acousticslib.processing``.
"""
from pathlib import Path

import numpy as np
import soundfile as sf
from scipy.signal import butter, sosfilt


def butter_lowpass_filter(
    file: str | Path,
    lowpass_freq_hz: float,
    order: int = 10,
) -> tuple[np.ndarray, int]:
    """Read a WAV file and apply a Butterworth lowpass filter.

    The output is normalised to the ±1 range and scaled by 0.95 to avoid
    clipping artefacts at the edges of the float32 range.

    Args:
        file:             Path to the WAV file.
        lowpass_freq_hz:  Cutoff frequency in Hz.
        order:            Filter order (default 10).

    Returns:
        ``(filtered_data, sample_rate)`` where *filtered_data* is float32 and
        *sample_rate* is in Hz.
    """
    data, sample_rate = sf.read(str(file))
    nyq = 0.5 * sample_rate
    sos = butter(order, lowpass_freq_hz / nyq, btype="lowpass", output="sos")
    filtered = sosfilt(sos, data, axis=0)

    # Normalise to ±1 and attenuate slightly to avoid clipping
    peak = np.max(np.abs(filtered))
    if peak > 0:
        filtered = filtered / peak
    filtered = (0.95 * filtered).astype(np.float32)

    return filtered, sample_rate


def butter_bandpass_filter(
    data: np.ndarray,
    sample_rate: int,
    low_hz: float,
    high_hz: float,
    order: int = 5,
) -> np.ndarray:
    """Apply a Butterworth bandpass filter to an in-memory array.

    Args:
        data:        Input signal (1-D or 2-D; filtering applied along axis 0).
        sample_rate: Sample rate in Hz.
        low_hz:      Lower cutoff frequency in Hz.
        high_hz:     Upper cutoff frequency in Hz.
        order:       Filter order (default 5).

    Returns:
        Filtered signal as float64 array, same shape as *data*.
    """
    nyq = 0.5 * sample_rate
    sos = butter(order, [low_hz / nyq, high_hz / nyq], btype="bandpass", output="sos")
    return sosfilt(sos, data, axis=0)
