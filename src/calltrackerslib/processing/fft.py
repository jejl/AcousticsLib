"""FFT utilities, complex conversions, Butterworth filters, and baseline iteration.

Ported from AudioFiles/batlib.py.

Provides:
    d2r, r2d                     Degree/radian conversion helpers
    degunwrap                    Unwrap an array of phases ignoring NaNs
    AmpPha_to_Complex            (amp, phase_rad) → complex array
    Complex_to_AmpPha            complex array → (amp, phase_rad)
    NumpyEncoder                 JSON encoder for numpy arrays
    baselines                    Generator over unique (i, j) antenna pairs
    rowscols                     Generator over (row, col) subplot indices
    gsamp                        Extract/detrend channels from a WAV sample array
    butter_bandpass              Design a Butterworth bandpass SOS filter
    butter_highpass_filter       Apply a Butterworth highpass filter along axis 0
    butter_bandpass_filter       Apply a Butterworth bandpass filter along axis 0
"""
import json
from cmath import rect
from typing import Generator, Iterator

import numpy as np
from numpy import pi
from scipy import signal
from scipy.signal import butter, sosfilt

twopi = 2.0 * pi


# ---------------------------------------------------------------------------
# Angle helpers
# ---------------------------------------------------------------------------

def d2r(d: float) -> float:
    """Degrees to radians."""
    return np.deg2rad(d)


def r2d(r: float) -> float:
    """Radians to degrees."""
    return np.rad2deg(r)


def degunwrap(phases: np.ndarray) -> np.ndarray:
    """Unwrap *phases* (radians) in-place, skipping NaN entries."""
    p = phases
    p[~np.isnan(p)] = np.unwrap(p[~np.isnan(p)])
    return p


# ---------------------------------------------------------------------------
# Complex ↔ (amplitude, phase) conversions
# ---------------------------------------------------------------------------

def AmpPha_to_Complex(amp: np.ndarray, pha: np.ndarray) -> np.ndarray:
    """Convert amplitude + phase (radians) to complex array.

    Equivalent to ``amp * exp(i * pha)``.
    """
    nprect = np.vectorize(rect)
    return nprect(amp, pha)


def Complex_to_AmpPha(cplx: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Convert complex array to (amplitude, phase_radians) tuple."""
    return np.abs(cplx), np.angle(cplx)


# ---------------------------------------------------------------------------
# JSON helper
# ---------------------------------------------------------------------------

class NumpyEncoder(json.JSONEncoder):
    """JSON encoder that serialises numpy arrays as Python lists."""

    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


# ---------------------------------------------------------------------------
# Baseline / subplot iterators
# ---------------------------------------------------------------------------

def baselines(
    n_stations: int,
    all: bool = False,
    auto: bool = False,
) -> Iterator[tuple[int, int]]:
    """Yield (i, j) antenna pairs.

    Parameters
    ----------
    n_stations:
        Total number of stations/microphones.
    all:
        If True, yield both (i, j) and (j, i); otherwise only i < j.
    auto:
        If True, include autocorrelation pairs where i == j.
    """
    for i in range(n_stations):
        for j in range(0 if all else i, n_stations):
            if (i == j and auto) or i != j:
                yield i, j


def rowscols(nrow: int, ncol: int) -> Iterator[tuple[int, int]]:
    """Yield (row, col) indices for a grid of ``nrow × ncol`` subplots."""
    for i in range(nrow):
        for j in range(ncol):
            yield i, j


# ---------------------------------------------------------------------------
# Sample extraction
# ---------------------------------------------------------------------------

def gsamp(
    samples: np.ndarray,
    start: int = 0,
    stop: int = 96000,
    detrend: bool = False,
) -> np.ndarray:
    """Extract a time slice from a multi-channel sample array.

    Parameters
    ----------
    samples:
        2-D array of shape ``(n_samples, n_channels)`` as returned by
        ``scipy.io.wavfile.read``.
    start:
        First sample index (inclusive).
    stop:
        Last sample index (exclusive).  0 or negative means end of array.
    detrend:
        If True, remove the DC component from each channel.

    Returns
    -------
    np.ndarray
        Sliced (and optionally detrended) copy.
    """
    if stop <= 0:
        stop = len(samples)
    newsamp = np.copy(samples[start:stop, :])
    if detrend:
        newsamp = signal.detrend(newsamp, axis=0)
    return newsamp


# ---------------------------------------------------------------------------
# Butterworth filters (operate on multi-channel arrays, axis=0)
# ---------------------------------------------------------------------------

def butter_bandpass(
    lowcut: float,
    highcut: float,
    fs: float,
    order: int = 5,
) -> np.ndarray:
    """Design a Butterworth bandpass SOS filter.

    Parameters
    ----------
    lowcut:   Lower cutoff frequency (Hz).
    highcut:  Upper cutoff frequency (Hz).
    fs:       Sample rate (Hz).
    order:    Filter order.

    Returns
    -------
    sos : array
        Second-order sections representation.
    """
    nyq = 0.5 * fs
    sos = butter(order, [lowcut / nyq, highcut / nyq], btype="band", output="sos")
    return sos


def butter_highpass_filter(
    data: np.ndarray,
    lowcut: float,
    fs: float,
    order: int = 10,
) -> np.ndarray:
    """Apply a Butterworth highpass filter along axis 0.

    Parameters
    ----------
    data:    Input array (samples × channels or 1-D).
    lowcut:  Cutoff frequency (Hz).
    fs:      Sample rate (Hz).
    order:   Filter order.

    Returns
    -------
    np.ndarray
        Filtered array, same shape as *data*.
    """
    nyq = 0.5 * fs
    sos = butter(order, lowcut / nyq, btype="highpass", output="sos")
    return sosfilt(sos, data, axis=0)


def butter_bandpass_filter(
    data: np.ndarray,
    lowcut: float,
    highcut: float,
    fs: float,
    order: int = 5,
) -> np.ndarray:
    """Apply a Butterworth bandpass filter along axis 0.

    Parameters
    ----------
    data:     Input array (samples × channels or 1-D).
    lowcut:   Lower cutoff frequency (Hz).
    highcut:  Upper cutoff frequency (Hz).
    fs:       Sample rate (Hz).
    order:    Filter order.

    Returns
    -------
    np.ndarray
        Filtered array, same shape as *data*.
    """
    sos = butter_bandpass(lowcut, highcut, fs, order=order)
    return sosfilt(sos, data, axis=0)
