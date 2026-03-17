"""Cross-correlation and multi-channel WAV loading.

Ported from AudioFiles/batlib.py and AudioFiles/DataHandler.py.

The ``Loader`` class loads multi-channel WAV files, applies optional
high-pass filtering and instrumental delay corrections, and computes
auto/cross-correlation spectrograms.  It depends on the private
``scipy.signal.spectral._spectral_helper`` for cross-correlation; this
is an internal scipy API that may change in future scipy releases.

Provides:
    crosscorr                      Lag-N Pearson cross-correlation (pandas Series)
    compute_shift                  Find integer sample shift via FFT cross-correlation
    cross_correlation_using_fft    FFT-based cross-correlation
    get_xcor                       Combined Pearson + FFT cross-correlation for a baseline
    Loader                         Multi-channel WAV data loader and correlator
"""
from copy import deepcopy
from typing import Callable, Optional

import numpy as np
import pandas as pd
from numpy import pi
from numpy.fft import fft, ifft, fftshift
from scipy import signal as scipy_signal
from scipy.io import wavfile
from scipy.signal.spectral import _spectral_helper  # private scipy API

from .fft import (
    AmpPha_to_Complex,
    Complex_to_AmpPha,
    baselines,
    butter_highpass_filter,
    twopi,
)

# ---------------------------------------------------------------------------
# Time-series cross-correlation helpers
# ---------------------------------------------------------------------------

def crosscorr(datax: pd.Series, datay: pd.Series, lag: int = 0, wrap: bool = False) -> float:
    """Lag-N Pearson cross-correlation.

    Parameters
    ----------
    datax, datay : Equal-length pandas Series.
    lag          : Number of samples to shift *datay* (positive = datay shifted forward).
    wrap         : If True, wrap the shifted data instead of filling with NaN.

    Returns
    -------
    float  Pearson correlation coefficient.
    """
    if wrap:
        shiftedy = datay.shift(lag)
        shiftedy.iloc[:lag] = datay.iloc[-lag:].values
        return datax.corr(shiftedy)
    return datax.corr(datay.shift(lag))


def cross_correlation_using_fft(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    """FFT-based cross-correlation of equal-length arrays.

    Returns the fftshift-ed real part so that zero lag is at the centre.
    """
    f1 = fft(x)
    f2 = fft(np.flipud(y))
    cc = np.real(ifft(f1 * f2))
    return fftshift(cc)


def compute_shift(x: np.ndarray, y: np.ndarray, nlags: int) -> tuple[int, np.ndarray]:
    """Find the integer sample shift between two equal-length arrays.

    Uses FFT cross-correlation restricted to ±nlags samples.

    Parameters
    ----------
    x, y   : Equal-length arrays.
    nlags  : Half-window of lags to search.

    Returns
    -------
    (shift, arr)
        shift : Integer sample offset (negative = y leads x).
        arr   : Cross-correlation values within the lag window.
    """
    assert len(x) == len(y)
    c = cross_correlation_using_fft(x, y)
    assert len(c) == len(x)
    zero_index = int(len(x) / 2) - 1
    arr = c[zero_index - nlags: zero_index + nlags]
    shift = int(np.argmax(arr)) - nlags
    return shift, arr


def get_xcor(
    ts1: np.ndarray,
    ts2: np.ndarray,
    sig,
    max_delay: float,
) -> tuple[np.ndarray, list, np.ndarray, float, float]:
    """Compute FFT and Pearson cross-correlations for a pair of time series.

    Parameters
    ----------
    ts1, ts2  : 1-D time series arrays.
    sig       : Signal object with ``.rate`` attribute (sample rate in Hz).
    max_delay : Maximum expected delay (s); restricts the Pearson lag search.

    Returns
    -------
    (xcdata/rate, rs_xc, dels, xcoff/rate, rs_xc_best)
        xcdata/rate  : FFT cross-correlation values (dimensionless, normalised by rate).
        rs_xc        : Pearson cross-correlation values over the lag range.
        dels         : Delay values (s) corresponding to Pearson lags.
        xcoff/rate   : Best FFT lag converted to seconds.
        rs_xc_best   : Best Pearson lag converted to seconds.
    """
    l = pd.Series(ts1)
    r = pd.Series(ts2)
    nlags = int(max_delay * sig.rate) + 2
    lags = range(-nlags, nlags)

    rs_xc = [crosscorr(l, r, lag) for lag in lags]
    dels = np.asarray(lags) / sig.rate
    rs_xc_best = dels[np.nanargmax(rs_xc)]

    xcoff, xcdata = compute_shift(l, r, nlags)
    return xcdata / sig.rate, rs_xc, dels, xcoff / sig.rate, rs_xc_best


# ---------------------------------------------------------------------------
# Multi-channel WAV loader and correlator
# ---------------------------------------------------------------------------

class Loader:
    """Load and correlate multi-channel acoustic WAV data.

    Initialised from a header object (e.g. an :class:`~acousticslib.processing.hardware.AudioData`
    subclass) that carries all hardware geometry and processing settings.

    Typical usage::

        header = recorder_data_FL_BAR_LT_generic()
        header.filename = "20240101_120000.wav"
        loader = Loader(header)
        channels = loader.load_time_series(bandpass_filter=True)
        loader.get_corr(channels, log=logger)
    """

    def __init__(self, header):
        self.data_dir = header.datadir
        self.rate = 16000  # default; overridden by wavfile.read
        try:
            self.sound_speed_mps = header.sound_speed_mps
        except AttributeError:
            self.sound_speed_mps = 343

        self.nchan = 2
        self.SNR_lim = header.SNR_lim
        self.low_SNR_lim = header.low_SNR_lim
        self.nbins = 400
        self.gamma = 0.7
        self.nfft = header.nfft
        self.nps = header.nps
        self.noverlap = header.noverlap
        self.xyz = header.xyz
        self.refant = header.refant
        self.start_sec = header.start_sec
        self.stop_sec = header.stop_sec
        self.location = header.location
        self.instrumental = header.instrumental
        self.filename = header.filename
        self.detrend = header.detrend
        self.low_freq_cut_hz = header.low_freq_cut_hz
        self.snr_shift = header.snr_shift
        self.snr_ratio_cut = header.snr_ratio_cut
        self.min_dec = header.min_dec
        self.bp_lowcut = header.low_freq_cut_hz
        self.bp_highcut = 64000.0
        self.bp_order = 13
        self.low_freq_delete_hz = header.low_freq_delete_hz
        self.high_freq_delete_hz = header.high_freq_delete_hz

        self.ts_correl_data_fft: dict = {}
        self.ts_correl_offset_fft: dict = {}
        self.ts_correl_data_pearson: dict = {}
        self.ts_correl_offset_pearson: dict = {}
        self.ts_correl_delays: dict = {}
        self.ranges_grid = header.ranges_grid

        self.instrumental_resid: list = []
        self.freq: dict = {}
        self.time: dict = {}
        self.corr_complex: dict = {}
        self.corr_amp: dict = {}
        self.corr_pha: dict = {}
        self.amp_rising: dict = {}
        self.pha_rising: dict = {}
        self.complex_rising: dict = {}
        self.wt_rising: dict = {}
        self.time_rising: dict = {}
        self.freq_rising: dict = {}
        self.model: dict = {}

        self.apply_instrumental = header.apply_instrumental
        if not self.apply_instrumental:
            self.instrumental = self.instrumental * 0

        # Re-centre XYZ on the reference antenna
        self.xyz[["X", "Y", "Z"]] = self.xyz[["X", "Y", "Z"]].sub(
            np.array(self.xyz[["X", "Y", "Z"]].iloc[self.refant - 1])
        )

        # Filter to selected microphones
        self.mic_select = np.array(header.mic_select)
        self.xyz.Select = self.mic_select
        self.gain_amp = np.array(header.gain_amp)
        self.xyz = self.xyz[self.xyz["Select"]].reset_index()
        self.nchan = len(self.xyz)

        self.source_offset_x = header.source_offset_x
        self.source_offset_y = header.source_offset_y
        self.instrumental = self.instrumental[self.mic_select]
        self.mic_names = header.mic_names[self.mic_select]
        self.refant -= int(
            np.sum(self.mic_select[: self.refant][~self.mic_select[: self.refant]])
        )
        self.gain = np.zeros(self.nchan, dtype=complex)

    def write_time_series(self, filename: str, data: np.ndarray):
        """Write *data* (int16) to a WAV file at self.rate."""
        wavfile.write(filename, self.rate, data.astype(np.int16))

    def load_time_series(
        self,
        shift: bool = False,
        bandpass_filter: bool = False,
    ) -> np.ndarray:
        """Load the WAV file and return an array of shape (nchan, n_samples).

        Parameters
        ----------
        shift           : If True, apply instrumental delay offsets to nearest sample.
        bandpass_filter : If True, apply a highpass filter at self.bp_lowcut.

        Returns
        -------
        np.ndarray  Shape (nchan, n_samples).
        """
        try:
            self.rate, samples = wavfile.read(f"{self.data_dir}/{self.filename}")
        except Exception as exc:
            raise RuntimeError(f"Failed to read WAV: {self.data_dir}/{self.filename}") from exc

        i_start = int(self.start_sec * self.rate) if self.start_sec > 0 else 0
        i_stop = int(self.stop_sec * self.rate) if self.stop_sec > 0 else 0

        channels = self._gsamp(samples, i_start, i_stop)

        if bandpass_filter:
            channels = butter_highpass_filter(
                channels, self.bp_lowcut, self.rate, self.bp_order
            )

        self.instrumental_resid = np.zeros(self.nchan)

        if shift:
            nsamp_shift = np.rint(self.instrumental * self.rate).astype(int)
            self.instrumental_resid = self.instrumental - (nsamp_shift / self.rate)
            for i in range(self.nchan):
                channels[i, :] = np.roll(channels[i, :], -nsamp_shift[i])

        return channels

    def _gsamp(self, samples: np.ndarray, start: int = 0, stop: int = 0) -> np.ndarray:
        """Extract selected channels from a WAV sample array."""
        if np.ndim(samples) == 1:
            self.nchan = 1

        if stop == 0:
            stop = len(samples)

        channels = []
        if self.nchan == 1:
            data = samples[start:stop]
            if self.detrend:
                channels.append(scipy_signal.detrend(data, type="constant"))
            else:
                channels.append(data)
        else:
            for i in range(np.shape(samples)[1]):
                if self.mic_select[i]:
                    data = self.gain_amp[i] * samples[start:stop, i]
                    if self.detrend:
                        channels.append(scipy_signal.detrend(data, type="constant"))
                    else:
                        channels.append(data)

        return np.asarray(channels)

    def get_corr(
        self,
        data: np.ndarray,
        log,
        noverlap: int = 0,
        apply_cal: bool = True,
        auto: bool = True,
    ):
        """Compute auto and cross-correlation spectrograms.

        Results are stored in ``self.freq``, ``self.time``, ``self.corr_amp``,
        ``self.corr_pha``, ``self.corr_complex``.

        Parameters
        ----------
        data       : Array of shape (nchan, n_samples).
        log        : Logger (loguru or stdlib logging).
        noverlap   : Number of samples to overlap in spectrogram windows.
        apply_cal  : If True, divide complex visibilities by instrumental corrections.
        auto       : If True, also compute autocorrelations.
        """
        for i, j in baselines(self.nchan, all=False, auto=auto):
            if i == j:
                log.info(f"Autocorrelation {i}-{j}")
                self.freq[i, j], self.time[i, j], self.corr_complex[i, j] = (
                    scipy_signal.spectrogram(
                        data[i],
                        self.rate,
                        nperseg=self.nps,
                        nfft=self.nfft,
                        scaling="spectrum",
                        noverlap=noverlap,
                        window="hann",
                    )
                )
                self.corr_amp[i, j], self.corr_pha[i, j] = Complex_to_AmpPha(
                    self.corr_complex[i, j]
                )
            else:
                log.info(f"Cross-correlation {i}-{j}")
                self.freq[i, j], self.time[i, j], self.corr_complex[i, j] = (
                    _spectral_helper(
                        data[i],
                        data[j],
                        self.rate,
                        nperseg=self.nps,
                        noverlap=0,
                        nfft=self.nfft,
                        detrend="constant",
                        return_onesided=True,
                        scaling="density",
                        axis=-1,
                        mode="psd",
                        window="hann",
                    )
                )
                self.corr_amp[i, j], self.corr_pha[i, j] = Complex_to_AmpPha(
                    self.corr_complex[i, j]
                )
                # Populate conjugate baseline
                self.freq[j, i] = deepcopy(self.freq[i, j])
                self.time[j, i] = deepcopy(self.time[i, j])
                self.corr_amp[j, i] = deepcopy(self.corr_amp[i, j])
                self.corr_pha[j, i] = -deepcopy(self.corr_pha[i, j])
                self.corr_complex[j, i] = np.conjugate(deepcopy(self.corr_complex[i, j]))

        if apply_cal:
            log.info("Applying instrumental phase corrections to all baselines")
            corrections = self.get_inst_corrections()
            for i, j in baselines(self.nchan, all=True, auto=False):
                self.corr_complex[i, j] = (self.corr_complex[i, j].T / corrections[i, j]).T
                self.corr_amp[i, j], self.corr_pha[i, j] = Complex_to_AmpPha(
                    self.corr_complex[i, j]
                )
        else:
            log.info("NOT applying instrumental phase corrections")

    def get_inst_corrections(self) -> dict:
        """Return complex instrumental delay corrections keyed by (i, j)."""
        d_i = {
            (i, j): self.instrumental[i] - self.instrumental[j]
            for i, j in baselines(self.nchan, all=True, auto=True)
        }
        cx_expect = {}
        for i, j in baselines(self.nchan, all=True, auto=True):
            p_expect = (twopi * d_i[i, j] * self.freq[i, j]) % twopi
            cx_expect[i, j] = AmpPha_to_Complex(np.ones(np.shape(p_expect)), p_expect)
        return cx_expect

    def get_highSNR(self, amp_ref: dict, median: bool = True) -> tuple[dict, dict]:
        """Return high-SNR amplitude and phase dicts."""
        amp_high_SNR: dict = {}
        pha_high_SNR: dict = {}
        for i, j in baselines(self.nchan, all=True, auto=True):
            amp_high_SNR[i, j], pha_high_SNR[i, j] = self._highSNR_bl(
                self.corr_amp[i, j], self.corr_pha[i, j], amp_ref[i, j], self.SNR_lim, median
            )
            amp_high_SNR[j, i] = deepcopy(amp_high_SNR[i, j])
            pha_high_SNR[j, i] = -deepcopy(pha_high_SNR[i, j])
        return amp_high_SNR, pha_high_SNR

    def get_highSNR2(
        self, amp: dict, pha: dict, amp_ref: dict, SNR_limit: float, median: bool = True
    ) -> tuple[dict, dict]:
        """Return high-SNR amplitude and phase dicts with explicit SNR limit."""
        amp_high_SNR: dict = {}
        pha_high_SNR: dict = {}
        for i, j in baselines(self.nchan, all=True, auto=True):
            amp_high_SNR[i, j], pha_high_SNR[i, j] = self._highSNR_scaled_bl(
                amp[i, j], pha[i, j], amp_ref[i, j], SNR_limit, median
            )
            amp_high_SNR[j, i] = deepcopy(amp_high_SNR[i, j])
            pha_high_SNR[j, i] = -deepcopy(pha_high_SNR[i, j])
        return amp_high_SNR, pha_high_SNR

    def get_lowSNR(self, amp_ref: dict, median: bool = True) -> tuple[dict, dict]:
        """Return low-SNR amplitude and phase dicts."""
        amp_low_SNR: dict = {}
        pha_low_SNR: dict = {}
        for i, j in baselines(self.nchan, all=True, auto=True):
            amp_low_SNR[i, j], pha_low_SNR[i, j] = self._lowSNR_bl(
                self.corr_amp[i, j], self.corr_pha[i, j], amp_ref[i, j], self.SNR_lim, median
            )
            amp_low_SNR[j, i] = deepcopy(amp_low_SNR[i, j])
            pha_low_SNR[j, i] = -deepcopy(pha_low_SNR[i, j])
        return amp_low_SNR, pha_low_SNR

    # ------------------------------------------------------------------
    # Per-baseline SNR helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _snr(amp: np.ndarray, amp_ref: np.ndarray, median: bool) -> np.ndarray:
        top = np.sqrt(amp)
        bot = np.sqrt(np.nanmedian(amp_ref, axis=1) if median else np.nanmean(amp_ref, axis=1))
        return top / bot[:, None] if np.ndim(amp) == 2 else top / bot

    def _highSNR_bl(
        self, amp: np.ndarray, pha: np.ndarray, amp_ref: np.ndarray, SNR_limit: float, median: bool
    ) -> tuple[np.ndarray, np.ndarray]:
        high_amp = np.copy(amp)
        high_pha = np.copy(pha)
        SNR = self._snr(amp, amp_ref, median)
        limit = np.nanmax(SNR) * SNR_limit / 100.0
        high_amp[SNR < limit] = None
        high_pha[SNR < limit] = None
        return high_amp, high_pha

    def _highSNR_scaled_bl(
        self, amp: np.ndarray, pha: np.ndarray, amp_ref: np.ndarray, SNR_limit: float, median: bool
    ) -> tuple[np.ndarray, np.ndarray]:
        high_amp = np.copy(amp)
        high_pha = np.copy(pha)
        SNR = self._snr(amp, amp_ref, median)
        max_snr = np.nanmax(SNR)
        high_amp[SNR < 0.6 * max_snr] = None
        high_pha[SNR < 0.6 * max_snr] = None
        return high_amp, high_pha

    def _lowSNR_bl(
        self, amp: np.ndarray, pha: np.ndarray, amp_ref: np.ndarray, SNR_limit: float, median: bool
    ) -> tuple[np.ndarray, np.ndarray]:
        low_amp = np.copy(amp)
        low_pha = np.copy(pha)
        SNR = self._snr(amp, amp_ref, median)
        low_amp[SNR > SNR_limit] = None
        low_pha[SNR > SNR_limit] = None
        return low_amp, low_pha
