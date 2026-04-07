"""Spectrogram generation utilities.

Ported from SoundClass/src/audio_utils.py.

All functions return matplotlib ``Figure`` objects so the caller controls
rendering (``fig.savefig()``, display in Streamlit via ``st.pyplot(fig)``, etc.).
"""
import numpy as np
import matplotlib.pyplot as plt
from scipy.signal import spectrogram

# Fixed margins (inches) used when cm_per_second layout is active.
# These reserve space for axis labels/ticks so the *data area* is exactly
# duration * cm_per_second / 2.54 inches wide.
_L_IN = 0.90   # left  — y-axis label + tick labels
_R_IN = 0.15   # right — small padding
_B_IN = 0.45   # bottom — x-axis label + tick labels
_T_IN = 0.10   # top    — small padding


def generate_spectrogram(
    data: np.ndarray,
    sample_rate: int,
    fmin: float,
    fmax: float,
    cmap: str,
    pixels_per_second: float = 50,
    dpi: int = 200,
    legend_fontsize: int = 6,
    low_freq_hz: float = 500.0,
    cm_per_second: float = None,
) -> plt.Figure:
    """Generate a two-panel power spectrogram figure.

    The top panel spans *fmin*–*fmax*; the bottom panel spans 0–*low_freq_hz*
    to highlight low-frequency content (e.g. bittern booms).

    Args:
        data:               1-D audio array (mono).
        sample_rate:        Sample rate in Hz.
        fmin:               Minimum frequency for the top panel (Hz).
        fmax:               Maximum frequency for the top panel (Hz).
        cmap:               Matplotlib colourmap name.
        pixels_per_second:  Figure width in pixels per second of audio.
        dpi:                Figure DPI (used to convert pixels to inches).
        legend_fontsize:    Font size for the panel labels.
        low_freq_hz:        Upper frequency limit for the bottom panel (Hz).
        cm_per_second:      If given, overrides *pixels_per_second* / *dpi* and
                            sets the figure width so 1 second = this many cm.

    Returns:
        matplotlib Figure.
    """
    f, t, Sxx = spectrogram(data, sample_rate, nperseg=1024)
    Sxx_db = 10 * np.log10(Sxx + 1e-7)

    duration = t[-1]
    fig_height = 600 / dpi
    if cm_per_second is not None:
        fig_width = max(duration * cm_per_second / 2.54, 0.5) + _L_IN + _R_IN
    else:
        fig_width = max(duration * pixels_per_second / dpi, 1.0)

    fig, (ax1, ax2) = plt.subplots(
        2, 1,
        figsize=(fig_width, fig_height),
        gridspec_kw={"height_ratios": [1, 1]},
        sharex=True,
    )

    # Top panel — full frequency range
    f_idx_full = np.where((f >= fmin) & (f <= fmax))
    ax1.pcolormesh(t, f[f_idx_full], Sxx_db[f_idx_full], shading="gouraud", cmap=cmap)
    ax1.tick_params(axis="both", which="major", labelsize=8)
    ax1.legend(
        handles=[plt.Line2D([], [], color="none", label="All frequencies")],
        loc="upper right", fontsize=legend_fontsize, frameon=True,
        handlelength=0, handletextpad=0.1, borderpad=0.5, labelspacing=0.2,
    )

    # Bottom panel — low frequencies
    f_idx_low = np.where((f >= 0) & (f <= low_freq_hz))
    ax2.pcolormesh(t, f[f_idx_low], Sxx_db[f_idx_low], shading="gouraud", cmap=cmap)
    ax2.tick_params(axis="both", which="major", labelsize=8)
    ax2.set_xlabel("Time (s)", fontsize=10)
    ax2.legend(
        handles=[plt.Line2D([], [], color="none", label="Low frequencies")],
        loc="upper right", fontsize=legend_fontsize, frameon=True,
        handlelength=0, handletextpad=0.1, borderpad=0.5, labelspacing=0.2,
    )

    if cm_per_second is not None:
        ax1.set_ylabel("Frequency (Hz)", fontsize=10)
        ax2.set_ylabel("Frequency (Hz)", fontsize=10)
        fig.subplots_adjust(
            left=_L_IN / fig_width,
            right=1.0 - _R_IN / fig_width,
            bottom=_B_IN / fig_height,
            top=1.0 - _T_IN / fig_height,
            hspace=0,
        )
    else:
        fig.text(0.0, 0.5, "Frequency (Hz)", va="center", rotation="vertical", fontsize=12)
        plt.subplots_adjust(hspace=0, left=0.13)
        plt.tight_layout()

    return fig


def generate_spectrogram_preview(cmap_name: str) -> plt.Figure:
    """Generate a synthetic spectrogram preview for colourmap selection UIs.

    Returns:
        matplotlib Figure containing a 2-D Gaussian + noise image.
    """
    x = np.linspace(-1, 1, 30)
    y = np.linspace(-1, 1, 30)
    x, y = np.meshgrid(x, y)
    gaussian = np.exp(-((x ** 2 + y ** 2) / (2 * 0.3 ** 2)))
    noise = np.random.default_rng().normal(0, 0.1, gaussian.shape)
    data = gaussian + noise

    fig, ax = plt.subplots(figsize=(4, 2))
    cax = ax.imshow(data, cmap=cmap_name, origin="lower")
    plt.colorbar(cax, ax=ax)
    ax.axis("off")
    plt.tight_layout()
    return fig


def _vmin_vmax_for_range(
    Sxx_db: np.ndarray,
    f: np.ndarray,
    f_lo: float,
    f_hi: float,
) -> tuple:
    """Return (vmin, vmax) from the 2nd–98th percentile of *Sxx_db* in [f_lo, f_hi].

    Falls back to the global min/max if no frequency bins fall in the band.
    """
    idx = np.where((f >= f_lo) & (f <= f_hi))[0]
    if idx.size == 0:
        return float(Sxx_db.min()), float(Sxx_db.max())
    subset = Sxx_db[idx, :]
    return float(np.percentile(subset, 2)), float(np.percentile(subset, 98))


def generate_spectrogram_single_panel(
    data: np.ndarray,
    sample_rate: int,
    fmin: float,
    fmax: float,
    cmap: str,
    scaling_fmin: float,
    scaling_fmax: float,
    pixels_per_second: float = 50,
    dpi: int = 200,
    legend_fontsize: int = 6,
    cm_per_second: float = None,
) -> plt.Figure:
    """Single-panel spectrogram with colour scaling from a specific frequency band.

    The colour limits (vmin/vmax) are derived from the 2nd–98th percentile of
    power in the band [*scaling_fmin*, *scaling_fmax*], so the display contrast
    is tuned to the species' call frequency range rather than the full spectrum.

    Args:
        data:            1-D audio array (mono).
        sample_rate:     Sample rate in Hz.
        fmin:            Minimum displayed frequency (Hz).
        fmax:            Maximum displayed frequency (Hz).
        cmap:            Matplotlib colourmap name.
        scaling_fmin:    Lower bound of the band used for colour scaling (Hz).
        scaling_fmax:    Upper bound of the band used for colour scaling (Hz).
        pixels_per_second: Figure width in pixels per second of audio.
        dpi:             Figure DPI.
        legend_fontsize: Font size for the panel label.
        cm_per_second:   If given, overrides *pixels_per_second* / *dpi* and
                         sets the figure width so 1 second = this many cm.

    Returns:
        matplotlib Figure.
    """
    f, t, Sxx = spectrogram(data, sample_rate, nperseg=1024)
    Sxx_db = 10 * np.log10(Sxx + 1e-7)

    vmin, vmax = _vmin_vmax_for_range(Sxx_db, f, scaling_fmin, scaling_fmax)

    duration = t[-1]
    fig_height = 300 / dpi
    if cm_per_second is not None:
        fig_width = max(duration * cm_per_second / 2.54, 0.5) + _L_IN + _R_IN
    else:
        fig_width = max(duration * pixels_per_second / dpi, 1.0)

    fig, ax = plt.subplots(1, 1, figsize=(fig_width, fig_height))

    f_idx = np.where((f >= fmin) & (f <= fmax))[0]
    ax.pcolormesh(
        t, f[f_idx], Sxx_db[f_idx, :],
        shading="gouraud", cmap=cmap, vmin=vmin, vmax=vmax,
    )
    ax.tick_params(axis="both", which="major", labelsize=8)
    ax.set_xlabel("Time (s)", fontsize=10)
    ax.set_ylabel("Frequency (Hz)", fontsize=10)
    ax.legend(
        handles=[plt.Line2D([], [], color="none", label="All frequencies")],
        loc="upper right", fontsize=legend_fontsize, frameon=True,
        handlelength=0, handletextpad=0.1, borderpad=0.5, labelspacing=0.2,
    )
    if cm_per_second is not None:
        fig.subplots_adjust(
            left=_L_IN / fig_width,
            right=1.0 - _R_IN / fig_width,
            bottom=_B_IN / fig_height,
            top=1.0 - _T_IN / fig_height,
        )
    else:
        plt.tight_layout()
    return fig


def generate_spectrogram_two_panel_scaled(
    data: np.ndarray,
    sample_rate: int,
    fmin: float,
    fmax: float,
    cmap: str,
    low_freq_hz: float,
    top_scaling_fmin: float,
    top_scaling_fmax: float,
    bot_scaling_fmin: float,
    bot_scaling_fmax: float,
    pixels_per_second: float = 50,
    dpi: int = 200,
    legend_fontsize: int = 6,
    cm_per_second: float = None,
) -> plt.Figure:
    """Two-panel spectrogram with independent colour scaling per panel.

    The top panel spans *fmin*–*fmax*; the bottom panel spans 0–*low_freq_hz*.
    Each panel's colour limits are derived from the 2nd–98th percentile of power
    within its designated scaling band.

    Args:
        data:              1-D audio array (mono).
        sample_rate:       Sample rate in Hz.
        fmin:              Minimum frequency for the top panel (Hz).
        fmax:              Maximum frequency for the top panel (Hz).
        cmap:              Matplotlib colourmap name.
        low_freq_hz:       Upper frequency limit for the bottom panel (Hz).
        top_scaling_fmin:  Lower bound used for top-panel colour scaling (Hz).
        top_scaling_fmax:  Upper bound used for top-panel colour scaling (Hz).
        bot_scaling_fmin:  Lower bound used for bottom-panel colour scaling (Hz).
        bot_scaling_fmax:  Upper bound used for bottom-panel colour scaling (Hz).
        pixels_per_second: Figure width in pixels per second of audio.
        dpi:               Figure DPI.
        legend_fontsize:   Font size for panel labels.
        cm_per_second:     If given, overrides *pixels_per_second* / *dpi* and
                           sets the figure width so 1 second = this many cm.

    Returns:
        matplotlib Figure.
    """
    f, t, Sxx = spectrogram(data, sample_rate, nperseg=1024)
    Sxx_db = 10 * np.log10(Sxx + 1e-7)

    vmin1, vmax1 = _vmin_vmax_for_range(Sxx_db, f, top_scaling_fmin, top_scaling_fmax)
    vmin2, vmax2 = _vmin_vmax_for_range(Sxx_db, f, bot_scaling_fmin, bot_scaling_fmax)

    duration = t[-1]
    fig_height = 600 / dpi
    if cm_per_second is not None:
        fig_width = max(duration * cm_per_second / 2.54, 0.5) + _L_IN + _R_IN
    else:
        fig_width = max(duration * pixels_per_second / dpi, 1.0)

    fig, (ax1, ax2) = plt.subplots(
        2, 1,
        figsize=(fig_width, fig_height),
        gridspec_kw={"height_ratios": [1, 1]},
        sharex=True,
    )

    f_idx_full = np.where((f >= fmin) & (f <= fmax))[0]
    ax1.pcolormesh(
        t, f[f_idx_full], Sxx_db[f_idx_full, :],
        shading="gouraud", cmap=cmap, vmin=vmin1, vmax=vmax1,
    )
    ax1.tick_params(axis="both", which="major", labelsize=8)
    ax1.legend(
        handles=[plt.Line2D([], [], color="none", label="All frequencies")],
        loc="upper right", fontsize=legend_fontsize, frameon=True,
        handlelength=0, handletextpad=0.1, borderpad=0.5, labelspacing=0.2,
    )

    f_idx_low = np.where((f >= 0) & (f <= low_freq_hz))[0]
    ax2.pcolormesh(
        t, f[f_idx_low], Sxx_db[f_idx_low, :],
        shading="gouraud", cmap=cmap, vmin=vmin2, vmax=vmax2,
    )
    ax2.tick_params(axis="both", which="major", labelsize=8)
    ax2.set_xlabel("Time (s)", fontsize=10)
    ax2.legend(
        handles=[plt.Line2D([], [], color="none", label="Low frequencies")],
        loc="upper right", fontsize=legend_fontsize, frameon=True,
        handlelength=0, handletextpad=0.1, borderpad=0.5, labelspacing=0.2,
    )

    if cm_per_second is not None:
        ax1.set_ylabel("Frequency (Hz)", fontsize=10)
        ax2.set_ylabel("Frequency (Hz)", fontsize=10)
        fig.subplots_adjust(
            left=_L_IN / fig_width,
            right=1.0 - _R_IN / fig_width,
            bottom=_B_IN / fig_height,
            top=1.0 - _T_IN / fig_height,
            hspace=0,
        )
    else:
        fig.text(0.0, 0.5, "Frequency (Hz)", va="center", rotation="vertical", fontsize=12)
        plt.subplots_adjust(hspace=0, left=0.13)
        plt.tight_layout()
    return fig


def generate_classifier_spectrogram(
    data: np.ndarray,
    sample_rate: int,
    classifier: str,
    fmin: float,
    fmax: float,
    cmap: str,
    low_freq_hz: float = 500.0,
    pixels_per_second: float = 50,
    dpi: int = 200,
    legend_fontsize: int = 6,
    cm_per_second: float = None,
) -> plt.Figure:
    """Generate a spectrogram appropriate for the given classifier.

    Behaviour by classifier:

    ``"curlew"``
        Single panel (full frequency range).  Colour is scaled using power in
        the 1 kHz–*low_freq_hz* band to maximise contrast for curlew calls.

    ``"bittern"``
        Two panels (full range + 0–*low_freq_hz*).  The bottom panel is colour-
        scaled using power in the 50 Hz–*low_freq_hz* band to highlight bittern
        booms while suppressing DC noise.  The top panel is scaled across the
        full displayed range.

    anything else (unknown / multiple classifiers)
        Two panels, each scaled by its own displayed frequency range.

    Args:
        data:            1-D audio array (mono).
        sample_rate:     Sample rate in Hz.
        classifier:      ``"bittern"``, ``"curlew"``, or ``""`` / other.
        fmin:            Minimum displayed frequency (Hz).
        fmax:            Maximum displayed frequency (Hz).
        cmap:            Matplotlib colourmap name.
        low_freq_hz:     Upper frequency for the low-frequency panel / scaling
                         band.  Typically 500 Hz for bittern, 5000 Hz for curlew.
        pixels_per_second: Figure width in pixels per second of audio.
        dpi:             Figure DPI.
        legend_fontsize: Font size for panel labels.
        cm_per_second:   If given, overrides *pixels_per_second* / *dpi* and
                         sets the figure width so 1 second = this many cm.

    Returns:
        matplotlib Figure.
    """
    common = dict(
        pixels_per_second=pixels_per_second,
        dpi=dpi,
        legend_fontsize=legend_fontsize,
        cm_per_second=cm_per_second,
    )

    if classifier == "curlew":
        return generate_spectrogram_single_panel(
            data, sample_rate, fmin, fmax, cmap,
            scaling_fmin=1000.0,
            scaling_fmax=low_freq_hz,
            **common,
        )

    if classifier == "bittern":
        return generate_spectrogram_two_panel_scaled(
            data, sample_rate, fmin, fmax, cmap,
            low_freq_hz=low_freq_hz,
            top_scaling_fmin=fmin,
            top_scaling_fmax=fmax,
            bot_scaling_fmin=50.0,
            bot_scaling_fmax=low_freq_hz,
            **common,
        )

    # Unknown or multiple classifiers: two panels, each scaled by its own range
    return generate_spectrogram_two_panel_scaled(
        data, sample_rate, fmin, fmax, cmap,
        low_freq_hz=low_freq_hz,
        top_scaling_fmin=fmin,
        top_scaling_fmax=fmax,
        bot_scaling_fmin=0.0,
        bot_scaling_fmax=low_freq_hz,
        **common,
    )


def calculate_max_frequency(directory: str) -> int:
    """Return the Nyquist frequency of the highest-sample-rate WAV in *directory*.

    Walks the directory tree and reads the sample rate of every ``.wav`` file.
    Returns half the maximum sample rate found (the Nyquist frequency), or 0
    if no WAV files are found.
    """
    import soundfile as sf
    import os

    max_freq = 0
    for root, _, files in os.walk(directory):
        for fname in files:
            if fname.lower().endswith(".wav"):
                try:
                    info = sf.info(os.path.join(root, fname))
                    max_freq = max(max_freq, info.samplerate // 2)
                except Exception:
                    pass
    return max_freq
