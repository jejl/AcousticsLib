"""Spectrogram generation utilities.

Ported from SoundClass/src/audio_utils.py.

All functions return matplotlib ``Figure`` objects so the caller controls
rendering (``fig.savefig()``, display in Streamlit via ``st.pyplot(fig)``, etc.).
"""
import numpy as np
import matplotlib.pyplot as plt
from scipy.signal import spectrogram


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

    Returns:
        matplotlib Figure.
    """
    f, t, Sxx = spectrogram(data, sample_rate, nperseg=1024)
    Sxx_db = 10 * np.log10(Sxx + 1e-7)

    duration = t[-1]
    fig_width  = max(duration * pixels_per_second / dpi, 1.0)
    fig_height = 600 / dpi

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
