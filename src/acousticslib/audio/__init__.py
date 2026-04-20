"""acousticslib.audio — audio file handling subpackage.

Modules:
    metadata       WAV file metadata extraction (GUANO + wavinfo BAR-LT + filename fallbacks)
                   Public utilities: read_wav_metadata, parse_bar_title_long, parse_bar_title_short
    spectrograms   Spectrogram generation (generate_spectrogram, generate_spectrogram_preview)
    filters        DSP filters (butter_lowpass_filter, butter_bandpass_filter)
    io             Audio file discovery (get_audio_file_names, build_file_index)
"""
from .filters import butter_bandpass_filter, butter_lowpass_filter
from .io import build_file_index, get_audio_file_names
from .metadata import WavMetadata, parse_bar_title_long, parse_bar_title_short, read_wav_metadata
from .spectrograms import generate_spectrogram, generate_spectrogram_preview

__all__ = [
    "WavMetadata",
    "build_file_index",
    "butter_bandpass_filter",
    "butter_lowpass_filter",
    "generate_spectrogram",
    "generate_spectrogram_preview",
    "get_audio_file_names",
    "parse_bar_title_long",
    "parse_bar_title_short",
    "read_wav_metadata",
]
