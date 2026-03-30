"""acousticslib.audio — audio file handling subpackage.

Modules:
    metadata       WAV file metadata extraction (GUANO + wavinfo BAR-LT + filename fallbacks)
                   Public utilities: read_wav_metadata, parse_bar_title_long, parse_bar_title_short
    spectrograms   Spectrogram generation (generate_spectrogram, generate_spectrogram_preview)
    filters        DSP filters (butter_lowpass_filter, butter_bandpass_filter)
    io             Audio file discovery (get_audio_file_names, build_file_index)
"""