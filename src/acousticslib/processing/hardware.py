"""Hardware geometry configurations and recorder data classes.

Ported from AudioFiles/AudioData.py.

Provides:
    AudioData                            Base class with default processing settings
    xyz_one_mic_generic                  Single-mic XYZ geometry
    xyz_WA_MiniBat_generic               Wildlife Acoustics MiniBat geometry (= 1 mic)
    xyz_WA_SM4_generic                   SM4 stereo geometry (2 mics, 147.31 mm separation)
    xyz_FL_BAT_LT_generic                FrontierLabs BAR-LT geometry (2 mics, 229.9 mm)
    recorder_data_WA_MiniBat_generic     MiniBat recorder defaults
    recorder_data_FL_BAR_LT_generic      FrontierLabs BAR-LT recorder defaults
    recorder_data_WA_SM4_stereo_generic  SM4 stereo recorder defaults
"""
import numpy as np
import pandas as pd


class AudioData:
    """Base class for recorder/processing configurations.

    Inherit and override attributes to define recorder-specific hardware
    geometry and default processing parameters.  Call ``channel_settings()``
    after changing ``hw_number_mics``.
    """

    def __init__(self):
        # --- hardware ---
        self.hw_number_mics = 3
        self.channel_settings()

        # --- processing defaults ---
        self.datadir = ""
        self.filename = ""
        self.start_sec = 0
        self.stop_sec = 0
        self.detrend = True
        self.nps = 64
        self.nfft = 1024
        self.noverlap = 0
        # low-frequency cutoff for high-pass filtering (Hz)
        self.low_freq_cut_hz = 1000
        # frequency band to delete entirely (set both to non-zero to activate)
        self.low_freq_delete_hz = 0
        self.high_freq_delete_hz = 0
        # SNR controls
        # number of nps segments to shift when comparing for a good phase estimate
        self.snr_shift = 3
        # if SNR / SNR_shift <= snr_ratio_cut the data point is excluded
        self.snr_ratio_cut = 1
        self.SNR_lim = 50
        self.low_SNR_lim = 3
        # minimum declination to search (degrees)
        self.min_dec = 0
        self.all_baselines = False

        self.refant = 8  # reference microphone (1-based index)
        self.instrumental = None
        self.apply_instrumental = True

        # source location (HA/Dec in radians, Dist in metres, Amp dimensionless)
        default_loc = {
            "HA": np.deg2rad([0.0]),
            "Dec": np.deg2rad([90.0]),
            "Dist": 1.0,
            "Amp": 1.0,
        }
        self.location = pd.DataFrame(data=default_loc)
        self.ranges_grid = (slice(-180, 185, 10), slice(self.min_dec, 95, 5))
        self.source_offset_x = 0
        self.source_offset_y = 0

    def channel_settings(self):
        """Initialise per-channel arrays to match ``hw_number_mics``."""
        self.xyz = pd.DataFrame(
            np.zeros((self.hw_number_mics, 4)),
            columns=["X", "Y", "Z", "Select"],
        )
        self.xyz = self.xyz.astype({"Select": bool})
        self.mic_names = None
        self.mic_select = np.array([True] * self.hw_number_mics)
        self.gain_amp = np.array([1.0] * self.hw_number_mics)


# ---------------------------------------------------------------------------
# Geometry helpers — return (xyz DataFrame, names array)
# ---------------------------------------------------------------------------

def xyz_one_mic_generic():
    """Return XYZ geometry and names for a single-microphone recorder."""
    xyz = pd.DataFrame(np.zeros((1, 4)), columns=["X", "Y", "Z", "Select"])
    xyz = xyz.astype({"Select": bool})
    xyz.loc[:, "Select"] = True
    names = np.array(["1 Mic"])
    return xyz, names


def xyz_WA_MiniBat_generic():
    """Wildlife Acoustics MiniBat: single mic at origin."""
    return xyz_one_mic_generic()


def xyz_WA_SM4_generic():
    """Wildlife Acoustics SM4 stereo: two mics, 147.31 mm apart (N-S axis).

    Based on calibration measurements (3 Sep 2024) on NT09-DEVIL S4A17292.
    Separation = 154.16 − 3.44 − 3.41 mm = 147.31 mm.
    Left mic is channel 0 (origin); Right mic is 147.31 mm north.
    """
    neu = pd.DataFrame(np.zeros((2, 3)), columns=list("NEU"))
    neu.loc[0] = [0.0, 0.0, 0.0]
    neu.loc[1] = [0.14731, 0.0, 0.0]
    xyz = pd.DataFrame(np.zeros((2, 4)), columns=["X", "Y", "Z", "Select"])
    xyz = xyz.astype({"Select": bool})
    xyz.loc[:, "X"] = neu.loc[:, "N"]
    xyz.loc[:, "Y"] = neu.loc[:, "E"]
    xyz.loc[:, "Z"] = neu.loc[:, "U"]
    xyz.loc[:, "Select"] = True
    names = np.array(["1 Mic_A", "2 Mic_B"])
    return xyz, names


def xyz_FL_BAT_LT_generic():
    """FrontierLabs BAR-LT: two mics, 229.9 mm apart (N-S axis).

    Based on calibration observations (11 Dec 2021) with Diprose Lagoon
    recorder (SN 00023696).  Mic A is on the left when viewed from the front.
    """
    neu = pd.DataFrame(np.zeros((2, 3)), columns=list("NEU"))
    neu.loc[0] = [0.0, 0.0, 0.0]
    neu.loc[1] = [0.2299, 0.0, 0.0]
    xyz = pd.DataFrame(np.zeros((2, 4)), columns=["X", "Y", "Z", "Select"])
    xyz = xyz.astype({"Select": bool})
    xyz.loc[:, "X"] = neu.loc[:, "N"]
    xyz.loc[:, "Y"] = neu.loc[:, "E"]
    xyz.loc[:, "Z"] = neu.loc[:, "U"]
    xyz.loc[:, "Select"] = True
    names = np.array(["1 Mic_A", "2 Mic_B"])
    return xyz, names


# ---------------------------------------------------------------------------
# Concrete recorder classes
# ---------------------------------------------------------------------------

class recorder_data_WA_MiniBat_generic(AudioData):
    """Wildlife Acoustics MiniBat — single-channel defaults."""

    def __init__(self):
        super().__init__()
        self.hw_number_mics = 1
        self.channel_settings()
        self.xyz, self.mic_names = xyz_WA_MiniBat_generic()
        self.location.loc[0, "HA"] = 0.0
        self.location.loc[0, "Dec"] = np.deg2rad(90.0)
        self.location.loc[0, "Dist"] = 10.0
        self.refant = 1
        self.apply_instrumental = False
        self.instrumental = np.zeros(self.hw_number_mics)


class recorder_data_FL_BAR_LT_generic(AudioData):
    """FrontierLabs BAR-LT — two-channel defaults."""

    def __init__(self):
        super().__init__()
        self.hw_number_mics = 2
        self.channel_settings()
        self.xyz, self.mic_names = xyz_FL_BAT_LT_generic()
        self.location.loc[0, "HA"] = 0.0
        self.location.loc[0, "Dec"] = np.deg2rad(90.0)
        self.location.loc[0, "Dist"] = 10.0
        self.refant = 1
        self.apply_instrumental = False
        self.instrumental = np.zeros(self.hw_number_mics)


class recorder_data_WA_SM4_stereo_generic(AudioData):
    """Wildlife Acoustics SM4 stereo — two-channel defaults."""

    def __init__(self):
        super().__init__()
        self.hw_number_mics = 2
        self.channel_settings()
        self.xyz, self.mic_names = xyz_WA_SM4_generic()
        self.location.loc[0, "HA"] = 0.0
        self.location.loc[0, "Dec"] = np.deg2rad(90.0)
        self.location.loc[0, "Dist"] = 10.0
        self.refant = 1
        self.apply_instrumental = False
        self.instrumental = np.zeros(self.hw_number_mics)
