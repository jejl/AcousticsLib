"""Direction-of-arrival (DoA) signal processing.

Ported from AudioFiles/batlib.py.

Geometric model: antennas are characterised by XYZ coordinates (metres) in a
North-East-Up frame.  Source direction is parameterised by Hour Angle (HA) and
Declination (Dec) in radians, mirroring a radio-astronomy aperture-synthesis
convention applied to acoustic near-field problems.

Far-field delays use the standard synthesis-imaging formula; near-field delays
use the full 3-D Euclidean path-length difference.  Both models can incorporate
instrumental (electronic/cable) delay offsets.

Optimisation uses ``scipy.optimize.shgo`` to find HA/Dec that minimise the
residual between observed and model cross-correlation phases.

Key public functions
--------------------
group_delay                      Far-field geometric delay for a baseline
group_delay_NF                   Near-field geometric delay
delta_pathlength_m               Path-length difference (m)
baseline_length_m                Euclidean baseline length (m)
baselines                        Re-exported from .fft for convenience
get_del / get_del_NF             Delay dict over all baselines
get_del_pha / get_del_pha_NF     Delay + phase dict over all baselines
global_optimisation              SHGO optimiser for HA/Dec
"""
import os
from copy import deepcopy

import numpy as np
from numpy import pi
from astropy.coordinates import spherical_to_cartesian

from loguru import logger

from .fft import (
    AmpPha_to_Complex,
    baselines,
    degunwrap,
    twopi,
)

# ---------------------------------------------------------------------------
# Simple geometric models
# ---------------------------------------------------------------------------

def d_ff(baseline: float, theta: float, vsound: float) -> float:
    """Far-field delay for a 1-D baseline.

    Parameters
    ----------
    baseline : Baseline length (m).
    theta    : Direction angle (radians, 90° = overhead).
    vsound   : Speed of sound (m/s).

    Returns
    -------
    float  Time delay (s).
    """
    return baseline * np.cos(theta) / vsound


def d_nf(baseline: float, theta: float, distance: float, vsound: float) -> float:
    """Near-field delay for a 1-D baseline.

    Parameters
    ----------
    baseline : Baseline length (m).
    theta    : Direction angle (radians, 90° = overhead).
    distance : Distance from reference antenna to source (m).
    vsound   : Speed of sound (m/s).

    Returns
    -------
    float  Time delay (s).
    """
    b, t, d = baseline, theta, distance
    C1 = (b - d * np.cos(t)) ** 2
    C2 = (d * np.sin(t)) ** 2
    return (d - np.sqrt(C1 + C2)) / vsound


# ---------------------------------------------------------------------------
# Path-length and baseline-length helpers
# ---------------------------------------------------------------------------

def delta_pathlength_m(
    HA: float,
    Dec: float,
    xyz_m,
    i_ant1: int,
    i_ant2: int,
) -> float:
    """Path-length difference between two antennas for a far-field source.

    Uses the standard synthesis-imaging formula (Synth Imaging, p. 85).

    Parameters
    ----------
    HA, Dec  : Source Hour Angle and Declination (radians).
    xyz_m    : DataFrame with columns X, Y, Z (metres).
    i_ant1, i_ant2 : Row indices into *xyz_m*.

    Returns
    -------
    float  Path-length difference (m).
    """
    ant1 = np.array(xyz_m[["X", "Y", "Z"]].iloc[i_ant1])
    ant2 = np.array(xyz_m[["X", "Y", "Z"]].iloc[i_ant2])
    L = np.subtract(ant2, ant1)
    return (
        L[0] * np.cos(HA) * np.cos(Dec)
        - L[1] * np.sin(HA) * np.cos(Dec)
        + L[2] * np.sin(Dec)
    )


def baseline_length_m(xyz_m, i_ant1: int, i_ant2: int) -> float:
    """Euclidean distance between two antennas (metres)."""
    ant1 = np.array(xyz_m[["X", "Y", "Z"]].iloc[i_ant1])
    ant2 = np.array(xyz_m[["X", "Y", "Z"]].iloc[i_ant2])
    return float(np.sqrt(np.sum(np.subtract(ant2, ant1) ** 2)))


# ---------------------------------------------------------------------------
# Far-field and near-field group-delay functions
# ---------------------------------------------------------------------------

def group_delay(
    soundspeed_m_persec: float,
    xyz_m,
    i_ant1: int,
    i_ant2: int,
    HA: float,
    Dec: float,
) -> float:
    """Far-field geometric group delay (seconds)."""
    return delta_pathlength_m(HA, Dec, xyz_m, i_ant1, i_ant2) / soundspeed_m_persec


def group_delay_NF(
    soundspeed_m_persec: float,
    HA: float,
    Dec: float,
    Dist: float,
    xyz_m,
    i_ant1: int,
    i_ant2: int,
    source_offset_x: float = 0.0,
    source_offset_y: float = 0.0,
) -> float:
    """Near-field geometric group delay (seconds).

    Converts (HA, Dec, Dist) to Cartesian, optionally shifts by source offsets,
    then returns (dist_ant1 − dist_ant2) / soundspeed.
    """
    xyz_source = np.array(spherical_to_cartesian(Dist, Dec, HA))
    xyz_source[0] += source_offset_x
    xyz_source[1] += source_offset_y

    ant1 = np.array(xyz_m[["X", "Y", "Z"]].iloc[i_ant1])
    ant2 = np.array(xyz_m[["X", "Y", "Z"]].iloc[i_ant2])
    L1 = np.subtract(xyz_source, ant1)
    L2 = np.subtract(xyz_source, ant2)
    delta_d = np.sqrt(np.sum(L1 ** 2)) - np.sqrt(np.sum(L2 ** 2))
    return delta_d / soundspeed_m_persec


def group_delay_NF_old(
    soundspeed_m_persec: float,
    HA: float,
    Dec: float,
    Dist: float,
    xyz_m,
    i_ant1: int,
    i_ant2: int,
) -> float:
    """Near-field delay — original geometric approximation (deprecated).

    Kept for compatibility.  Prefer :func:`group_delay_NF`.
    """
    D = Dist
    p = delta_pathlength_m(HA, Dec, xyz_m, i_ant1, i_ant2)
    B = baseline_length_m(xyz_m, i_ant1, i_ant2)
    a_squared = B ** 2 - p ** 2
    g = D - p
    y = np.sqrt(a_squared + g ** 2)
    return (D - y) / soundspeed_m_persec


# ---------------------------------------------------------------------------
# Delay and phase computation over all baselines
# ---------------------------------------------------------------------------

def get_del(sig, HA: float, Dec: float) -> dict:
    """Far-field delay dict keyed by (i, j) for all baselines."""
    return {
        (i, j): group_delay(sig.sound_speed_mps, sig.xyz, i, j, HA, Dec)
        for i, j in baselines(sig.nchan)
    }


def get_del_NF(sig, HA: float, Dec: float, Dist: float) -> dict:
    """Near-field delay dict keyed by (i, j) for all baselines."""
    return {
        (i, j): group_delay_NF(sig.sound_speed_mps, HA, Dec, Dist, sig.xyz, i, j)
        for i, j in baselines(sig.nchan)
    }


def get_del_pha(sig, HA: float, Dec: float) -> tuple[dict, dict]:
    """Return (delay_dict, phase_dict) for a far-field source direction."""
    d_g = get_del(sig, HA, Dec)
    p_expect = {
        (i, j): (twopi * d_g[i, j] * sig.freq[i, j]) % twopi
        for i, j in baselines(sig.nchan)
    }
    return d_g, p_expect


def get_del_pha_NF(sig, HA: float, Dec: float, Dist: float) -> tuple[dict, dict]:
    """Return (delay_dict, phase_dict) for a near-field source direction."""
    d_g = get_del_NF(sig, HA, Dec, Dist)
    p_expect = {
        (i, j): (twopi * d_g[i, j] * sig.freq[i, j]) % twopi
        for i, j in baselines(sig.nchan)
    }
    return d_g, p_expect


def get_pha_from_del(sig, dels: dict) -> dict:
    """Compute phase dict from a delay dict."""
    return {
        (i, j): (twopi * dels[i, j] * sig.freq[i, j]) % twopi
        for i, j in baselines(sig.nchan)
    }


def get_expected_phase(sig, d_g: dict) -> dict:
    """Convert delay dict to wrapped phase dict (±π)."""
    p = {}
    for i, j in baselines(sig.nchan):
        p[i, j] = (twopi * d_g[i, j] * sig.freq[i, j]) % twopi
        p[i, j][p[i, j] > pi] -= twopi
        p[i, j][p[i, j] < -pi] += twopi
    return p


# ---------------------------------------------------------------------------
# Delay corrections (geometry + instrumental offsets)
# ---------------------------------------------------------------------------

def get_del_pha_corrections(sig, HA: float, Dec: float, delay_inst) -> tuple[dict, dict]:
    """Return (delay_dict, phase_dict) including instrumental delay offsets (far-field)."""
    d_g = get_del(sig, HA, Dec)
    for i, j in baselines(sig.nchan):
        d_g[i, j] += delay_inst[i] - delay_inst[j]
        if np.abs(d_g[i, j]) < 1.0e-9:
            d_g[i, j] = 0.0
    return d_g, get_expected_phase(sig, d_g)


def get_del_pha_corrections_NF(
    sig, HA: float, Dec: float, Dist: float, delay_inst
) -> tuple[dict, dict]:
    """Return (delay_dict, phase_dict) including instrumental offsets (near-field)."""
    d_g = get_del_NF(sig, HA, Dec, Dist)
    for i, j in baselines(sig.nchan):
        d_g[i, j] += delay_inst[i] - delay_inst[j]
        if np.abs(d_g[i, j]) < 1.0e-9:
            d_g[i, j] = 0.0
    return d_g, get_expected_phase(sig, d_g)


# ---------------------------------------------------------------------------
# Phase solution for a single baseline
# ---------------------------------------------------------------------------

def get_pha_solns(sig, instrumental_delays, i: int, j: int):
    """Return (Pha_sol_FF, Pha_sol_NF, pha_diff_FF, pha_diff_NF, Del_sol_FF, Del_sol_NF)."""
    HA = float(sig.location["HA"])
    Dec = float(sig.location["Dec"])
    Dist = float(sig.location["Dist"])

    Del_sol_FF = group_delay(sig.sound_speed_mps, sig.xyz, i, j, HA, Dec)
    Del_sol_FF += instrumental_delays[i] - instrumental_delays[j]
    if np.abs(Del_sol_FF) < 1.0e-9:
        Del_sol_FF = 0.0
    Pha_sol_FF = (twopi * Del_sol_FF * sig.freq_rising[i, j]) % twopi

    Del_sol_NF = group_delay_NF(sig.sound_speed_mps, HA, Dec, Dist, sig.xyz, i, j)
    Del_sol_NF += instrumental_delays[i] - instrumental_delays[j]
    if np.abs(Del_sol_NF) < 1.0e-9:
        Del_sol_NF = 0.0
    Pha_sol_NF = (twopi * Del_sol_NF * sig.freq_rising[i, j]) % twopi

    pha_diff_NF = np.full(np.shape(Pha_sol_NF), np.nan)
    pha_diff_NF = degunwrap(np.subtract(sig.pha_rising[i, j], Pha_sol_NF))
    pha_diff_FF = np.full(np.shape(Pha_sol_FF), np.nan)
    pha_diff_FF = degunwrap(np.subtract(sig.pha_rising[i, j], Pha_sol_FF))

    return Pha_sol_FF, Pha_sol_NF, pha_diff_FF, pha_diff_NF, Del_sol_FF, Del_sol_NF


# ---------------------------------------------------------------------------
# Source model (fill sig.model)
# ---------------------------------------------------------------------------

def srcmod(sig, freq_array: np.ndarray, nearfield: bool = True):
    """Populate ``sig.model[i, j]`` with complex visibilities for the current location."""
    HA = float(sig.location["HA"])
    Dec = float(sig.location["Dec"])
    Dist = float(sig.location["Dist"])
    Flux = float(sig.location["Amp"])

    for i, j in baselines(sig.nchan, all=True):
        if nearfield:
            Del = group_delay_NF(
                sig.sound_speed_mps, HA, Dec, Dist, sig.xyz, i, j,
                sig.source_offset_x, sig.source_offset_y,
            )
        else:
            Del = group_delay(sig.sound_speed_mps, sig.xyz, i, j, HA, Dec)
        if np.abs(Del) < 1.0e-9:
            Del = 0.0
        Pha = (twopi * Del * freq_array) % twopi
        Amp = np.ones(np.shape(Pha)) * Flux
        sig.model[i, j] = AmpPha_to_Complex(Amp, Pha)
        sig.model[j, i] = AmpPha_to_Complex(Amp, -Pha)


# ---------------------------------------------------------------------------
# Near-field delay for all baselines (used in Loader)
# ---------------------------------------------------------------------------

def delay_calc_NF(signal, baseline: dict) -> tuple[dict, dict]:
    """Return (max_delay, group_delay) dicts for all baselines (near-field).

    Parameters
    ----------
    signal  : Loader or compatible object with .sound_speed_mps, .location, .xyz, .nchan.
    baseline: Dict keyed by (i, j) of baseline lengths (m).
    """
    max_delay = {}
    gd = {}
    for i, j in baselines(signal.nchan, all=True, auto=False):
        max_delay[i, j] = baseline[i, j] / signal.sound_speed_mps
        gd[i, j] = group_delay_NF(
            signal.sound_speed_mps,
            float(signal.location["HA"].iloc[0]),
            float(signal.location["Dec"].iloc[0]),
            float(signal.location["Dist"].iloc[0]),
            signal.xyz,
            i,
            j,
            signal.source_offset_x,
            signal.source_offset_y,
        )
    return max_delay, gd


# ---------------------------------------------------------------------------
# Residual computation
# ---------------------------------------------------------------------------

def get_resid(x, sig, pha_measured: dict) -> float:
    """Sum-of-squared phase residuals for a far-field source at (HA, Dec)."""
    HA, Dec = x[0], x[1]
    _, Pha_sol = get_del_pha(sig, HA, Dec)
    resid_arr = []
    for i, j in baselines(sig.nchan):
        r = (pha_measured[i, j].T % twopi) - (Pha_sol[i, j]).T % twopi
        resid_arr.append(np.nansum((r ** 2).flatten()))
    return float(np.nansum(resid_arr))


def get_coher_av(x, sig, pha_for_resid, amp_for_resid) -> float:
    """Coherence-averaged residual for far-field optimisation (minimise)."""
    HA, Dec = float(x[0]), float(x[1])
    _, Pha_sol = get_del_pha(sig, HA, Dec)
    nb = int(sig.nchan * (sig.nchan - 1) / 2)
    pha_sol_stack = np.full((np.shape(Pha_sol[0, 1])[0], nb), np.nan)
    for bn, (i, j) in enumerate(baselines(sig.nchan)):
        pha_sol_stack[:, bn] = Pha_sol[i, j]
    pha_transposed = np.transpose(pha_for_resid, axes=(1, 0, 2)) - pha_sol_stack
    mask_amp = ~np.isnan(amp_for_resid)
    mask_pha = ~np.isnan(pha_transposed)
    r = -np.abs(np.nanmean(AmpPha_to_Complex(amp_for_resid[mask_amp], pha_transposed[mask_pha])))
    return float(r)


def resid_calc_preparation(sig, amp: dict, pha: dict) -> tuple[np.ndarray, np.ndarray]:
    """Stack amp/phase dicts into arrays suitable for ``get_coher_av``."""
    nb = int(sig.nchan * (sig.nchan - 1) / 2)
    shape = np.shape(amp[0, 1])
    amp_arr = np.full((*shape, nb), np.nan)
    pha_arr = np.full((*shape, nb), np.nan)
    for bn, (i, j) in enumerate(baselines(sig.nchan)):
        pha_arr[:, :, bn] = pha[i, j]
        amp_arr[:, :, bn] = np.sqrt(amp[i, j])
    return amp_arr, pha_arr


def get_coher_av_delsol_all_baselines(
    x, sig, pha_obs, amp_obs, single_return: bool = True
):
    """Phase residual summed over all baselines using NF delays (for optimisation)."""
    HA, Dec, Dist = float(x[0]), float(x[1]), float(x[2])
    _, Pha_sol_NF = get_del_pha_NF(sig, HA, Dec, Dist)

    nb = int(sig.nchan * (sig.nchan - 1) / 2)
    resid_sum = std_sum = 0.0
    resid_weighted_sum = 0.0
    for baseline_number, (i, j) in enumerate(baselines(sig.nchan)):
        pha_diff = np.full(np.shape(Pha_sol_NF[0, 1]), np.nan)
        pha_diff = degunwrap(
            np.subtract(np.transpose(pha_obs[:, :, baseline_number]), Pha_sol_NF[i, j])
        )
        std = np.nanstd(pha_diff)
        pha_diff_weighted = (
            np.abs(np.nanmean(pha_diff * np.transpose(amp_obs[:, :, baseline_number])))
            + std
        )
        resid = np.abs(np.nanmean(pha_diff)) + std
        resid_sum += resid
        resid_weighted_sum += np.abs(pha_diff_weighted) + std
        std_sum += std

    if single_return:
        return resid_sum + std_sum
    return resid_sum + std_sum, resid_weighted_sum + std_sum, std_sum


def get_coher_av_delsol_1bl(
    instrumental_delay, x0, sig, pha_obs, amp_obs, ant1: int, ant2: int,
    single_return: bool = True,
):
    """Single-baseline phase residual (far-field corrected)."""
    i, j = (ant1, ant2) if ant1 < ant2 else (ant2, ant1)
    HA = float(sig.location["HA"])
    Dec = float(sig.location["Dec"])
    x0[ant1] = instrumental_delay
    _, Pha_sol = get_del_pha_corrections(sig, HA, Dec, x0)
    pha_diff = degunwrap(
        np.transpose(np.subtract(np.transpose(pha_obs[i, j]), Pha_sol[i, j]))
    )
    std = np.nanstd(pha_diff)
    pha_diff_weighted = np.abs(np.nanmean(pha_diff * amp_obs[i, j])) + std
    resid = np.abs(np.nanmean(pha_diff)) + std
    resid_weighted = np.abs(pha_diff_weighted) + std
    if single_return:
        return resid + std
    return resid + std, resid_weighted + std, std


def get_coher_av_delsol_1bl_NF(
    instrumental_delay, x0, sig, pha_obs, amp_obs, ant1: int, ant2: int,
    single_return: bool = True,
):
    """Single-baseline phase residual (near-field corrected)."""
    i, j = (ant1, ant2) if ant1 < ant2 else (ant2, ant1)
    HA = float(sig.location["HA"])
    Dec = float(sig.location["Dec"])
    Dist = float(sig.location["Dist"])
    x0[ant1] = instrumental_delay
    _, Pha_sol_NF = get_del_pha_corrections_NF(sig, HA, Dec, Dist, x0)
    pha_diff = degunwrap(
        np.transpose(np.subtract(np.transpose(pha_obs[i, j]), Pha_sol_NF[i, j]))
    )
    std = np.nanstd(pha_diff)
    pha_diff_weighted = np.abs(np.nanmean(pha_diff * amp_obs[i, j])) + std
    resid = np.abs(np.nanmean(pha_diff)) + std
    resid_weighted = np.abs(pha_diff_weighted) + std
    if single_return:
        return resid
    return resid, resid_weighted, std


def get_coher_av_delsol_allbl_NF_flat(
    test_instrumental_delays, instrumental_delays, sig, instrumental_cal_applied_to_data: bool
) -> float:
    """All-baseline NF residual for flat-phase instrumental calibration."""
    if instrumental_cal_applied_to_data:
        instrumental_delays = np.zeros(np.shape(instrumental_delays)) + test_instrumental_delays
    else:
        instrumental_delays = test_instrumental_delays

    resid_sum = 0.0
    for i, j in baselines(sig.nchan):
        _, _, _, pha_diff_NF, _, _ = get_pha_solns(sig, instrumental_delays, i, j)
        std = np.nanstd(pha_diff_NF)
        resid = ((np.abs(np.nanmean(pha_diff_NF)) + 1) * std) - std
        resid_sum += resid

    return resid_sum


def get_resid_varHADecDist_allbl_NF_flat(
    test_HA_Dec_Dist, instrumental_delays, sig, instrumental_cal_applied_to_data: bool, log
) -> float:
    """All-baseline NF residual varying HA, Dec, Dist simultaneously."""
    sig.location["HA"] = test_HA_Dec_Dist[0]
    sig.location["Dec"] = test_HA_Dec_Dist[1]
    sig.location["Dist"] = test_HA_Dec_Dist[2]

    if instrumental_cal_applied_to_data:
        instrumental_delays = np.zeros(np.shape(instrumental_delays))

    resid_sum = 0.0
    for i, j in baselines(sig.nchan):
        _, _, _, pha_diff_NF, _, _ = get_pha_solns(sig, instrumental_delays, i, j)
        std = np.nanstd(pha_diff_NF)
        resid_sum += np.abs(np.nanmean(pha_diff_NF)) + std

    log.info(f"HaDecDist = {test_HA_Dec_Dist}, resid = {resid_sum}")
    return resid_sum


def get_resid_varXYZ1ant_allbl_NF_flat(
    test_pos_ant, instrumental_delays, ant_no: int, sig, instrumental_cal_applied_to_data: bool
) -> float:
    """All-baseline NF residual varying XYZ of a single antenna."""
    sig.xyz.loc[ant_no, "X"] = test_pos_ant[0]
    sig.xyz.loc[ant_no, "Y"] = test_pos_ant[1]
    sig.xyz.loc[ant_no, "Z"] = test_pos_ant[2]

    resid_sum = 0.0
    for i, j in baselines(sig.nchan):
        if instrumental_cal_applied_to_data:
            instrumental_delays = np.zeros(np.shape(instrumental_delays))
        _, _, _, pha_diff_NF, _, _ = get_pha_solns(sig, instrumental_delays, i, j)
        std = np.nanstd(pha_diff_NF)
        resid_sum += np.abs(np.nanmean(pha_diff_NF)) + std

    return resid_sum


# ---------------------------------------------------------------------------
# Global optimisation
# ---------------------------------------------------------------------------

def global_optimisation(x, sig, amp, pha, dist_tolerance: float = 0.1, dec_min: float = -45.0):
    """SHGO global search for best-fit HA/Dec.

    Parameters
    ----------
    x            : Initial/current position (unused by SHGO, kept for API symmetry).
    sig          : Signal object.
    amp, pha     : Amplitude and phase dicts.
    dist_tolerance : Minimum angular separation (deg) between returned solutions.
    dec_min      : Minimum declination bound (degrees).

    Returns
    -------
    OptimizeResult or []  SHGO result, or empty list on failure.
    """
    from scipy import optimize

    logger.info(f"Global optimisation: x = {x}")
    amp_for_resid, pha_for_resid = resid_calc_preparation(sig, amp, pha)
    bounds = [(-180.20, 180.20, 0.2), (dec_min - 0.2, 90.20, 30.0)]
    results = {}
    try:
        results["shgo_sobol"] = optimize.shgo(
            get_coher_av,
            bounds,
            args=(sig, pha_for_resid, amp_for_resid),
            n=100,
            iters=1,
            sampling_method="sobol",
        )
    except Exception:
        import sys
        logger.error(f"Optimisation failed: {sys.exc_info()}")
        return []

    my_results = results["shgo_sobol"]
    ri = 1
    while ri < len(my_results.xl):
        while (
            abs(my_results.xl[ri][0] - my_results.xl[ri - 1][0]) < dist_tolerance
            and abs(my_results.xl[ri][1] - my_results.xl[ri - 1][1]) < dist_tolerance
        ):
            my_results.xl = np.delete(my_results.xl, ri, 0)
            my_results.funl = np.delete(my_results.funl, ri, 0)
            if ri == len(my_results.xl):
                break
        if ri == len(my_results.xl):
            break
        ri += 1

    return my_results


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def get_location_delay_map(
    i_ant1: int, i_ant2: int, delay_map: dict, delay_measured: float, delay_error: float
):
    """Binary map of possible source locations given measured delay and error."""
    i, j = i_ant1, i_ant2
    if np.isnan(delay_measured):
        binary_map = np.zeros(delay_map[i, j].shape)
        return binary_map

    delay_map_possible = np.copy(delay_map[i, j])
    min_delay = delay_measured - delay_error
    max_delay = delay_measured + delay_error
    delay_map_possible[
        np.logical_or(delay_map[i, j] < min_delay, delay_map[i, j] > max_delay)
    ] = None
    binary_map = np.zeros(delay_map_possible.shape)
    binary_map[delay_map_possible == delay_map_possible] = 1
    return binary_map


def get_istart(i: int, signal) -> int:
    """Return start index for baseline iteration (0 if all_baselines, else i)."""
    return 0 if signal.all_baselines else i


def get_baseline_lengths(sig) -> dict:
    """Calculate Euclidean baseline lengths (m) for all pairs including autocorrelations."""
    baseline = {}
    for i, j in baselines(sig.nchan, all=True, auto=True):
        L = np.subtract(np.array(sig.xyz)[i], np.array(sig.xyz)[j])[1:4]
        baseline[i, j] = float(np.sqrt(np.sum(L ** 2)))
    return baseline


def get_SNR(signal) -> dict:
    """Compute per-baseline SNR: sqrt(amp) / sqrt(median(amp)) over time axis."""
    SNR = {}
    for i, j in baselines(signal.nchan, all=True, auto=False):
        top = np.sqrt(signal.corr_amp[i, j])
        bot = np.sqrt(np.nanmedian(signal.corr_amp[i, j], axis=1))
        SNR[i, j] = top / bot[:, None]
    return SNR


def get_uvw(
    wavelength_m: float,
    xyz_m,
    i_ant1: int,
    i_ant2: int,
    HA: float,
    Dec: float,
) -> tuple[float, float, float]:
    """Return (u, v, w) spatial-frequency coordinates for a baseline."""
    inv_lambda = 1.0 / wavelength_m
    ant1 = np.array(xyz_m[["X", "Y", "Z"]].iloc[i_ant1])
    ant2 = np.array(xyz_m[["X", "Y", "Z"]].iloc[i_ant2])
    L = np.subtract(ant1, ant2)
    row1 = [np.sin(HA), np.cos(HA), 0]
    row2 = [-np.sin(Dec) * np.cos(HA), np.sin(Dec) * np.sin(HA), np.cos(Dec)]
    row3 = [np.cos(Dec) * np.cos(HA), -np.cos(Dec) * np.sin(HA), np.sin(Dec)]
    u = float(np.sum(inv_lambda * L * row1))
    v = float(np.sum(inv_lambda * L * row2))
    w = float(np.sum(inv_lambda * L * row3))
    return u, v, w


# ---------------------------------------------------------------------------
# Data selection helpers
# ---------------------------------------------------------------------------

def get_rising_phase_data(SNR, amp_SNRlow, amp_highSNR, signal):
    """Select data with rising amplitude (onset of a call)."""
    amp_s_rising = {}
    pha_s_rising = {}
    complex_s_rising = {}

    for i, j in baselines(signal.nchan):
        SNR_shift = np.roll(SNR[i, j], signal.snr_shift, axis=1)
        amp_s_rising[i, j] = deepcopy(signal.corr_amp[i, j])
        amp_s_rising[i, j][SNR[i, j] / SNR_shift <= signal.snr_ratio_cut] = None
        pha_s_rising[i, j] = deepcopy(signal.corr_pha[i, j])
        pha_s_rising[i, j][SNR[i, j] / SNR_shift <= signal.snr_ratio_cut] = None
        complex_s_rising[i, j] = deepcopy(signal.corr_complex[i, j])
        complex_s_rising[i, j][SNR[i, j] / SNR_shift <= signal.snr_ratio_cut] = None

        lfr = len(signal.freq[i, j])
        for fno in range(lfr):
            rising = np.logical_not(np.isnan(amp_highSNR[i, j][fno, :]))
            recover = np.logical_not(np.isnan(amp_SNRlow[i, j][fno, :]))
            k = 0
            while k < len(rising[:-2]):
                if rising[k] and not rising[k + 1] and k < len(rising[:-2]):
                    recover_found = False
                    while not recover_found and k < len(rising[:-2]):
                        if not recover[k]:
                            k += 1
                            amp_highSNR[fno, k] = None
                        else:
                            recover_found = True
                k += 1
            amp_s_rising[i, j][np.isnan(amp_highSNR[i, j])] = None
            pha_s_rising[i, j][np.isnan(amp_highSNR[i, j])] = None
            complex_s_rising[i, j][np.isnan(amp_highSNR[i, j])] = None

        amp_s_rising[j, i] = deepcopy(amp_s_rising[i, j])
        pha_s_rising[j, i] = -deepcopy(pha_s_rising[i, j])
        complex_s_rising[j, i] = np.conjugate(deepcopy(complex_s_rising[i, j]))

    return amp_s_rising, pha_s_rising, complex_s_rising


def tidy_rising_data(
    signal,
    amp_s_rising: dict,
    pha_s_rising: dict,
    complex_s_rising: dict,
    match_baselines: bool = False,
    flatten: bool = False,
):
    """Remove empty freq/time rows and optionally flatten baseline data."""
    empty_rows: list = []
    empty_cols: list = []

    if match_baselines:
        template_array = np.zeros(np.shape(amp_s_rising[0, 1]))
        for i, j in baselines(signal.nchan):
            test = np.copy(amp_s_rising[i, j])
            test[~np.isnan(test)] = 1
            test[np.isnan(test)] = 0
            template_array += test
        template_array[template_array == 0.0] = np.nan

        for i, j in baselines(signal.nchan):
            empty_rows = np.isnan(template_array).all(axis=1)
            empty_cols = np.isnan(template_array).all(axis=0)
            amp_s_rising[i, j] = np.delete(amp_s_rising[i, j], empty_cols, axis=1)
            amp_s_rising[i, j] = np.delete(amp_s_rising[i, j], empty_rows, axis=0)
            pha_s_rising[i, j] = np.delete(pha_s_rising[i, j], empty_cols, axis=1)
            pha_s_rising[i, j] = np.delete(pha_s_rising[i, j], empty_rows, axis=0)
            complex_s_rising[i, j] = np.delete(complex_s_rising[i, j], empty_cols, axis=1)
            complex_s_rising[i, j] = np.delete(complex_s_rising[i, j], empty_rows, axis=0)
    else:
        for i, j in baselines(signal.nchan):
            empty_rows = np.isnan(amp_s_rising[i, j]).all(axis=1)
            empty_cols = np.isnan(amp_s_rising[i, j]).all(axis=0)
            amp_s_rising[i, j] = np.delete(amp_s_rising[i, j], empty_cols, axis=1)
            amp_s_rising[i, j] = np.delete(amp_s_rising[i, j], empty_rows, axis=0)
            pha_s_rising[i, j] = np.delete(pha_s_rising[i, j], empty_cols, axis=1)
            pha_s_rising[i, j] = np.delete(pha_s_rising[i, j], empty_rows, axis=0)
            complex_s_rising[i, j] = np.delete(complex_s_rising[i, j], empty_cols, axis=1)
            complex_s_rising[i, j] = np.delete(complex_s_rising[i, j], empty_rows, axis=0)

    if flatten:
        for i, j in baselines(signal.nchan):
            signal.amp_rising[i, j] = np.transpose(amp_s_rising[i, j]).flatten()
            signal.pha_rising[i, j] = np.transpose(pha_s_rising[i, j]).flatten()
            signal.complex_rising[i, j] = np.transpose(complex_s_rising[i, j]).flatten()
            signal.time_rising[i, j] = np.delete(signal.time[i, j], empty_cols)
            signal.freq_rising[i, j] = np.delete(signal.freq[i, j], empty_rows)
            n_times = len(signal.time_rising[i, j])
            n_freqs = len(signal.freq_rising[i, j])
            signal.freq_rising[i, j] = np.transpose(
                np.repeat(
                    np.reshape(signal.freq_rising[i, j], (n_freqs, 1)),
                    repeats=n_times,
                    axis=1,
                )
            ).flatten()
            signal.time_rising[i, j] = np.repeat(
                np.reshape(signal.time_rising[i, j], (n_times, 1)),
                repeats=n_freqs,
            )
            if not match_baselines:
                empty_data = np.isnan(signal.amp_rising[i, j])
                signal.freq_rising[i, j] = np.delete(signal.freq_rising[i, j], empty_data)
                signal.time_rising[i, j] = np.delete(signal.time_rising[i, j], empty_data)
                signal.amp_rising[i, j] = np.delete(signal.amp_rising[i, j], empty_data)
                signal.pha_rising[i, j] = np.delete(signal.pha_rising[i, j], empty_data)
                signal.complex_rising[i, j] = np.delete(signal.complex_rising[i, j], empty_data)

            signal.pha_rising[i, j] = signal.pha_rising[i, j] % twopi
            signal.wt_rising[i, j] = np.ones(np.shape(signal.amp_rising[i, j]))
            signal.pha_rising[j, i] = -signal.pha_rising[i, j]
            signal.amp_rising[j, i] = signal.amp_rising[i, j]
            signal.wt_rising[j, i] = -signal.wt_rising[i, j]
            signal.complex_rising[j, i] = np.conjugate(signal.complex_rising[i, j])
            signal.time_rising[j, i] = signal.time_rising[i, j]
            signal.freq_rising[j, i] = signal.freq_rising[i, j]
    else:
        for i, j in baselines(signal.nchan):
            signal.amp_rising[i, j] = amp_s_rising[i, j]
            signal.pha_rising[i, j] = pha_s_rising[i, j]
            signal.complex_rising[i, j] = complex_s_rising[i, j]
            signal.time_rising[i, j] = np.delete(signal.time[i, j], empty_cols)
            signal.freq_rising[i, j] = np.delete(signal.freq[i, j], empty_rows)
            signal.wt_rising[i, j] = np.ones(np.shape(signal.amp_rising[i, j]))
            signal.pha_rising[j, i] = -signal.pha_rising[i, j]
            signal.amp_rising[j, i] = signal.amp_rising[i, j]
            signal.wt_rising[j, i] = -signal.wt_rising[i, j]
            signal.complex_rising[j, i] = np.conjugate(signal.complex_rising[i, j])
            signal.time_rising[j, i] = signal.time_rising[i, j]
            signal.freq_rising[j, i] = signal.freq_rising[i, j]


def offset_times(signal, noise):
    """Shift time arrays by start_sec for signal and noise objects."""
    for i in range(signal.nchan):
        for j in range(signal.nchan):
            signal.time[i, j] += signal.start_sec
            noise.time[i, j] += noise.start_sec


def trim_by_freq_cutoff(noise, signal, log):
    """Delete data outside the frequency window defined by signal settings."""
    if signal.low_freq_delete_hz < signal.high_freq_delete_hz:
        log.info(
            f"Apply low freq cutoff at {signal.low_freq_delete_hz / 1000:.1f} kHz and high "
            f"freq cutoff at {signal.high_freq_delete_hz / 1000:.1f} kHz"
        )
        reject_freqs = np.zeros(np.shape(signal.freq[0, 1]), dtype=bool)
        reject_freqs[signal.freq[0, 1] < signal.low_freq_delete_hz] = True
        reject_freqs[signal.freq[0, 1] > signal.high_freq_delete_hz] = True
        for i, j in baselines(signal.nchan, all=True, auto=True):
            signal.corr_amp[i, j] = np.delete(signal.corr_amp[i, j], reject_freqs, axis=0)
            signal.corr_pha[i, j] = np.delete(signal.corr_pha[i, j], reject_freqs, axis=0)
            signal.corr_complex[i, j] = np.delete(signal.corr_complex[i, j], reject_freqs, axis=0)
            signal.freq[i, j] = np.delete(noise.freq[i, j], reject_freqs)
            noise.corr_amp[i, j] = np.delete(noise.corr_amp[i, j], reject_freqs, axis=0)
            noise.corr_pha[i, j] = np.delete(noise.corr_pha[i, j], reject_freqs, axis=0)
            noise.corr_complex[i, j] = np.delete(noise.corr_complex[i, j], reject_freqs, axis=0)
            noise.freq[i, j] = np.delete(noise.freq[i, j], reject_freqs)
    elif signal.low_freq_cut_hz > 0:
        log.info(f"Apply low freq cutoff at {signal.low_freq_cut_hz / 1000:.1f} kHz")
        for i, j in baselines(signal.nchan, all=True, auto=True):
            signal.corr_amp[i, j][signal.freq[i, j] < signal.low_freq_cut_hz, :] = None
            noise.corr_amp[i, j][noise.freq[i, j] < noise.low_freq_cut_hz, :] = None


# ---------------------------------------------------------------------------
# File I/O
# ---------------------------------------------------------------------------

def read_chirp_times(signal_header) -> tuple[int, np.ndarray, str]:
    """Read chirp onset times from a companion ``_bat_times.txt`` file.

    Parameters
    ----------
    signal_header : Object with ``datadir`` and ``filename`` attributes.

    Returns
    -------
    (ntimes, times_data, times_file)
    """
    times_file = "{}/{}_bat_times.txt".format(
        signal_header.datadir, signal_header.filename[:-4]
    )
    if not os.path.exists(times_file):
        raise FileNotFoundError(f"Chirp times file not found: {times_file}")
    times_data = np.loadtxt(times_file, delimiter=",", usecols=(0, 1))[:, 0]
    return len(times_data), times_data, times_file
