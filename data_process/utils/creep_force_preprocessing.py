"""Native-sample despiking, timestamp alignment and force-defined creep phases."""

from pathlib import Path

import numpy as np
import pandas as pd


def _fill_short_interior_gaps(series, max_bins):
    """Fill an entire missing run only when bounded and no longer than max_bins."""
    result = series.copy()
    missing = series.isna().to_numpy()
    starts = np.flatnonzero(missing & ~np.r_[False, missing[:-1]])
    ends = np.flatnonzero(missing & ~np.r_[missing[1:], False])
    interpolated = series.interpolate(method="linear", limit_area="inside")
    for start, end in zip(starts, ends):
        if start > 0 and end + 1 < len(series) and end - start + 1 <= max_bins:
            result.iloc[start:end + 1] = interpolated.iloc[start:end + 1]
    return result


def despike_raw_signal(time_s, values, *, window_s, threshold, n_sigma=6.,
                       max_duration_s, max_gap_s=.2):
    """Interpolate short returning excursions; retain steps, NaNs and long gaps."""
    t = np.asarray(time_s, dtype=float)
    y = np.asarray(values, dtype=float)
    cleaned, repaired = y.copy(), np.zeros(len(y), dtype=bool)
    finite = np.isfinite(y)
    # Split the detector as well as its interpolation at invalid data/long gaps.
    breaks = (~finite[:-1] | ~finite[1:] | (np.diff(t) > max_gap_s))
    starts = np.flatnonzero(finite & np.r_[True, breaks])
    ends = np.flatnonzero(finite & np.r_[breaks, True]) + 1
    window = pd.Timedelta(seconds=window_s)
    for first, stop in zip(starts, ends):
        if stop - first < 3:
            continue
        local_t, local_y = t[first:stop], y[first:stop]
        signal = pd.Series(local_y, index=pd.to_timedelta(local_t, unit="s"))
        center = signal.rolling(window, center=True, min_periods=3).median()
        residual = (signal - center).abs()
        noise = 1.4826 * residual.rolling(window, center=True, min_periods=3).median()
        cutoff = np.maximum(threshold, n_sigma * noise.to_numpy())
        candidate = residual.to_numpy() > cutoff
        run_starts = np.flatnonzero(candidate & ~np.r_[False, candidate[:-1]])
        run_ends = np.flatnonzero(candidate & ~np.r_[candidate[1:], False])
        nominal_dt = float(np.median(np.diff(local_t)))
        for start, end in zip(run_starts, run_ends):
            if start == 0 or end + 1 == len(local_y):
                continue
            left, right = start - 1, end + 1
            duration = local_t[end] - local_t[start] + nominal_dt
            if (duration > max_duration_s + 1e-12 or
                    local_t[right] - local_t[left] > max_gap_s + 1e-12):
                continue
            # A true loading/unloading step does not return to its prior level.
            if abs(local_y[right] - local_y[left]) > threshold:
                continue
            excursion = local_y[start:end + 1]
            outside_anchors = ((excursion < min(local_y[left], local_y[right]) - threshold) |
                               (excursion > max(local_y[left], local_y[right]) + threshold))
            if not outside_anchors.all():
                continue
            selection = slice(first + start, first + end + 1)
            cleaned[selection] = np.interp(local_t[start:end + 1],
                                           local_t[[left, right]], local_y[[left, right]])
            repaired[selection] = True
    return cleaned, repaired


def load_trial(trial_dir, dt=.02, baseline_s=.8, max_gap_s=.2,
               force_axis="Fz", compression_sign=-1.0, displacement_sign=1.0,
               displacement_despike=None, force_despike=None):
    """Baseline-correct native signals, then align them on common time bins.

    Force uses bin means and displacement uses bin medians. Only short interior
    gaps are interpolated. Repairs remain visible but are excluded from fitting
    when despiking is enabled. Raw and before-despiking values are retained.
    """
    directory = Path(trial_dir)
    force = pd.read_csv(directory / "Force.csv", dtype={"timestamp_ns": "int64"})
    lvdt = pd.read_csv(directory / "LVDT.csv", dtype={"timestamp_ns": "int64"})
    origin = min(int(force.timestamp_ns.min()), int(lvdt.timestamp_ns.min()))
    for frame in (force, lvdt):
        # Subtract int64 epochs BEFORE float conversion, retaining ns resolution.
        frame["time_s"] = (frame.timestamp_ns - np.int64(origin)).to_numpy() / 1e9
        frame.sort_values("time_s", kind="stable", inplace=True)
        frame.drop_duplicates("timestamp_ns", keep="first", inplace=True)
        frame.reset_index(drop=True, inplace=True)
    force["force_raw_n"] = pd.to_numeric(force[force_axis], errors="coerce")
    lvdt["displacement_raw_mm"] = pd.to_numeric(lvdt["Position (mm)"], errors="coerce")
    voltage = pd.to_numeric(lvdt["Voltage (V)"], errors="coerce")
    lvdt["sensor_valid"] = voltage.gt(.1) & voltage.lt(9.99) & np.isfinite(lvdt.displacement_raw_mm)
    clean_force = force.force_raw_n.to_numpy(copy=True)
    clean_displacement = lvdt.displacement_raw_mm.where(lvdt.sensor_valid).to_numpy(copy=True)
    force["force_spike"], lvdt["displacement_spike"] = False, False
    if force_despike is not None:
        clean_force, force["force_spike"] = despike_raw_signal(
            force.time_s, clean_force, **force_despike)
    if displacement_despike is not None:
        clean_displacement, lvdt["displacement_spike"] = despike_raw_signal(
            lvdt.time_s, clean_displacement, **displacement_despike)
    force_base = pd.Series(clean_force[force.time_s < baseline_s]).median()
    displacement_base = pd.Series(clean_displacement[lvdt.time_s < baseline_s]).median()
    force["force_before_despike_n"] = compression_sign * (force.force_raw_n - force_base)
    lvdt["displacement_before_despike_mm"] = displacement_sign * (lvdt.displacement_raw_mm - displacement_base)
    lvdt.loc[~lvdt.sensor_valid, "displacement_before_despike_mm"] = np.nan
    force["force_n"] = compression_sign * (clean_force - force_base)
    lvdt["displacement_mm"] = displacement_sign * (clean_displacement - displacement_base)
    start = max(force.time_s.min(), lvdt.time_s.min())
    end = min(force.time_s.max(), lvdt.time_s.max())
    first_bin, last_bin = int(np.ceil(start / dt)), int(np.floor(end / dt)) - 1
    bins = pd.Index(np.arange(first_bin, last_bin + 1), name="bin")
    force_group = force.groupby(np.floor(force.time_s / dt).astype("int64"))
    lvdt_group = lvdt.groupby(np.floor(lvdt.time_s / dt).astype("int64"))
    force_bins = force_group["force_n"]
    lvdt_bins = lvdt_group["displacement_mm"]
    synced = pd.DataFrame({"time_s": (bins.to_numpy() + .5) * dt}, index=bins)
    synced["force_n"] = force_bins.mean().reindex(bins)
    synced["displacement_mm"] = lvdt_bins.median().reindex(bins)
    synced["force_before_despike_n"] = force_group["force_before_despike_n"].mean().reindex(bins)
    synced["displacement_before_despike_mm"] = lvdt_group["displacement_before_despike_mm"].median().reindex(bins)
    synced["force_spike"] = force_group.force_spike.any().reindex(bins, fill_value=False)
    synced["displacement_spike"] = lvdt_group.displacement_spike.any().reindex(bins, fill_value=False)
    synced["force_observed"] = synced.force_n.notna()
    synced["displacement_observed"] = synced.displacement_mm.notna()
    invalid_bins = (~lvdt.sensor_valid).groupby(np.floor(lvdt.time_s / dt).astype("int64")).any()
    invalid_force_bins = (~np.isfinite(force.force_raw_n)).groupby(np.floor(force.time_s / dt).astype("int64")).any()
    synced["displacement_gap_repaired"] = invalid_bins.reindex(bins, fill_value=False)
    synced["force_gap_repaired"] = invalid_force_bins.reindex(bins, fill_value=False)
    # A run of N missing centers has its bracketing observations (N+1)*dt apart.
    max_bins = max(0, int(np.floor(max_gap_s / dt + 1e-9)) - 1)
    for column in ("force_n", "displacement_mm", "force_before_despike_n",
                   "displacement_before_despike_mm"):
        synced[column] = _fill_short_interior_gaps(synced[column], max_bins)
    synced["displacement_gap_repaired"] |= ~synced.displacement_observed & synced.displacement_mm.notna()
    synced["force_gap_repaired"] |= ~synced.force_observed & synced.force_n.notna()
    synced["repaired_displacement"] = synced.displacement_gap_repaired | synced.displacement_spike
    synced["repaired_force"] = synced.force_gap_repaired | synced.force_spike
    synced["valid"] = np.isfinite(synced.force_n) & np.isfinite(synced.displacement_mm)
    synced["fit_valid"] = synced.valid
    if displacement_despike is not None or force_despike is not None:
        synced["fit_valid"] &= ~synced.repaired_force & ~synced.repaired_displacement
    synced.reset_index(drop=True, inplace=True)
    return {"force": force, "lvdt": lvdt, "sync": synced}


def _hysteresis_intervals(values, on, off, debounce_bins):
    """Return confirmed load intervals; a missing force bin resets a candidate."""
    intervals, loaded, candidate, start = [], False, None, None
    for i, value in enumerate(values):
        qualifies = np.isfinite(value) and (value >= on if not loaded else value <= off)
        if not qualifies:
            candidate = None
            continue
        if candidate is None:
            candidate = i
        if i - candidate + 1 >= debounce_bins:
            if not loaded:
                start, loaded = candidate, True
            else:
                intervals.append((start, candidate, False))
                loaded = False
            candidate = None
    if loaded:
        intervals.append((start, len(values) - 1, True))
    return intervals


def flag_isolated_spikes(sync, phases, window_s=.2, threshold_mm=.12,
                         max_duration_s=.06):
    """Flag short displacement excursions within the same stable phase/cycle."""
    result = pd.Series(False, index=sync.index, name="suspected_displacement_spike")
    t = sync.time_s.to_numpy(float)
    dt = float(np.median(np.diff(t)))
    n_window = max(3, int(round(window_s / dt)))
    if n_window % 2 == 0:
        n_window += 1
    h = sync.displacement_mm.where(sync.valid)
    center = h.rolling(n_window, center=True, min_periods=3).median()
    deviation = (h - center).abs()
    local_mad = deviation.rolling(n_window, center=True, min_periods=3).median()
    eligible = (phases.phase.isin(["stable_load", "recovery"]).to_numpy() &
                sync.valid.to_numpy(bool) & np.isfinite(h.to_numpy()))
    candidate = eligible & (deviation.to_numpy() > np.maximum(threshold_mm, 8 * local_mad.to_numpy()))
    starts = np.flatnonzero(candidate & ~np.r_[False, candidate[:-1]])
    ends = np.flatnonzero(candidate & ~np.r_[candidate[1:], False])
    phase_values = phases.phase.to_numpy()
    cycle_values = phases.cycle.to_numpy()
    for start, end in zip(starts, ends):
        short = (end - start + 1) * dt <= max_duration_s + 1e-9
        bounded = start > 0 and end + 1 < len(sync)
        if not short or not bounded or not eligible[start - 1] or not eligible[end + 1]:
            continue
        neighborhood = slice(start - 1, end + 2)
        if (np.all(phase_values[neighborhood] == phase_values[start]) and
                np.all(cycle_values[neighborhood] == cycle_values[start])):
            result.iloc[start:end + 1] = True
    return result


def segment_cycles(sync, on_fraction=.2, off_fraction=.1, onset_fraction=.05,
                   ramp_fraction=.9, debounce_s=.1, min_loaded_s=5.,
                   smoothing_s=.1, plateau_guard_s=.8, stability_window_s=.5,
                   stable_std_fraction=.03, stable_slope_fraction=.05,
                   stable_confirm_s=.5, unload_guard_s=.25,
                   recovery_guard_s=1., min_force_n=.2):
    """Find loading, stable hold, unloading and recovery from measured force."""
    t = sync.time_s.to_numpy(float)
    dt = float(np.median(np.diff(t)))
    smooth_bins = max(1, round(smoothing_s / dt))
    if smooth_bins % 2 == 0:
        smooth_bins += 1
    force = sync.force_n.rolling(smooth_bins, center=True, min_periods=1).median()
    force = force.where(sync.force_n.notna())
    f = force.to_numpy()
    finite = f[np.isfinite(f)]
    high = float(np.quantile(finite, .9))
    intervals = _hysteresis_intervals(f, on_fraction * high, off_fraction * high,
                                     max(1, int(np.ceil(debounce_s / dt))))
    intervals = [(a, b, partial) for a, b, partial in intervals
                 if high > min_force_n and t[b] - t[a] >= min_loaded_s]
    phases = pd.DataFrame({"time_s": t, "force_segment_n": f, "cycle": 0,
                           "phase": "unloaded", "fit_valid": sync.fit_valid.to_numpy(bool),
                           "stable_mask": False})
    rows = []
    previous_end = 0
    window = max(3, round(stability_window_s / dt))
    force_std = force.rolling(window, center=True, min_periods=window).std()
    force_mean = force.rolling(window, center=True, min_periods=window).mean()
    force_slope = (force_mean.shift(-window // 2) - force_mean.shift(window // 2)) / (window * dt)
    for cycle, (start, end, partial) in enumerate(intervals, 1):
        # Search backward only within the current unloaded interval.
        preceding = np.flatnonzero(np.isfinite(f[previous_end:start]) &
                                   (f[previous_end:start] <= onset_fraction * high))
        onset = previous_end + int(preceding[-1]) + 1 if len(preceding) else start
        level_region = f[start:end]
        level = float(np.nanmedian(level_region))
        ramp_candidates = np.flatnonzero(f[onset:end] >= ramp_fraction * level)
        ramp = onset + int(ramp_candidates[0]) if len(ramp_candidates) else start
        falling = np.flatnonzero(f[ramp:end] >= ramp_fraction * level)
        unload = ramp + int(falling[-1]) + 1 if len(falling) else end
        unload = min(unload, end)
        if partial:
            unload = end
        stable = ((t >= t[onset] + plateau_guard_s) & (t >= t[ramp]) &
                  (t <= t[unload] - unload_guard_s) & sync.valid.to_numpy(bool) &
                  (np.abs(force_mean.to_numpy() - level) <= .1 * abs(level)) &
                  (force_std.to_numpy() <= stable_std_fraction * abs(level)) &
                  (np.abs(force_slope.to_numpy()) <= stable_slope_fraction * abs(level)))
        n_confirm = max(1, int(np.ceil(stable_confirm_s / dt)))
        stable_run = pd.Series(stable).rolling(n_confirm, min_periods=n_confirm).sum()
        confirmations = np.flatnonzero(stable_run.to_numpy() >= n_confirm)
        stable_start = int(confirmations[0] - n_confirm + 1) if len(confirmations) else None
        stable_end_t = t[unload] - unload_guard_s
        stable_mask = ((t >= t[stable_start]) & (t <= stable_end_t)) if stable_start is not None else np.zeros(len(t), bool)
        plateau = stable_mask & sync.valid.to_numpy(bool)
        next_start = intervals[cycle][0] if cycle < len(intervals) else len(t) - 1
        next_preceding = np.flatnonzero(np.isfinite(f[end:next_start]) &
                                        (f[end:next_start] <= onset_fraction * high))
        next_onset = end + int(next_preceding[-1]) + 1 if cycle < len(intervals) and len(next_preceding) else next_start
        recovery_start_t = t[end] + recovery_guard_s
        recovery_end_t = t[next_onset] - unload_guard_s if cycle < len(intervals) else t[-1]
        force_mean_n = float(sync.loc[plateau, "force_n"].mean())
        start_h_mask = plateau & (t <= (t[stable_start] + .5 if stable_start is not None else -np.inf))
        end_h_mask = plateau & (t >= stable_end_t - .5)
        creep_gain = float(sync.loc[end_h_mask, "displacement_mm"].median() -
                           sync.loc[start_h_mask, "displacement_mm"].median())
        for label, left, right in (
                ("loading", t[onset], t[ramp]),
                ("settling", t[ramp], t[stable_start] if stable_start is not None else t[unload]),
                ("stable_load", t[stable_start] if stable_start is not None else t[unload], stable_end_t),
                ("pre_unload_guard", stable_end_t, t[unload]),
                ("unloading", t[unload], t[end]),
                ("recovery_settling", t[end], recovery_start_t),
                ("recovery", recovery_start_t, recovery_end_t + dt / 2)):
            mask = (t >= left) & (t < right)
            phases.loc[mask, ["cycle", "phase"]] = [cycle, label]
        phases.loc[stable_mask, "stable_mask"] = True
        rows.append({
            "cycle": cycle, "onset_s": t[onset], "ramp_end_s": t[ramp],
            "stable_start_s": t[stable_start] if stable_start is not None else np.nan,
            "stable_end_s": stable_end_t, "unload_start_s": t[unload],
            "unload_end_s": t[end], "recovery_start_s": recovery_start_t,
            "recovery_end_s": recovery_end_t, "ramp_duration_s": t[ramp] - t[onset],
            "loaded_duration_s": t[end] - t[onset], "mean_force_n": force_mean_n,
            "creep_gain_mm": creep_gain,
        })
        previous_end = end
    return pd.DataFrame(rows), phases
