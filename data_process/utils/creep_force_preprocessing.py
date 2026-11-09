"""Native LVDT despiking, linear timestamp alignment and creep cycle export."""

from pathlib import Path
import re

import numpy as np
import pandas as pd


OUTPUT_COLUMNS = ["Time, s", "Indentation Position, mm", "Indentation Force, N"]


def _linear_interpolate(time_s, values, grid_s, max_gap_s):
    """Interpolate native samples without extrapolation or bridging long gaps."""
    t, y, grid = map(lambda a: np.asarray(a, dtype=float), (time_s, values, grid_s))
    finite = np.isfinite(y)
    if finite.sum() < 2:
        raise ValueError("At least two finite native samples are required for interpolation.")
    t, y = t[finite], y[finite]
    result = np.interp(grid, t, y, left=np.nan, right=np.nan)
    right = np.searchsorted(t, grid, side="left").clip(0, len(t) - 1)
    left = (right - 1).clip(0)
    exact = grid == t[right]
    result[(t[right] - t[left] > max_gap_s + 1e-12) & ~exact] = np.nan
    return result


def _interpolation_touches(time_s, flags, grid_s):
    """Track native repairs contributing to each interpolated grid sample."""
    t, flags = np.asarray(time_s), np.asarray(flags, dtype=bool)
    grid = np.asarray(grid_s)
    right = np.searchsorted(t, grid, side="left").clip(0, len(t) - 1)
    left = np.where(grid == t[right], right, (right - 1).clip(0))
    return flags[left] | flags[right]


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


def load_trial(trial_dir, dt=.005, baseline_s=.8, max_gap_s=.2,
               force_axis="Fz", compression_sign=-1.0, displacement_sign=1.0,
               displacement_despike=None, force_despike=None):
    """Despike native LVDT data and linearly align both signals on one grid.

    A single initial baseline is removed for the whole raw trial. Integer epoch
    timestamps share an origin before conversion to seconds. Only overlapping
    acquisition time is retained; gaps larger than max_gap_s remain NaN.
    No bin averaging, median resampling or extrapolation is performed.
    """
    if not np.isfinite(dt) or dt <= 0 or not np.isfinite(baseline_s) or baseline_s <= 0:
        raise ValueError("dt and baseline_s must be finite and positive.")
    if not np.isfinite(max_gap_s) or max_gap_s <= 0:
        raise ValueError("max_gap_s must be finite and positive.")
    directory = Path(trial_dir)
    force = pd.read_csv(directory / "Force.csv", dtype={"timestamp_ns": "int64"})
    lvdt = pd.read_csv(directory / "LVDT.csv", dtype={"timestamp_ns": "int64"})
    for frame, required in ((force, ["timestamp_ns", force_axis]),
                            (lvdt, ["timestamp_ns", "Position (mm)", "Voltage (V)"])):
        if len(frame) < 2 or not set(required).issubset(frame.columns):
            raise ValueError(f"{directory}: Expected at least two rows with columns {required}.")
        if frame.timestamp_ns.isna().any():
            raise ValueError(f"{directory}: Native timestamps must not be missing.")
    origin = min(int(force.timestamp_ns.min()), int(lvdt.timestamp_ns.min()))
    for frame in (force, lvdt):
        # Subtract int64 epochs BEFORE float conversion, retaining ns resolution.
        frame["time_s"] = (frame.timestamp_ns - np.int64(origin)).to_numpy() / 1e9
        frame.sort_values("time_s", kind="stable", inplace=True)
        frame.drop_duplicates("timestamp_ns", keep="first", inplace=True)
        frame.reset_index(drop=True, inplace=True)
        if len(frame) < 2:
            raise ValueError(f"{directory}: At least two distinct native timestamps are required.")
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
    if not np.isfinite([force_base, displacement_base]).all():
        raise ValueError(f"{directory}: The initial baseline contains no valid sensor samples.")
    force["force_before_despike_n"] = compression_sign * (force.force_raw_n - force_base)
    lvdt["displacement_before_despike_mm"] = displacement_sign * (lvdt.displacement_raw_mm - displacement_base)
    lvdt.loc[~lvdt.sensor_valid, "displacement_before_despike_mm"] = np.nan
    force["force_n"] = compression_sign * (clean_force - force_base)
    lvdt["displacement_mm"] = displacement_sign * (clean_displacement - displacement_base)
    start = max(force.time_s.min(), lvdt.time_s.min())
    end = min(force.time_s.max(), lvdt.time_s.max())
    first_bin, last_bin = int(np.ceil(start / dt)), int(np.floor(end / dt))
    if last_bin - first_bin < 1:
        raise ValueError(f"{directory}: Sensor acquisition intervals do not overlap on this grid.")
    grid = np.arange(first_bin, last_bin + 1, dtype=np.int64) * dt
    synced = pd.DataFrame({"time_s": grid})
    for native, columns in ((force, ["force_n", "force_before_despike_n"]),
                            (lvdt, ["displacement_mm", "displacement_before_despike_mm"])):
        for column in columns:
            synced[column] = _linear_interpolate(native.time_s, native[column], grid, max_gap_s)
    for native, prefix, column, flag, invalid in (
            (force, "force", "force_n", "force_spike", ~np.isfinite(force.force_raw_n)),
            (lvdt, "displacement", "displacement_mm", "displacement_spike", ~lvdt.sensor_valid)):
        synced[f"{prefix}_spike"] = _interpolation_touches(native.time_s, native[flag], grid)
        synced[f"{prefix}_gap_repaired"] = (
            _interpolation_touches(native.time_s, invalid, grid) & synced[column].notna())
    synced["repaired_displacement"] = synced.displacement_gap_repaired | synced.displacement_spike
    synced["repaired_force"] = synced.force_gap_repaired | synced.force_spike
    synced["valid"] = np.isfinite(synced.force_n) & np.isfinite(synced.displacement_mm)
    synced["fit_valid"] = synced.valid
    if displacement_despike is not None or force_despike is not None:
        synced["fit_valid"] &= ~synced.repaired_force & ~synced.repaired_displacement
    synced.reset_index(drop=True, inplace=True)
    return {"force": force, "lvdt": lvdt, "sync": synced, "origin_ns": origin,
            "baseline": {"force_raw_n": float(force_base),
                         "displacement_raw_mm": float(displacement_base)}}


def detect_indentation_cycles(trial_dir, trial, *, method="auto", force_settings=None,
                              force_release_guard_s=.3, force_pre_indent_s=.2):
    """Locate complete indentation windows, excluding release and recovery.

    Prefer recorded controller commands: moving to bottom starts indentation,
    moving to top starts release. Force-only fallback estimates the interval,
    retaining a short pre-trigger and conservatively trimming its release edge.
    A malformed or incomplete existing sequence log raises rather than silently
    substituting less precise force boundaries.
    """
    if method not in {"auto", "sequence", "force"}:
        raise ValueError("Cycle detection method must be 'auto', 'sequence' or 'force'.")
    if (not np.isfinite([force_release_guard_s, force_pre_indent_s]).all()
            or force_release_guard_s < 0 or force_pre_indent_s < 0):
        raise ValueError("Force boundary guards must be finite and nonnegative.")
    sync = trial["sync"]
    paths = sorted(Path(trial_dir).glob("sequence_*.csv"))
    use_sequence = method == "sequence" or (method == "auto" and bool(paths))
    if use_sequence:
        if len(paths) != 1:
            raise ValueError(f"{trial_dir}: Expected exactly one sequence_*.csv, found {len(paths)}.")
        events = pd.read_csv(paths[0], usecols=["timestamp_ns", "sequence_status"],
                             dtype={"timestamp_ns": "int64"})
        events.sort_values("timestamp_ns", kind="stable", inplace=True)
        labels = events.sequence_status.str.extract(
            r"^Cycle\s+(\d+)/(\d+):\s*(moving to bottom|moving to top)\s*$", flags=re.I)
        selected = labels[0].notna()
        events = events.loc[selected].copy()
        events["cycle"] = labels.loc[selected, 0].astype(int)
        events["event"] = labels.loc[selected, 2].str.lower()
        events["time_s"] = (events.timestamp_ns - np.int64(trial["origin_ns"])).to_numpy() / 1e9
        totals = labels.loc[selected, 1].astype(int).unique()
        if (len(totals) != 1 or totals[0] < 1
                or sorted(events.cycle.unique()) != list(range(1, totals[0] + 1))):
            raise ValueError(f"{trial_dir}: Sequence cycle IDs do not match the declared cycle count.")
        rows = []
        for cycle, group in events.groupby("cycle", sort=True):
            transitions = group.loc[group.event != group.event.shift(), "event"].tolist()
            if transitions != ["moving to bottom", "moving to top"]:
                raise ValueError(f"{trial_dir}: Cycle {cycle} has missing or repeated command transitions.")
            indent = group.loc[group.event == "moving to bottom", "time_s"]
            release = group.loc[group.event == "moving to top", "time_s"]
            if indent.empty or release.empty or release.iloc[0] <= indent.iloc[0]:
                raise ValueError(f"{trial_dir}: Cycle {cycle} has incomplete or out-of-order commands.")
            rows.append({"cycle": int(cycle), "onset_s": float(indent.iloc[0]),
                         "release_s": float(release.iloc[0]), "boundary_source": "sequence"})
        cycles = pd.DataFrame(rows)
    else:
        detected, _ = segment_cycles(sync, **(force_settings or {}))
        rows = []
        for event in detected.itertuples():
            if event.unload_end_s >= sync.time_s.iloc[-1]:
                raise ValueError(f"{trial_dir}: Cycle {event.cycle} has no complete measured release.")
            rows.append({"cycle": int(event.cycle),
                         "onset_s": max(float(sync.time_s.iloc[0]), event.onset_s - force_pre_indent_s),
                         "release_s": event.unload_start_s - force_release_guard_s,
                         "boundary_source": "force_estimate"})
        cycles = pd.DataFrame(rows)
    if cycles.empty:
        raise ValueError(f"{trial_dir}: No complete indentation cycles were detected.")
    cycles.sort_values("onset_s", inplace=True, ignore_index=True)
    if (cycles.release_s <= cycles.onset_s).any():
        raise ValueError(f"{trial_dir}: A cycle ends before it starts; review boundary settings.")
    if np.any(cycles.onset_s.to_numpy()[1:] < cycles.release_s.to_numpy()[:-1]):
        raise ValueError(f"{trial_dir}: Detected indentation windows overlap.")
    dt = float(np.median(np.diff(sync.time_s)))
    if ((cycles.onset_s < sync.time_s.iloc[0] - dt).any()
            or (cycles.release_s > sync.time_s.iloc[-1] + dt).any()):
        raise ValueError(f"{trial_dir}: Sensor data does not cover a complete indentation window.")
    return cycles


def extract_indentation_cycles(sync, cycles, sampling_hz=200.):
    """Return exactly three columns on a continuous cycle-relative time grid.

    Preserve the trial-wide force/displacement baseline, including residual
    indentation in later cycles. Reject gaps instead of dropping samples or
    interpolating across unsupported intervals. The release boundary is strict.
    """
    if not np.isfinite(sampling_hz) or sampling_hz <= 0:
        raise ValueError("sampling_hz must be finite and positive.")
    dt = 1. / sampling_hz
    if len(sync) < 2 or not np.allclose(np.diff(sync.time_s), dt, rtol=1e-7, atol=1e-10):
        raise ValueError(f"Synchronized timestamps must be spaced at {dt:g} s.")
    extracted = []
    for event in cycles.itertuples():
        segment = sync.loc[(sync.time_s >= event.onset_s) & (sync.time_s < event.release_s)]
        values = segment[["displacement_mm", "force_n"]].to_numpy(float)
        if len(segment) < 2 or not np.isfinite(values).all():
            raise ValueError(f"Cycle {event.cycle}: Missing sensor values or fewer than two samples. "
                             "Review native sensor gaps before exporting.")
        frame = pd.DataFrame({OUTPUT_COLUMNS[0]: np.arange(len(segment)) / sampling_hz,
                              OUTPUT_COLUMNS[1]: values[:, 0], OUTPUT_COLUMNS[2]: values[:, 1]})
        extracted.append((int(event.cycle), frame))
    return extracted


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
