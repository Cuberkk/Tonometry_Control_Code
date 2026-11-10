"""GKV creep analysis for steady-force segments and complete force histories.

Use ``fit_gkv_hold`` for an already segmented steady-force hold. It estimates
an initial position, two remaining creep amplitudes and two time constants.
Those amplitudes include unknown internal states at the segment start and
cannot independently determine material moduli or viscosities.

The separate ``fit_gkv_history`` API requires a complete force history starting
from relaxed contact; it is not suitable for a truncated steady-force segment.
E0 is a spring in series with two Kelvin-Voigt elements (E1, eta1) and
(E2, eta2). The original flat-punch scaling is retained:
h = (F/E0 + x1/E1 + x2/E2)/(2*a), tauj*xj' + xj = F.
Force is N, depth/radius are mm, apparent E is MPa, and eta is MPa*s.
Inputs are baseline-subtracted, with relaxed contact at the first sample.
"""

import numpy as np
from scipy.optimize import least_squares, lsq_linear
from scipy.signal import lfilter


def _hold_time(t):
    """Validate a segment time axis and retain its original first-sample origin."""
    t = np.asarray(t, dtype=float)
    if (t.ndim != 1 or t.size < 2 or not np.isfinite(t).all()
            or np.any(np.diff(t) <= 0)):
        raise ValueError("Time must be a finite, strictly increasing one-dimensional array.")
    return t - t[0]


def simulate_gkv_hold(t, h_start, A1, tau1, A2, tau2):
    """Predict a conditional two-branch creep curve during a constant-force hold.

    Time is in seconds and position/amplitudes are in mm. ``h_start`` is the
    position at the first supplied timestamp. For a constant force F and
    Kelvin state q_i at segment start s, A_i = (F - q_i(s)) / (2*a*E_i).
    Thus A_i is a remaining creep amplitude, not a material compliance.
    Nonnegative amplitudes describe monotonically increasing, concave creep;
    recovery and changing-force transients require a different model.
    """
    local_time = _hold_time(t)
    parameters = np.asarray([h_start, A1, tau1, A2, tau2], dtype=float)
    if (parameters.shape != (5,) or not np.isfinite(parameters).all()
            or A1 < 0 or A2 < 0 or tau1 <= 0 or tau2 <= 0):
        raise ValueError("Use finite parameters, nonnegative amplitudes and positive time constants.")
    return (h_start - A1 * np.expm1(-local_time / tau1)
            - A2 * np.expm1(-local_time / tau2))


def fit_gkv_hold(t, force, displacement, fit_mask=None,
                 tau_bounds=(0.5, 120.0), max_points=4000, max_starts=5):
    """Fit h_start, A1, tau1, A2, tau2 to an exported steady-force segment.

    Fit h(t) = h_start + sum(A_i * (1 - exp(-(t-t[0])/tau_i))) with a free
    initial position, nonnegative amplitudes, and tau1 <= tau2. The supplied
    force is retained for diagnostics only; do not subtract the hold force.
    This constant-force approximation does not model measured force drift.
    Neither E0 nor independent E_i/eta_i values are inferred from these data.

    ``fit_mask`` selects residual samples without changing the time origin.
    All arrays must be finite, including masked samples. ``max_points`` limits
    optimization residuals only; RMSE and R-squared use every selected sample.
    Numerical centering/scaling of displacement is undone in the returned
    free h_start and does not impose an unloaded or zero-state reference.

    Diagnostics are heuristic checks, not uncertainty estimates. Warnings flag
    taus within 1% of either log-bound, a tau ratio below 1.5, a branch supplying
    less than 1% of fitted creep over the selected interval, taus longer than
    that interval, or R-squared below 0.9. Force CV and absolute end-to-start
    drift above 2% are flagged. Drift uses means in the first/last
    min(1 s, full duration/4) windows of the supplied segment. Initial residual
    is measured minus predicted at the first selected sample; it is flagged
    above both five times RMSE and 5% of the selected displacement range.
    """
    local_time = _hold_time(t)
    force = np.asarray(force, dtype=float)
    displacement = np.asarray(displacement, dtype=float)
    for name, values in (("Force", force), ("Displacement", displacement)):
        if values.shape != local_time.shape or not np.isfinite(values).all():
            raise ValueError(f"{name} must be a finite one-dimensional array matching time.")
    if np.any(force <= 0):
        raise ValueError("A steady hold requires positive compression force at every sample.")
    if fit_mask is None:
        fit_mask = np.ones(local_time.size, dtype=bool)
    else:
        fit_mask = np.asarray(fit_mask)
        if fit_mask.dtype != np.dtype(bool) or fit_mask.shape != local_time.shape:
            raise ValueError("fit_mask must be a Boolean array matching time.")
    all_indices = np.flatnonzero(fit_mask)
    if all_indices.size < 10:
        raise ValueError("At least ten selected displacement samples are required.")
    observed_range = float(np.ptp(displacement[all_indices]))
    if observed_range <= 1e-9:
        raise ValueError("The selected displacement must vary by more than 1e-9 mm.")
    tau_bounds = np.asarray(tau_bounds, dtype=float)
    if (tau_bounds.shape != (2,) or not np.isfinite(tau_bounds).all()
            or not 0 < tau_bounds[0] < tau_bounds[1]):
        raise ValueError("Use two finite, positive, increasing time-constant bounds.")
    if (isinstance(max_starts, (bool, np.bool_))
            or not isinstance(max_starts, (int, np.integer)) or not 1 <= max_starts <= 8):
        raise ValueError("max_starts must be an integer from one through eight.")
    if max_points is not None and (
            isinstance(max_points, (bool, np.bool_))
            or not isinstance(max_points, (int, np.integer)) or max_points < 10):
        raise ValueError("max_points must be None or an integer of at least ten.")

    indices = all_indices
    if max_points is not None and indices.size > max_points:
        indices = indices[np.linspace(0, indices.size - 1, max_points).astype(int)]
    selected_time = local_time[all_indices]
    first_window = selected_time < selected_time[0] + 0.05
    center = float(np.median(displacement[all_indices[first_window]]))
    scale = max(float(np.percentile(np.abs(displacement[all_indices] - center), 95)), 1e-9)
    y = (displacement[indices] - center) / scale
    log_low, log_high = np.log(tau_bounds)
    lower = np.array([-np.inf, 0., 0., log_low, log_low])
    upper = np.array([np.inf, np.inf, np.inf, log_high, log_high])

    def basis(log_tau, times):
        return np.column_stack((np.ones(times.size),
                                -np.expm1(-times[:, None] / np.exp(log_tau))))

    def residual(parameters):
        return basis(parameters[3:], local_time[indices]) @ parameters[:3] - y

    fractions = np.array([[0.25, 0.70], [0.10, 0.50], [0.45, 0.90], [0.10, 0.90],
                          [0.40, 0.65], [0.05, 0.30], [0.65, 0.95], [0.30, 0.80]])[:max_starts]
    results = []
    for fraction in fractions:
        log_tau = log_low + (log_high - log_low) * fraction
        initial = lsq_linear(basis(log_tau, local_time[indices]), y,
                             bounds=(lower[:3], upper[:3])).x
        results.append(least_squares(
            residual, np.r_[initial, log_tau], bounds=(lower, upper),
            loss="soft_l1", f_scale=0.03, x_scale="jac", max_nfev=600))
    converged = [result for result in results
                 if result.success and np.isfinite(result.cost) and np.isfinite(result.x).all()]
    if not converged:
        raise RuntimeError("No steady-hold fit converged; inspect the data and time-constant bounds.")
    best = min(converged, key=lambda result: result.cost)
    order = np.argsort(best.x[3:])
    tau = np.exp(best.x[3:])[order]
    amplitude = scale * best.x[1:3][order]
    parameters = dict(h_start=float(center + scale * best.x[0]),
                      A1=float(amplitude[0]), tau1=float(tau[0]),
                      A2=float(amplitude[1]), tau2=float(tau[1]))
    prediction = simulate_gkv_hold(local_time, **parameters)
    error = displacement[all_indices] - prediction[all_indices]
    rmse = float(np.sqrt(np.mean(error**2)))
    ss_total = float(np.sum((displacement[all_indices] - np.mean(displacement[all_indices]))**2))
    r2 = float(1 - np.sum(error**2) / ss_total)
    duration = float(selected_time[-1] - selected_time[0])
    force_window = min(1., float(local_time[-1]) / 4)
    force_start = float(np.mean(force[local_time < force_window]))
    force_end = float(np.mean(force[local_time > local_time[-1] - force_window]))
    mean_force = float(np.mean(force))
    force_cv = float(100 * np.std(force) / mean_force)
    force_drift = float(100 * (force_end - force_start) / mean_force)
    diagnostics = dict(
        mean_force_n=mean_force, force_cv_pct=force_cv, force_drift_pct=force_drift,
        force_window_s=force_window, initial_residual_mm=float(error[0]),
        max_abs_residual_mm=float(np.max(np.abs(error))),
        n_fit_points=int(all_indices.size), n_optimization_points=int(indices.size),
        n_starts_converged=len(converged), nfev=int(best.nfev),
        optimizer_message=str(best.message))

    warnings = []
    log_distance = np.minimum(np.log(tau) - log_low, log_high - np.log(tau))
    for branch in np.flatnonzero(log_distance <= 0.01 * (log_high - log_low)):
        warnings.append(f"tau{branch + 1} is near a bound; inspect time-constant sensitivity.")
    contribution = amplitude * (np.exp(-selected_time[0] / tau)
                                - np.exp(-selected_time[-1] / tau))
    for branch in np.flatnonzero(contribution <= max(1e-9, 0.01 * np.sum(contribution))):
        warnings.append(f"Branch {branch + 1} contributes negligible creep; its tau is poorly constrained.")
    if tau[1] / tau[0] < 1.5:
        warnings.append("The two time constants are close; separate branches may not be resolved.")
    if np.any(tau > duration):
        warnings.append("A time constant exceeds the fitted duration; its amplitude and tau may trade off.")
    if force_cv > 2:
        warnings.append("Force CV exceeds 2%; the constant-force approximation may be inadequate.")
    if abs(force_drift) > 2:
        warnings.append("Force drift exceeds 2%; the constant-force approximation may be inadequate.")
    if abs(error[0]) > max(5 * rmse, 0.05 * observed_range):
        warnings.append("The initial residual is large; inspect early transients and the free initial position.")
    if all_indices[0] != 0:
        warnings.append("The fit mask excludes the segment start; h_start is extrapolated.")
    if r2 < 0.9:
        warnings.append("R-squared is below 0.9; inspect residuals before interpreting the fitted curve.")
    if len(converged) < max_starts:
        warnings.append(f"Only {len(converged)} of {max_starts} optimization starts converged.")
    return {"parameters": parameters, "prediction": prediction,
            "tau1": parameters["tau1"], "tau2": parameters["tau2"],
            "rmse": rmse, "r2": r2, "diagnostics": diagnostics, "warnings": warnings}


def _history(t, force):
    t = np.asarray(t, dtype=float)
    dt = np.diff(t)
    return np.asarray(force, dtype=float), dt, np.allclose(dt, dt[0], rtol=1e-6, atol=1e-10)


def _ramp_coefficients(z):
    """Exact, numerically stable coefficients for piecewise-linear force."""
    z = np.asarray(z)
    a = np.exp(-z)
    c = np.where(z < 1e-4, z / 2 - z**2 / 6 + z**3 / 24, 1 + np.expm1(-z) / z)
    return a, -np.expm1(-z) - c, c


def _states(force, dt, tau, uniform):
    states = np.zeros((force.size, len(tau)))
    for j, relaxation_time in enumerate(tau):
        if uniform:
            a, b, c = _ramp_coefficients(dt[0] / relaxation_time)
            # zi enforces x(0)=0, including an instantaneous initial load F(0).
            states[:, j], _ = lfilter([c, b], [1, -a], force, zi=[-c * force[0]])
        else:
            a, b, c = _ramp_coefficients(dt / relaxation_time)
            for i in range(1, force.size):
                states[i, j] = (a[i - 1] * states[i - 1, j]
                                + b[i - 1] * force[i - 1] + c[i - 1] * force[i])
    return states


def simulate_gkv_history(t, force, E0, E1, eta1, E2, eta2, radius_mm=6.225):
    """Predict depth from a complete relaxed-start force history, not a cut hold."""
    force, dt, uniform = _history(t, force)
    tau = np.array([eta1 / E1, eta2 / E2])
    return (force / E0 + _states(force, dt, tau, uniform) @ np.array([1 / E1, 1 / E2])) / (2 * radius_mm)


def fit_gkv_history(t, force, displacement, fit_mask, radius_mm=6.225,
                    tau_bounds=(0.1, 120.0), max_points=4000, max_starts=5):
    """Fit E0, E1, eta1, E2, eta2 using robust masked residuals.

    ``force`` and ``displacement`` must already have their contact baseline
    removed. Every force sample drives the model, including samples where the
    displacement fit mask is false. ``max_points`` subsamples residuals only.
    Returned parameters are ordered by tau1 <= tau2. Supply the loading history
    from relaxed contact; do not subtract a steady plateau as a force baseline.
    Use ``fit_gkv_hold`` when only a steady-force segment is available.

    Signed force samples are retained, including pre-contact baseline noise.
    Nonfinite displacement samples are excluded from the residual mask; force
    and time must remain finite throughout the complete input history.
    Diagnostic warnings flag bounds, weak or similar branches, extrapolated
    time constants, poor fit, and initial mismatch. Force variation is modeled.
    """
    local_time = _hold_time(t)
    force, dt, uniform = _history(local_time, force)
    displacement = np.asarray(displacement, dtype=float)
    if force.shape != local_time.shape or not np.isfinite(force).all():
        raise ValueError("Force must be a finite one-dimensional array matching time.")
    if not np.any(force > 0):
        raise ValueError("The force history must contain positive compression force.")
    if displacement.shape != local_time.shape:
        raise ValueError("Displacement must be a one-dimensional array matching time.")
    fit_mask = np.asarray(fit_mask)
    if fit_mask.dtype != np.dtype(bool) or fit_mask.shape != local_time.shape:
        raise ValueError("fit_mask must be a Boolean array matching time.")
    all_indices = np.flatnonzero(fit_mask & np.isfinite(displacement))
    if all_indices.size < 10:
        raise ValueError("At least ten finite selected displacement samples are required.")
    observed_range = float(np.ptp(displacement[all_indices]))
    if observed_range <= 1e-9:
        raise ValueError("The selected displacement must vary by more than 1e-9 mm.")
    if not np.isfinite(radius_mm) or radius_mm <= 0:
        raise ValueError("radius_mm must be finite and positive.")
    tau_bounds = np.asarray(tau_bounds, dtype=float)
    if (tau_bounds.shape != (2,) or not np.isfinite(tau_bounds).all()
            or not 0 < tau_bounds[0] < tau_bounds[1]):
        raise ValueError("Use two finite, positive, increasing time-constant bounds.")
    if (isinstance(max_starts, (bool, np.bool_))
            or not isinstance(max_starts, (int, np.integer)) or not 1 <= max_starts <= 8):
        raise ValueError("max_starts must be an integer from one through eight.")
    if max_points is not None and (
            isinstance(max_points, (bool, np.bool_))
            or not isinstance(max_points, (int, np.integer)) or max_points < 10):
        raise ValueError("max_points must be None or an integer of at least ten.")
    indices = all_indices
    if max_points is not None and indices.size > max_points:
        indices = indices[np.linspace(0, indices.size - 1, max_points).astype(int)]
    f_scale = max(float(np.percentile(np.abs(force), 95)), float(np.max(np.abs(force))) * 0.01)
    y_scale = max(float(np.percentile(np.abs(displacement[indices]), 95)), 1e-9)
    normalized_force = force / f_scale
    y = displacement[indices] / y_scale
    log_low, log_high = np.log(tau_bounds)
    # Three positive compliances and two log time constants represent the same model.
    lower = np.r_[np.full(3, 1e-12), np.full(2, log_low)]
    upper = np.r_[np.full(3, np.inf), np.full(2, log_high)]

    def basis(log_tau, selected):
        states = _states(normalized_force, dt, np.exp(log_tau), uniform)
        return np.column_stack((normalized_force[selected], states[selected]))

    def residual(parameters):
        return basis(parameters[3:], indices) @ parameters[:3] - y

    fractions = np.array([[0.25, 0.70], [0.10, 0.50], [0.45, 0.90], [0.10, 0.90],
                          [0.40, 0.65], [0.05, 0.30], [0.65, 0.95], [0.30, 0.80]])[:max_starts]
    results = []
    for fraction in fractions:
        log_tau = log_low + (log_high - log_low) * fraction
        initial_c = lsq_linear(basis(log_tau, indices), y, bounds=(lower[:3], upper[:3])).x
        results.append(least_squares(
            residual, np.r_[initial_c, log_tau], bounds=(lower, upper),
            loss="soft_l1", f_scale=0.03, x_scale="jac", max_nfev=350))
    converged = [result for result in results
                 if result.success and np.isfinite(result.cost) and np.isfinite(result.x).all()]
    if not converged:
        raise RuntimeError("No force-history fit converged; inspect the data and time-constant bounds.")
    best = min(converged, key=lambda result: result.cost)
    order = np.argsort(best.x[3:])
    tau = np.exp(best.x[3:])[order]
    compliance = np.r_[best.x[0], best.x[1:3][order]] * y_scale / f_scale
    moduli = 1 / (2 * radius_mm * compliance)
    params = dict(E0=float(moduli[0]), E1=float(moduli[1]), eta1=float(tau[0] * moduli[1]),
                  E2=float(moduli[2]), eta2=float(tau[1] * moduli[2]))
    prediction = simulate_gkv_history(t, force, **params, radius_mm=radius_mm)
    error = displacement[all_indices] - prediction[all_indices]
    rmse = float(np.sqrt(np.mean(error**2)))
    ss_total = float(np.sum((displacement[all_indices] - np.mean(displacement[all_indices]))**2))
    r2 = float(1 - np.sum(error**2) / ss_total)
    diagnostics = dict(
        mean_force_n=float(np.mean(force)), initial_residual_mm=float(error[0]),
        max_abs_residual_mm=float(np.max(np.abs(error))),
        n_fit_points=int(all_indices.size), n_optimization_points=int(indices.size),
        n_starts_converged=len(converged), nfev=int(best.nfev),
        optimizer_message=str(best.message))
    warnings = []
    log_distance = np.minimum(np.log(tau) - log_low, log_high - np.log(tau))
    for branch in np.flatnonzero(log_distance <= 0.01 * (log_high - log_low)):
        warnings.append(f"tau{branch + 1} is near a bound; inspect time-constant sensitivity.")
    branch_response = _states(force, dt, tau, uniform) * compliance[1:]
    contribution = np.ptp(branch_response[all_indices], axis=0)
    for branch in np.flatnonzero(contribution <= max(1e-9, 0.01 * np.sum(contribution))):
        warnings.append(f"Branch {branch + 1} contributes negligible displacement; its tau is poorly constrained.")
    if tau[1] / tau[0] < 1.5:
        warnings.append("The two time constants are close; separate branches may not be resolved.")
    if np.any(tau > local_time[-1]):
        warnings.append("A time constant exceeds the supplied history duration; inspect parameter sensitivity.")
    if abs(error[0]) > max(5 * rmse, 0.05 * observed_range):
        warnings.append("The initial residual is large; inspect the contact baseline and relaxed-start assumption.")
    if r2 < 0.9:
        warnings.append("R-squared is below 0.9; inspect residuals before interpreting material parameters.")
    if len(converged) < max_starts:
        warnings.append(f"Only {len(converged)} of {max_starts} optimization starts converged.")
    return {
        "parameters": params, "prediction": prediction,
        "tau1": float(tau[0]), "tau2": float(tau[1]),
        "rmse": rmse, "r2": r2, "diagnostics": diagnostics, "warnings": warnings,
    }
