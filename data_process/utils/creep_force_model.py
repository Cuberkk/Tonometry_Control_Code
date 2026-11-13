"""Fit initial-displacement-subtracted creep with two exponential branches.

Amplitudes are remaining creep in mm, and time constants are in seconds.
Measured force is retained for steady-force diagnostics only. These response
parameters do not independently determine material moduli or viscosities.
"""

import numpy as np
from scipy.optimize import least_squares, lsq_linear


def _relative_time(t):
    """Validate a segment time axis and retain its original first-sample origin."""
    t = np.asarray(t, dtype=float)
    if (t.ndim != 1 or t.size < 2 or not np.isfinite(t).all()
            or np.any(np.diff(t) <= 0)):
        raise ValueError("Time must be a finite, strictly increasing one-dimensional array.")
    return t - t[0]


def simulate_gkv_tau(t, A1, tau1, A2, tau2):
    """Predict A1*(1-exp(-u/tau1)) + A2*(1-exp(-u/tau2)), u=t-t[0]."""
    local_time = _relative_time(t)
    parameters = np.asarray([A1, tau1, A2, tau2], dtype=float)
    if (parameters.shape != (4,) or not np.isfinite(parameters).all()
            or A1 < 0 or A2 < 0 or tau1 <= 0 or tau2 <= 0):
        raise ValueError("Use finite parameters, nonnegative amplitudes and positive time constants.")
    return (-A1 * np.expm1(-local_time / tau1)
            - A2 * np.expm1(-local_time / tau2))


def fit_gkv_tau(t, force, displacement, fit_mask=None,
                tau_bounds=(0.5, 120.0), max_points=4000, max_starts=5):
    """Subtract displacement[0], then fit A1, tau1, A2, tau2 without an offset.

    Fit delta_h(u) = sum(A_i * (1 - exp(-u/tau_i))), with u=t-t[0],
    nonnegative amplitudes and tau1 <= tau2. Time is in seconds; displacement
    and amplitudes are in mm. Force is in N and used only for diagnostics.
    This constant-force approximation does not model measured force drift.

    ``fit_mask`` selects residual samples without changing either the time
    origin or the initial displacement reference. All arrays must be finite,
    including masked samples. ``max_points`` limits optimization residuals
    only; RMSE and R-squared use every selected sample. Returned displacement
    and prediction both use the initial-displacement-subtracted reference.

    Diagnostics are heuristic checks, not uncertainty estimates. Warnings flag
    taus within 1% of either log-bound, a tau ratio below 1.5, a branch supplying
    less than 1% of fitted creep over the selected interval, taus longer than
    that interval, or R-squared below 0.9. Force CV and absolute end-to-start
    drift above 2% are flagged. Drift uses means in the first/last
    min(1 s, full duration/4) windows of the supplied segment. Initial residual
    is measured minus predicted at the first selected sample; it is flagged
    above both five times RMSE and 5% of the selected displacement range.
    """
    local_time = _relative_time(t)
    force = np.asarray(force, dtype=float)
    displacement = np.asarray(displacement, dtype=float)
    for name, values in (("Force", force), ("Displacement", displacement)):
        if values.shape != local_time.shape or not np.isfinite(values).all():
            raise ValueError(f"{name} must be a finite one-dimensional array matching time.")
    if np.any(force <= 0):
        raise ValueError("A steady hold requires positive compression force at every sample.")
    initial_displacement = float(displacement[0])
    displacement = displacement - initial_displacement
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
    scale = max(float(np.percentile(np.abs(displacement[all_indices]), 95)), 1e-9)
    y = displacement[indices] / scale
    log_low, log_high = np.log(tau_bounds)
    lower = np.array([0., 0., log_low, log_low])
    upper = np.array([np.inf, np.inf, log_high, log_high])

    def basis(log_tau, times):
        return -np.expm1(-times[:, None] / np.exp(log_tau))

    def residual(parameters):
        return basis(parameters[2:], local_time[indices]) @ parameters[:2] - y

    fractions = np.array([[0.25, 0.70], [0.10, 0.50], [0.45, 0.90], [0.10, 0.90],
                          [0.40, 0.65], [0.05, 0.30], [0.65, 0.95], [0.30, 0.80]])[:max_starts]
    results = []
    for fraction in fractions:
        log_tau = log_low + (log_high - log_low) * fraction
        initial = lsq_linear(basis(log_tau, local_time[indices]), y,
                             bounds=(lower[:2], upper[:2])).x
        results.append(least_squares(
            residual, np.r_[initial, log_tau], bounds=(lower, upper),
            loss="soft_l1", f_scale=0.03, x_scale="jac", max_nfev=600))
    converged = [result for result in results
                 if result.success and np.isfinite(result.cost) and np.isfinite(result.x).all()]
    if not converged:
        raise RuntimeError("No GKV tau fit converged; inspect the data and time-constant bounds.")
    best = min(converged, key=lambda result: result.cost)
    order = np.argsort(best.x[2:])
    tau = np.exp(best.x[2:])[order]
    amplitude = scale * best.x[:2][order]
    parameters = dict(A1=float(amplitude[0]), tau1=float(tau[0]),
                      A2=float(amplitude[1]), tau2=float(tau[1]))
    prediction = simulate_gkv_tau(local_time, **parameters)
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
        warnings.append(f"Force drift:{abs(force_drift):.2f}% exceeds 2%; the constant-force approximation may be inadequate.")
    if abs(error[0]) > max(5 * rmse, 0.05 * observed_range):
        warnings.append("The initial residual is large; inspect early transients and the initial displacement reference.")
    if all_indices[0] != 0:
        warnings.append("The fit mask excludes the segment start; displacement[0] still sets the reference.")
    if r2 < 0.9:
        warnings.append("R-squared is below 0.9; inspect residuals before interpreting the fitted curve.")
    if len(converged) < max_starts:
        warnings.append(f"Only {len(converged)} of {max_starts} optimization starts converged.")
    return {"parameters": parameters, "prediction": prediction,
            "displacement": displacement, "initial_displacement_mm": initial_displacement,
            "tau1": parameters["tau1"], "tau2": parameters["tau2"],
            "rmse": rmse, "r2": r2, "diagnostics": diagnostics, "warnings": warnings}
