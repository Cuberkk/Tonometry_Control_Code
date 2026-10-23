"""Original five-parameter GKV model driven by measured force history.

E0 is a spring in series with two Kelvin-Voigt elements (E1, eta1) and
(E2, eta2). The original flat-punch scaling is retained:
h = (F/E0 + x1/E1 + x2/E2)/(2*a), tauj*xj' + xj = F.
Force is N, depth/radius are mm, apparent E is MPa, and eta is MPa*s.
There is no fitted offset, initial memory, Poisson or thickness correction.
The first sample must represent the baseline-subtracted, relaxed contact.
"""

from __future__ import annotations

import numpy as np
from scipy.optimize import least_squares, lsq_linear
from scipy.signal import lfilter


def _history(t, force):
    t, force = np.asarray(t, dtype=float), np.asarray(force, dtype=float)
    if t.ndim != 1 or force.shape != t.shape or t.size < 2:
        raise ValueError("Time and force must be matching 1-D arrays with at least two samples.")
    if not np.all(np.isfinite(t)) or not np.all(np.isfinite(force)):
        raise ValueError("Time and force must be finite; forcing gaps cannot be silently dropped.")
    dt = np.diff(t)
    if np.any(dt <= 0):
        raise ValueError("Time must be strictly increasing.")
    return t - t[0], force, dt, bool(np.allclose(dt, dt[0], rtol=1e-6, atol=1e-10))


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


def _radius(radius_mm):
    if not np.isfinite(radius_mm) or radius_mm <= 0:
        raise ValueError("radius_mm must be finite and positive.")
    return float(radius_mm)


def simulate_gkv_history(t, force, E0, E1, eta1, E2, eta2, radius_mm=6.225):
    """Predict depth in mm from the entire force history without cycle resets.

    The baseline-subtracted force increment is allowed to be signed. The two
    internal states start at zero. Parameters retain the original notebook's
    apparent flat-punch modulus convention; they are not geometry-corrected.
    """
    _, force, dt, uniform = _history(t, force)
    radius_mm = _radius(radius_mm)
    parameters = np.asarray([E0, E1, eta1, E2, eta2], dtype=float)
    if not np.all(np.isfinite(parameters)) or np.any(parameters <= 0):
        raise ValueError("E0, E1, eta1, E2 and eta2 must all be finite and positive.")
    tau = np.array([eta1 / E1, eta2 / E2])
    return (force / E0 + _states(force, dt, tau, uniform) @ np.array([1 / E1, 1 / E2])) / (2 * radius_mm)


def fit_gkv_history(t, force, displacement, fit_mask, radius_mm=6.225,
                    tau_bounds=(0.1, 120.0), max_points=4000, max_starts=5):
    """Fit exactly E0, E1, eta1, E2, eta2 using robust masked residuals.

    ``force`` and ``displacement`` must already have their contact baseline
    removed. Every force sample drives the model, including samples where the
    displacement fit mask is false. ``max_points`` subsamples residuals only.
    Returned parameters are ordered by tau1 <= tau2. The Jacobian condition
    number and bound flags diagnose identifiability, not confidence intervals.
    """
    elapsed, force, dt, uniform = _history(t, force)
    radius_mm = _radius(radius_mm)
    displacement = np.asarray(displacement, dtype=float)
    mask = np.asarray(fit_mask, dtype=bool)
    if displacement.shape != force.shape or mask.shape != force.shape:
        raise ValueError("Displacement and fit_mask must match force.")
    if len(tau_bounds) != 2 or not 0 < tau_bounds[0] < tau_bounds[1] < np.inf:
        raise ValueError("tau_bounds must be finite, positive and increasing.")
    if max_points is not None and (int(max_points) != max_points or max_points < 10):
        raise ValueError("max_points must be at least 10 or None.")
    if int(max_starts) != max_starts or not 1 <= max_starts <= 8:
        raise ValueError("max_starts must be between 1 and 8.")
    mask = mask & np.isfinite(displacement)
    all_indices = np.flatnonzero(mask)
    if all_indices.size < 10:
        raise ValueError("At least ten finite selected displacement observations are required.")
    if np.max(np.abs(force)) < 1e-12:
        raise ValueError("A nonzero force history is required to identify compliance.")
    indices = all_indices
    if max_points is not None and indices.size > max_points:
        indices = indices[np.linspace(0, indices.size - 1, int(max_points)).astype(int)]
    f_scale = max(float(np.percentile(np.abs(force), 95)), float(np.max(np.abs(force))) * 0.01)
    y_scale = max(float(np.percentile(np.abs(displacement[indices]), 95)), 1e-9)
    normalized_force = force / f_scale
    y = displacement[indices] / y_scale
    log_low, log_high = np.log(tau_bounds)
    # Fit three positive structural compliances and two log time constants.
    # This is a change of variables, not a change to the five-parameter model.
    lower = np.r_[np.full(3, 1e-12), np.full(2, log_low)]
    upper = np.r_[np.full(3, np.inf), np.full(2, log_high)]

    def basis(log_tau, selected):
        states = _states(normalized_force, dt, np.exp(log_tau), uniform)
        return np.column_stack((normalized_force[selected], states[selected]))

    def residual(parameters):
        return basis(parameters[3:], indices) @ parameters[:3] - y

    fractions = np.array([[0.25, 0.70], [0.10, 0.50], [0.45, 0.90], [0.10, 0.90],
                          [0.40, 0.65], [0.05, 0.30], [0.65, 0.95], [0.30, 0.80]])[:int(max_starts)]
    results = []
    for fraction in fractions:
        log_tau = log_low + (log_high - log_low) * fraction
        initial_c = lsq_linear(basis(log_tau, indices), y, bounds=(lower[:3], upper[:3])).x
        results.append(least_squares(
            residual, np.r_[initial_c, log_tau], bounds=(lower, upper),
            loss="soft_l1", f_scale=0.03, x_scale="jac", max_nfev=350))
    converged = [result for result in results if result.success]
    best = min(converged or results, key=lambda result: result.cost)
    order = np.argsort(best.x[3:])
    tau = np.exp(best.x[3:])[order]
    compliance = np.r_[best.x[0], best.x[1:3][order]] * y_scale / f_scale
    moduli = 1 / (2 * radius_mm * compliance)
    params = dict(E0=float(moduli[0]), E1=float(moduli[1]), eta1=float(tau[0] * moduli[1]),
                  E2=float(moduli[2]), eta2=float(tau[1] * moduli[2]))
    prediction = simulate_gkv_history(elapsed, force, **params, radius_mm=radius_mm)
    error = prediction[all_indices] - displacement[all_indices]
    ss_total = float(np.sum((displacement[all_indices] - np.mean(displacement[all_indices]))**2))
    column_norms = np.linalg.norm(best.jac, axis=0)
    if np.any(column_norms < 1e-12):
        jac_condition = np.inf
    else:
        singular_values = np.linalg.svd(best.jac / column_norms, compute_uv=False)
        jac_condition = float(singular_values[0] / max(singular_values[-1], np.finfo(float).tiny))
    bound_fraction = np.minimum(np.log(tau) - log_low, log_high - np.log(tau)) / (log_high - log_low)
    flags = []
    if not best.success:
        flags.append("optimizer_did_not_converge")
    if jac_condition > 1e6:
        flags.append("weak_parameter_identifiability")
    if np.any(bound_fraction < 0.02):
        flags.append("time_constant_near_bound")
    if np.any(compliance < 1e-5 * float(np.sum(compliance))):
        flags.append("compliance_near_zero")
    if tau[1] / tau[0] < 2:
        flags.append("poorly_separated_time_constants")
    if tau[-1] > elapsed[all_indices[-1]] - elapsed[all_indices[0]]:
        flags.append("time_constant_longer_than_fit_window")
    if tau[0] < 2 * np.median(dt):
        flags.append("time_constant_shorter_than_two_samples")
    return {
        **params, "parameters": params, "tau": tau, "tau1": float(tau[0]), "tau2": float(tau[1]),
        "compliance_mm_per_N": compliance, "prediction": prediction,
        "rmse": float(np.sqrt(np.mean(error**2))),
        "r2": float(1 - np.sum(error**2) / ss_total) if ss_total > 0 else np.nan,
        "optimizer_success": bool(best.success), "optimizer_message": str(best.message),
        "jac_condition": jac_condition, "tau_bound_fraction": bound_fraction,
        "flags": flags, "fit_mask": mask, "n_observations": int(all_indices.size),
        "n_optimization_points": int(indices.size), "cost": float(best.cost),
        "nfev": int(best.nfev), "tau_bounds": tuple(float(v) for v in tau_bounds),
        "radius_mm": radius_mm,
    }
