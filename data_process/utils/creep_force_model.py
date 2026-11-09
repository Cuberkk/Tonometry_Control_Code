"""Five-parameter GKV model driven by measured force history.

E0 is a spring in series with two Kelvin-Voigt elements (E1, eta1) and
(E2, eta2). The original flat-punch scaling is retained:
h = (F/E0 + x1/E1 + x2/E2)/(2*a), tauj*xj' + xj = F.
Force is N, depth/radius are mm, apparent E is MPa, and eta is MPa*s.
Inputs are baseline-subtracted, with relaxed contact at the first sample.
"""

import numpy as np
from scipy.optimize import least_squares, lsq_linear
from scipy.signal import lfilter


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
    """Predict depth in mm using zero initial states and continuous cycle history."""
    force, dt, uniform = _history(t, force)
    tau = np.array([eta1 / E1, eta2 / E2])
    return (force / E0 + _states(force, dt, tau, uniform) @ np.array([1 / E1, 1 / E2])) / (2 * radius_mm)


def fit_gkv_history(t, force, displacement, fit_mask, radius_mm=6.225,
                    tau_bounds=(0.1, 120.0), max_points=4000, max_starts=5):
    """Fit E0, E1, eta1, E2, eta2 using robust masked residuals.

    ``force`` and ``displacement`` must already have their contact baseline
    removed. Every force sample drives the model, including samples where the
    displacement fit mask is false. ``max_points`` subsamples residuals only.
    Returned parameters are ordered by tau1 <= tau2.
    """
    force, dt, uniform = _history(t, force)
    displacement = np.asarray(displacement, dtype=float)
    all_indices = np.flatnonzero(np.asarray(fit_mask, dtype=bool) & np.isfinite(displacement))
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
    best = min((result for result in results if result.success), key=lambda result: result.cost)
    order = np.argsort(best.x[3:])
    tau = np.exp(best.x[3:])[order]
    compliance = np.r_[best.x[0], best.x[1:3][order]] * y_scale / f_scale
    moduli = 1 / (2 * radius_mm * compliance)
    params = dict(E0=float(moduli[0]), E1=float(moduli[1]), eta1=float(tau[0] * moduli[1]),
                  E2=float(moduli[2]), eta2=float(tau[1] * moduli[2]))
    prediction = simulate_gkv_history(t, force, **params, radius_mm=radius_mm)
    error = prediction[all_indices] - displacement[all_indices]
    ss_total = float(np.sum((displacement[all_indices] - np.mean(displacement[all_indices]))**2))
    return {
        "parameters": params, "prediction": prediction,
        "tau1": float(tau[0]), "tau2": float(tau[1]),
        "rmse": float(np.sqrt(np.mean(error**2))),
        "r2": float(1 - np.sum(error**2) / ss_total) if ss_total > 0 else np.nan,
    }
