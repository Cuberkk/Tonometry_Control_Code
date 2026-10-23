"""Control abstractions for manual axes and position PID weights control."""

from __future__ import annotations

import math
import threading
import time
from collections import deque
from typing import Optional

from . import config
from .hardware import Motoron


class AxisManual:
    """Manual hold-to-run control for the gantry."""

    def __init__(self, name: str, motor_ids: tuple[int, ...], motor_invert: tuple[bool, ...], torque_floor: Optional[int] = None):
        self.name = name
        self.motor_ids = motor_ids
        self.motor_invert = list(motor_invert)
        self.speed_percent = 32
        self.dir = 0
        self.last_cmd = 0
        self.torque_floor = config.TORQUE_FLOOR if torque_floor is None else int(torque_floor)

    def _map_speed(self, speed_percent: float) -> int:
        pct = max(0.0, min(100.0, float(speed_percent)))
        if pct <= 0.0:
            return 0
        cmd = int((pct / 100.0) * config.MOTORON_MAX)
        cmd = max(cmd, self.torque_floor)
        return min(cmd, config.MOTORON_MAX)

    def apply(self, mot: Motoron, mag_percent: int = None):
        if self.dir == 0:
            for m in self.motor_ids:
                mot.set_speed(m, 0)
            self.last_cmd = 0
            return
        if mag_percent is None:
            mag = self._map_speed(self.speed_percent)
        else:
            mag = self._map_speed(mag_percent)

        # Offsetting speed to account for gravity, weight based on gantry moving direction
        if self.dir > 0:
            mag = float(mag * config.GANTRY_UP_SPEED_MULTIPLIER)
        else:
            mag = float(mag * config.GANTRY_DOWN_SPEED_MULTIPLIER)

        mag = max(0, min(config.MOTORON_MAX, mag))

        raw = mag if self.dir > 0 else -mag
        self.last_cmd = raw
        for m, inv in zip(self.motor_ids, self.motor_invert):
            mot.set_speed(m, (-raw if inv else raw))

    def brake(self, mot: Motoron, amount: int = config.BRAKE_FLOOR):
        self.dir = 0
        for m in self.motor_ids:
            mot.set_speed(m, 0)
            mot.brake_motor(m, amount)
        self.last_cmd = 0


class PIDController:
    """PID controller with integral term + anti-windup + output clamp."""

    def __init__(
        self,
        kp: float,
        ki: float,
        kd: float,
        output_limits: tuple[float, float],
        integral_limits: tuple[float, float] | None = None,
    ):
        self.kp = float(kp)
        self.ki = float(ki)
        self.kd = float(kd)
        self.output_limits = (float(output_limits[0]), float(output_limits[1]))

        # Integral state clamping (prevents windup)
        self.integral_limits = None
        if integral_limits is not None:
            self.integral_limits = (float(integral_limits[0]), float(integral_limits[1]))

        # Optional: use derivative on measurement to reduce derivative kick

        self.reset()

    def reset(self):
        self._prev_error = None
        self._prev_measurement = None
        self._integral = 0.0

    def compute(self, error: float, dt: float,) -> float:
        # Basic dt sanity
        if dt <= 0.0:
            dt = 1e-3

        # P
        p = self.kp * error

        # I (tentatively integrate; may be corrected by anti-windup below)
        self._integral += error * dt
        if self.integral_limits is not None:
            lo_i, hi_i = self.integral_limits
            self._integral = max(lo_i, min(hi_i, self._integral))
        i = self.ki * self._integral

        # D
        d = 0.0
        if self.kd != 0.0:
            if self._prev_error is not None:
                d = self.kd * (error - self._prev_error) / dt

        self._prev_error = error

        # Unclamped output
        u = p + i + d

        # Output clamp + anti-windup (back-calculate by undoing the last integration step)
        low, high = self.output_limits
        u_sat = max(low, min(high, u))

        if self.ki != 0.0 and u_sat != u:
            # If saturated, revert this step's integral accumulation (simple, robust)
            self._integral -= error * dt
            if self.integral_limits is not None:
                lo_i, hi_i = self.integral_limits
                self._integral = max(lo_i, min(hi_i, self._integral))
            # Recompute i and saturated output (optional but cleaner)
            i = self.ki * self._integral
            u = p + i + d
            u_sat = max(low, min(high, u))

        return u_sat

class WeightsPositionController:
    """Position PID while moving, followed by fixed holding or release."""

    def __init__(self, motor: Motoron, motor_id: int, encoder, invert: bool, torque_floor: int = 250, refresh_hz: float = 200.0):
        if encoder is None:
            raise ValueError("Encoder instance required for weights controller")
        self.motor = motor
        self.motor_id = int(motor_id)
        self.encoder = encoder
        self.invert = bool(invert)
        self.torque_floor = int(max(0, torque_floor))
        self.refresh_hz = max(5.0, float(refresh_hz))
        self.speed_percent = config.PID_LIFT_SPEED_PCT
        self.tolerance_counts = config.WEIGHTS_TOLERANCE_DEFAULT
        self.top_holding_speed_pct = config.TOP_HOLDING_SPEED_PCT
        self._target = None
        self.pre_target = None
        self._arrive_flag = False
        self._released = False
        self._release_at_rest = False
        self._stationary_samples = deque()
        self._failure_reason = ""
        self._state = "idle"
        self._last_cmd = 0
        self._estopped = False
        self._shutdown = threading.Event()
        self._pause = threading.Event()
        self._lock = threading.Lock()
        self.pid = PIDController(
            config.WEIGHTS_PID_KP,
            config.WEIGHTS_PID_KI,
            config.WEIGHTS_PID_KD,
            output_limits=(-config.MOTORON_MAX, config.MOTORON_MAX),
            integral_limits=(-config.WEIGHTS_PID_I_LIM, config.WEIGHTS_PID_I_LIM),
        )
        self.gravity_output = max(0.0, config.WEIGHTS_PID_GRAVITY_OUTPUT)
        self.pid_deadband = config.WEIGHTS_PID_DEADBAND
        self._min_output = max(0.0, config.WEIGHTS_PID_MIN_OUTPUT)
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    @property
    def state(self) -> str:
        return self._state

    @property
    def last_cmd(self) -> int:
        return self._last_cmd

    @property
    def target_position(self) -> Optional[int]:
        with self._lock:
            return self._target

    @property
    def failure_reason(self) -> str:
        with self._lock:
            return self._failure_reason

    def set_speed_percent(self, pct: float):
        self.speed_percent = max(5.0, min(100.0, float(pct)))

    def set_tolerance(self, counts: int):
        self.tolerance_counts = max(1, int(counts))

    def move_to(self, target: int, *, release_at_rest: bool = False,
                cancel_events: tuple[threading.Event, ...] = ()):
        """Position-control a move; bottom moves coast on arrival or rest."""
        with self._lock:
            if self._estopped or any(event.is_set() for event in cancel_events):
                return
            self._released = False
            self._release_at_rest = bool(release_at_rest)
            self._stationary_samples.clear()
            self._failure_reason = ""
            self._arrive_flag = False
            self.pid.reset()
            self._target = int(target)
            self.pre_target = int(target)
            self._state = "queued"

    def _cancel_target_locked(self):
        self._target = None
        self.pre_target = None
        self._release_at_rest = False
        self._stationary_samples.clear()
        self._arrive_flag = False
        self.pid.reset()

    def stop(self, *, coast: bool = False):
        with self._lock:
            self._cancel_target_locked()
            self._released = bool(coast)
            if coast:
                self.motor.coast_motor(self.motor_id)
            else:
                self.motor.set_speed(self.motor_id, 0)
            self._last_cmd = 0
            self._state = "released" if coast else "idle"

    def release(self):
        """Cancel the position target and coast without a retained hold."""
        self.stop(coast=True)

    def set_estop(self, active: bool):
        with self._lock:
            self._estopped = bool(active)
            self._cancel_target_locked()
            self._released = True
            self.motor.coast_motor(self.motor_id)
            self._last_cmd = 0
            self._state = "estopped" if active else "released"

    def shutdown(self):
        self._shutdown.set()
        self._pause.set()
        self._thread.join(timeout=1.0)
        self.stop(coast=True)

    def _loop(self):
        sleep_base = 1.0 / self.refresh_hz
        last_time = time.monotonic()
        while not self._shutdown.is_set():
            self._pause.wait()
            if self._shutdown.is_set():
                break
            now = time.monotonic()
            dt = min(0.05, max(1e-3, now - last_time))
            last_time = now
            # Serialize PID state and motor writes with move, stop and E-stop.
            with self._lock:
                if not self._pause.is_set():
                    continue
                try:
                    if self._estopped or self._released:
                        self.motor.coast_motor(self.motor_id)
                        self._last_cmd = 0
                    elif self._target is None:
                        self._apply_holding_command(self.pre_target)
                        self._state = "holding" if self.pre_target is not None and self.pre_target > 0 else "idle"
                    else:
                        current = int(self.encoder.get_position())
                        self._run_position_move_locked(current, now, dt)
                except Exception as exc:
                    self._failure_reason = str(exc) or type(exc).__name__
                    self._cancel_target_locked()
                    self._released = True
                    self._state = "control error"
                    self._last_cmd = 0
                    try:
                        self.motor.coast_motor(self.motor_id)
                    except Exception:
                        pass
            self._shutdown.wait(sleep_base)

    def _run_position_move_locked(self, current: int, now: float, dt: float):
        target = self._target
        if target is None:
            return
        error = target - current
        at_target = abs(error) <= self.tolerance_counts
        if self._release_at_rest:
            # A bottom move may reach the sample before the encoder target.
            # Coast once it rests for the configured duration, or passes bottom.
            samples = self._stationary_samples
            samples.append((now, current))
            cutoff = now - config.WEIGHTS_RELEASE_STATIONARY_S
            while len(samples) > 2 and samples[1][0] <= cutoff:
                samples.popleft()
            duration = samples[-1][0] - samples[0][0]
            positions = [sample[1] for sample in samples]
            at_rest = (
                duration >= config.WEIGHTS_RELEASE_STATIONARY_S
                and max(positions) - min(positions) <= config.WEIGHTS_RELEASE_STATIONARY_COUNTS
            )
            if current <= target + self.tolerance_counts or at_rest:
                self.motor.coast_motor(self.motor_id)
                self._cancel_target_locked()
                self._released = True
                self._arrive_flag = True
                self._last_cmd = 0
                self._state = "released"
                return
        elif at_target:
            self._apply_holding_command(target)
            self._target = None
            self._arrive_flag = True
            self._state = "at target"
            self.pid.reset()
            return

        self._state = "moving_up" if error > 0 else "moving_down"
        command = self._compute_pid_command(error, dt)
        self.motor.set_speed(self.motor_id, command)
        self._last_cmd = command

    def _holding_command(self) -> float:
        return max(
            0.0,
            min(float(config.MOTORON_MAX),
                (self.top_holding_speed_pct / 100.0) * config.MOTORON_MAX),
        )

    def _apply_holding_command(self, target):
        logical_command = int(self._holding_command()) if target is not None and target > 0 else 0
        motor_command = -logical_command if self.invert else logical_command
        self.motor.set_speed(self.motor_id, motor_command)
        self._last_cmd = motor_command

    def _compute_pid_command(self, error: float, dt: float) -> int:
        output = self.pid.compute(error, dt)
        # Use moving compensation only when lifting to a positive target.
        if not self._release_at_rest and self.pre_target is not None and self.pre_target > 0:
            output += self.gravity_output
        max_cmd = self._max_output()
        output = max(-max_cmd, min(max_cmd, output))

        if self._min_output > 0 and abs(output) > 0:
            min_out = min(max_cmd, self._min_output)
            if abs(output) < min_out:
                output = math.copysign(min_out, output)

        if self.invert:
            output = -output

        output = max(-config.MOTORON_MAX, min(config.MOTORON_MAX, output))
        return int(output)

    def _max_output(self) -> int:
        pct = max(5.0, min(100.0, float(self.speed_percent)))
        return max(1, int((pct / 100.0) * config.MOTORON_MAX))
