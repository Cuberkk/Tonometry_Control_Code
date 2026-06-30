"""Control abstractions for manual axes and PD-governed weights."""

from __future__ import annotations

import math
import threading
import time
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

        # print(f"cmd:{mag}")
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

# class PIDController:
#     """Minimal PD helper used by the weights axis (no integral term)."""

#     def __init__(self, kp: float, ki: float, kd: float, output_limits: tuple[float, float]):
#         self.kp = float(kp)
#         self.kd = float(kd)
#         self.output_limits = (float(output_limits[0]), float(output_limits[1]))
#         self.reset()

#     def reset(self):
#         self._prev_error = None

#     def compute(self, error: float, dt: float) -> float:
#         if dt <= 0.0:
#             dt = 1e-3
#         proportional = self.kp * error
#         derivative = 0.0
#         if self._prev_error is not None:
#             derivative = self.kd * (error - self._prev_error) / dt
#         self._prev_error = error
#         output = proportional + derivative
#         low, high = self.output_limits
#         if output > high:
#             output = high
#         elif output < low:
#             output = low
#         return output


class WeightsPositionController:
    """Encoder-based positioning for the weights axis using PD instead of hardware braking."""

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
        self._arrive_flag = None
        self._state = "idle"
        self._last_cmd = 0
        self._estopped = False
        self.pre_target = None
        self._shutdown = threading.Event()
        self._pause = threading.Event()
        self._pause.clear()
        self._lock = threading.Lock()
        self.pid = PIDController(
            config.WEIGHTS_PID_KP,
            config.WEIGHTS_PID_KI,
            config.WEIGHTS_PID_KD,
            output_limits=(-config.MOTORON_MAX, config.MOTORON_MAX),
            integral_limits=(-config.WEIGHTS_PID_I_LIM, config.WEIGHTS_PID_I_LIM),
        )
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
            return None if self._target is None else int(self._target)

    def set_speed_percent(self, pct: float):
        self.speed_percent = max(5.0, min(100.0, float(pct)))

    def set_tolerance(self, counts: int):
        counts = int(counts)
        self.tolerance_counts = max(1, counts)

    def move_to(self, target: int):
        if self._estopped:
            return
        with self._lock:
            self._target = int(target)
        self._state = "queued"

    def stop(self, *, coast: bool = False):
        with self._lock:
            self._target = None
        self.pid.reset()
        self._last_cmd = 0
        if coast:
            self.motor.brake_motor(self.motor_id, config.BRAKE_FLOOR)
            self._state = "coast"
        else:
            self.motor.set_speed(self.motor_id, 0)
            self._state = "idle"

    def set_estop(self, active: bool):
        self._estopped = bool(active)
        if active:
            with self._lock:
                self._target = None
            self.pid.reset()
            self.motor.coast(self.motor_id)
            self._last_cmd = 0
            self._state = "estopped"
        else:
            self._state = "idle"

    def shutdown(self):
        self._shutdown.set()
        try:
            self._thread.join(timeout=1.0)
        except Exception:
            pass
        self.stop(coast=True)

    def _loop(self):
        sleep_base = 1.0 / self.refresh_hz
        last_time = time.monotonic()
        i = 0
        while not self._shutdown.is_set():
            self._pause.wait()
            # print("Still in the loop")
            now = time.monotonic()
            dt = max(1e-3, now - last_time)
            last_time = now

            if self._estopped:
                # print('The motor is estopped, coasting')
                self.motor.coast_motor(self.motor_id)
                self._last_cmd = 0
                self.pid.reset()
                time.sleep(0.1)
                continue

            with self._lock:
                target = self._target
                # print("Current target:", target)

            if target is None:
                if self.pre_target is not None and self.pre_target > 0:
                    self.motor.set_speed(self.motor_id, int((self.top_holding_speed_pct / 100.0) * config.MOTORON_MAX))
                else:
                    self.motor.set_speed(self.motor_id, 0)
                self._last_cmd = 0
                self.pid.reset()
                if self._state != "idle":
                    self._state = "idle"
                time.sleep(0.1)
                continue
            
            self.pre_target = target
            self._arrive_flag = False
            try:
                # print("Try to get the position")
                current = self.encoder.get_position()
                # print("Current position:", current)
            except Exception as exc:
                self._state = f"encoder error: {exc}"
                self.motor.brake_motor(self.motor_id, config.BRAKE_FLOOR)
                self._last_cmd = 0
                time.sleep(0.2)
                continue

            error = int(target) - current
            holding = abs(error) <= self.tolerance_counts
            if holding:
                with self._lock:
                    self._target = None
                self._arrive_flag = True
                # print('Motor is arrived')
                self._state = "at target"
                
                time.sleep(sleep_base)
                continue
            else:
                # print("Motor is moving")
                self._state = "moving_up" if error > 0 else "moving_down"

            # print("Send speed command")
            cmd = self._compute_pid_command(error, dt)
            self._last_cmd = cmd

            # if cmd == 0 and holding:
            #     self.motor.set_speed(self.motor_id, 0)
            #     time.sleep(0.1)
            #     continue
            # print("Set motor speed", cmd)
            self.motor.set_speed(self.motor_id, cmd)
            i+=1
            # print(f"Iteration: {i}")
            time.sleep(sleep_base)

    def _compute_pid_command(self, error: float, dt: float) -> int:
        output = self.pid.compute(error, dt)
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
