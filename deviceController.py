#!/usr/bin/env python3
"""Tonometry controller with encoder-driven weights axis."""

import csv
import datetime
import json
import os
import sys
import threading
import time
import tkinter as tk
from pathlib import Path
from tkinter import ttk
from typing import Optional

# ------------------------------ CONFIG ---------------------------------------

I2C_BUS = 1
MOTORON_ADDR = 0x10

MOTOR_INVERT = {
    1: True,   # Weights
    2: True,   # Gantry motor A
    3: False,  # Gantry motor B
}

MOTORON_MAX = 600
TORQUE_FLOOR = 110
CMD_REFRESH_HZ = 10.0
VIN_REFRESH_HZ = 2.0

# Encoder GPIO pins (BCM numbering)
ENCODER_GPIO_A = 6
ENCODER_GPIO_B = 5
ENCODER_INVERT = False

# ------------------------------ CALIBRATION ----------------------------------

calibration_file = {}
try:
    with open("calibration.json", "r", encoding="utf-8") as fh:
        calibration_file = json.load(fh)
except Exception:
    calibration_file = {}

# LVDT calibration (optional and unchanged from previous tooling)
LVDT_SLOPE = calibration_file.get("LVDT", {}).get("slope", 2.0114)
LVDT_INTERCEPT = calibration_file.get("LVDT", {}).get("intercept", 0.7342)
LVDT_VOLTAGE_SCALE = float(calibration_file.get("LVDT", {}).get("voltage_scale", 1.5))


def _load_weight_default(name: str, default: int) -> int:
    try:
        value = calibration_file.get("weights_encoder", {}).get(name, default)
        return int(value)
    except Exception:
        return int(default)


def _load_weight_float(name: str, default: float) -> float:
    try:
        value = calibration_file.get("weights_encoder", {}).get(name, default)
        return float(value)
    except Exception:
        return float(default)


WEIGHTS_TOP_DEFAULT = _load_weight_default("top_counts", 5000)
WEIGHTS_BOTTOM_DEFAULT = _load_weight_default("bottom_counts", 0)
WEIGHTS_TOLERANCE_DEFAULT = max(1, _load_weight_default("tolerance_counts", 20))
WEIGHTS_SLOW_ZONE_COUNTS = max(1, _load_weight_default("slow_zone_counts", 400))
WEIGHTS_SLOW_MIN_SCALE = max(0.05, min(1.0, _load_weight_float("slow_zone_scale", 0.25)))
WEIGHTS_SLOW_TORQUE_FLOOR = max(20, _load_weight_default("slow_zone_torque_floor", 80))

# ------------------------------ LIBS -----------------------------------------

BASE_DIR = Path(__file__).resolve().parent
MOTORON_PATH = BASE_DIR / "motoron-python"
if MOTORON_PATH.exists():
    sys.path.insert(1, str(MOTORON_PATH))
try:
    from motoron import MotoronI2C
except ImportError as exc:
    raise ImportError(f"Motoron library not found. Expected directory: {MOTORON_PATH}") from exc


ADC_AVAILABLE = True
try:
    from dfrobot_ads1115_fast import FastADS1115I2C
except Exception:
    ADC_AVAILABLE = False


class QuadratureEncoder:
    """Simple quadrature encoder reader built on pigpio callbacks."""

    # Transition lookup table (prev<<2 | new_state)
    _TRANSITIONS = {
        0b0001: +1,
        0b0111: +1,
        0b1110: +1,
        0b1000: +1,
        0b0010: -1,
        0b1011: -1,
        0b1101: -1,
        0b0100: -1,
    }

    def __init__(self, gpio_a: int, gpio_b: int, invert: bool = False):
        import pigpio

        self.gpio_a = int(gpio_a)
        self.gpio_b = int(gpio_b)
        self._invert = -1 if invert else 1
        self._position = 0
        self._lock = threading.Lock()
        self._pi = pigpio.pi()
        if not self._pi.connected:
            raise RuntimeError("pigpio daemon not running; start with `sudo pigpiod`.")

        for pin in (self.gpio_a, self.gpio_b):
            self._pi.set_mode(pin, pigpio.INPUT)
            self._pi.set_pull_up_down(pin, pigpio.PUD_UP)

        self._last_state = (self._pi.read(self.gpio_a) << 1) | self._pi.read(self.gpio_b)
        self._cb_a = self._pi.callback(self.gpio_a, pigpio.EITHER_EDGE, self._pulse)
        self._cb_b = self._pi.callback(self.gpio_b, pigpio.EITHER_EDGE, self._pulse)

    def _pulse(self, _gpio, _level, _tick):
        a = self._pi.read(self.gpio_a)
        b = self._pi.read(self.gpio_b)
        new_state = (a << 1) | b
        prev_state = self._last_state
        if new_state == prev_state:
            return
        transition = ((prev_state << 2) | new_state) & 0b1111
        delta = self._TRANSITIONS.get(transition)
        self._last_state = new_state
        if delta is None:
            return
        with self._lock:
            self._position += self._invert * delta

    def get_position(self) -> int:
        with self._lock:
            return int(self._position)

    def set_position(self, value: int = 0):
        with self._lock:
            self._position = int(value)

    def close(self):
        try:
            if hasattr(self, "_cb_a") and self._cb_a:
                self._cb_a.cancel()
            if hasattr(self, "_cb_b") and self._cb_b:
                self._cb_b.cancel()
        except Exception:
            pass
        try:
            if hasattr(self, "_pi") and self._pi:
                self._pi.stop()
        except Exception:
            pass

    def __del__(self):
        self.close()


# ------------------------------ MOTORON --------------------------------------

class Motoron:
    def __init__(self, address=MOTORON_ADDR, bus=I2C_BUS):
        self.mc = MotoronI2C(address=address, bus=bus)
        self.mc.reinitialize()
        self.mc.disable_crc()
        self.mc.clear_reset_flag()
        self.mc.set_command_timeout_milliseconds(500)
        for m in (1, 2, 3):
            self.mc.set_max_acceleration(m, 300)
            self.mc.set_max_deceleration(m, 300)

    def set_speed(self, m: int, speed: int):
        s = max(-MOTORON_MAX, min(MOTORON_MAX, int(speed)))
        self.mc.set_speed(m, s)

    def coast_all(self):
        try:
            self.mc.coast_now()
        except Exception:
            pass

    def brake_all(self, amount=800):
        for m in (1, 2, 3):
            try:
                self.mc.set_braking_now(m, int(max(0, min(800, amount))))
            except Exception:
                pass

    def coast_motor(self, m: int):
        try:
            self.mc.set_braking_now(m, 0)
        except Exception:
            pass

    def brake_motor(self, m: int, amount=800):
        try:
            self.mc.set_braking_now(m, int(max(0, min(800, amount))))
        except Exception:
            pass

    def vin_v(self):
        try:
            return float(self.mc.get_vin_voltage_mv()) / 1000.0
        except Exception:
            return float("nan")


# ------------------------------ ADC THREAD -----------------------------------

class ADCReader(threading.Thread):
    def __init__(self, bus=I2C_BUS, addr=0x48, channel=1):
        super().__init__(daemon=True)
        self.enabled = ADC_AVAILABLE
        self.latest_v = 0.0
        self.pos_offset = self.convert_volt_to_pos(0.388 * LVDT_VOLTAGE_SCALE)
        self.latest_pos = 0.0
        self.hz = 0.0
        self._stop = threading.Event()
        self.channel = channel

        if not self.enabled:
            return
        try:
            self.adc = FastADS1115I2C(
                bus=bus,
                addr=addr,
                mv_to_v=True,
                init_conv_delay_s=0.0012,
                min_conv_delay_s=0.0010,
                max_conv_delay_s=0.0030,
            )
            if not self.adc.begin():
                self.enabled = False
        except Exception:
            self.enabled = False

    def run(self):
        if not self.enabled:
            return
        t0 = time.time()
        n = 0
        while not self._stop.is_set():
            try:
                v = float(self.adc.get_value(self.channel)) / 100.0
                v *= LVDT_VOLTAGE_SCALE
                self.latest_v = v
                self.latest_pos = self.convert_volt_to_pos(v) - self.pos_offset
                n += 1
            except Exception:
                time.sleep(0.001)
                continue

            now = time.time()
            if now - t0 >= 1.0:
                self.hz = n / (now - t0)
                n = 0
                t0 = now

    def convert_volt_to_pos(self, v: float) -> float:
        return v * LVDT_SLOPE + LVDT_INTERCEPT

    def stop(self):
        self._stop.set()


# ------------------------------ AXES -----------------------------------------

class AxisManual:
    """Manual hold-to-run control for the gantry."""

    def __init__(self, name: str, motor_ids: tuple[int, ...], motor_invert: tuple[bool, ...], torque_floor: Optional[int] = None):
        self.name = name
        self.motor_ids = motor_ids
        self.motor_invert = list(motor_invert)
        self.speed_percent = 30.0
        self.dir = 0
        self.last_cmd = 0
        self.torque_floor = TORQUE_FLOOR if torque_floor is None else int(torque_floor)

    def _map_speed(self) -> int:
        pct = max(0.0, min(100.0, float(self.speed_percent)))
        if pct <= 0.0:
            return 0
        cmd = int((pct / 100.0) * MOTORON_MAX)
        cmd = max(cmd, self.torque_floor)
        return min(cmd, MOTORON_MAX)

    def apply(self, mot: Motoron):
        if self.dir == 0:
            for m in self.motor_ids:
                mot.set_speed(m, 0)
            self.last_cmd = 0
            return
        mag = self._map_speed()
        raw = mag if self.dir > 0 else -mag
        self.last_cmd = raw
        for m, inv in zip(self.motor_ids, self.motor_invert):
            mot.set_speed(m, (-raw if inv else raw))

    def brake(self, mot: Motoron, amount=800):
        self.dir = 0
        for m in self.motor_ids:
            mot.brake_motor(m, amount)
        self.last_cmd = 0


class WeightsPositionController:
    """Encoder-based positioning for the weights axis."""

    def __init__(self, motor: Motoron, motor_id: int, encoder: QuadratureEncoder, invert: bool, torque_floor: int = 250, refresh_hz: float = 50.0):
        if encoder is None:
            raise ValueError("Encoder instance required for weights controller")
        self.motor = motor
        self.motor_id = int(motor_id)
        self.encoder = encoder
        self.invert = bool(invert)
        self.torque_floor = int(max(0, torque_floor))
        self.refresh_hz = max(5.0, float(refresh_hz))
        self.speed_percent = 40.0
        self.tolerance_counts = WEIGHTS_TOLERANCE_DEFAULT
        self.slow_zone_counts = WEIGHTS_SLOW_ZONE_COUNTS
        self.slow_min_scale = WEIGHTS_SLOW_MIN_SCALE
        self.slow_torque_floor = WEIGHTS_SLOW_TORQUE_FLOOR
        self._target = None
        self._state = "idle"
        self._last_cmd = 0
        self._mode = "brake"
        self._estopped = False
        self._shutdown = threading.Event()
        self._lock = threading.Lock()
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
        if coast:
            self._apply_coast()
            self._state = "coast"
        else:
            self._apply_brake()
            self._state = "brake"

    def set_estop(self, active: bool):
        self._estopped = bool(active)
        if active:
            self.stop(coast=True)
            self._state = "estopped"

    def shutdown(self):
        self._shutdown.set()
        try:
            self._thread.join(timeout=1.0)
        except Exception:
            pass
        self.stop(coast=True)

    def _command_for_direction(self, direction: int, error: int) -> int:
        pct = max(5.0, min(100.0, float(self.speed_percent)))
        base_cmd = int((pct / 100.0) * MOTORON_MAX)
        base_cmd = max(base_cmd, self.torque_floor)
        magnitude = base_cmd
        if direction < 0:
            distance = abs(int(error))
            if distance <= self.slow_zone_counts:
                scale = distance / self.slow_zone_counts if self.slow_zone_counts else 0.0
                scale = max(scale, self.slow_min_scale)
                scaled = int(base_cmd * scale)
                scaled = max(scaled, self.slow_torque_floor)
                magnitude = min(base_cmd, max(0, scaled))

        if direction < 0:
            magnitude = -magnitude
        if self.invert:
            magnitude = -magnitude
        return max(-MOTORON_MAX, min(MOTORON_MAX, magnitude))

    def _apply_speed(self, cmd: int):
        self.motor.set_speed(self.motor_id, cmd)
        self._last_cmd = int(cmd)
        self._mode = "drive"

    def _apply_brake(self):
        if self._mode != "brake":
            self.motor.brake_motor(self.motor_id)
            self._mode = "brake"
        self._last_cmd = 0

    def _apply_coast(self):
        if self._mode != "coast":
            self.motor.coast_motor(self.motor_id)
            self._mode = "coast"
        self._last_cmd = 0

    def _loop(self):
        sleep_base = 1.0 / self.refresh_hz
        while not self._shutdown.is_set():
            try:
                if self._estopped:
                    self._apply_coast()
                    time.sleep(0.1)
                    continue

                with self._lock:
                    target = self._target

                if target is None:
                    if self._mode == "coast":
                        self._state = "coast"
                        time.sleep(0.1)
                        continue
                    if self._state not in ("idle", "brake"):
                        self._state = "idle"
                    self._apply_brake()
                    time.sleep(0.1)
                    continue

                current = self.encoder.get_position()
                error = int(target) - current
                if abs(error) <= self.tolerance_counts:
                    self._apply_brake()
                    with self._lock:
                        self._target = None
                    self._state = "at target"
                    time.sleep(0.1)
                    continue

                direction = 1 if error > 0 else -1
                self._state = "moving_up" if direction > 0 else "moving_down"
                cmd = self._command_for_direction(direction, error)
                self._apply_speed(cmd)
                time.sleep(sleep_base)
            except Exception as exc:
                self._state = f"error: {exc}"
                self._apply_brake()
                time.sleep(0.2)


# ------------------------------ GUI ------------------------------------------


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Tonometry Controller")
        self.geometry("900x420")

        self.mot = Motoron(MOTORON_ADDR, I2C_BUS)
        self.encoder = None
        self.encoder_error = None
        try:
            self.encoder = QuadratureEncoder(ENCODER_GPIO_A, ENCODER_GPIO_B, invert=ENCODER_INVERT)
        except Exception as exc:
            self.encoder = None
            self.encoder_error = str(exc)

        self.adc = ADCReader()
        if self.adc.enabled:
            self.adc.start()

        self.weights_controller = None
        if self.encoder is not None:
            try:
                self.weights_controller = WeightsPositionController(
                    motor=self.mot,
                    motor_id=1,
                    encoder=self.encoder,
                    invert=MOTOR_INVERT.get(1, False),
                    torque_floor=250,
                )
            except Exception as exc:
                self.weights_controller = None
                self.encoder_error = str(exc)

        self.gantry = AxisManual("Gantry", (2, 3), (MOTOR_INVERT[2], MOTOR_INVERT[3]))

        self.estopped = False

        self.weights_top_target = tk.IntVar(value=WEIGHTS_TOP_DEFAULT)
        self.weights_bottom_target = tk.IntVar(value=WEIGHTS_BOTTOM_DEFAULT)
        self.weights_tolerance = tk.IntVar(value=WEIGHTS_TOLERANCE_DEFAULT)
        self.weights_nudge = tk.IntVar(value=50)

        self.seq_cycles = tk.IntVar(value=3)
        self.seq_top_dwell = tk.DoubleVar(value=1.0)
        self.seq_bottom_dwell = tk.DoubleVar(value=1.0)

        self.weights_tolerance.trace_add("write", lambda *_: self._on_weights_tolerance_change())

        self.sequence_thread: Optional[threading.Thread] = None
        self.sequence_stop = threading.Event()
        self.sequence_running = False
        self._sequence_result: Optional[tuple[bool, int, int]] = None

        self.logging_active = False
        self.logger_stop = threading.Event()
        self.logger_thread: Optional[threading.Thread] = None
        self.log_fh: Optional[object] = None
        self.log_writer: Optional[csv.writer] = None
        self.log_path: Optional[str] = None
        self._sequence_status_text = "Sequence idle"
        self.sequence_logging_owned = False

        self._build_ui()

        self.after(int(1000.0 / CMD_REFRESH_HZ), self._refresh_commands)
        self.after(int(1000.0 / VIN_REFRESH_HZ), self._refresh_vin)
        if self.adc.enabled:
            self.after(200, self._refresh_adc)
        self.after(100, self._refresh_weights_status)
        self.after(200, self._refresh_sequence_label)
        self.after(100, self._poll_sequence_thread)

        self.bind("<Escape>", self.emergency_stop)
        self.protocol("WM_DELETE_WINDOW", self.on_close)

    def _build_ui(self):
        pad = {"padx": 8, "pady": 6}
        top = ttk.Frame(self)
        top.pack(fill=tk.X, **pad)

        self.lbl_status = ttk.Label(top, text="Ready")
        self.lbl_status.pack(side=tk.LEFT)

        self.lbl_vin = ttk.Label(top, text="VIN: --.- V")
        self.lbl_vin.pack(side=tk.LEFT, padx=(12, 0))

        if self.adc.enabled:
            self.lbl_adc = ttk.Label(top, text="LVDT: ---.--- V | ---.--- mm |  ---.- Hz")
        else:
            self.lbl_adc = ttk.Label(top, text="LVDT: (disabled)")
        self.lbl_adc.pack(side=tk.LEFT, padx=(12, 0))

        btn_estop = tk.Button(
            top,
            text="E-STOP (COAST)",
            command=self.emergency_stop,
            bg="#c62828",
            fg="white",
            activebackground="#b71c1c",
            font=("TkDefaultFont", 10, "bold"),
        )
        btn_estop.pack(side=tk.RIGHT, padx=(8, 0))
        ttk.Button(top, text="Reset E-STOP", command=self.reset_estop).pack(side=tk.RIGHT, padx=(6, 0))
        ttk.Button(top, text="STOP (Brake)", command=self.stop_all).pack(side=tk.RIGHT)

        self.btn_log = ttk.Button(top, text="Start Log", command=self.toggle_logging)
        self.btn_log.pack(side=tk.RIGHT, padx=(6, 0))

        ttk.Separator(self, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=(2, 2))

        self._weights_card().pack(fill=tk.X, **pad)
        self._axis_card(self.gantry).pack(fill=tk.X, **pad)
        self._sequence_panel().pack(fill=tk.X, **pad)

    def _weights_card(self):
        f = ttk.LabelFrame(self, text="Weights (Motor 1)")

        row0 = ttk.Frame(f)
        row0.pack(fill=tk.X, padx=8, pady=(8, 2))

        ttk.Label(row0, text="Encoder:").pack(side=tk.LEFT)
        self.var_weights_pos = tk.StringVar(value="--")
        ttk.Label(row0, textvariable=self.var_weights_pos, width=10).pack(side=tk.LEFT, padx=(4, 12))

        ttk.Label(row0, text="State:").pack(side=tk.LEFT)
        self.var_weights_state = tk.StringVar(value="idle")
        ttk.Label(row0, textvariable=self.var_weights_state, width=14).pack(side=tk.LEFT, padx=(4, 12))

        ttk.Label(row0, text="Cmd:").pack(side=tk.LEFT)
        self.var_weights_cmd = tk.StringVar(value="0")
        ttk.Label(row0, textvariable=self.var_weights_cmd, width=10).pack(side=tk.LEFT, padx=(4, 12))

        ttk.Label(row0, text="Target:").pack(side=tk.LEFT)
        self.var_weights_target = tk.StringVar(value="--")
        ttk.Label(row0, textvariable=self.var_weights_target, width=10).pack(side=tk.LEFT, padx=(4, 0))

        row1 = ttk.Frame(f)
        row1.pack(fill=tk.X, padx=8, pady=(4, 2))

        ttk.Label(row1, text="Speed %:").pack(side=tk.LEFT)
        self.weights_speed_slider = ttk.Scale(row1, from_=5, to=100, orient=tk.HORIZONTAL, length=320, command=self._on_weight_speed)
        initial_speed = self.weights_controller.speed_percent if self.weights_controller else 40.0
        self.weights_speed_slider.set(initial_speed)
        self.weights_speed_slider.pack(side=tk.LEFT, padx=(6, 12))
        if not self.weights_controller:
            self.weights_speed_slider.state(["disabled"])

        ttk.Label(row1, text="Tolerance (counts):").pack(side=tk.LEFT)
        self.spin_tolerance = tk.Spinbox(
            row1,
            from_=1,
            to=5000,
            increment=1,
            width=6,
            textvariable=self.weights_tolerance,
            command=self._on_weights_tolerance_change,
        )
        self.spin_tolerance.pack(side=tk.LEFT, padx=(4, 0))
        if not self.weights_controller:
            self.spin_tolerance.config(state=tk.DISABLED)

        row2 = ttk.Frame(f)
        row2.pack(fill=tk.X, padx=8, pady=(4, 2))

        ttk.Label(row2, text="Top target:").pack(side=tk.LEFT)
        self.spin_top = tk.Spinbox(row2, from_=-500000, to=500000, increment=10, width=10, textvariable=self.weights_top_target)
        self.spin_top.pack(side=tk.LEFT, padx=(4, 6))
        self.btn_weights_go_top = ttk.Button(row2, text="Run to Top", command=self.move_weights_to_top)
        self.btn_weights_go_top.pack(side=tk.LEFT, padx=(0, 6))
        self.btn_weights_set_top = ttk.Button(row2, text="Set Current", command=lambda: self._set_target_from_current("top"))
        self.btn_weights_set_top.pack(side=tk.LEFT)

        row3 = ttk.Frame(f)
        row3.pack(fill=tk.X, padx=8, pady=(4, 2))

        ttk.Label(row3, text="Bottom target:").pack(side=tk.LEFT)
        self.spin_bottom = tk.Spinbox(row3, from_=-500000, to=500000, increment=10, width=10, textvariable=self.weights_bottom_target)
        self.spin_bottom.pack(side=tk.LEFT, padx=(4, 6))
        self.btn_weights_go_bottom = ttk.Button(row3, text="Run to Bottom", command=self.move_weights_to_bottom)
        self.btn_weights_go_bottom.pack(side=tk.LEFT, padx=(0, 6))
        self.btn_weights_set_bottom = ttk.Button(row3, text="Set Current", command=lambda: self._set_target_from_current("bottom"))
        self.btn_weights_set_bottom.pack(side=tk.LEFT)

        row4 = ttk.Frame(f)
        row4.pack(fill=tk.X, padx=8, pady=(6, 2))

        ttk.Label(row4, text="Manual nudge (counts):").pack(side=tk.LEFT)
        self.spin_nudge = tk.Spinbox(
            row4,
            from_=1,
            to=100000,
            increment=1,
            width=8,
            textvariable=self.weights_nudge,
        )
        self.spin_nudge.pack(side=tk.LEFT, padx=(4, 12))

        self.btn_weights_nudge_up = ttk.Button(row4, text="Nudge Up", command=lambda: self.nudge_weights(+1))
        self.btn_weights_nudge_up.pack(side=tk.LEFT, padx=(0, 6))
        self.btn_weights_nudge_down = ttk.Button(row4, text="Nudge Down", command=lambda: self.nudge_weights(-1))
        self.btn_weights_nudge_down.pack(side=tk.LEFT)

        row5 = ttk.Frame(f)
        row5.pack(fill=tk.X, padx=8, pady=(6, 8))

        self.btn_weights_stop = ttk.Button(row5, text="Stop (Brake)", command=self.stop_weights)
        self.btn_weights_stop.pack(side=tk.LEFT)
        self.btn_weights_coast = ttk.Button(row5, text="Coast", command=lambda: self.stop_weights(coast=True))
        self.btn_weights_coast.pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(row5, text="Save Targets", command=self._save_weights_targets).pack(side=tk.LEFT, padx=(12, 0))

        if self.encoder_error:
            ttk.Label(f, text=f"Encoder unavailable: {self.encoder_error}", foreground="#c62828").pack(fill=tk.X, padx=8, pady=(0, 8))

        self._update_weights_controls_state()
        return f

    def _sequence_panel(self):
        f = ttk.LabelFrame(self, text="Automated Sequence")

        row0 = ttk.Frame(f)
        row0.pack(fill=tk.X, padx=8, pady=(8, 2))

        ttk.Label(row0, text="Cycles:").pack(side=tk.LEFT)
        tk.Spinbox(
            row0,
            from_=1,
            to=999,
            increment=1,
            width=6,
            textvariable=self.seq_cycles,
        ).pack(side=tk.LEFT, padx=(4, 12))

        ttk.Label(row0, text="High dwell (s):").pack(side=tk.LEFT)
        tk.Spinbox(
            row0,
            from_=0.0,
            to=60.0,
            increment=0.1,
            width=8,
            textvariable=self.seq_top_dwell,
        ).pack(side=tk.LEFT, padx=(4, 12))

        ttk.Label(row0, text="Low dwell (s):").pack(side=tk.LEFT)
        tk.Spinbox(
            row0,
            from_=0.0,
            to=60.0,
            increment=0.1,
            width=8,
            textvariable=self.seq_bottom_dwell,
        ).pack(side=tk.LEFT, padx=(4, 0))

        row1 = ttk.Frame(f)
        row1.pack(fill=tk.X, padx=8, pady=(4, 2))

        self.btn_seq_start = ttk.Button(row1, text="Start Sequence", command=self.start_sequence)
        self.btn_seq_start.pack(side=tk.LEFT)
        self.btn_seq_stop = ttk.Button(row1, text="Stop Sequence", command=self.stop_sequence)
        self.btn_seq_stop.pack(side=tk.LEFT, padx=(6, 0))

        self.var_seq_status = tk.StringVar(value="Sequence idle")
        ttk.Label(f, textvariable=self.var_seq_status).pack(fill=tk.X, padx=8, pady=(4, 8))

        self._update_sequence_controls()
        return f

    def _axis_card(self, axis: AxisManual):
        f = ttk.LabelFrame(self, text=f"{axis.name} (Motors: {', '.join(map(str, axis.motor_ids))})")

        row0 = ttk.Frame(f)
        row0.pack(fill=tk.X, padx=8, pady=(8, 2))
        ttk.Label(row0, text="Speed %:").pack(side=tk.LEFT)
        slider = ttk.Scale(row0, from_=0, to=100, orient=tk.HORIZONTAL, length=360, command=lambda v, a=axis: setattr(a, "speed_percent", float(v)))
        slider.set(axis.speed_percent)
        slider.pack(side=tk.LEFT, padx=(6, 12))

        ttk.Label(row0, text="cmd:").pack(side=tk.LEFT)
        var_cmd = tk.StringVar(value="0")
        setattr(self, f"var_{axis.name}_cmd", var_cmd)
        ttk.Label(row0, textvariable=var_cmd, width=10).pack(side=tk.LEFT)

        row1 = ttk.Frame(f)
        row1.pack(fill=tk.X, padx=8, pady=(4, 10))
        b_up = ttk.Button(row1, text="▲ Hold UP")
        b_dn = ttk.Button(row1, text="▼ Hold DOWN")
        b_up.pack(side=tk.LEFT, padx=(0, 8))
        b_dn.pack(side=tk.LEFT)

        def press(dirn: int):
            if self.estopped:
                return
            axis.dir = 1 if dirn > 0 else -1
            axis.apply(self.mot)
            getattr(self, f"var_{axis.name}_cmd").set(f"{axis.last_cmd:+d}")

        def release(_e=None):
            axis.brake(self.mot)
            getattr(self, f"var_{axis.name}_cmd").set("0")

        b_up.bind("<ButtonPress-1>", lambda _e: press(+1))
        b_up.bind("<ButtonRelease-1>", release)
        b_dn.bind("<ButtonPress-1>", lambda _e: press(-1))
        b_dn.bind("<ButtonRelease-1>", release)

        setattr(self, f"btn_{axis.name.lower()}_up", b_up)
        setattr(self, f"btn_{axis.name.lower()}_down", b_dn)

        return f

    # ---------- weights helpers ----------

    def _on_weight_speed(self, value):
        if not self.weights_controller:
            return
        try:
            pct = float(value)
        except Exception:
            return
        self.weights_controller.set_speed_percent(pct)

    def _on_weights_tolerance_change(self):
        if not self.weights_controller:
            return
        try:
            tol = int(self.weights_tolerance.get())
        except Exception:
            return
        self.weights_controller.set_tolerance(tol)

    def _set_target_from_current(self, which: str):
        if self.encoder is None:
            return
        pos = self.encoder.get_position()
        if which == "top":
            self.weights_top_target.set(pos)
        else:
            self.weights_bottom_target.set(pos)
        self._save_weights_targets()

    def move_weights_to_top(self):
        if not self.weights_controller or self.estopped:
            return
        target = int(self.weights_top_target.get())
        self.weights_controller.move_to(target)
        self.lbl_status.config(text=f"Moving weights to top ({target} counts)")

    def move_weights_to_bottom(self):
        if not self.weights_controller or self.estopped:
            return
        target = int(self.weights_bottom_target.get())
        self.weights_controller.move_to(target)
        self.lbl_status.config(text=f"Moving weights to bottom ({target} counts)")

    def nudge_weights(self, direction: int):
        if not self.weights_controller or self.estopped:
            return
        if self.sequence_running:
            self.lbl_status.config(text="Nudge unavailable during sequence")
            return
        if self.encoder is None:
            self.lbl_status.config(text="Encoder unavailable for nudge")
            return
        try:
            step = int(self.weights_nudge.get())
        except Exception:
            step = 0
        if step <= 0:
            self.lbl_status.config(text="Set nudge step > 0")
            return
        current = self.encoder.get_position()
        target = current + (step if direction > 0 else -step)
        self.weights_controller.move_to(target)
        self.lbl_status.config(text=f"Nudging weights to {target} counts")

    def stop_weights(self, coast: bool = False):
        if not self.weights_controller:
            return
        self.weights_controller.stop(coast=coast)
        msg = "Weights coast" if coast else "Weights braked"
        self.lbl_status.config(text=msg)

    def _save_weights_targets(self):
        global calibration_file
        data = dict(calibration_file)
        weights_cfg = data.setdefault("weights_encoder", {})
        weights_cfg["top_counts"] = int(self.weights_top_target.get())
        weights_cfg["bottom_counts"] = int(self.weights_bottom_target.get())
        weights_cfg["tolerance_counts"] = int(self.weights_tolerance.get())
        try:
            with open("calibration.json", "w", encoding="utf-8") as fh:
                json.dump(data, fh, indent=2, sort_keys=True)
            calibration_file = data
            self.lbl_status.config(text="Saved weights targets")
        except Exception as exc:
            self.lbl_status.config(text=f"Failed to save: {exc}")

    def _update_weights_controls_state(self):
        motion_enabled = bool(self.weights_controller and not self.estopped)
        state_motion = tk.NORMAL if motion_enabled else tk.DISABLED
        for btn in (
            getattr(self, "btn_weights_go_top", None),
            getattr(self, "btn_weights_go_bottom", None),
            getattr(self, "btn_weights_stop", None),
            getattr(self, "btn_weights_coast", None),
        ):
            if btn is not None:
                btn.config(state=state_motion)
        can_nudge = motion_enabled and not self.sequence_running and self.encoder is not None
        nudge_state = tk.NORMAL if can_nudge else tk.DISABLED
        for btn in (
            getattr(self, "btn_weights_nudge_up", None),
            getattr(self, "btn_weights_nudge_down", None),
        ):
            if btn is not None:
                btn.config(state=nudge_state)
        spin_nudge = getattr(self, "spin_nudge", None)
        if spin_nudge is not None:
            spin_nudge_state = tk.NORMAL if can_nudge else tk.DISABLED
            spin_nudge.config(state=spin_nudge_state)
        if self.weights_controller is None or self.encoder is None:
            self.var_weights_state.set("encoder unavailable")
        self._update_sequence_controls()

    def _update_sequence_controls(self):
        can_run = bool(self.weights_controller and not self.estopped)
        btn_start = getattr(self, "btn_seq_start", None)
        btn_stop = getattr(self, "btn_seq_stop", None)
        if btn_start is not None:
            if can_run and not self.sequence_running:
                btn_start.state(["!disabled"])
            else:
                btn_start.state(["disabled"])
        if btn_stop is not None:
            if self.sequence_running:
                btn_stop.state(["!disabled"])
            else:
                btn_stop.state(["disabled"])

    # ---------- loops & status ----------

    def _refresh_commands(self):
        if not self.estopped:
            for axis in (self.gantry,):
                if axis.dir != 0:
                    axis.apply(self.mot)
                    getattr(self, f"var_{axis.name}_cmd").set(f"{axis.last_cmd:+d}")
        self.after(int(1000.0 / CMD_REFRESH_HZ), self._refresh_commands)

    def _refresh_vin(self):
        try:
            v = self.mot.vin_v()
            if v == v:
                self.lbl_vin.config(text=f"VIN: {v:0.1f} V")
        except Exception:
            pass
        self.after(int(1000.0 / VIN_REFRESH_HZ), self._refresh_vin)

    def _refresh_adc(self):
        if self.adc.enabled:
            self.lbl_adc.config(
                text=f"LVDT: {self.adc.latest_v:0.3f} V | {self.adc.latest_pos:0.3f} mm  |  {self.adc.hz:0.1f} Hz"
            )
            self.after(200, self._refresh_adc)

    def _refresh_weights_status(self):
        if self.encoder is not None:
            pos = self.encoder.get_position()
            self.var_weights_pos.set(f"{pos:d}")
        else:
            self.var_weights_pos.set("--")

        if self.weights_controller:
            self.var_weights_state.set(self.weights_controller.state)
            self.var_weights_cmd.set(f"{self.weights_controller.last_cmd:+d}")
            tgt = self.weights_controller.target_position
            self.var_weights_target.set("--" if tgt is None else f"{tgt:d}")
        else:
            self.var_weights_cmd.set("0")
            self.var_weights_target.set("--")

        self.after(100, self._refresh_weights_status)

    def _refresh_sequence_label(self):
        try:
            current = self.var_seq_status.get()
        except Exception:
            current = ""
        if current != self._sequence_status_text:
            try:
                self.var_seq_status.set(self._sequence_status_text)
            except Exception:
                pass
        self.after(200, self._refresh_sequence_label)

    # ---------- automated sequence ----------

    def _set_sequence_status(self, text: str):
        self._sequence_status_text = text

    def start_sequence(self):
        if not self.weights_controller or self.estopped:
            self._set_sequence_status("Sequence unavailable")
            return
        if self.sequence_running:
            self._set_sequence_status("Sequence already running")
            return
        try:
            cycles = max(1, int(self.seq_cycles.get()))
            top_dwell = max(0.0, float(self.seq_top_dwell.get()))
            bottom_dwell = max(0.0, float(self.seq_bottom_dwell.get()))
        except Exception as exc:
            self._set_sequence_status(f"Invalid sequence input: {exc}")
            return

        auto_log_started = False
        if not self.logging_active:
            auto_log_started = self._start_logging(auto=True)
        else:
            self.sequence_logging_owned = False

        self.sequence_stop.clear()
        self.sequence_running = True
        self._update_sequence_controls()
        self._set_sequence_status(f"Sequence starting ({cycles} cycles)")
        self.lbl_status.config(text="Weights sequence running")
        self.sequence_logging_owned = auto_log_started
        self._update_weights_controls_state()

        self.sequence_thread = threading.Thread(
            target=self._sequence_worker,
            args=(cycles, top_dwell, bottom_dwell),
            daemon=True,
        )
        self.sequence_thread.start()

    def stop_sequence(self):
        if not self.sequence_running:
            return
        self.sequence_stop.set()
        if self.weights_controller:
            self.weights_controller.stop()
        self._set_sequence_status("Stopping sequence...")
        self._update_sequence_controls()
        self._update_weights_controls_state()

    def _sequence_worker(self, cycles: int, top_dwell: float, bottom_dwell: float):
        top_target = int(self.weights_top_target.get())
        bottom_target = int(self.weights_bottom_target.get())
        completed = 0
        success = True

        try:
            for idx in range(1, cycles + 1):
                if self.sequence_stop.is_set() or self.estopped:
                    success = False
                    break

                self._set_sequence_status(f"Cycle {idx}/{cycles}: moving to top")
                self.weights_controller.move_to(top_target)
                if not self._wait_for_weights_target(top_target, timeout=120.0):
                    success = False
                    break
                if not self._sleep_with_cancel(top_dwell):
                    success = False
                    break

                if self.sequence_stop.is_set() or self.estopped:
                    success = False
                    break

                self._set_sequence_status(f"Cycle {idx}/{cycles}: moving to bottom")
                self.weights_controller.move_to(bottom_target)
                if not self._wait_for_weights_target(bottom_target, timeout=120.0):
                    success = False
                    break
                if not self._sleep_with_cancel(bottom_dwell):
                    success = False
                    break

                completed = idx

        finally:
            self._sequence_result = (success, completed, cycles)

    def _sequence_finished(self, success: bool, completed: int, requested: int):
        self.sequence_running = False
        self.sequence_stop.clear()
        if self.weights_controller and not self.estopped:
            self.weights_controller.stop()

        if success and completed == requested:
            msg = f"Sequence complete ({completed} cycles)"
            self.lbl_status.config(text="Sequence complete")
        else:
            msg = f"Sequence stopped ({completed}/{requested} cycles)"
            self.lbl_status.config(text="Sequence stopped")

        self._set_sequence_status(msg)
        self._update_sequence_controls()
        self._update_weights_controls_state()
        if self.sequence_logging_owned and self.logging_active:
            self._stop_logging()
        self.sequence_logging_owned = False

    def _wait_for_weights_target(self, target: int, timeout: Optional[float] = None) -> bool:
        start = time.time()
        while True:
            if self.sequence_stop.is_set() or self.estopped:
                return False
            wc = self.weights_controller
            if wc is None:
                return False
            current = self.encoder.get_position() if self.encoder else None
            if current is not None and abs(int(target) - int(current)) <= wc.tolerance_counts:
                return True
            if wc.state == "at target" and wc.target_position is None:
                return True
            if timeout is not None and (time.time() - start) > timeout:
                return False
            time.sleep(0.05)

    def _sleep_with_cancel(self, duration: float) -> bool:
        if duration <= 0:
            return True
        end = time.time() + duration
        while time.time() < end:
            if self.sequence_stop.is_set() or self.estopped:
                return False
            time.sleep(0.05)
        return True

    def _poll_sequence_thread(self):
        thread = self.sequence_thread
        if thread is not None and not thread.is_alive():
            result = self._sequence_result or (False, 0, 0)
            self._sequence_result = None
            self.sequence_thread = None
            self._sequence_finished(*result)
        self.after(100, self._poll_sequence_thread)

    # ---------- data logging ----------

    def toggle_logging(self):
        if self.logging_active:
            self.sequence_logging_owned = False
            self._stop_logging()
        else:
            self._start_logging()

    def _start_logging(self, auto: bool = False) -> bool:
        if self.logging_active:
            return False
        try:
            os.makedirs("logs", exist_ok=True)
            ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            prefix = "sequence" if auto else "session"
            path = os.path.join("logs", f"{prefix}_{ts}.csv")
            fh = open(path, "w", newline="", encoding="utf-8")
            writer = csv.writer(fh)
            writer.writerow(
                [
                    "timestamp_iso",
                    "encoder_counts",
                    "lvdt_v",
                    "lvdt_mm",
                    "weights_state",
                    "weights_target",
                    "weights_cmd",
                    "sequence_status",
                ]
            )
        except Exception as exc:
            self.lbl_status.config(text=f"Log start failed: {exc}")
            return False

        self.log_path = path
        self.log_fh = fh
        self.log_writer = writer
        self.logger_stop.clear()
        self.logging_active = True
        self.sequence_logging_owned = auto
        self.btn_log.config(text="Stop Log")
        self.lbl_status.config(text=f"Logging to {path}")

        self.logger_thread = threading.Thread(target=self._log_loop, daemon=True)
        self.logger_thread.start()
        return True

    def _stop_logging(self):
        if not self.logging_active:
            return
        self.logger_stop.set()
        if self.logger_thread:
            self.logger_thread.join(timeout=1.0)
        self.logger_thread = None

        if self.log_fh:
            try:
                self.log_fh.flush()
                self.log_fh.close()
            except Exception:
                pass

        self.logging_active = False
        self.log_fh = None
        self.log_writer = None
        self.log_path = None
        self.sequence_logging_owned = False
        self.btn_log.config(text="Start Log")
        self.lbl_status.config(text="Logging stopped")

    def _log_loop(self):
        interval = 0.05  # 20 Hz
        rows_since_flush = 0
        next_time = time.time()
        while not self.logger_stop.is_set():
            next_time += interval
            ts_iso = datetime.datetime.now().isoformat()
            encoder = self.encoder.get_position() if self.encoder else ""
            lvdt_v = self.adc.latest_v if self.adc and self.adc.enabled else ""
            lvdt_mm = self.adc.latest_pos if self.adc and self.adc.enabled else ""
            if self.weights_controller:
                weights_state = self.weights_controller.state
                weights_target = self.weights_controller.target_position
                weights_cmd = self.weights_controller.last_cmd
            else:
                weights_state = ""
                weights_target = ""
                weights_cmd = ""
            seq_status = self._sequence_status_text

            try:
                if self.log_writer:
                    self.log_writer.writerow(
                        [
                            ts_iso,
                            encoder,
                            lvdt_v,
                            lvdt_mm,
                            weights_state,
                            weights_target if weights_target is not None else "",
                            weights_cmd,
                            seq_status,
                        ]
                    )
                    rows_since_flush += 1
            except Exception:
                pass

            if rows_since_flush >= 40 and self.log_fh:
                try:
                    self.log_fh.flush()
                except Exception:
                    pass
                rows_since_flush = 0

            sleep = next_time - time.time()
            if sleep > 0:
                time.sleep(min(sleep, interval))
            else:
                next_time = time.time()
    # ---------- global actions ----------

    def stop_all(self):
        self.stop_sequence()
        if self.weights_controller:
            self.weights_controller.stop()
        self.gantry.brake(self.mot)
        self.lbl_status.config(text="STOPPED")

    def emergency_stop(self, *_):
        self.estopped = True
        self.stop_sequence()
        if self.weights_controller:
            self.weights_controller.set_estop(True)
        for axis in (self.gantry,):
            axis.dir = 0
        self.mot.coast_all()
        self._update_weights_controls_state()
        self.lbl_status.config(text="E-STOP LATCHED", foreground="#c62828")

    def reset_estop(self):
        self.estopped = False
        if self.weights_controller:
            self.weights_controller.set_estop(False)
            self.weights_controller.stop()
        self.gantry.dir = 0
        self.gantry.apply(self.mot)
        self._update_weights_controls_state()
        self.lbl_status.config(text="Ready", foreground="")

    def on_close(self):
        try:
            self.estopped = True
            if self.weights_controller:
                self.weights_controller.shutdown()
            self.mot.coast_all()
        except Exception:
            pass
        try:
            self.stop_sequence()
        except Exception:
            pass
        try:
            self._stop_logging()
        except Exception:
            pass
        try:
            if self.adc:
                self.adc.stop()
        except Exception:
            pass
        try:
            if self.encoder:
                self.encoder.close()
        except Exception:
            pass
        self.destroy()


if __name__ == "__main__":
    App().mainloop()
