"""Tkinter UI for the Tonometry controller."""

from __future__ import annotations

import csv
import datetime
import os
import threading
import time
import tkinter as tk
from tkinter import ttk
from typing import Optional

from . import config
from .controllers import AxisManual, WeightsPositionController
from .hardware import ADCReader, Motoron, QuadratureEncoder


class ControllerApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Tonometry Controller")
        self.geometry("900x800")

        self.mot = Motoron(config.MOTORON_ADDR, config.I2C_BUS)
        self.encoder = None
        self.encoder_error = None
        try:
            self.encoder = QuadratureEncoder(config.ENCODER_GPIO_A, config.ENCODER_GPIO_B, invert=config.ENCODER_INVERT)
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
                    invert=config.MOTOR_INVERT.get(1, False),
                    torque_floor=250,
                )
            except Exception as exc:
                self.weights_controller = None
                self.encoder_error = str(exc)

        self.gantry = AxisManual("Gantry", (2, 3), (config.MOTOR_INVERT[2], config.MOTOR_INVERT[3]))
        self.weights_manual = AxisManual("Weights", (1,), (config.MOTOR_INVERT[1],), torque_floor=400)

        self.estopped = False

        self.weights_top_target = tk.IntVar(value=config.WEIGHTS_TOP_DEFAULT)
        self.weights_bottom_target = tk.IntVar(value=config.WEIGHTS_BOTTOM_DEFAULT)
        self.weights_tolerance = tk.IntVar(value=config.WEIGHTS_TOLERANCE_DEFAULT)
        self.pid_kp = tk.DoubleVar(value=config.WEIGHTS_PID_KP)
        self.pid_kd = tk.DoubleVar(value=config.WEIGHTS_PID_KD)
        self.pid_deadband = tk.DoubleVar(value=config.WEIGHTS_PID_DEADBAND)
        self.pid_min_output = tk.DoubleVar(value=config.WEIGHTS_PID_MIN_OUTPUT)

        self.seq_cycles = tk.IntVar(value=3)
        self.seq_top_dwell = tk.DoubleVar(value=1.0)
        self.seq_bottom_dwell = tk.DoubleVar(value=1.0)

        self.weights_tolerance.trace_add("write", lambda *_: self._on_weights_tolerance_change())

        self.var_tolerance_mm = tk.StringVar(value="")

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
        self._update_tolerance_label()

        self.after(int(1000.0 / config.CMD_REFRESH_HZ), self._refresh_commands)
        self.after(int(1000.0 / config.VIN_REFRESH_HZ), self._refresh_vin)
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
        ttk.Button(top, text="Zero Encoder", command=self.zero_weights_encoder).pack(side=tk.RIGHT, padx=(6, 0))

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
        initial_speed = (
            self.weights_controller.speed_percent
            if self.weights_controller
            else self.weights_manual.speed_percent
        )
        self.weights_speed_slider.set(initial_speed)
        self.weights_speed_slider.pack(side=tk.LEFT, padx=(6, 12))
        if not self.weights_controller and not self.weights_manual:
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
        self.lbl_tolerance_mm = ttk.Label(row1, textvariable=self.var_tolerance_mm, width=12)
        self.lbl_tolerance_mm.pack(side=tk.LEFT, padx=(8, 0))

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

        ttk.Label(row4, text="Manual control:").pack(side=tk.LEFT)
        self.btn_weights_manual_up = ttk.Button(row4, text="▲ Hold UP")
        self.btn_weights_manual_up.pack(side=tk.LEFT, padx=(6, 6))
        self.btn_weights_manual_down = ttk.Button(row4, text="▼ Hold DOWN")
        self.btn_weights_manual_down.pack(side=tk.LEFT)
        self.btn_weights_manual_up.bind("<ButtonPress-1>", lambda _e: self._weights_manual_press(+1))
        self.btn_weights_manual_up.bind("<ButtonRelease-1>", self._weights_manual_release)
        self.btn_weights_manual_down.bind("<ButtonPress-1>", lambda _e: self._weights_manual_press(-1))
        self.btn_weights_manual_down.bind("<ButtonRelease-1>", self._weights_manual_release)

        row5 = ttk.Frame(f)
        row5.pack(fill=tk.X, padx=8, pady=(6, 8))

        self.btn_weights_stop = ttk.Button(row5, text="Stop", command=self.stop_weights)
        self.btn_weights_stop.pack(side=tk.LEFT)
        ttk.Button(row5, text="Save Targets", command=self._save_weights_targets).pack(side=tk.LEFT, padx=(12, 0))

        row6 = ttk.LabelFrame(f, text="PD Gains")
        row6.pack(fill=tk.X, padx=8, pady=(4, 8))

        ttk.Label(row6, text="Kp:").grid(row=0, column=0, padx=(4, 2), pady=2)
        tk.Entry(row6, width=8, textvariable=self.pid_kp).grid(row=0, column=1, padx=(0, 8))
        ttk.Label(row6, text="Kd:").grid(row=0, column=2, padx=(4, 2))
        tk.Entry(row6, width=8, textvariable=self.pid_kd).grid(row=0, column=3, padx=(0, 8))

        ttk.Label(row6, text="Deadband (cnt):").grid(row=1, column=0, padx=(4, 2), pady=2, columnspan=2, sticky="w")
        tk.Entry(row6, width=8, textvariable=self.pid_deadband).grid(row=1, column=2, padx=(0, 8))
        ttk.Label(row6, text="Min Output:").grid(row=1, column=3, padx=(4, 2))
        tk.Entry(row6, width=8, textvariable=self.pid_min_output).grid(row=1, column=4, padx=(0, 8))
        ttk.Button(row6, text="Save PD", command=self._save_pid_settings).grid(row=1, column=5, padx=(4, 4))

        if self.encoder_error:
            ttk.Label(f, text=f"Encoder unavailable: {self.encoder_error}", foreground="#c62828").pack(fill=tk.X, padx=8, pady=(0, 8))

        self._update_weights_controls_state()
        return f

    def _axis_card(self, axis: AxisManual):
        f = ttk.LabelFrame(self, text=f"{axis.name} (Motors: {', '.join(map(str, axis.motor_ids))})")

        r0 = ttk.Frame(f)
        r0.pack(fill=tk.X, padx=8, pady=(8, 2))
        ttk.Label(r0, text="Speed %:").pack(side=tk.LEFT)
        s = ttk.Scale(r0, from_=0, to=100, orient=tk.HORIZONTAL, length=360, command=lambda v, a=axis: setattr(a, "speed_percent", float(v)))
        s.set(axis.speed_percent)
        s.pack(side=tk.LEFT, padx=(6, 12))
        ttk.Label(r0, text=f"(floor {axis.torque_floor})").pack(side=tk.LEFT)
        ttk.Label(r0, text=" | cmd: ").pack(side=tk.LEFT, padx=(12, 0))
        var_cmd = tk.StringVar(value="0")
        setattr(self, f"var_{axis.name}_cmd", var_cmd)
        ttk.Label(r0, textvariable=var_cmd, width=8).pack(side=tk.LEFT)

        r1 = ttk.Frame(f)
        r1.pack(fill=tk.X, padx=8, pady=(4, 10))
        b_up = ttk.Button(r1, text="▲ Hold UP")
        b_dn = ttk.Button(r1, text="▼ Hold DOWN")
        b_up.pack(side=tk.LEFT, padx=(0, 8))
        b_dn.pack(side=tk.LEFT)

        setattr(self, f"btn_{axis.name.lower()}_up", b_up)
        setattr(self, f"btn_{axis.name.lower()}_down", b_dn)

        def press(dirn):
            if self.estopped:
                return
            axis.dir = +1 if dirn > 0 else -1
            axis.apply(self.mot)
            getattr(self, f"var_{axis.name}_cmd").set(f"{axis.last_cmd:+d}")

        def release(_e=None):
            axis.brake(self.mot)
            getattr(self, f"var_{axis.name}_cmd").set("0")

        b_up.bind("<ButtonPress-1>", lambda _e: press(+1))
        b_up.bind("<ButtonRelease-1>", release)
        b_dn.bind("<ButtonPress-1>", lambda _e: press(-1))
        b_dn.bind("<ButtonRelease-1>", release)

        return f

    def _sequence_panel(self):
        f = ttk.LabelFrame(self, text="Automated Sequence")

        row0 = ttk.Frame(f)
        row0.pack(fill=tk.X, padx=8, pady=(8, 2))

        ttk.Label(row0, text="Cycles:").pack(side=tk.LEFT)
        tk.Spinbox(row0, from_=1, to=999, increment=1, width=6, textvariable=self.seq_cycles).pack(side=tk.LEFT, padx=(4, 12))

        ttk.Label(row0, text="High dwell (s):").pack(side=tk.LEFT)
        tk.Spinbox(row0, from_=0.0, to=60.0, increment=0.1, width=8, textvariable=self.seq_top_dwell).pack(side=tk.LEFT, padx=(4, 12))

        ttk.Label(row0, text="Low dwell (s):").pack(side=tk.LEFT)
        tk.Spinbox(row0, from_=0.0, to=60.0, increment=0.1, width=8, textvariable=self.seq_bottom_dwell).pack(side=tk.LEFT, padx=(4, 0))

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

    # ---------- callbacks ----------

    def _on_weight_speed(self, value: str):
        if self.weights_controller:
            self.weights_controller.set_speed_percent(float(value))
        self.weights_manual.speed_percent = float(value)

    def _on_weights_tolerance_change(self, *_):
        if self.weights_controller:
            try:
                tol = int(self.weights_tolerance.get())
            except Exception:
                tol = config.WEIGHTS_TOLERANCE_DEFAULT
            self.weights_controller.set_tolerance(tol)
        self._update_tolerance_label()

    def _set_target_from_current(self, target: str):
        if not self.encoder:
            return
        current = self.encoder.get_position()
        if target == "top":
            self.weights_top_target.set(current)
        else:
            self.weights_bottom_target.set(current)
        self.lbl_status.config(text=f"Set {target} target to {current} counts")

    def _weights_manual_press(self, direction: int):
        if self.estopped:
            return
        if self.weights_controller:
            self.weights_controller.stop()
        if self.weights_controller:
            self.weights_controller.stop(coast=True)
        self.weights_manual.dir = 1 if direction > 0 else -1
        self.weights_manual.apply(self.mot)
        self.var_weights_cmd.set(f"{self.weights_manual.last_cmd:+d}")

    def _weights_manual_release(self, _event=None):
        self.weights_manual.brake(self.mot)
        if self.weights_controller and not self.sequence_running and not self.estopped:
            self.weights_controller.stop()
        self.var_weights_cmd.set("0")

    def _move_weights_with_speed(self, target: int, speed_override: float | None = None, retension: bool = False, label: str = "") -> bool:
        """Move weights to a target with optional temporary speed override and retension lift."""
        wc = self.weights_controller
        if not wc:
            return False
        prev_speed = wc.speed_percent
        success = False
        try:
            if speed_override is not None:
                wc.set_speed_percent(speed_override)
            wc.move_to(int(target))
            if label:
                self.lbl_status.config(text=f"Moving weights to {label} ({target} counts)")
            success = self._wait_for_weights_target(target, timeout=120.0)
            if retension and config.WEIGHTS_BOTTOM_RETENSION_COUNTS > 0 and success:
                up_target = int(target) + int(config.WEIGHTS_BOTTOM_RETENSION_COUNTS)
                wc.set_speed_percent(config.WEIGHTS_BOTTOM_RETENSION_SPEED_PCT)
                self.lbl_status.config(text=f"Retensioning (+{config.WEIGHTS_BOTTOM_RETENSION_COUNTS} counts)")
                wc.move_to(up_target)
                success = self._wait_for_weights_target(up_target, timeout=20.0) and success
        finally:
            wc.set_speed_percent(prev_speed)
        return success

    def move_weights_to_top(self):
        if not self.weights_controller:
            return
        target = int(self.weights_top_target.get())
        self._move_weights_with_speed(target, label="top")

    def move_weights_to_bottom(self):
        if not self.weights_controller:
            return
        target = int(self.weights_bottom_target.get())
        self._move_weights_with_speed(
            target,
            speed_override=config.WEIGHTS_BOTTOM_SLOW_SPEED_PCT,
            retension=True,
            label="bottom",
        )

    def stop_weights(self):
        if not self.weights_controller:
            return
        self.weights_controller.stop()
        self.lbl_status.config(text="Weights stopped")

    def zero_weights_encoder(self):
        if not self.encoder:
            self.lbl_status.config(text="Encoder unavailable")
            return
        try:
            self.encoder.set_position(0)
            if self.weights_controller:
                self.weights_controller.stop()
            self.weights_top_target.set(config.WEIGHTS_TOP_DEFAULT)
            self.weights_bottom_target.set(0)
            self.lbl_status.config(text="Weights encoder zeroed")
        except Exception as exc:
            self.lbl_status.config(text=f"Zero failed: {exc}")

    def _save_pid_settings(self):
        try:
            kp = float(self.pid_kp.get())
            kd = float(self.pid_kd.get())
            deadband = float(self.pid_deadband.get())
            min_output = float(self.pid_min_output.get())
        except Exception as exc:
            self.lbl_status.config(text=f"Invalid PD input: {exc}")
            return

        if self.weights_controller:
            self.weights_controller.pid.kp = kp
            self.weights_controller.pid.kd = kd
            self.weights_controller.pid.reset()
            self.weights_controller.pid_deadband = deadband
            self.weights_controller._min_output = max(0.0, min_output)
            self.lbl_status.config(text="Applied PD gains")

        data = config.load_calibration()
        pid_cfg = data.setdefault("weights_pid", {})
        pid_cfg["kp"] = kp
        pid_cfg["kd"] = kd
        pid_cfg["deadband_counts"] = deadband
        pid_cfg["min_output"] = min_output
        try:
            config.save_calibration(data)
            config.reload_calibration()
            self.lbl_status.config(text="Saved PD gains")
        except Exception as exc:
            self.lbl_status.config(text=f"PD save failed: {exc}")

    def _save_weights_targets(self):
        data = config.load_calibration()
        weights_cfg = data.setdefault("weights_encoder", {})
        weights_cfg["top_counts"] = int(self.weights_top_target.get())
        weights_cfg["bottom_counts"] = int(self.weights_bottom_target.get())
        weights_cfg["tolerance_counts"] = int(self.weights_tolerance.get())
        try:
            config.save_calibration(data)
            config.reload_calibration()
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
            getattr(self, "btn_weights_set_top", None),
            getattr(self, "btn_weights_set_bottom", None),
            getattr(self, "btn_weights_manual_up", None),
            getattr(self, "btn_weights_manual_down", None),
        ):
            if btn is not None:
                btn.config(state=state_motion)
        self._update_sequence_controls()

    def _update_tolerance_label(self):
        try:
            tol_counts = int(self.weights_tolerance.get())
        except Exception:
            tol_counts = 0
        mm = config.counts_to_mm(tol_counts)
        self.var_tolerance_mm.set(f"≈ {mm:0.2f} mm")

    # ---------- refresh loops ----------

    def _refresh_commands(self):
        if not self.estopped:
            for axis in (self.gantry, self.weights_manual):
                if axis and axis.dir != 0:
                    axis.apply(self.mot)
                    if axis is self.weights_manual:
                        self.var_weights_cmd.set(f"{axis.last_cmd:+d}")
                    else:
                        getattr(self, f"var_{axis.name}_cmd").set(f"{axis.last_cmd:+d}")
        self.after(int(1000.0 / config.CMD_REFRESH_HZ), self._refresh_commands)

    def _refresh_vin(self):
        try:
            v = self.mot.vin_v()
            if v == v:
                self.lbl_vin.config(text=f"VIN: {v:0.1f} V")
        except Exception:
            pass
        self.after(int(1000.0 / config.VIN_REFRESH_HZ), self._refresh_vin)

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

    # ---------- sequences ----------

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
                if not self._move_weights_with_speed(top_target, label="top"):
                    success = False
                    break
                if not self._sleep_with_cancel(top_dwell):
                    success = False
                    break

                if self.sequence_stop.is_set() or self.estopped:
                    success = False
                    break

                self._set_sequence_status(f"Cycle {idx}/{cycles}: moving to bottom")
                if not self._move_weights_with_speed(
                    bottom_target,
                    speed_override=config.WEIGHTS_BOTTOM_SLOW_SPEED_PCT,
                    label="bottom",
                ):
                    success = False
                    break
                if not self._sleep_with_cancel(bottom_dwell):
                    success = False
                    break

                if config.WEIGHTS_BOTTOM_RETENSION_COUNTS > 0:
                    self._set_sequence_status(f"Cycle {idx}/{cycles}: retensioning")
                    ret_target = bottom_target + config.WEIGHTS_BOTTOM_RETENSION_COUNTS
                    if not self._move_weights_with_speed(
                        ret_target,
                        speed_override=config.WEIGHTS_BOTTOM_RETENSION_SPEED_PCT,
                        label="retension",
                    ):
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

    # ---------- logging ----------

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
            os.makedirs(config.LOG_DIR, exist_ok=True)
            ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            prefix = "sequence" if auto else "session"
            path = config.LOG_DIR / f"{prefix}_{ts}.csv"
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
        self.mot.coast_all()
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
            if self.adc:
                self.adc.stop()
        except Exception:
            pass
        self.destroy()
