"""Tkinter UI for the Tonometry controller."""

from __future__ import annotations

import csv
import datetime
import queue
import signal
import threading
import time
import tkinter as tk
from tkinter import filedialog, ttk
from pathlib import Path
from typing import Optional
from collections import deque

from .live_plot import LiveLineRenderer
from .sensor_logging import SensorLog
from . import config
from .controllers import AxisManual, WeightsPositionController
from .hardware import ADCReader, Motoron, QuadratureEncoder

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure

GUI_REFRESH_HZ = 30
GUI_REFRESH_MS = round(1000 / GUI_REFRESH_HZ)


class ControllerApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self._closing = False
        self._shutdown_requested = False
        self._previous_sigint_handler = None
        self.title("Tonometry Controller")
        self.geometry("1100x800")

        self.mot = Motoron(config.MOTORON_ADDR, config.I2C_BUS, allow_braking=False)
        self.encoder = None
        self.encoder_error = None
        try:
            self.encoder = QuadratureEncoder(config.ENCODER_GPIO_A, config.ENCODER_GPIO_B, invert=config.ENCODER_INVERT)
        except Exception as exc:
            self.encoder = None
            self.encoder_error = str(exc)

        self._sensor_log = None
        self.adc = ADCReader(queue_samples=False)
        self.adc.sample_sink = self._record_lvdt_sample
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
        self.weights_manual = AxisManual("Weights", (1,), (config.MOTOR_INVERT[1],), torque_floor=config.TORQUE_FLOOR)

        # The acquisition thread owns the LabJack handle; Tk only reads snapshots.
        self._ati_stop = threading.Event()
        self._ati_lock = threading.Lock()
        self._ati_sample = None
        self._ati_error = "Connecting..."
        self._ati_bias = None
        self._ati_thread = threading.Thread(target=self._ati_worker, daemon=True)
        self._ati_thread.start()

        self.estopped = False
        self._ui_messages = queue.SimpleQueue()
        self._motion_thread = None
        self._motion_stop = threading.Event()
        self._manual_speed_override = None

        self.weights_top_target = tk.IntVar(value=config.WEIGHTS_TOP_DEFAULT)
        self.weights_bottom_target = tk.IntVar(value=config.WEIGHTS_BOTTOM_DEFAULT)
        self.weights_tolerance = tk.IntVar(value=config.WEIGHTS_TOLERANCE_DEFAULT)
        self.pid_kp = tk.DoubleVar(value=config.WEIGHTS_PID_KP)
        self.pid_ki = tk.DoubleVar(value=config.WEIGHTS_PID_KI)
        self.pid_kd = tk.DoubleVar(value=config.WEIGHTS_PID_KD)
        self.pid_deadband = tk.DoubleVar(value=config.WEIGHTS_PID_DEADBAND)
        self.pid_min_output = tk.DoubleVar(value=config.WEIGHTS_PID_MIN_OUTPUT)

        self.save_directory = tk.StringVar(value=str(config.REPO_ROOT))
        self.user_number = tk.StringVar(value="0")
        self.trial_number = tk.StringVar(value="0")
        self.seq_cycles = tk.IntVar(value=3)
        self.seq_top_dwell = tk.DoubleVar(value=2.0)
        self.seq_bottom_dwell = tk.DoubleVar(value=2.0)

        self.weights_tolerance.trace_add("write", lambda *_: self._on_weights_tolerance_change())

        self.var_tolerance_mm = tk.StringVar(value="")

        self.sequence_thread: Optional[threading.Thread] = None
        self.sequence_stop = threading.Event()
        self.sequence_running = False
        self._sequence_result: Optional[tuple[bool, int, int]] = None

        self._diagnostic_target = None
        self.logging_active = False
        self.logger_stop = threading.Event()
        self.logger_thread: Optional[threading.Thread] = None
        self.log_fh: Optional[object] = None
        self.log_writer: Optional[csv.writer] = None
        self.log_path: Optional[str] = None
        self._sequence_status_text = "Sequence idle"
        self.sequence_logging_owned = False

        self.manual_control = tk.BooleanVar(value=True)
        self.manual_control.trace_add("write", lambda *_: self._update_control_mode())
        self._gui_ticks = {name: deque() for name in ("gui", "lvdt", "ati")}
        self._gui_report_time = time.monotonic()
        self._build_ui()
        self._update_tolerance_label()

        self.after(int(1000.0 / config.CMD_REFRESH_HZ), self._refresh_commands)
        self.after(int(1000.0 / config.VIN_REFRESH_HZ), self._refresh_vin)
        if self.adc.enabled:
            self.after(200, self._refresh_adc)
        self.after(200, self._refresh_adc_plot)

        self.after(GUI_REFRESH_MS, self._refresh_gui_rate)
        self.after(GUI_REFRESH_MS, self._refresh_ati_plot)
        self.after(100, self._refresh_weights_status)
        self.after(200, self._refresh_sequence_label)
        self.after(100, self._poll_sequence_thread)

        self.bind("<Escape>", self.emergency_stop)
        self.protocol("WM_DELETE_WINDOW", self.on_close)
        if threading.current_thread() is threading.main_thread():
            self._previous_sigint_handler = signal.getsignal(signal.SIGINT)
            signal.signal(signal.SIGINT, self._request_shutdown)


    def _build_ui(self):
        pad = {"padx": 8, "pady": 6}
        top = ttk.Frame(self)
        top.pack(fill=tk.X, **pad)

        self.adc_plot_window_sec = 30.0   # Recent 30 seconds
        self.adc_plot_dt = GUI_REFRESH_MS / 1000.0
        self._lvdt_bias = None
        self.adc_time_buf = deque()
        self.adc_pos_buf = deque()
        self.adc_plot_t0 = None
        self.ati_plot_t0 = None

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
        ttk.Button(top, text="STOP (Coast)", command=self.stop_all).pack(side=tk.RIGHT)

        self.btn_log = ttk.Button(top, text="Start Log", command=self.toggle_logging)
        self.btn_log.pack(side=tk.RIGHT, padx=(6, 0))
        ttk.Button(top, text="Zero Encoder", command=self.zero_weights_encoder).pack(side=tk.RIGHT, padx=(6, 0))
        ttk.Separator(self, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=(2, 2))

        self.var_gui_rate = tk.StringVar(value="GUI callback: -- Hz | LVDT draw: -- FPS | ATI draw: -- FPS")
        ttk.Label(self, textvariable=self.var_gui_rate).pack(anchor=tk.W, padx=8, pady=2)

        controls = ttk.Frame(self)
        controls.pack(fill=tk.X, **pad)
        controls.columnconfigure(0, weight=3)
        controls.columnconfigure(1, weight=2)
        self._weights_card(controls).grid(row=0, column=0, sticky="nsew", padx=(0, 4))
        self._axis_card(self.gantry, controls).grid(row=0, column=1, sticky="nsew", padx=(4, 0))
        self._sequence_panel().pack(fill=tk.X, **pad)
        plots = ttk.Frame(self)
        plots.pack(fill=tk.BOTH, expand=True, **pad)
        plots.columnconfigure((0, 1), weight=1, uniform="sensor_plots")
        plots.rowconfigure(0, weight=1)
        self._adc_plot_panel(plots).grid(row=0, column=0, sticky="nsew")
        self._ati_plot_panel(plots).grid(row=0, column=1, sticky="nsew")

    def _adc_plot_panel(self, master=None):
        f = ttk.Frame(master or self)
        controls = ttk.Frame(f)
        controls.pack(fill=tk.X, padx=8, pady=4)
        ttk.Button(controls, text="Bias Displacement", command=self._bias_lvdt,
                   state=tk.NORMAL if self.adc.enabled else tk.DISABLED).pack(side=tk.LEFT)
        ttk.Button(controls, text="Remove Bias", command=self._clear_lvdt_bias).pack(side=tk.LEFT, padx=8)
        self.var_lvdt_status = tk.StringVar(value="LVDT: Unbiased" if self.adc.enabled else "LVDT: disabled")
        ttk.Label(f, textvariable=self.var_lvdt_status).pack(fill=tk.X, padx=8)

        self.adc_fig = Figure(figsize=(5, 3), dpi=100, constrained_layout=True)
        self.adc_ax = self.adc_fig.add_subplot(111)
        self.adc_ax.set_title("LVDT Position")
        self.adc_ax.set_xlabel("Time (s)")
        self.adc_ax.set_ylabel("Position (mm)")
        self.adc_ax.grid(True)
        self.adc_ax.set_ylim(-3, 33)
        self.adc_ax.set_xlim(0, self.adc_plot_window_sec)

        self.adc_line, = self.adc_ax.plot([], [])

        self.adc_canvas = FigureCanvasTkAgg(self.adc_fig, master=f)
        self.adc_canvas.draw()
        self._adc_renderer = LiveLineRenderer(self.adc_canvas, self.adc_ax, self.adc_line,
                                               lambda: self._mark_gui_tick("lvdt"))
        self._adc_unavailable = ttk.Label(f, text="LVDT: Initializing…", anchor=tk.CENTER, wraplength=400)
        self._adc_unavailable.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        self._adc_plot_visible = False

        return f

    def _set_sensor_plot_available(self, kind, available, reason=""):
        canvas = self.adc_canvas if kind == "adc" else self.ati_canvas
        message = getattr(self, f"_{kind}_unavailable")
        flag = f"_{kind}_plot_visible"
        if available:
            if not getattr(self, flag):
                message.pack_forget()
                canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
                setattr(self, flag, True)
        else:
            if getattr(self, flag):
                canvas.get_tk_widget().pack_forget()
                message.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
                setattr(self, flag, False)
            name = "LVDT" if kind == "adc" else "LabJack ATI"
            message.config(text=f"{name} Not connected / Initialization failed\n{reason}")

    def _mark_gui_tick(self, name):
        self._gui_ticks[name].append(time.monotonic())

    def _request_shutdown(self, _signum, _frame):
        # Defer cleanup to Tk's event loop instead of running it in a signal handler.
        self._shutdown_requested = True

    def _refresh_gui_rate(self):
        if self._shutdown_requested:
            self.on_close()
            return
        now = time.monotonic()
        self._mark_gui_tick("gui")
        rates = {}
        for name, ticks in self._gui_ticks.items():
            while ticks and ticks[0] < now - 2.0:
                ticks.popleft()
            rates[name] = ((len(ticks) - 1) / (ticks[-1] - ticks[0])
                           if len(ticks) > 1 and ticks[-1] > ticks[0] else 0.0)
        if now - self._gui_report_time >= 1.0:
            self.var_gui_rate.set(
                f"GUI callback: {rates['gui']:.1f} Hz | "
                f"LVDT draw: {rates['lvdt']:.1f} FPS | ATI draw: {rates['ati']:.1f} FPS"
            )
            self._gui_report_time = now
        self.after(GUI_REFRESH_MS, self._refresh_gui_rate)

    def _record_lvdt_sample(self, timestamp_ns, monotonic_time, voltage, position):
        log = self._sensor_log
        if log is not None:
            log.offer("lvdt", timestamp_ns, monotonic_time, (voltage, position))

    def _ati_worker(self):
        """Acquire raw calibrated forces without calling Tk from this thread."""
        while not self._ati_stop.is_set():
            reader = None
            try:
                from .labjackati import LabjackATIReader

                reader = LabjackATIReader(
                    sensor_num="44297", bias_mode=False, acquisition_rate=800,
                )
                started = time.monotonic()
                count = 0
                hz = 0.0
                while not self._ati_stop.is_set():
                    values = reader.read()
                    now = time.monotonic()
                    timestamp_ns = time.time_ns()
                    values = tuple(float(v) for v in values)
                    log = self._sensor_log
                    if log is not None:
                        bias = self._ati_bias
                        offset = (0.0, 0.0, 0.0) if bias is None else bias
                        log.offer("ati", timestamp_ns, now, (*values, bias is not None, *offset))
                    count += 1
                    if now - started >= 1.0:
                        hz = count / (now - started)
                        started, count = now, 0
                    with self._ati_lock:
                        self._ati_sample = (now, tuple(float(v) for v in values), hz)
                        self._ati_error = ""
            except Exception as exc:
                with self._ati_lock:
                    self._ati_sample = None
                    self._ati_error = str(exc)
            finally:
                if reader is not None:
                    try:
                        reader.shutdown()
                    except Exception:
                        pass
            if not self._ati_stop.is_set():
                self._ati_stop.wait(5.0)

    def _ati_plot_panel(self, master):
        panel = ttk.Frame(master)
        controls = ttk.Frame(panel)
        controls.pack(fill=tk.X, padx=8, pady=4)
        self.btn_ati_bias = ttk.Button(controls, text="Bias ATI Force", command=self._bias_ati)
        self.btn_ati_bias.pack(side=tk.LEFT)
        ttk.Button(controls, text="Remove Bias", command=self._clear_ati_bias).pack(side=tk.LEFT, padx=8)
        self.var_ati_status = tk.StringVar(value="ATI: Connecting...")
        ttk.Label(panel, textvariable=self.var_ati_status).pack(fill=tk.X, padx=8)
        self.ati_time_buf = deque()
        self.ati_force_buf = deque()
        self._ati_last_time = None
        self.ati_fig = Figure(figsize=(5, 3), dpi=100, constrained_layout=True)
        self.ati_ax = self.ati_fig.add_subplot(111)
        self.ati_ax.set_xlabel("Time (s)")
        self.ati_ax.set_ylabel("Fz (N)")
        self.ati_ax.set_title("ATI Fz — Unbiased")
        self.ati_ax.set_xlim(0, self.adc_plot_window_sec)
        self.ati_ax.set_ylim(-25, 25)
        self.ati_ax.grid(True)
        self.ati_line, = self.ati_ax.plot([], [])
        self.ati_canvas = FigureCanvasTkAgg(self.ati_fig, master=panel)
        self.ati_canvas.draw()
        self._ati_renderer = LiveLineRenderer(self.ati_canvas, self.ati_ax, self.ati_line,
                                               lambda: self._mark_gui_tick("ati"))
        self._ati_unavailable = ttk.Label(panel, text="LabJack ATI: Connecting…", anchor=tk.CENTER, wraplength=400)
        self._ati_unavailable.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        self._ati_plot_visible = False
        return panel

    def _bias_ati(self):
        with self._ati_lock:
            sample = self._ati_sample
        if sample is None:
            self.var_ati_status.set("ATI unavailable: cannot bias")
            return
        # Keep raw history so switching modes updates the entire visible curve.
        self._ati_bias = sample[1][:3]

    def _clear_ati_bias(self):
        self._ati_bias = None

    def _refresh_ati_plot(self):
        with self._ati_lock:
            sample, error = self._ati_sample, self._ati_error
        self.btn_ati_bias.state(["!disabled"] if sample is not None else ["disabled"])
        self._set_sensor_plot_available("ati", sample is not None, error or "Waiting for valid data")
        if sample is None:
            self.var_ati_status.set(f"ATI unavailable: {error}")
        else:
            timestamp, forces, hz = sample
            if timestamp != self._ati_last_time:
                if self.ati_plot_t0 is None:
                    self.ati_plot_t0 = timestamp
                now = timestamp - self.ati_plot_t0
                self.ati_time_buf.append(now)
                self.ati_force_buf.append(forces[2])
                self._ati_last_time = timestamp
                while self.ati_time_buf and now - self.ati_time_buf[0] > self.adc_plot_window_sec:
                    self.ati_time_buf.popleft()
                    self.ati_force_buf.popleft()
            offset = 0.0 if self._ati_bias is None else self._ati_bias[2]
            mode = "Unbiased" if self._ati_bias is None else "Biased"
            self.var_ati_status.set(f"Fz: {forces[2] - offset:+.3f} N | {hz:.1f} Hz | {mode}")
            # Move the axis at one-second intervals; keep line updates at the GUI refresh rate.
            right = max(self.adc_plot_window_sec, float(int(self.ati_time_buf[-1]) + 1))
            self._ati_renderer.render(
                list(self.ati_time_buf), [v - offset for v in self.ati_force_buf],
                xlim=(max(0.0, right - self.adc_plot_window_sec), right),
                ylim=(-25.0, 25.0), title=f"ATI Fz — {mode}",
            )
        self.after(GUI_REFRESH_MS, self._refresh_ati_plot)

    def _weights_card(self, master=None):
        f = ttk.LabelFrame(master or self, text="Weights (Motor 1)")

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

        row7 = ttk.Frame(f)
        row7.pack(fill=tk.X, padx=8, pady=(4, 2))
        self.chk_retension = ttk.Checkbutton(
            row7,
            text="Manual Control Mode",
            variable=self.manual_control,
        )
        self.chk_retension.pack(side=tk.LEFT, padx=(12, 0))

        row4 = ttk.Frame(f)
        row4.pack(fill=tk.X, padx=8, pady=(6, 2))
        ttk.Label(row4, text="Manual control:").pack(side=tk.LEFT)
        self.btn_weights_manual_up = ttk.Button(row4, text="▲ Hold UP")
        self.btn_weights_manual_up.pack(side=tk.LEFT, padx=(6, 6))
        self.btn_weights_manual_down = ttk.Button(row4, text="▼ Hold DOWN")
        self.btn_weights_manual_down.pack(side=tk.LEFT)
        self.btn_weights_manual_up.bind("<ButtonPress-1>", lambda _e: self._weights_manual_press_up(+1))
        self.btn_weights_manual_up.bind("<ButtonRelease-1>", self._weights_manual_release)
        self.btn_weights_manual_down.bind("<ButtonPress-1>", lambda _e: self._weights_manual_press_down(-1))
        self.btn_weights_manual_down.bind("<ButtonRelease-1>", self._weights_manual_release)

        row5 = ttk.Frame(f)
        row5.pack(fill=tk.X, padx=8, pady=(6, 8))

        self.btn_weights_stop = ttk.Button(row5, text="Stop", command=self.stop_weights)
        self.btn_weights_stop.pack(side=tk.LEFT)
        ttk.Button(row5, text="Save Targets", command=self._save_weights_targets).pack(side=tk.LEFT, padx=(12, 0))

        if self.encoder_error:
            ttk.Label(f, text=f"Encoder unavailable: {self.encoder_error}", foreground="#c62828").pack(fill=tk.X, padx=8, pady=(0, 8))

        self._update_weights_controls_state()
        return f

    def _axis_card(self, axis: AxisManual, master=None):
        f = ttk.LabelFrame(master or self, text=f"{axis.name} (Motors: {', '.join(map(str, axis.motor_ids))})")

        r0 = ttk.Frame(f)
        r0.pack(fill=tk.X, padx=8, pady=(8, 2))
        ttk.Label(r0, text="Speed %:").pack(side=tk.LEFT)
        s = ttk.Scale(r0, from_=0, to=100, orient=tk.HORIZONTAL, length=180, command=lambda v, a=axis: setattr(a, "speed_percent", float(v)))
        s.set(axis.speed_percent)
        s.pack(side=tk.LEFT, padx=(6, 12))
        status = ttk.Frame(f)
        status.pack(fill=tk.X, padx=8, pady=4)
        ttk.Label(status, text=f"(floor {axis.torque_floor})").pack(side=tk.LEFT)
        ttk.Label(status, text=" | cmd: ").pack(side=tk.LEFT, padx=(12, 0))
        var_cmd = tk.StringVar(value="0")
        setattr(self, f"var_{axis.name}_cmd", var_cmd)
        ttk.Label(status, textvariable=var_cmd, width=8).pack(side=tk.LEFT)

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
            getattr(self, f"var_{axis.name}_cmd").set(f"{int(axis.last_cmd):+d}")

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

        directory_row = ttk.Frame(f)
        directory_row.pack(fill=tk.X, padx=8, pady=4)
        ttk.Label(directory_row, text="User #:").pack(side=tk.LEFT)
        ttk.Entry(directory_row, textvariable=self.user_number, width=8).pack(side=tk.LEFT, padx=(4, 12))
        ttk.Label(directory_row, text="Trial #:").pack(side=tk.LEFT)
        ttk.Entry(directory_row, textvariable=self.trial_number, width=8).pack(side=tk.LEFT, padx=(4, 12))
        ttk.Label(directory_row, text="Project directory:").pack(side=tk.LEFT)
        ttk.Entry(directory_row, textvariable=self.save_directory).pack(
            side=tk.LEFT, fill=tk.X, expand=True, padx=6,
        )
        ttk.Button(directory_row, text="Browse...", command=self._browse_save_directory).pack(side=tk.LEFT)
        ttk.Label(f, text="Saved to: Project directory/User_Data/User#/Trial#/Force.csv and LVDT.csv. Changes apply to the next recording.").pack(
            anchor=tk.W, padx=8,
        )

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

    def _browse_save_directory(self):
        current = Path(self.save_directory.get().strip()).expanduser()
        selected = filedialog.askdirectory(
            parent=self, title="Select project directory",
            initialdir=str(current) if current.is_dir() else str(config.REPO_ROOT),
            mustexist=False,
        )
        if selected:
            self.save_directory.set(selected)

    # ---------- callbacks ----------

    def _bias_lvdt(self):
        if self.adc.enabled:
            self._lvdt_bias = self.adc.latest_pos
            self._update_lvdt_display()

    def _clear_lvdt_bias(self):
        self._lvdt_bias = None
        self._update_lvdt_display()

    def _update_lvdt_display(self):
        offset = 0.0 if self._lvdt_bias is None else self._lvdt_bias
        mode = "Unbiased" if self._lvdt_bias is None else "Biased"
        self.var_lvdt_status.set(f"Position: {self.adc.latest_pos - offset:+.3f} mm | {self.adc.hz:.1f} Hz | {mode}")
        right = max(self.adc_plot_window_sec,
                    float(int(self.adc_time_buf[-1]) + 1) if self.adc_time_buf else self.adc_plot_window_sec)
        self._adc_renderer.render(
            list(self.adc_time_buf), [v - offset for v in self.adc_pos_buf],
            xlim=(max(0.0, right - self.adc_plot_window_sec), right),
            ylim=(-3.0 - offset, 33.0 - offset), title=f"LVDT Position — {mode}",
        )

    def _refresh_adc_plot(self):
        available = self.adc.enabled and self.adc.has_sample and not self.adc.error
        self._set_sensor_plot_available("adc", available, self.adc.error or "Waiting for valid data")
        if not available:
            self.var_lvdt_status.set("LVDT: Not connected / Waiting for data")
            self.after(int(self.adc_plot_dt * 1000), self._refresh_adc_plot)
            return

        timestamp = time.monotonic()
        if self.adc_plot_t0 is None:
            self.adc_plot_t0 = timestamp
        now = timestamp - self.adc_plot_t0
        pos = self.adc.latest_pos

        self.adc_time_buf.append(now)
        self.adc_pos_buf.append(pos)

        # Only keep recent data within the plot window
        while self.adc_time_buf and (now - self.adc_time_buf[0] > self.adc_plot_window_sec):
            self.adc_time_buf.popleft()
            self.adc_pos_buf.popleft()

        self._update_lvdt_display()

        self.after(int(self.adc_plot_dt * 1000), self._refresh_adc_plot)

    def _on_weight_speed(self, value: str):
        speed = float(value)
        if self.weights_controller:
            self.weights_controller.set_speed_percent(speed)
        self.weights_manual.speed_percent = speed

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

    def _weights_manual_press_up(self, direction: int):
        if self.estopped or not self.manual_control.get():
            return
        self._manual_speed_override = None
        self.weights_manual.dir = 1 if direction > 0 else -1
        self.weights_manual.apply(self.mot)
        self.var_weights_cmd.set(f"{int(self.weights_manual.last_cmd):+d}")

    def _weights_manual_press_down(self, direction: int):
        if self.estopped or not self.manual_control.get():
            return
        self.weights_manual.dir = 1 if direction > 0 else -1
        self._manual_speed_override = config.MANUAL_DOWN_SPEED_PCT
        self.weights_manual.apply(self.mot, self._manual_speed_override)
        self.var_weights_cmd.set(f"{int(self.weights_manual.last_cmd):+d}")

    def _weights_manual_release(self, _event=None):
        self._manual_speed_override = None
        self.weights_manual.brake(self.mot)
        if self.weights_controller and not self.sequence_running and not self.estopped:
            self.weights_controller.stop()
        self.var_weights_cmd.set("0")

    def _move_weights_with_speed(self, target: int, speed_override: float | None = None, retension: bool = False, label: str = "") -> bool:
        """Move weights to a target with optional temporary speed override and retension lift."""
        if not self.weights_controller or self.estopped or self._motion_stop.is_set():
            return False
        prev_speed = self.weights_controller.speed_percent
        success = False
        try:
            # Change the commanded speed
            if speed_override is not None:
                self.weights_controller.set_speed_percent(speed_override)
            else:
                self.weights_controller.set_speed_percent(config.PID_LIFT_SPEED_PCT)
            # Set the target position
            self._diagnostic_target = int(target)
            self.weights_controller.move_to(int(target))

            # Send status to the main UI thread.
            if label:
                self._ui_messages.put(f"Moving weights to {label} ({target} counts)")
            
            success = self._wait_for_weights_target(target, timeout=120.0)
            if success and not (self.estopped or self._motion_stop.is_set() or self.sequence_stop.is_set()):
                if label == "top":
                    self._ui_messages.put("On top")
                elif label == "bottom":
                    self._ui_messages.put("On bottom")
                else:
                    self._ui_messages.put(f"Target reached ({target} counts)")



            if retension and config.WEIGHTS_BOTTOM_RETENSION_COUNTS > 0 and success:
                up_target = int(target) + int(config.WEIGHTS_BOTTOM_RETENSION_COUNTS)
                self.weights_controller.set_speed_percent(config.WEIGHTS_BOTTOM_RETENSION_SPEED_PCT)
                self._ui_messages.put(f"Retensioning (+{config.WEIGHTS_BOTTOM_RETENSION_COUNTS} counts)")
                self.weights_controller.move_to(up_target)
                success = self._wait_for_weights_target(up_target, timeout=20.0) and success
        finally:
            self.weights_controller.set_speed_percent(prev_speed)
        return success

    def move_weights_to_top(self):
        if not self.weights_controller or self.manual_control.get():
            print("Button Disabled")
            return
        target = int(self.weights_top_target.get())
        self._start_target_move(target, label="top")

    def move_weights_to_bottom(self):
        if not self.weights_controller or self.manual_control.get():
            return
        target = int(self.weights_bottom_target.get())
        self._start_target_move(
            target,
            speed_override=config.WEIGHTS_BOTTOM_SLOW_SPEED_PCT,
            label="bottom",
        )

    def _start_target_move(self, target: int, **options):
        if (self.estopped or self.sequence_running
                or (self._motion_thread and self._motion_thread.is_alive())):
            return
        self._motion_stop.clear()

        def worker():
            try:
                self._move_weights_with_speed(target, **options)
            except Exception as exc:
                self._ui_messages.put(f"Move failed: {exc}")

        self._motion_thread = threading.Thread(target=worker, daemon=True)
        self._motion_thread.start()

    def stop_weights(self):
        self._motion_stop.set()
        self.weights_manual.brake(self.mot)
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
            ki = float(self.pid_ki.get())
            kd = float(self.pid_kd.get())
            deadband = float(self.pid_deadband.get())
            min_output = float(self.pid_min_output.get())
        except Exception as exc:
            self.lbl_status.config(text=f"Invalid PD input: {exc}")
            return

        if self.weights_controller:
            self.weights_controller.pid.kp = kp
            self.weights_controller.pid.ki = ki
            self.weights_controller.pid.kd = kd
            self.weights_controller.pid.reset()
            self.weights_controller.pid_deadband = deadband
            self.weights_controller._min_output = max(0.0, min_output)
            self.lbl_status.config(text="Applied PD gains")

        data = config.load_calibration()
        pid_cfg = data.setdefault("weights_pid", {})
        pid_cfg["kp"] = kp
        pid_cfg["ki"] = ki
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

    def _update_control_mode(self):
        if not self.weights_controller:
            return
        state_1 = tk.NORMAL if self.manual_control.get() else tk.DISABLED
        state_2 = tk.DISABLED if self.manual_control.get() else tk.NORMAL
        if self.manual_control.get():
            self.weights_controller._pause.clear()
        else:
            self.weights_controller._pause.set()
        for btn in (
            getattr(self, "btn_weights_manual_up", None),
            getattr(self, "btn_weights_manual_down", None),
        ):
            if btn is not None:
                btn.config(state=state_1)

        for btn in (
            getattr(self, "btn_weights_go_top", None),
            getattr(self, "btn_weights_go_bottom", None),
        ):
            if btn is not None:
                btn.config(state=state_2)


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
                    if axis is self.weights_manual:
                        axis.apply(self.mot, self._manual_speed_override)
                    else:
                        axis.apply(self.mot)
                    if axis is self.weights_manual:
                        self.var_weights_cmd.set(f"{int(axis.last_cmd):+d}")
                    else:
                        getattr(self, f"var_{axis.name}_cmd").set(f"{int(axis.last_cmd):+d}")
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
        if self.sequence_running or (self._motion_thread and self._motion_thread.is_alive()):
            self._set_sequence_status("Motion already running")
            return
        try:
            top_target = int(self.weights_top_target.get())
            bottom_target = int(self.weights_bottom_target.get())
            cycles = max(1, int(self.seq_cycles.get()))
            top_dwell = max(0.0, float(self.seq_top_dwell.get()))
            bottom_dwell = max(0.0, float(self.seq_bottom_dwell.get()))
        except Exception as exc:
            self._set_sequence_status(f"Invalid sequence input: {exc}")
            return

        auto_log_started = False
        if not self.logging_active:
            auto_log_started = self._start_logging(auto=True)
            if not auto_log_started:
                self._set_sequence_status("Cannot start sequence: check recording directory and trial number")
                return
        else:
            self.sequence_logging_owned = False

        self.sequence_stop.clear()
        self._motion_stop.clear()
        self.sequence_running = True
        self._update_sequence_controls()
        self._set_sequence_status(f"Sequence starting ({cycles} cycles)")
        self.lbl_status.config(text="Weights sequence running")
        self.sequence_logging_owned = auto_log_started
        self._update_weights_controls_state()

        self.sequence_thread = threading.Thread(
            target=self._sequence_worker,
            args=(cycles, top_dwell, bottom_dwell, top_target, bottom_target),
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

    def _sequence_worker(self, cycles: int, top_dwell: float, bottom_dwell: float,
                         top_target: int, bottom_target: int):
        completed = 0
        success = True

        try:
            for idx in range(1, cycles + 1):
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

                completed = idx

        except Exception as exc:
            success = False
            self._ui_messages.put(f"Sequence failed: {exc}")
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
        start = time.monotonic()
        while True:
            if self.sequence_stop.is_set() or self._motion_stop.is_set() or self.estopped:
                return False
            if self.weights_controller is None:
                return False
            current = self.encoder.get_position() if self.encoder else None
            if current is not None and abs(int(target) - int(current)) <= self.weights_controller.tolerance_counts:
                return True
            if self.weights_controller._arrive_flag and self.weights_controller.target_position is None:
                return True
            if timeout is not None and (time.monotonic() - start) > timeout:
                return False
            self.sequence_stop.wait(0.1)

    def _sleep_with_cancel(self, duration: float) -> bool:
        if duration <= 0:
            return True
        end = time.monotonic() + duration
        while time.monotonic() < end:
            if self.sequence_stop.is_set() or self._motion_stop.is_set() or self.estopped:
                return False
            self.sequence_stop.wait(min(0.05, max(0.0, end - time.monotonic())))
        return True

    def _poll_sequence_thread(self):
        while True:
            try:
                message = self._ui_messages.get_nowait()
            except queue.Empty:
                break
            self.lbl_status.config(text=message)
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
        fh = None
        try:
            directory_text = self.save_directory.get().strip()
            if not directory_text:
                raise ValueError("Please enter a save directory")
            user = self.user_number.get().strip()
            trial = self.trial_number.get().strip()
            if not user or not trial or any(c not in "0123456789" for c in user + trial):
                raise ValueError("User # and Trial # must contain digits only")
            directory = Path(directory_text).expanduser().resolve() / "User_Data" / f"User{user}" / f"Trial{trial}"
            if any((directory / name).exists() for name in ("Force.csv", "LVDT.csv")):
                raise FileExistsError("Trial already has sensor data; choose a new Trial #")
            directory.mkdir(parents=True, exist_ok=True)
            ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            prefix = "sequence" if auto else "session"
            path = directory / f"{prefix}_{ts}.csv"
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
                    "timestamp_ns", "elapsed_time", "record_dt_s",
                    "requested_target", "encoder_error_counts",
                    "manual_direction", "manual_cmd", "pid_enabled",
                    "arrive_flag", "holding_speed_pct",

                ]
            )
            sensor_log = SensorLog(path, filenames={"ati": "Force.csv", "lvdt": "LVDT.csv"})
        except Exception as exc:
            if fh is not None:
                fh.close()
            self.lbl_status.config(text=f"Log start failed: {exc}")
            return False

        self._sensor_log = sensor_log
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
        sensor_log = self._sensor_log
        self._sensor_log = None
        if sensor_log is not None:
            sensor_log.stop()
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
        message = "Logging stopped"
        if sensor_log is not None:
            message += f" | LVDT: {sensor_log.rows['lvdt']} | ATI: {sensor_log.rows['ati']} | dropped: {sensor_log.dropped}"
            if sensor_log.error:
                message += f" | Save error: {sensor_log.error}"
        self.lbl_status.config(text=message)

    def _log_loop(self):
        interval = 0.005  # Request 200 Hz encoder/control snapshots; actual dt is saved.
        rows_since_flush = 0
        next_time = time.monotonic()
        started = next_time
        previous = None
        while not self.logger_stop.is_set():
            next_time += interval
            now = time.monotonic()
            record_dt = "" if previous is None else now - previous
            previous = now
            timestamp_ns = time.time_ns()
            ts_iso = timestamp_ns / 1e9
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
                            timestamp_ns, now - started, record_dt,
                            self._diagnostic_target if self._diagnostic_target is not None else "",
                            self._diagnostic_target - encoder if self._diagnostic_target is not None and encoder != "" else "",
                            self.weights_manual.dir, self.weights_manual.last_cmd,
                            self.weights_controller._pause.is_set() if self.weights_controller else False,
                            self.weights_controller._arrive_flag if self.weights_controller else "",
                            self.weights_controller.top_holding_speed_pct if self.weights_controller else "",

                        ]
                    )
                    rows_since_flush += 1
            except Exception:
                pass

            if rows_since_flush >= 400 and self.log_fh:
                try:
                    self.log_fh.flush()
                except Exception:
                    pass
                rows_since_flush = 0

            sleep = next_time - time.monotonic()
            if sleep > 0:
                self.logger_stop.wait(min(sleep, interval))
            else:
                next_time = time.monotonic()

    # ---------- global actions ----------

    def stop_all(self):
        self._motion_stop.set()
        self.weights_manual.brake(self.mot)
        self.stop_sequence()
        if self.weights_controller:
            self.weights_controller.stop()
        self.gantry.brake(self.mot)
        self.mot.coast_all()
        self.lbl_status.config(text="STOPPED")

    def emergency_stop(self, *_):
        self.estopped = True
        self._motion_stop.set()
        self.weights_manual.dir = 0
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
        if self._closing:
            return
        self._closing = True
        self._ati_stop.set()
        self.estopped = True
        self._motion_stop.set()
        self.sequence_stop.set()
        self.weights_manual.dir = 0
        self.gantry.dir = 0
        self.mot.coast_all()
        self._stop_logging()
        for thread in (self.sequence_thread, self._motion_thread):
            if thread is not None:
                thread.join(timeout=0.5)
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
        self._ati_thread.join(timeout=1.0)
        if self.encoder is not None:
            self.encoder.close()
        if self._previous_sigint_handler is not None:
            signal.signal(signal.SIGINT, self._previous_sigint_handler)
        self.destroy()
