#!/usr/bin/env python3
"""
Manual Hold-to-Run Controller (Time-Based)
- Raspberry Pi + Pololu Motoron (I2C)
- Optional LVDT via dfrobot_ads1115_fast.FastADS1115I2C

Axes:
  - Weights = Motor 1
  - Gantry  = Motors 2 & 3 (sent same command)

Behavior:
  - Only Up and Down buttons per axis; motors run only while button is held.
  - Speed slider (0–100%) per axis.
  - On button release: **brake** that axis (holds tension).
  - STOP button: brake all.  E-STOP: **coast all** (latched) + ignore input until reset.
  - Top bar shows VIN and (if available) LVDT voltage, position (mm), and ADC sample rate (Hz).

Sequence (time-based only):
  - Repeated raise-for-time and lower-for-time.
  - Lowering speed is forced to ~50 (Motoron units), independent of slider.
  - CSV logger writes one row per **ADC sample** with columns:
        t_rel_s, lvdt_v_div100, lvdt_mm_div100, state
    (voltage and position are divided by 100 as requested).
"""

import sys, time, threading, json, os, csv, datetime
import tkinter as tk
from tkinter import ttk
from queue import Queue, Empty, Full

# ------------------------------ CONFIG ---------------------------------------

I2C_BUS = 1
MOTORON_ADDR = 0x10

# If "up" is the wrong way for a motor, flip it here:
MOTOR_INVERT = {
    1: True,   # Weights
    2: True,   # Gantry motor A
    3: False,  # Gantry motor B
}

# Motoron units
MOTORON_MAX    = 600
TORQUE_FLOOR   = 110     # min |command| when speed% > 0 to overcome stiction
CMD_REFRESH_HZ = 10.0    # periodic re-send to survive Motoron timeout
VIN_REFRESH_HZ = 2.0

# Load calibration (optional file)
calibration_file = {}
try:
    with open("calibration.json", "r", encoding="utf-8") as f:
        calibration_file = json.load(f)
except Exception:
    calibration_file = {}

# LVDT Calibration
LVDT_SLOPE = calibration_file.get("LVDT", {}).get("slope", 2.0114)
LVDT_INTERCEPT = calibration_file.get("LVDT", {}).get("intercept", 0.7342)

# ------------------------------ LIBS -----------------------------------------

sys.path.insert(1, "motoron-python/")
from motoron import MotoronI2C

ADC_AVAILABLE = True
try:
    from dfrobot_ads1115_fast import FastADS1115I2C
except Exception:
    ADC_AVAILABLE = False

# ------------------------------ LOW LEVEL ------------------------------------

class Motoron:
    def __init__(self, address=MOTORON_ADDR, bus=I2C_BUS):
        self.mc = MotoronI2C(address=address, bus=bus)
        self.mc.reinitialize()
        self.mc.disable_crc()
        self.mc.clear_reset_flag()
        self.mc.set_command_timeout_milliseconds(500)
        for m in (1,2,3):
            self.mc.set_max_acceleration(m, 300)
            self.mc.set_max_deceleration(m, 300)

    def set_speed(self, m: int, speed: int):
        s = max(-MOTORON_MAX, min(MOTORON_MAX, int(speed)))
        self.mc.set_speed(m, s)

    def coast_all(self):
        try: self.mc.coast_now()
        except Exception: pass

    def brake_all(self, amount=800):
        for m in (1,2,3):
            try: self.mc.set_braking_now(m, int(max(0, min(800, amount))))
            except Exception: pass

    def coast_motor(self, m: int):
        try: self.mc.set_braking_now(m, 0)
        except Exception: pass

    def brake_motor(self, m: int, amount=800):
        try: self.mc.set_braking_now(m, int(max(0, min(800, amount))))
        except Exception: pass

    def vin_v(self):
        try: return float(self.mc.get_vin_voltage_mv()) / 1000.0
        except Exception: return float("nan")

# ------------------------------ ADC THREAD -----------------------------------

class ADCReader(threading.Thread):
    def __init__(self, bus=I2C_BUS, addr=0x48, channel=1):
        super().__init__(daemon=True)
        self.enabled = ADC_AVAILABLE
        self.latest_v = 0.0
        self.pos_offset = self.convert_volt_to_pos(0.388)
        self.latest_pos = 0.0
        self.hz = 0.0
        self._stop = threading.Event()
        self.channel = channel

        # Per-sample queue so logger can run at ADC max rate
        self.samples: Queue[tuple[float, float, float]] = Queue(maxsize=20000)

        if not self.enabled: return
        try:
            self.adc = FastADS1115I2C(
                bus=bus, addr=addr, mv_to_v=True,
                init_conv_delay_s=0.0012,
                min_conv_delay_s=0.0010,
                max_conv_delay_s=0.0030
            )
            if not self.adc.begin():
                self.enabled = False
        except Exception:
            self.enabled = False

    def run(self):
        if not self.enabled: return
        t0, n = time.time(), 0
        while not self._stop.is_set():
            try:
                # The wrapper returns centivolts; convert to volts
                v = float(self.adc.get_value(self.channel)) / 100.0
                self.latest_v = v
                self.latest_pos = self.convert_volt_to_pos(v) - self.pos_offset
                n += 1

                # Push each sample with a timestamp
                ts = time.time()
                try:
                    self.samples.put_nowait((ts, self.latest_v, self.latest_pos))
                except Full:
                    # Drop oldest then insert newest
                    try:
                        _ = self.samples.get_nowait()
                        self.samples.put_nowait((ts, self.latest_v, self.latest_pos))
                    except Exception:
                        pass

            except Exception:
                time.sleep(0.001)

            now = time.time()
            if now - t0 >= 1.0:
                self.hz = n/(now - t0); n = 0; t0 = now

    def convert_volt_to_pos(self, v: float) -> float:
        # Input v: Volt; Output: mm
        return v * LVDT_SLOPE + LVDT_INTERCEPT

    def stop(self): self._stop.set()

# ------------------------------ AXIS MODEL -----------------------------------

class AxisManual:
    """Manual only; no feedback control."""
    def __init__(self, name: str, motor_ids: tuple[int, ...], motor_invert: tuple[bool, ...], torque_floor: int = None):
        self.name = name
        self.motor_ids = motor_ids
        self.motor_invert = list(motor_invert)
        self.speed_percent = 30.0
        self.dir = 0            # -1, 0, +1
        self.last_cmd = 0
        self.torque_floor = TORQUE_FLOOR if torque_floor is None else int(torque_floor)

    def _map_speed(self) -> int:
        pct = max(0.0, min(100.0, float(self.speed_percent)))
        if pct <= 0.0: return 0
        cmd = int((pct/100.0) * MOTORON_MAX)
        cmd = max(cmd, self.torque_floor)
        return min(cmd, MOTORON_MAX)

    def apply(self, mot: Motoron):
        if self.dir == 0:
            for m in self.motor_ids: mot.set_speed(m, 0)
            self.last_cmd = 0
            return
        mag = self._map_speed()
        raw = mag if self.dir > 0 else -mag
        self.last_cmd = raw
        for m, inv in zip(self.motor_ids, self.motor_invert):
            mot.set_speed(m, (-raw if inv else raw))

    def brake(self, mot: Motoron, amount=800):
        self.dir = 0
        for m in self.motor_ids: mot.brake_motor(m, amount)
        self.last_cmd = 0

# ------------------------------ GUI ------------------------------------------

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Manual Hold-to-Run Controller (Time-Based)")
        self.geometry("900x430")

        self.mot = Motoron(MOTORON_ADDR, I2C_BUS)
        self.adc = ADCReader(); self.adc.start()

        self.weights = AxisManual("Weights", (1,),   (MOTOR_INVERT[1],), torque_floor=250)
        self.gantry  = AxisManual("Gantry",  (2,3),  (MOTOR_INVERT[2], MOTOR_INVERT[3]))

        self.estopped = False

        # --- sequence & logging state ---
        self.seq_running = threading.Event()
        self.seq_thread = None
        self.log_thread = None
        self.log_fh = None  
        self.log_writer = None
        self.seq_state = "idle"
        self._last_vin = float("nan")

        self._build_ui()

        # timers
        self.after(int(1000.0/CMD_REFRESH_HZ), self._refresh_commands)
        self.after(int(1000.0/VIN_REFRESH_HZ), self._refresh_vin)
        if self.adc.enabled:
            self.after(200, self._refresh_adc)
        self.bind("<Escape>", self.emergency_stop)
        self.protocol("WM_DELETE_WINDOW", self.on_close)

    # ---------- UI ----------
    def _build_ui(self):
        pad = {"padx": 8, "pady": 6}
        top = ttk.Frame(self); top.pack(fill=tk.X, **pad)
        self.lbl_status = ttk.Label(top, text="Ready"); self.lbl_status.pack(side=tk.LEFT)
        self.lbl_vin = ttk.Label(top, text="VIN: --.- V"); self.lbl_vin.pack(side=tk.LEFT, padx=(12,0))
        if self.adc.enabled:
            self.lbl_adc = ttk.Label(top, text="LVDT: ---.--- V | ---.--- mm |  ---.- Hz")
        else:
            self.lbl_adc = ttk.Label(top, text="LVDT: (disabled)")
        self.lbl_adc.pack(side=tk.LEFT, padx=(12,0))

        # Global controls
        btn_estop = tk.Button(top, text="E-STOP (COAST)", command=self.emergency_stop,
                              bg="#c62828", fg="white", activebackground="#b71c1c",
                              font=("TkDefaultFont", 10, "bold"))
        btn_estop.pack(side=tk.RIGHT, padx=(8,0))
        ttk.Button(top, text="Reset E-STOP", command=self.reset_estop).pack(side=tk.RIGHT, padx=(6,0))
        ttk.Button(top, text="STOP (Brake)", command=self.stop_all).pack(side=tk.RIGHT)

        ttk.Separator(self, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=(2,2))

        # Axis cards
        self._axis_card(self.weights).pack(fill=tk.X, **pad)
        self._axis_card(self.gantry).pack(fill=tk.X, **pad)

        # ---------------- SEQUENCE PANEL (time-based) ----------------
        seq = ttk.LabelFrame(self, text="Weights Sequence (Time-Based Raise/Lower Repeat)")
        seq.pack(fill=tk.X, padx=8, pady=6)

        self.seq_raise_time = tk.DoubleVar(value=0.4)   # seconds moving up
        self.seq_lower_time = tk.DoubleVar(value=0.3)   # seconds moving down
        self.seq_total_time = tk.DoubleVar(value=60.0)  # total runtime
        self.seq_coast_bottom = tk.BooleanVar(value=True)
        self.seq_top_dwell   = tk.DoubleVar(value=30) # optional dwell at top
        self.seq_bottom_dwell= tk.DoubleVar(value=30) # optional dwell at bottom

        row1 = ttk.Frame(seq); row1.pack(fill=tk.X, padx=8, pady=4)
        ttk.Label(row1, text="Raise time (s):").pack(side=tk.LEFT)
        tk.Spinbox(row1, from_=0.05, to=10.0, increment=0.05, width=8,
                   textvariable=self.seq_raise_time).pack(side=tk.LEFT, padx=(6,18))

        ttk.Label(row1, text="Lower time (s):").pack(side=tk.LEFT)
        tk.Spinbox(row1, from_=0.05, to=10.0, increment=0.05, width=8,
                   textvariable=self.seq_lower_time).pack(side=tk.LEFT, padx=(6,18))

        ttk.Label(row1, text="Total run time (s):").pack(side=tk.LEFT)
        tk.Spinbox(row1, from_=1.0, to=3600.0, increment=1.0, width=8,
                   textvariable=self.seq_total_time).pack(side=tk.LEFT, padx=(6,18))

        row2 = ttk.Frame(seq); row2.pack(fill=tk.X, padx=8, pady=4)
        ttk.Checkbutton(row2, text="Coast after lower", variable=self.seq_coast_bottom)\
            .pack(side=tk.LEFT, padx=(0,18))
        ttk.Label(row2, text="Top dwell (s):").pack(side=tk.LEFT)
        tk.Spinbox(row2, from_=0.0, to=10.0, increment=0.05, width=8,
                   textvariable=self.seq_top_dwell).pack(side=tk.LEFT, padx=(6,18))
        ttk.Label(row2, text="Bottom dwell (s):").pack(side=tk.LEFT)
        tk.Spinbox(row2, from_=0.0, to=15.0, increment=0.05, width=8,
                   textvariable=self.seq_bottom_dwell).pack(side=tk.LEFT, padx=(6,18))

        ttk.Button(seq, text="Start Sequence", command=self.start_sequence)\
            .pack(side=tk.LEFT, padx=(0,8))
        ttk.Button(seq, text="Stop Sequence", command=self.stop_sequence)\
            .pack(side=tk.LEFT, padx=(0,8))

        self.lbl_log = ttk.Label(seq, text="Log: (not started)")
        self.lbl_log.pack(side=tk.LEFT, padx=(12,0))

    def _axis_card(self, axis: AxisManual):
        f = ttk.LabelFrame(self, text=f"{axis.name} (Motors: {', '.join(map(str, axis.motor_ids))})")

        # Speed row
        r0 = ttk.Frame(f); r0.pack(fill=tk.X, padx=8, pady=(8,2))
        ttk.Label(r0, text="Speed %:").pack(side=tk.LEFT)
        s = ttk.Scale(r0, from_=0, to=100, orient=tk.HORIZONTAL, length=360,
                      command=lambda v, a=axis: setattr(a, "speed_percent", float(v)))
        s.set(axis.speed_percent); s.pack(side=tk.LEFT, padx=(6,12))
        ttk.Label(r0, text=f"(floor {axis.torque_floor})").pack(side=tk.LEFT)
        ttk.Label(r0, text=" | cmd: ").pack(side=tk.LEFT, padx=(12,0))
        var_cmd = tk.StringVar(value="0"); setattr(self, f"var_{axis.name}_cmd", var_cmd)
        ttk.Label(r0, textvariable=var_cmd, width=8).pack(side=tk.LEFT)

        # Hold buttons row
        r1 = ttk.Frame(f); r1.pack(fill=tk.X, padx=8, pady=(4,10))
        b_up = ttk.Button(r1, text="▲ Hold UP")
        b_dn = ttk.Button(r1, text="▼ Hold DOWN")
        b_up.pack(side=tk.LEFT, padx=(0,8)); b_dn.pack(side=tk.LEFT)

        # keep handles for optional enable/disable
        setattr(self, f"btn_{axis.name.lower()}_up", b_up)
        setattr(self, f"btn_{axis.name.lower()}_down", b_dn)

        def press(dirn):
            if self.estopped: return
            axis.dir = +1 if dirn > 0 else -1
            axis.apply(self.mot)
            getattr(self, f"var_{axis.name}_cmd").set(f"{axis.last_cmd:+d}")

        def release(_e=None):
            axis.brake(self.mot)   # brake on release to hold tension
            getattr(self, f"var_{axis.name}_cmd").set("0")

        b_up.bind("<ButtonPress-1>",   lambda _e: press(+1))
        b_up.bind("<ButtonRelease-1>", release)
        b_dn.bind("<ButtonPress-1>",   lambda _e: press(-1))
        b_dn.bind("<ButtonRelease-1>", release)

        if axis.name == "Weights":
            # Save references
            setattr(self, "btn_weights_up", b_up)
            setattr(self, "btn_weights_down", b_dn)

            # Convert DOWN into 0.2s pulse + coast
            b_dn.config(text="▼ Down 0.2s (coast)")
            try: b_dn.unbind("<ButtonPress-1>")
            except Exception: pass
            try: b_dn.unbind("<ButtonRelease-1>")
            except Exception: pass
            b_dn.config(command=self._weights_pulse_down)

        return f

    # ---------- loops & helpers ----------

    def _weights_pulse_down(self, duration_s: float = 0.2):
        """Give the weights motor a short DOWN push, then coast."""
        if getattr(self, "estopped", False):
            return
        btn = getattr(self, "btn_weights_down", None)
        if btn:
            btn.config(state=tk.DISABLED)

        def run():
            try:
                # drive DOWN using the same mapping as hold mode
                self.weights.dir = -1
                self.weights.apply(self.mot)
                try:
                    self.var_Weights_cmd.set(f"{self.weights.last_cmd:+d}")
                except Exception:
                    pass
                time.sleep(max(0.05, float(duration_s)))
            finally:
                # stop driving and coast motor 1
                self.weights.dir = 0
                self.weights.apply(self.mot)   # send 0 speed
                self.mot.coast_motor(1)        # immediate coast
                try:
                    self.var_Weights_cmd.set("0")
                except Exception:
                    pass
                if btn:
                    try: btn.config(state=tk.NORMAL)
                    except Exception: pass

        threading.Thread(target=run, daemon=True).start()

    def _drive_weights_raw(self, dirn: int, magnitude: int):
        """Bypass torque floor: drive motor 1 at a raw magnitude considering inversion."""
        raw = int(max(0, min(MOTORON_MAX, magnitude)))
        cmd = raw if dirn > 0 else -raw
        if self.weights.motor_invert[0]:
            cmd = -cmd
        self.mot.set_speed(1, cmd)
        self.weights.last_cmd = cmd

    def _refresh_commands(self):
        # Re-issue while held to survive Motoron timeout and catch speed slider changes
        if not self.estopped:
            for axis in (self.weights, self.gantry):
                if axis.dir != 0:
                    axis.apply(self.mot)
                    getattr(self, f"var_{axis.name}_cmd").set(f"{axis.last_cmd:+d}")
        self.after(int(1000.0/CMD_REFRESH_HZ), self._refresh_commands)

    def _refresh_vin(self):
        try:
            v = self.mot.vin_v()
            if v == v:
                self._last_vin = v
                self.lbl_vin.config(text=f"VIN: {v:0.1f} V")
        except Exception:
            pass
        self.after(int(1000.0/VIN_REFRESH_HZ), self._refresh_vin)

    def _refresh_adc(self):
        if self.adc.enabled:
            self.lbl_adc.config(text=f"LVDT: {self.adc.latest_v:0.3f} V | {self.adc.latest_pos:0.3f} mm  |  {self.adc.hz:0.1f} Hz")
            self.after(200, self._refresh_adc)

    # ---------- sequence control & logging ----------

    def start_sequence(self):
        if self.seq_running.is_set():
            self.lbl_status.config(text="Sequence already running")
            return
        # open CSV
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs("logs", exist_ok=True)
        path = os.path.join("logs", f"weights_sequence_{ts}.csv")
        try:
            self.log_fh = open(path, "w", newline="", encoding="utf-8")
            self.log_writer = csv.writer(self.log_fh)
            # t_rel_s, lvdt_v_div100, lvdt_mm_div100, state
            self.log_writer.writerow(["t_rel_s","lvdt_v","lvdt_mm","state"])
            self.lbl_log.config(text=f"Log: {path}")
        except Exception as e:
            self.lbl_status.config(text=f"Log open failed: {e}")
            return

        # disable manual weight buttons during sequence (optional/safety)
        try:
            self.btn_weights_up.config(state=tk.DISABLED)
            self.btn_weights_down.config(state=tk.DISABLED)
        except Exception:
            pass

        self.seq_running.set()
        self.seq_state = "idle"
        self.seq_thread = threading.Thread(target=self._seq_loop_time_based, daemon=True)
        self.seq_thread.start()
        self.log_thread = threading.Thread(target=self._log_loop, daemon=True)
        self.log_thread.start()
        self.lbl_status.config(text="Sequence running")

    def stop_sequence(self):
        self.seq_running.clear()
        # allow threads to end
        try:
            if self.seq_thread and self.seq_thread.is_alive():
                self.seq_thread.join(timeout=1.0)
        except Exception:
            pass
        try:
            if self.log_thread and self.log_thread.is_alive():
                self.log_thread.join(timeout=1.0)
        except Exception:
            pass
        # re-enable manual buttons
        try:
            self.btn_weights_up.config(state=tk.NORMAL)
            self.btn_weights_down.config(state=tk.NORMAL)
        except Exception:
            pass
        # close log
        try:
            if self.log_fh:
                self.log_fh.flush()
                self.log_fh.close()
        except Exception:
            pass
        self.log_fh = None
        self.log_writer = None
        self.seq_state = "idle"
        self.lbl_status.config(text="Sequence stopped")

    def _finish_sequence_cleanup(self):
        # Wait briefly for logger to exit, then close file and restore UI
        try:
            if self.log_thread and self.log_thread.is_alive():
                self.log_thread.join(timeout=1.0)
        except Exception:
            pass
        try:
            if self.log_fh:
                self.log_fh.flush()
                self.log_fh.close()
        except Exception:
            pass
        self.log_fh = None
        self.log_writer = None

        # Re-enable manual buttons
        try:
            self.btn_weights_up.config(state=tk.NORMAL)
            self.btn_weights_down.config(state=tk.NORMAL)
        except Exception:
            pass

        self.lbl_status.config(text="Sequence finished")


    def _seq_loop_time_based(self):
        """Repeat: raise for time, brake/dwell; then lower for time (slow ~50).
        Stop automatically right after the last LOWER finishes if total time is up."""
        total_s = float(self.seq_total_time.get())
        raise_s = max(0.05, float(self.seq_raise_time.get()))
        lower_s = max(0.05, float(self.seq_lower_time.get()))
        coast_after_lower = bool(self.seq_coast_bottom.get())
        top_dwell = max(0.0, float(self.seq_top_dwell.get()))
        bottom_dwell = max(0.0, float(self.seq_bottom_dwell.get()))

        t_end = time.time() + total_s
        final_cycle = False
        # Rough overhead margin so we don't mis-predict by a few tens of ms
        cycle_overhead = 0.05
        full_cycle_est = raise_s + top_dwell + lower_s + bottom_dwell + cycle_overhead

        while self.seq_running.is_set() and not self.estopped:
            # If no time remains and we haven't started a new cycle, we're done
            rem = t_end - time.time()
            if rem <= 0 and not final_cycle:
                break

            # If there's not enough time left for a full cycle, mark this as the final one.
            if (not final_cycle) and (rem <= full_cycle_est):
                final_cycle = True

            # ---- LOWER (time-based with forced slow speed ~50) ----
            self.seq_state = "lowering"
            self.weights.dir = 0  # prevent refresh loop from overriding our raw command
            t1 = time.time()
            while self.seq_running.is_set() and not self.estopped and (time.time() - t1) < lower_s:
                self._drive_weights_raw(dirn=-1, magnitude=100)
                time.sleep(0.05)

            # End of lower: coast or brake + optional dwell
            if coast_after_lower:
                self.mot.coast_motor(1)
                self.seq_state = "bottom_coast"
            else:
                self.weights.brake(self.mot)
                self.seq_state = "bottom_brake"

            if bottom_dwell > 0:
                t_d2 = time.time()
                while self.seq_running.is_set() and not self.estopped and (time.time() - t_d2) < bottom_dwell:
                    time.sleep(0.01)


            # If time expired during the raise/top dwell, ensure this next lower is the last
            if (not final_cycle) and (time.time() >= t_end):
                final_cycle = True

            if not self.seq_running.is_set() or self.estopped:
                break
                    
            # ---- RAISE (time-based using slider/torque floor) ----
            self.seq_state = "raising"
            self.weights.dir = +1
            self.weights.apply(self.mot)
            t0 = time.time()
            while self.seq_running.is_set() and not self.estopped and (time.time() - t0) < raise_s:
                self.weights.apply(self.mot)  # keep alive & reflect slider changes
                time.sleep(0.05)

            # Top brake + optional dwell
            self.weights.brake(self.mot)
            self.seq_state = "top_brake"
            if top_dwell > 0:
                t_d = time.time()
                while self.seq_running.is_set() and not self.estopped and (time.time() - t_d) < top_dwell:
                    time.sleep(0.01)


            # If that was the final cycle, exit right after completing the lower (and bottom dwell)
            if final_cycle:
                break

        # Tidy and signal done
        self.weights.dir = 0
        self.weights.brake(self.mot)
        self.seq_state = "idle"
        self.seq_running.clear()
        # Defer full cleanup (join logger, close file, re-enable buttons) to the Tk thread
        self.after(0, self._finish_sequence_cleanup)


    def _log_loop(self):
        """Write one CSV row per ADC sample at the ADC's true sample rate."""
        if not (self.log_writer and self.adc and self.adc.enabled):
            return

        t0 = None
        rows_since_flush = 0

        while self.seq_running.is_set():
            try:
                # Block for the next sample (up to 0.5 s to allow graceful stop)
                ts, lv, lm = self.adc.samples.get(timeout=0.5)
            except Empty:
                continue

            # Drain any additional queued samples to write in a batch
            batch = [(ts, lv, lm)]
            while True:
                try:
                    batch.append(self.adc.samples.get_nowait())
                except Empty:
                    break

            for (ts_i, lv_i, lm_i) in batch:
                if t0 is None:
                    t0 = ts_i
                t_rel = ts_i - t0

                try:
                    self.log_writer.writerow([
                        f"{t_rel:.6f}",
                        f"{lv_i:.6f}",
                        f"{lm_i:.6f}",
                        self.seq_state
                    ])
                    rows_since_flush += 1
                except Exception:
                    pass

            # Periodic flush to keep file current with minimal overhead
            if rows_since_flush >= 250 and self.log_fh:
                try:
                    self.log_fh.flush()
                except Exception:
                    pass
                rows_since_flush = 0

    # ---------- global actions ----------
    def stop_all(self):
        for axis in (self.weights, self.gantry):
            axis.brake(self.mot)
        self.lbl_status.config(text="STOPPED")

    def emergency_stop(self, *_):
        self.estopped = True
        self.stop_sequence()   # ensure sequence + logging halt
        for axis in (self.weights, self.gantry):
            axis.dir = 0
        self.mot.coast_all()  # coast on E-STOP
        self.lbl_status.config(text="E-STOP LATCHED", foreground="#c62828")

    def reset_estop(self):
        self.estopped = False
        for axis in (self.weights, self.gantry):
            axis.dir = 0
            axis.apply(self.mot)
        self.lbl_status.config(text="Ready", foreground="")

    def on_close(self):
        try:
            self.estopped = True
            self.stop_sequence()    # stop sequence/logging if active
            self.mot.coast_all()
        except Exception: pass
        try:
            if self.adc: self.adc.stop()
        except Exception: pass
        self.destroy()

# ------------------------------ RUN ------------------------------------------

if __name__ == "__main__":
    App().mainloop()
