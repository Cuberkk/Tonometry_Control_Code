"""Hardware abstractions: Motoron driver, encoder, ADC reader."""

from __future__ import annotations

import sys
import threading
import time
from queue import Full, Queue

from . import config

try:
    from motoron import MotoronI2C
except ImportError as exc:  # pragma: no cover - hardware specific
    raise ImportError(f"Motoron library not found. Expected directory: {config.MOTORON_PATH}") from exc

ADC_AVAILABLE = True
try:
    from dfrobot_ads1115_fast import FastADS1115I2C
except Exception:  # pragma: no cover - optional hardware
    ADC_AVAILABLE = False


class Motoron:
    def __init__(self, address=config.MOTORON_ADDR, bus=config.I2C_BUS):
        self.mc = MotoronI2C(address=address, bus=bus)
        self.mc.reinitialize()
        self.mc.disable_crc()
        self.mc.clear_reset_flag()
        self.mc.set_command_timeout_milliseconds(500)
        for m in (1, 2, 3):
            self.mc.set_max_acceleration(m, 300)
            self.mc.set_max_deceleration(m, 300)

    def set_speed(self, m: int, speed: int):
        s = max(-config.MOTORON_MAX, min(config.MOTORON_MAX, int(speed)))
        self.mc.set_speed(m, s)

    def coast_all(self):
        try:
            self.mc.coast_now()
        except Exception:
            pass

    def coast_motor(self, m: int):
        try:
            self.mc.set_braking_now(m, 0)
        except Exception:
            pass

    def vin_v(self):
        try:
            return float(self.mc.get_vin_voltage_mv()) / 1000.0
        except Exception:
            return float("nan")


class QuadratureEncoder:
    """Simple quadrature encoder reader built on pigpio callbacks."""

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


class ADCReader(threading.Thread):
    def __init__(self, bus=config.I2C_BUS, addr=0x48, channel=1):
        super().__init__(daemon=True)
        self.enabled = ADC_AVAILABLE
        self.latest_v = 0.0
        self.pos_offset = self.convert_volt_to_pos(0.388 * config.LVDT_VOLTAGE_SCALE)
        self.latest_pos = 0.0
        self.hz = 0.0
        self._stop = threading.Event()
        self.channel = channel

        # Per-sample queue so logger can run at ADC max rate
        self.samples: Queue[tuple[float, float, float]] = Queue(maxsize=20000)

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
        t0, n = time.time(), 0
        while not self._stop.is_set():
            try:
                v = float(self.adc.get_value(self.channel)) / 100.0
                v *= config.LVDT_VOLTAGE_SCALE
                self.latest_v = v
                self.latest_pos = self.convert_volt_to_pos(v) - self.pos_offset
                n += 1

                ts = time.time()
                try:
                    self.samples.put_nowait((ts, self.latest_v, self.latest_pos))
                except Full:
                    try:
                        _ = self.samples.get_nowait()
                        self.samples.put_nowait((ts, self.latest_v, self.latest_pos))
                    except Exception:
                        pass

            except Exception:
                time.sleep(0.001)
                continue

            now = time.time()
            if now - t0 >= 1.0:
                self.hz = n / (now - t0)
                n = 0
                t0 = now

    def convert_volt_to_pos(self, v: float) -> float:
        return v * config.LVDT_SLOPE + config.LVDT_INTERCEPT

    def stop(self):
        self._stop.set()
