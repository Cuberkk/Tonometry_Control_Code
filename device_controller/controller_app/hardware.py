"""Hardware abstractions: Motoron driver, encoder, ADC reader."""

from __future__ import annotations

import threading
import time
from queue import Full, Queue
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .labjackati import LabjackATIReader

from . import config

try:
    from motoron import MotoronI2C
except ImportError as exc:  # pragma: no cover - hardware specific
    raise ImportError(f"Motoron library not found. Expected directory: {config.MOTORON_PATH}") from exc
from smbus2 import SMBus, i2c_msg
ADC_IMPORT_ERROR = None
ADC_AVAILABLE = True
try:
    from dfrobot_ads1115_fast import FastADS1115I2C
except Exception as exc:  # pragma: no cover - optional hardware
    ADC_AVAILABLE = False
    ADC_IMPORT_ERROR = str(exc)


class Motoron:
    def __init__(self, address=config.MOTORON_ADDR, bus=config.I2C_BUS, allow_braking=True):
        self.allow_braking = allow_braking
        self._io_lock = threading.RLock()
        i2c = SMBus(bus)
        try:
            msg = i2c_msg.write(
                address,
                [0x96, 0x74],
            )
            i2c.i2c_rdwr(msg)
            print(
                f"[Motoron] raw reinitialize succeeded "
                f"at 0x{address:02X} on bus {bus}"
            )
        finally:
            i2c.close()

        time.sleep(0.05)

        self.mc = MotoronI2C(address=address, bus=bus)
        self.mc.set_protocol_options_locally(7)
        # Retry reinitialize up to 3 times with a delay.
        # If the Motoron is in Pololu protocol mode or mid-transaction,
        # it will time out and return to normal I2C mode within ~1.5s  ^1^ .
        last_exc = None
        
        try:
            self.mc.get_status_flags()
        except OSError:
            pass

        try:
            self.mc.get_vin_voltage_mv()
        except OSError:
            pass
    
        for attempt in range(3):
            try:
                self.mc.reinitialize()
                last_exc = None
                break
            except OSError as exc:
                last_exc = exc
                print(f"[Motoron] reinitialize attempt {attempt+1} failed: {exc} — retrying in 2s")
                time.sleep(2.0)  # wait for Motoron to exit any stuck protocol state

        if last_exc is not None:
            raise RuntimeError(
                f"Motoron at 0x{address:02X} on bus {bus} failed to reinitialize "
                f"after 3 attempts. Power cycle the Motoron and try again. "
                f"Detail: {last_exc}"
            ) from last_exc

        self.mc.clear_reset_flag()
        self.mc.disable_crc()
        self.mc.set_command_timeout_milliseconds(200)
        for m in (1, 2, 3):
            self.mc.set_max_acceleration(m, 150)
            self.mc.set_max_deceleration(m, 100)

    def set_speed(self, m: int, speed: int):
        s = max(-config.MOTORON_MAX, min(config.MOTORON_MAX, int(speed)))
        if s == 0 and not self.allow_braking:
            self.coast_motor(m)
            return
        with self._io_lock:
            self.mc.set_speed(m, s)

    def coast_all(self):
        try:
            with self._io_lock:
                self.mc.coast_now()
        except Exception:
            pass

    # Old method use leaves motor with too much momentum, gravity pulling gantry down 
    def coast_motor(self, m: int):
        try:
            with self._io_lock:
                self.mc.set_braking_now(m, 0)
        except Exception:
            pass

    # New method to prevent the gantry to give over to gravity
    def brake_motor(self, m: int, amount: int):
        if not self.allow_braking:
            self.coast_motor(m)
            return
        try:
            with self._io_lock:
                self.mc.set_braking_now(m, max(0, min(800, int(amount))))
        except Exception:
            pass

    def vin_v(self):
        try:
            with self._io_lock:
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

        glitch_us = 20   # use 100 us
        self._pi.set_glitch_filter(self.gpio_a, glitch_us)
        self._pi.set_glitch_filter(self.gpio_b, glitch_us)

        self._last_state = (self._pi.read(self.gpio_a) << 1) | self._pi.read(self.gpio_b)
        self._cb_a = self._pi.callback(self.gpio_a, pigpio.EITHER_EDGE, self._pulse)
        self._cb_b = self._pi.callback(self.gpio_b, pigpio.EITHER_EDGE, self._pulse)

    def _pulse(self, gpio, level, tick):
        if level not in (0, 1):
            return # ignore watchdog or bad levels
        prev_state = self._last_state  # read without lock

        if gpio == self.gpio_a:
            a = level
            b = prev_state & 0b01
        else:
            b = level
            a = (prev_state >> 1) & 0b01

        new_state = (a << 1) | b
        if new_state == prev_state:
            return
        
        transition = ((prev_state << 2) | new_state) & 0b1111
        delta = self._TRANSITIONS.get(transition)
        self._last_state = new_state   # write without lock
        
        if delta is not None:
            self._position += self._invert * delta  # atomic under CPython GIL

    def get_position(self) -> int:
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
    """Read LVDT samples continuously, with no software acquisition-rate cap."""

    def __init__(self, bus=config.I2C_BUS, addr=0x48, channel=1, queue_samples=True):
        super().__init__(daemon=True)
        self.enabled = ADC_AVAILABLE
        self.error = ADC_IMPORT_ERROR
        self.has_sample = False
        self.latest_v = 0.0
        self.pos_offset = self.convert_volt_to_pos(0.388 * config.LVDT_VOLTAGE_SCALE)
        self.latest_pos = 0.0
        self.sample_sink = None
        self.queue_samples = queue_samples
        self.hz = 0.0
        self._stop_event = threading.Event()
        self.channel = channel

        # Optional legacy queue; the GUI records through sample_sink instead.
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
                self.error = getattr(self.adc, "last_error", None) or f"No ADC response on I2C bus {bus}, address 0x{addr:02X}"
        except Exception as e:
            self.enabled = False
            self.error = str(e)

    def run(self):
        if not self.enabled:
            return
        t0, n = time.monotonic(), 0
        while not self._stop_event.is_set():
            try:
                v = float(self.adc.get_value(self.channel)) / 100.0
                self.latest_v = v + config.ADC_OFFSET
                self.latest_pos = self.convert_volt_to_pos(v)
                self.error = None
                self.has_sample = True
                n += 1

                ts = time.time()
                if self.sample_sink is not None:
                    self.sample_sink(time.time_ns(), time.monotonic(), self.latest_v, self.latest_pos)
                if self.queue_samples:
                    try:
                        self.samples.put_nowait((ts, self.latest_v, self.latest_pos))
                    except Full:
                        try:
                            _ = self.samples.get_nowait()
                            self.samples.put_nowait((ts, self.latest_v, self.latest_pos))
                        except Exception:
                            pass

            except Exception as exc:
                self.error = str(exc)

            now = time.monotonic()
            if now - t0 >= 1.0:
                self.hz = n / (now - t0)
                n = 0
                t0 = now

            # Free-run: immediately read the next sample. Conversion and I2C
            # timing are handled by the ADC driver.

    def convert_volt_to_pos(self, v: float) -> float:
        return v * config.LVDT_SLOPE + config.LVDT_INTERCEPT

    def stop(self):
        self._stop_event.set()

class ATIReader:
    def __init__(self, sensor_num: str = "44297",
                 bias_mode: bool = False,
                 acquisition_rate: float | None = None):
        self.enabled = False
        self.latest_fx: float = 0.0
        self.latest_fy: float = 0.0
        self.latest_fz: float = 0.0
        self.hz: float = 0.0
        self._sensor_num = sensor_num
        self._bias_mode = bias_mode
        self._acquisition_rate = (
            config.LABJACK_ACQUISITION_HZ if acquisition_rate is None else acquisition_rate
        )
        self._sensor: LabjackATIReader | None = None
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

        # Try to connect immediately; if it fails, a retry thread takes over
        self._try_connect()

        # Always start the background thread — it retries if not yet enabled,
        # or streams data if already connected
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def _try_connect(self) -> bool:
        """Attempt one connection. Returns True on success."""
        try:
            from .labjackati import LabjackATIReader

            self._sensor = LabjackATIReader(
                sensor_num=self._sensor_num,
                bias_mode=self._bias_mode,
                acquisition_rate=self._acquisition_rate,
            )
            self.enabled = True
            print(f"[ATI] LabJack connected — sensor {self._sensor_num}")
            return True
        except Exception as exc:
            self._sensor = None
            self.enabled = False
            print(f"[ATI] Not available: {exc}")
            return False

    def start(self) -> None:
        # No-op: thread is already started in __init__
        pass

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=1.0)
            self._thread = None
        if self._sensor:
            try:
                self._sensor.shutdown()
            except Exception:
                pass

    def _loop(self) -> None:
        RETRY_INTERVAL = 5.0

        while not self._stop_event.is_set():

            # ---- not connected yet: keep retrying ----
            if not self.enabled:
                self._stop_event.wait(RETRY_INTERVAL)
                if not self._stop_event.is_set():
                    self._try_connect()
                continue

            # ---- connected: initialise counters ONCE per connection ----
            t_last = time.monotonic()
            count = 0

            # ---- inner read loop — stays here until a read error ----
            while self.enabled and not self._stop_event.is_set():
                try:
                    reading = self._sensor.read()
                    self.latest_fx = reading[0]
                    self.latest_fy = reading[1]
                    self.latest_fz = -reading[2] - config.WEIGHTS_LOAD
                    count += 1
                    now = time.monotonic()
                    dt = now - t_last
                    if dt >= 1.0:
                        self.hz = count / dt
                        count = 0
                        t_last = now
                except Exception as exc:
                    print(f"[ATI] Read error: {exc} — will retry in {RETRY_INTERVAL}s")
                    self.enabled = False
                    if self._sensor:
                        try:
                            self._sensor.shutdown()
                        except Exception:
                            pass
                    self._sensor = None
                    self._stop_event.wait(RETRY_INTERVAL)
                    break   # break inner loop → outer loop retries
