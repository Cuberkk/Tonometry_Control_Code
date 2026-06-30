# dfrobot_ads1115_fast.py
import time
try:
    from smbus2 import SMBus
except ImportError:
    from smbus import SMBus

CHANNEL_SELECT_ADDRESS = 0x20
CHANNEL_DATA_ADDRESS   = 0x31

class FastADS1115I2C:
    """
    DFRobot 0–10V ADS1115 fast reader with pipelined conversions.
    - Robust against occasional I2C NACKs on write/read.
    - Returns volts by default (module payload is typically mV).
    """

    def __init__(self, bus=1, addr=0x48, mv_to_v=True,
                 init_conv_delay_s=0.0018,    # start safe
                 min_conv_delay_s=0.0010,     # try to approach this
                 max_conv_delay_s=0.0030,     # don't exceed this
                 retry_sleep_s=0.0003,        # read retry sleep
                 write_retries=3,             # write retry count
                 write_retry_sleep_s=0.0003,  # write retry sleep
                 min_write_spacing_s=0.0002   # guard gap between writes (~200 µs)
                 ):
        self.addr = addr
        self.bus  = SMBus(bus)
        self.mv_to_v = mv_to_v

        self.delay = float(init_conv_delay_s)
        self.min_delay = float(min_conv_delay_s)
        self.max_delay = float(max_conv_delay_s)

        self.retry_sleep  = float(retry_sleep_s)
        self.write_retries = int(write_retries)
        self.write_retry_sleep = float(write_retry_sleep_s)
        self.min_write_spacing = float(min_write_spacing_s)
        self._t_last_write = 0.0
        self._stable_count = 0

    def begin(self) -> bool:
        try:
            self.bus.read_byte(self.addr)
            return True
        except Exception:
            return False

    def _write_channel(self, ch: int):
        """Write with small guard spacing + retry on NACK (Errno 121)."""
        # Ensure a tiny gap between consecutive writes (module MCU can be busy)
        now = time.perf_counter()
        gap = now - self._t_last_write
        if gap < self.min_write_spacing:
            time.sleep(self.min_write_spacing - gap)

        last_err = None
        for _ in range(self.write_retries + 1):
            try:
                self.bus.write_byte_data(self.addr, CHANNEL_SELECT_ADDRESS, ch & 0xFF)
                self._t_last_write = time.perf_counter()
                return
            except OSError as e:
                last_err = e
                if getattr(e, "errno", None) == 121:  # Remote I/O (NACK/busy)
                    time.sleep(self.write_retry_sleep)
                    continue
                raise
        # Escalate if still failing: let caller decide (we’ll back off delay)
        raise last_err

    def _read_raw3(self):
        return self.bus.read_i2c_block_data(self.addr, CHANNEL_DATA_ADDRESS, 3)

    def _conv_to_volts(self, raw: int) -> float:
        return (raw / 1000.0) if self.mv_to_v else float(raw)

    def get_value(self, ch: int) -> float:
        """Single-shot: write → wait → read (with simple retry)."""
        self._write_channel(ch)
        time.sleep(self.delay)
        for _ in range(3):
            try:
                b0, b1, b2 = self._read_raw3()
                raw = (b0 << 16) | (b1 << 8) | b2
                return self._conv_to_volts(raw)
            except OSError as e:
                if getattr(e, "errno", None) == 121:
                    time.sleep(self.retry_sleep)
                    continue
                raise
        raise

    def stream_pipelined(self, ch: int):
        """
        Pipelined generator:
          1) Prime: write channel, wait conversion
          2) Loop:
             - read completed sample (retry once on NACK; back off delay)
             - immediately trigger next conversion (write with retry/guard)
             - if stable, shave delay slightly (aim for min_delay)
        """
        # Prime
        self._write_channel(ch)
        time.sleep(self.delay)

        while True:
            # Read completed conversion
            try:
                b0, b1, b2 = self._read_raw3()
            except OSError as e:
                if getattr(e, "errno", None) == 121:
                    # Device still busy: back off a bit and retry read
                    self.delay = min(self.delay + 0.0002, self.max_delay)
                    time.sleep(self.retry_sleep)
                    b0, b1, b2 = self._read_raw3()
                else:
                    raise
            raw = (b0 << 16) | (b1 << 8) | b2
            val = self._conv_to_volts(raw)

            # Immediately trigger the next conversion (with guarded write)
            try:
                self._write_channel(ch)
            except OSError as e:
                if getattr(e, "errno", None) == 121:
                    # Back off conversion delay and retry write once
                    self.delay = min(self.delay + 0.0002, self.max_delay)
                    time.sleep(self.write_retry_sleep)
                    self._write_channel(ch)
                else:
                    raise

            # Adaptive t
