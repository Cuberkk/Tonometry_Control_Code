#!/usr/bin/env python3
"""
get_value.py — robust single-shot reader for DFRobot 0–10V ADS1115
- Prints V and measured ADC read rate (Hz) once per second.
- Uses FastADS1115I2C.get_value() (write -> delay -> read with retry).
- No generator/pipelining, so it won't hang if a read blocks.
"""

import os, sys, time, traceback

# --- CONFIG ---
I2C_BUS = 1
ADDR    = 0x48
CHAN    = 1
PRINT_INTERVAL = 1.0  # seconds
# -------------

here = os.path.abspath(os.path.dirname(__file__))
parent = os.path.abspath(os.path.join(here, ".."))
if parent not in sys.path:
    sys.path.append(parent)

from dfrobot_ads1115_fast import FastADS1115I2C

def main():
    # Tune these if needed:
    adc = FastADS1115I2C(
        bus=I2C_BUS,
        addr=ADDR,
        mv_to_v=True,           # True => returns Volts (module reports mV)
        init_conv_delay_s=0.0010,  # good starting point for ~800 Hz
        min_conv_delay_s=0.0010,   # don’t go below 1.0 ms on ADS1115
        max_conv_delay_s=0.0030,
    )

    print(f"[BOOT] Using wrapper: {FastADS1115I2C.__module__}", flush=True)

    if not adc.begin():
        print("[FATAL] ADC begin() failed. Check wiring and I2C address.", flush=True)
        sys.exit(1)

    print(f"[BOOT] Connected ADS1115 @ 0x{ADDR:02X}", flush=True)

    t0, n, last_v = time.time(), 0, 0.0
    while True:
        try:
            v = adc.get_value(CHAN)  # write->sleep->read with retry
            last_v = v
            n += 1
        except OSError as e:
            # Benign I2C hiccup: brief pause & continue
            # (e.errno == 121 is Remote I/O error/NACK)
            time.sleep(0.0005)
            continue
        except Exception:
            # Unexpected: print once and keep trying
            traceback.print_exc()
            time.sleep(0.005)
            continue

        now = time.time()
        if (now - t0) >= PRINT_INTERVAL:
            hz = n / (now - t0)
            print(f"V={last_v/100:0.4f} V  |  ADC rate={hz:0.1f} Hz", flush=True)
            t0, n = now, 0

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[EXIT] KeyboardInterrupt", flush=True)
        sys.exit(0)
