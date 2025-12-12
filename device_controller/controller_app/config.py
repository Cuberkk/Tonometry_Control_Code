"""Configuration and calibration helpers for the Tonometry controller."""

from __future__ import annotations

import json
import sys
from pathlib import Path
import numpy as np
DEVICE_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = DEVICE_DIR.parent

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
CALIBRATION_PATH = REPO_ROOT / "calibration.json"
LOG_DIR = REPO_ROOT / "logs"

I2C_BUS = 1
MOTORON_ADDR = 0x10
MOTOR_ID = 1

MOTOR_INVERT = {
    1: False,   # Weights
    2: True,   # Gantry motor A
    3: False,  # Gantry motor B
}

MOTORON_MAX = 600
TORQUE_FLOOR = 110
CMD_REFRESH_HZ = 10.0
VIN_REFRESH_HZ = 2.0

ENCODER_GPIO_A = 6
ENCODER_GPIO_B = 5
ENCODER_INVERT = True

ENCODER_CPR = 1200
PULLEY_DIAMETER_M = 0.029

PULLEY_CIRCUMFERENCE_M = np.pi * PULLEY_DIAMETER_M if PULLEY_DIAMETER_M > 0 else 0.0
COUNTS_PER_METER = (ENCODER_CPR / PULLEY_CIRCUMFERENCE_M) if PULLEY_CIRCUMFERENCE_M > 0 else 0.0
METERS_PER_COUNT = (1.0 / COUNTS_PER_METER) if COUNTS_PER_METER > 0 else 0.0
MM_PER_COUNT = METERS_PER_COUNT * 1000.0


def load_calibration() -> dict:
    try:
        with CALIBRATION_PATH.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {}


def save_calibration(data: dict):
    tmp = CALIBRATION_PATH.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, sort_keys=True)
    tmp.replace(CALIBRATION_PATH)


calibration_data = load_calibration()


def reload_calibration() -> dict:
    global calibration_data
    calibration_data = load_calibration()
    return calibration_data


def _weights_cfg() -> dict:
    return calibration_data.get("weights_encoder", {})


def _load_weight_default(name: str, default: int) -> int:
    try:
        value = _weights_cfg().get(name, default)
        return int(value)
    except Exception:
        return int(default)


def _load_weight_float(name: str, default: float) -> float:
    try:
        value = _weights_cfg().get(name, default)
        return float(value)
    except Exception:
        return float(default)


WEIGHTS_TOP_DEFAULT = _load_weight_default("top_counts", 5000)
WEIGHTS_BOTTOM_DEFAULT = _load_weight_default("bottom_counts", 0)
WEIGHTS_TOLERANCE_DEFAULT = max(1, _load_weight_default("tolerance_counts", 20))
WEIGHTS_TRAVEL_DEFAULT = max(50, _load_weight_default("default_travel_counts", 200))

_span = abs(WEIGHTS_TOP_DEFAULT - WEIGHTS_BOTTOM_DEFAULT)
if _span <= 0 or _span > WEIGHTS_TRAVEL_DEFAULT * 4:
    if WEIGHTS_TOP_DEFAULT >= WEIGHTS_BOTTOM_DEFAULT:
        WEIGHTS_TOP_DEFAULT = WEIGHTS_BOTTOM_DEFAULT + WEIGHTS_TRAVEL_DEFAULT
    else:
        WEIGHTS_TOP_DEFAULT = WEIGHTS_BOTTOM_DEFAULT - WEIGHTS_TRAVEL_DEFAULT
WEIGHTS_SLOW_ZONE_COUNTS = max(1, _load_weight_default("slow_zone_counts", 400))
WEIGHTS_SLOW_MIN_SCALE = max(0.05, min(1.0, _load_weight_float("slow_zone_scale", 0.25)))
WEIGHTS_SLOW_TORQUE_FLOOR = max(20, _load_weight_default("slow_zone_torque_floor", 80))
# Slow-down and retension behavior near bottom to avoid unspooling
WEIGHTS_BOTTOM_SLOW_SPEED_PCT = max(5.0, min(100.0, _load_weight_float("bottom_slow_speed_pct", 30.0)))
WEIGHTS_BOTTOM_RETENSION_COUNTS = max(0, _load_weight_default("bottom_retension_counts", 10))
WEIGHTS_BOTTOM_RETENSION_SPEED_PCT = max(5.0, min(100.0, _load_weight_float("bottom_retension_speed_pct", 25.0)))

LVDT_SLOPE = calibration_data.get("LVDT", {}).get("slope", 2.0114)
LVDT_INTERCEPT = calibration_data.get("LVDT", {}).get("intercept", 0.7342)
LVDT_VOLTAGE_SCALE = float(calibration_data.get("LVDT", {}).get("voltage_scale", 1.5))

PID_CFG = calibration_data.get("weights_pid", {})
WEIGHTS_PID_KP = float(PID_CFG.get("kp", 0.4))
WEIGHTS_PID_KI = float(PID_CFG.get("ki", 0.0))  # Integral unused (PD control)
WEIGHTS_PID_KD = float(PID_CFG.get("kd", 0.0))
WEIGHTS_PID_DEADBAND = float(PID_CFG.get("deadband_counts", 2.0))
WEIGHTS_PID_MIN_OUTPUT = float(PID_CFG.get("min_output", 0.0))


def counts_to_mm(counts: float) -> float:
    return float(counts) * MM_PER_COUNT


def mm_to_counts(distance_mm: float) -> int:
    if COUNTS_PER_METER <= 0:
        return 0
    counts = (float(distance_mm) / 1000.0) * COUNTS_PER_METER
    return int(round(counts))


MOTORON_PATH = REPO_ROOT / "motoron-python"
if MOTORON_PATH.exists():
    sys.path.insert(1, str(MOTORON_PATH))
