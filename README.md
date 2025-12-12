# Tonometry Gantry/Weights Controller

Python tooling to drive the tonometry gantry and weights using a Motoron controller, encoder, and ADS1115/LVDT input. It includes a  GUI for operation.

## Layout
- `device_controller/controller_app/` – core package: config, hardware wrappers, PD weights controller, Tk UI.
- `device_controller/deviceController2.py` – GUI entrypoint.
- `calibration.json` – persisted targets and gain settings (written by the GUI).
- `logs/` – CSV logs captured by the GUI and autotuner.

## Requirements
- Python 3.x with `pigpiod` running for encoder support.
- Motoron I2C library available (repo vendor copy at `motoron-python` is added to `sys.path` by the app).
- ADS1115/LVDT input if present (`dfrobot_ads1115_fast`).

## Running the GUI
From repo root:
```
python device_controller/deviceController2.py
```
Workflow:
1) Zero the encoder in the UI before moving.
2) Set weights top/bottom targets, tolerance, PD gains, speed, and Min Output as needed.
3) Jog manually with “▲/▼ Hold” buttons; run sequences via the sequence panel (cycles + dwell).
4) Start/stop logging to write CSVs to `logs/`.(data_type: `time`, `LVDT_voltage`, `LVDT_mm`, `State`(moving_top, moving_bottom, stay_top, stay_bottom))
5) E-STOP latches a coast; Reset E-STOP to resume.

Notes:
- After editing config, restart the GUI and re-zero before moving.

## Configuration
Defaults live in `device_controller/controller_app/config.py` and are overridden by `calibration.json` when present. The GUI “Save Targets” and “Save PD” buttons write back to `calibration.json`.

Key settings:
- `weights_pid`: `kp`, `kd` (Ki unused), `deadband_counts`, `min_output` (torque floor).
- `weights_encoder`: `top_counts`, `bottom_counts`, `tolerance_counts`, `default_travel_counts`.
- Bottom protection to avoid unspooling:
  - `bottom_slow_speed_pct`: temporary speed when driving to bottom.
  - `bottom_retension_counts`: lift after hitting bottom (0 to disable).
  - `bottom_retension_speed_pct`: speed for that lift.
