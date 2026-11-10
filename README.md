# General Ethernet Connection Guidelines
To connect to the Raspberry Pi from your computer, please ensure that `OpenSSH` and an `X11 server` are installed on your system.

After plugging in the Ethernet cable, open a terminal and run the following command:
```
ssh -Y erie@192.168.100.1
```
After entering the user password we provided, you will be able to connect to the Raspberry Pi.

To launch the Tonometry control GUI, run the following command:
```
tonometry-gui
```

The tonometry-gui alias encapsulates the following sequence of commands:
```
tonometry-gui='conda activate tonometry && (pgrep pigpiod > /dev/null || sudo pigpiod) && python /home/erie/Tonometry/device_controller/deviceController2.py'
```

Specifically, this alias:

1. Activates the `tonometry` Conda environment  
2. Checks whether the `pigpiod` daemon is running and starts it if necessary  
3. Executes the Tonometry device controller GUI script  

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
2) Set weights top/bottom targets, tolerance, and speed as needed; edit PID profiles and Min Output in `calibration.json`.
3) Jog manually with “▲/▼ Hold” buttons; run sequences via the sequence panel (cycles + dwell).
4) Start/stop logging to write CSVs to `logs/`.(data_type: `time`, `LVDT_voltage`, `LVDT_mm`, `State`(moving_top, moving_bottom, stay_top, stay_bottom))
5) E-STOP latches a coast; Reset E-STOP to resume.

Notes:
- After editing config, restart the GUI and re-zero before moving.

## Configuration
Defaults live in `device_controller/controller_app/config.py` and are overridden by `calibration.json` when present. The GUI “Save Targets” button writes targets back to `calibration.json`; edit PID profiles in that file directly.

Key settings:
- `weights_pid`: `kp`, `ki`, and `kd` apply to weights other than 800 g; `gains_800g` contains the gains for exactly 800 g. Edit these profiles in `calibration.json` and restart the GUI. This section also contains `deadband_counts`, `min_output`, and moving gravity compensation settings.
- `weights_encoder`: `top_counts`, `bottom_counts`, `tolerance_counts`, `default_travel_counts`, and command limits: `pid_lift_speed_pct` (30% for non-800 g) and `pid_lift_speed_pct_800g` (50% for exactly 800 g). With the current 800 g startup weight, the GUI slider shows 50%.
- Bottom protection to avoid unspooling:
  - `bottom_slow_speed_pct`: temporary speed when driving to bottom.
  - `bottom_retension_counts`: lift after hitting bottom (0 to disable).
  - `bottom_retension_speed_pct`: speed for that lift.


  this is a test
