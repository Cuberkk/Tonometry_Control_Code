# New User Guide: Changing Tonometry Controller Settings

This guide answers practical questions raised while developing the controller: where to change a value, what it affects, and how to verify the result.

## Which application should I edit?

The current GUI is `device_controller/controller_app/app.py`. Earlier development referred to this implementation as `app_KZ.py`.

The startup path is:

```text
device_controller/deviceController2.py
  -> controller_app/__init__.py
  -> app.py: ControllerApp
```

Start from the project root:

```bash
conda activate tonometry
python device_controller/deviceController2.py
```

Use the environment containing Tkinter, Matplotlib, NumPy, smbus2, the LabJack Python package, and the native LabJack LJM driver. Motor and sensor hardware dependencies must also be available.

Most source/configuration edits require a full application restart. Editing a file while the GUI is running does not reload the existing controller.

## Quick reference

Line numbers below refer to the source checked when this guide was updated. `path:123` means line 123; `path:123–125` means lines 123 through 125. If later edits move a line, search for the variable/function name shown in the corresponding section. Paths are relative to the project root. Repeated `:number` entries in a cell refer to the same file.

| I want to change… | Exact file and line(s) | Effect |
| --- | --- | --- |
| Startup Top target | `device_controller/controller_app/config.py:102`; JSON value at `calibration.json:36` is currently overridden | Encoder counts; GUI target field changes the active value |
| Startup Bottom / tolerance | `calibration.json:31` / `calibration.json:35` | Encoder counts; GUI Save Targets persists edits |
| Startup holding weight | `calibration.json` → `weights_encoder.holding_weight_g` | Defaults to 400 g; restart GUI after editing |
| Default cycles / Top dwell / Bottom dwell | `device_controller/controller_app/app.py:98–100` | Startup values; dwell times in seconds |
| Default project directory | `device_controller/controller_app/app.py:95` | Startup recording folder |
| Default User / Trial | `device_controller/controller_app/app.py:96–97` | Currently both `"0"` |
| Displacement Y limits | `device_controller/controller_app/app.py:224` and `:633` | Change both initialization and render limits |
| Force Y limits | `device_controller/controller_app/app.py:349` and `:401` | Change both initialization and render limits |
| Displacement / Force zero-line appearance | `device_controller/controller_app/app.py:223` / `:351` | Line color, style, and width |
| Visible time window | `device_controller/controller_app/app.py:152` | Shared history window in seconds |
| GUI/curve refresh request | `device_controller/controller_app/app.py:26–27` | Requested Hz and derived milliseconds |
| ATI acquisition rate | `calibration.json` → `acquisition.labjack_hz` | 400 Hz requested by default; restart GUI after editing |
| LVDT acquisition rate | `device_controller/controller_app/hardware.py` → `ADCReader.run()` | Free-running; no software rate cap |
| ATI sensor number | `device_controller/controller_app/app.py` → `_ati_worker()` | Calibration selector 44297 |
| LVDT conversion delay | `device_controller/controller_app/hardware.py:237` | Seconds; currently 0.0012 |
| Encoder/control log interval | `device_controller/controller_app/app.py:1239` | 0.005 seconds requests 200 Hz |
| PID Kp / Ki / Kd | `calibration.json:42` / `:41` / `:40` | Movement gains |
| Minimum PID output / gravity feedforward | `calibration.json:43–44` | Currently 0 and 170 |
| Gravity compensation implementation | `device_controller/controller_app/controllers.py:310–314` | Added before command clamping for positive targets |
| Lift / automatic downward speed defaults | `device_controller/controller_app/config.py:110` / `:107` | JSON keys may override these defaults |
| Top holding output default | `calibration.json` → `weights_encoder.holding_weight_g` | GUI derives holding output from the configured weight |
| Manual downward speed | `device_controller/controller_app/config.py:130` | Manual downward percentage |
| Gantry direction speed multipliers | `device_controller/controller_app/config.py:113–114` | Upward and downward multipliers |
| Motor short-circuit braking policy | `device_controller/controller_app/app.py:39` | Current GUI uses `allow_braking=False` |

## How do I set Top, Bottom, tolerance, and zero?

**Code location:** `_set_target_from_current` at `device_controller/controller_app/app.py:677`; `zero_weights_encoder` at `device_controller/controller_app/app.py:791`; `_save_weights_targets` at `device_controller/controller_app/app.py:839`.

Use the GUI's **Top target**, **Bottom target**, and **Tolerance (counts)** fields. **Set Current** copies the current encoder position into the corresponding target field. **Save Targets** writes these settings to `calibration.json`.

**Zero Encoder** changes the current encoder coordinate to zero, stops the weight position controller, restores the configured Top default, and sets Bottom to zero. It does not physically home the mechanism. Changing the coordinate origin changes what existing numerical targets mean.

`On top` and `On bottom` mean that arrival was accepted within the configured tolerance. For example, target 275 with tolerance 20 accepts positions from 255 through 295. These labels describe arrival, not continuous position holding.

### Important Top-default override

`config.py` currently loads `weights_encoder.top_counts` and later overrides it with:

```python
WEIGHTS_TOP_DEFAULT = 315
```

Consequently, changing only `top_counts` in JSON does not change the startup Top default while this override remains. For a persistent configuration-driven default, remove that override so the existing JSON-loading assignment takes effect, or update the override deliberately. Check the displayed target after restarting.

## How do I change the default holding weight?

Set `holding_weight_g` in the existing `weights_encoder` section of the project-root `calibration.json` (preserve the other entries):

```json
"holding_weight_g": 400
```

The value is in grams. Save the file and restart the GUI. The **Holding weight (g)** field, recorded weight, holding output, and moving gravity compensation all initialize from this value. A missing value defaults to 400 g; invalid values (including negative, non-finite, or weights exceeding the holding output limit) produce a warning and fall back to 400 g. At 400 g, the holding command is 107.5 and moving gravity compensation is 137.5 before integer conversion.

You can still change the field in the GUI and apply it with Enter or by leaving the field. That changes the current session; edit the JSON to change the startup default.

## How do I choose the save directory and filenames?

**Code location:** Recording input defaults: `device_controller/controller_app/app.py:95–97`. GUI input widgets: `app.py:576–584`. Recording path construction starts at `app.py:1152`.

Fill in **User #**, **Trial #**, and **Project directory**, or use **Browse...**. For User 2 and Trial 5:

```text
<Project directory>/User_Data/User2/Trial5/
├── Force.csv
├── LVDT.csv
└── sequence_<timestamp>.csv
```

Manual recording uses a `session_<timestamp>.csv` control log instead. User and Trial must contain digits. They both default to zero.

Choose a new Trial number for each recording. The application refuses to replace existing Force/LVDT files at the selected location. Changes to these fields apply to the next recording, not a recording already in progress.

**Start Log** records during manual operation. Automated sequences start logging automatically if logging is not already active. If recording is already active, a sequence uses that recording's existing destination rather than opening a new trial.

## Does Bias change the saved data?

**Code location:** ATI Bias handlers: `device_controller/controller_app/app.py:362–372`. LVDT Bias handlers: `app.py:615–623`.

**Bias Displacement** subtracts the current position from the displacement display; **Remove Bias** restores the calibrated position display. `LVDT.csv` continues to contain the original calibrated voltage and position.

**Bias ATI Force** captures the current Fx/Fy/Fz offsets for display. `Force.csv` stores the raw calibrated six-axis readings, the Bias-enabled flag, and the three Bias offsets. It does not contain display-force columns.

To reconstruct displayed Fz offline:

```python
fz_display = fz_raw - bias_fz if bias_enabled else fz_raw
```

When parsing CSV, convert the text `"True"`/`"False"` explicitly rather than applying `bool()` to the nonempty string.

Bias is a display zero adjustment. It does not replace calibration and does not change encoder targets or PID behavior.

## How do I change plot ranges and the zero line?

**Code location:** `device_controller/controller_app/app.py:223–224`, `:349–351`, `:401`, and `:633`. Change both limit assignments for each sensor.

In `app.py`, change both the initial axis limits and the matching limits passed to `LiveLineRenderer.render()`. Changing initialization alone is insufficient because the renderer reapplies its limits.

Current displacement settings:

```python
self.adc_ax.set_ylim(-1, 30)
# In _update_lvdt_display():
ylim=(-1.0, 30.0)
```

Current force settings:

```python
self.ati_ax.set_ylim(-25, 5)
# In _refresh_ati_plot():
ylim=(-25.0, 5.0)
```

Both plots mark zero with:

```python
axis.axhline(0, color="black", linestyle="-", linewidth=1.0)
```

Keep the lower value first. Limits are fixed even when Bias is enabled; data outside the range is clipped visually but is still recorded.

Change `self.adc_plot_window_sec = 30.0` to adjust the visible history. The shared setting controls both plots. Each plot starts at its first displayed sample and then scrolls; cross-sensor comparison should use the recorded timestamps, not assume the two independent plot origins are identical.

## How do I make the GUI refresh faster?

**Code location:** `device_controller/controller_app/app.py:26–27`.

Change the constant near the top of `app.py`:

```python
GUI_REFRESH_HZ = 30
GUI_REFRESH_MS = round(1000 / GUI_REFRESH_HZ)
```

For example, setting it to 40 requests roughly a 25 ms callback interval. The actual frame rate depends on drawing and other work; Tk scheduling is not a real-time guarantee. Existing motor command refreshes, VIN polling, and status callbacks have their own intervals.

The interface reports:

- **GUI callback Hz:** measured periodic main-thread callbacks.
- **LVDT draw FPS:** actual displacement plot drawing events/updates.
- **ATI draw FPS:** actual force plot drawing events/updates.
- **Sensor Hz:** successful acquisition/read rate, independent of display FPS.

Higher GUI FPS does not increase sensor bandwidth or saved samples. Record a new Trial after changing GUI frequency and check whether acquisition gaps increase. Keep `live_plot.py` background invalidation intact: it ensures resized plots and changed axes do not display an old cached background.

## How do I change ATI sampling frequency or sensor number?

Edit the `acquisition` section in the project-root `calibration.json`:

```json
"acquisition": {
  "labjack_hz": 400
}
```

`labjack_hz` is in Hz and must be a finite positive number. Save the JSON and restart the GUI. A missing value uses the default above; an invalid value produces a terminal warning and uses the default. GUI calibration saves preserve this section.

`labjack_hz` is passed to the LabJack stream. The native driver reports the accepted stream rate, while the GUI reports software read throughput. Neither makes each host receipt timestamp uniformly spaced.

For the sensor number, find `_ati_worker()` in `app.py` and choose `sensor_num` to match the actual sensor. `labjackati.py` contains separate matrices for `44297` and `44298`; selecting the wrong one changes force/torque calibration. Leave the worker's `bias_mode=False` when using the GUI's Bias controls, which apply offsets separately and preserve raw recording.

## How do I increase LVDT sampling frequency?

LVDT acquisition runs freely: the next ADC read starts as soon as the previous read and recording callback finish. There is no software rate cap or `acquisition.lvdt_hz` setting. Actual throughput depends on ADC conversion, I2C, recording overhead, and system scheduling.

`ADC_test_reading.py` is an independent throughput diagnostic. GUI drawing FPS and encoder logging frequency have separate settings.

The driver keeps its existing 1.2 ms conversion delay:

```python
init_conv_delay_s=0.0012
```

A shorter delay is not automatically a higher valid sample rate. It may cause I2C errors or read an old conversion. Compare LVDT-only operation with plotting, ATI acquisition, and motor control enabled before changing hardware timing.

The previous collection repository recorded approximately 392 Hz, while application Trial5 recorded approximately 198 Hz. This shows a difference in recorded throughput, but does not establish independent conversion freshness or identify the bottleneck.

The current app disables the unused legacy ADC sample queue and saves through a per-sample callback. The writer is separate from drawing; there is no need to raise GUI FPS to save more LVDT samples.

## How do I measure the actual saved frequency?

Use each sensor file's `elapsed_time` column. From the project root:

```python
import csv
from pathlib import Path

path = Path("User_Data/User0/Trial0/LVDT.csv")
with path.open() as file:
    rows = list(csv.DictReader(file))

times = [float(row["elapsed_time"]) for row in rows]
if len(times) < 2 or times[-1] <= times[0]:
    raise ValueError("At least two samples over a positive duration are required")

frequency = (len(times) - 1) / (times[-1] - times[0])
max_gap = max(b - a for a, b in zip(times, times[1:]))
print(f"Average: {frequency:.2f} Hz; largest gap: {max_gap * 1000:.2f} ms")
```

Use `Force.csv` for ATI. Check long gaps as well as the average. The timestamps describe software receipt, not the precise physical conversion instant; ATI reads can arrive in bursts. Keep original experimental files unchanged when analyzing them.

## How do I change encoder/control recording frequency?

**Code location:** `device_controller/controller_app/app.py:1239`.

Find `_log_loop()` in `app.py`:

```python
interval = 0.005  # Request 200 Hz snapshots
```

The requested rate is `1 / interval`. This log samples encoder/controller state independently of the PID loop. Use `record_dt_s` and `elapsed_time` to assess its actual rate. Increasing it does not increase PID frequency.

The `Force.csv` and `LVDT.csv` writers receive successful acquisition samples directly; they do not use this interval.

## How do I adjust the PID parameters?

**Code location:** `calibration.json:38–45`. Configuration loading: `device_controller/controller_app/config.py:121–128`. Gravity addition: `controllers.py:310–314`.

Edit the root `calibration.json`, preserving all other sections:

```json
"weights_pid": {
  "deadband_counts": 8.0,
  "kd": 0.04,
  "ki": 0.0,
  "kp": 1.0,
  "min_output": 0.0,
  "gravity_output": 170.0
}
```

Restart after editing. These settings are shared by applications using the same controller/configuration modules.

| Value | Meaning in this implementation |
| --- | --- |
| `kp` | Proportional correction based on encoder error. |
| `ki` | Integral correction; currently disabled. |
| `kd` | Correction based on the rate of change of error. |
| `min_output` | Raises small nonzero PID commands to a minimum magnitude; currently disabled. |
| `gravity_output` | Feedforward added for positive-position targets before output clamping. |
| `deadband_counts` | Loaded into the controller but currently not applied in `_compute_pid_command()`; changing it does not currently implement a PID deadband. |

Do not restore the previous `min_output=400` to solve an upward stall. It exceeded movement limits and caused small errors to generate saturated commands. Trial2 stalled at output 162 after removing that floor; adding gravity feedforward 170 allowed Trial3 to reach tolerance. That value is load-dependent, not a universal setting.

Change one parameter at a time, record to a new Trial, and compare arrival time, overshoot, command reversals, and dwell drift. The current parameters were evaluated from limited trials, not a complete plant identification or stability proof.

## Why does the Speed slider not set every movement speed?

**Code location:** `device_controller/controller_app/config.py:107`, `:110–114`, and `:130`.

Manual operation uses its speed setting, with a separate downward override. Automatic moves temporarily apply configuration-defined limits.

The following optional entries belong under `weights_encoder` in `calibration.json`:

| JSON key | Default | Used for |
| --- | ---: | --- |
| `pid_lift_speed_pct` | 41 | Automatic lift limit |
| `bottom_slow_speed_pct` | 10 | Automatic move to Bottom |
| `top_lift_speed_pct` | 20 | Controller holding fallback; GUI overrides it using `holding_weight_g` |

The limit is approximately `percentage / 100 * MOTORON_MAX`; with maximum 600, 41% gives 246 before integer truncation. Existing float arithmetic can produce a logged value of 245. These are driver command units, not measured RPM or percent torque.

`MANUAL_DOWN_SPEED_PCT` in `config.py` separately controls manual downward operation. Gantry direction multipliers are also in `config.py`. Avoid changing `MOTORON_MAX` as a substitute for choosing the correct per-mode speed limit.

## Why does the motor drift after reaching Top?

**Code location:** Holding output default: `device_controller/controller_app/config.py:111`. Holding implementation: `controllers.py:303–308`.

After arrival, the current controller exits movement PID and applies fixed holding output. Changing movement `gravity_output` does not change that holding policy.

The GUI derives the holding request from **Holding weight (g)**. The default 400 g requests command 107.5 before integer conversion (about 17.92% of maximum 600). Trial3 showed continued upward displacement during dwell. Increasing Kp/Kd does not directly fix drift after movement PID has exited; holding requires a separate evaluation or closed-loop holding implementation.

## Can the GUI electrically brake/short the motor?

**Code location:** `device_controller/controller_app/app.py:39`.

The current `app.py` creates its motor driver with:

```python
allow_braking=False
```

Under that policy, zero-speed and hardware-brake requests become zero-braking coast operations. The STOP button is labeled **STOP (Coast)**. Do not re-enable braking by changing `BRAKE_FLOOR`; that value does not override the no-braking policy.

Coast releases torque and can allow a suspended load to descend. It is not a position-holding mechanism. Start Log only controls recording and has no braking role.

## What if a sensor does not initialize?

Read the message occupying that sensor's plot area. Check the indicated dependency, power/connection, I2C bus/address, or LabJack access issue. Do not interpret a missing trace as a valid zero measurement.

LabJack retries connection automatically. An ADC that fails construction requires restarting after its initialization problem is corrected. Successful reads after a runtime LVDT error restore the plot.

## How should I exit?

Close the window or press **Ctrl+C in the launching terminal**. Both invoke normal cleanup after GUI initialization, including draining accepted sensor samples. Avoid interrupting file writes by forcibly killing the process. A blocked native device call or a process crash may still prevent complete cleanup.

For implementation history and test limitations, see [CHANGELOG_2026-10-16.md](CHANGELOG_2026-10-16.md).
