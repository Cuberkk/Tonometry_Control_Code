# Tonometry Controller Changes — October 16, 2026

This document explains the controller changes developed during today's work: what changed, why it changed, and which earlier problems the changes address. It describes the current source tree rather than every intermediate implementation.

The GUI was developed as `app_KZ.py`; its current implementation is `device_controller/controller_app/app.py`. The package's `__init__.py` now imports `ControllerApp` from `.app`, and `device_controller/deviceController2.py` starts that application.

## 1. Files and responsibilities

| File | Responsibility and changes |
| --- | --- |
| `device_controller/controller_app/app.py` | GUI layout, asynchronous movement, LabJack acquisition, Bias controls, recording setup, diagnostics, and shutdown handling. |
| `device_controller/controller_app/live_plot.py` | New reusable renderer that updates sensor curves with Matplotlib blitting. |
| `device_controller/controller_app/sensor_logging.py` | New buffered, per-sample sensor CSV writer. |
| `device_controller/controller_app/labjackati.py` | LabJack T7 ATI acquisition adapted from the separate `Tonometry_Data_Collection` repository. |
| `device_controller/controller_app/hardware.py` | ADC sample delivery and initialization errors; configurable motor braking policy; ATI wrapper. |
| `device_controller/controller_app/controllers.py` | Arrival-state fixes, PID reset, gravity compensation, holding-command reporting, and shutdown fixes. |
| `device_controller/controller_app/config.py` | Loads the configurable PID gravity-compensation output. |
| `calibration.json` | Stores the revised PID settings and existing calibration/target settings. |
| `dfrobot_ads1115_fast.py` | Preserves the initialization error so the GUI can report the ADC connection failure. |

## 2. Responsive motion and reliable GUI updates

Previously, Run to Top/Bottom could wait for arrival inside a Tk button callback. The wait loop blocked the main thread, preventing normal drawing and delaying other interface actions.

Movement waiting now runs in a background thread. Worker messages go through a queue and are applied by the Tk main thread. Automated sequence inputs are read before starting the worker instead of reading Tk variables from the worker. Overlapping single-target moves are rejected.

The movement routine posts `On top` or `On bottom` when arrival is accepted within the configured tolerance, replacing the old status that could remain `Moving`. These messages indicate an arrival event, not a guarantee that the motor will remain inside tolerance indefinitely.

Manual downward speed overrides are retained during periodic command refreshes. Manual direction state is cleared on stopping so old direction values do not restart movement. Float-valued manual commands are converted to integers for display, avoiding integer-formatting exceptions.

## 3. Layout and plots

- Weights and Gantry controls are arranged side by side.
- LVDT displacement and ATI Fz plots are arranged side by side in equal-width grid columns, with matching figure sizes and control/status rows.
- Both plots use constrained layout to recalculate margins as the canvas changes size. This addresses clipped axis labels and tick labels.
- Each plot starts its time axis at its first displayed valid sample, eliminating the startup gap. The two display time origins are independent; use the CSV timestamps for cross-sensor alignment.
- The first display window is 0–30 seconds. The window then rolls forward in approximately one-second steps.
- LVDT limits are fixed at **-1 to 30 mm** and ATI Fz limits at **-25 to 5 N**, including after Bias changes.
- A solid black horizontal line marks **y = 0** on both plots.
- Remaining Chinese interface messages were translated into English. User and Trial defaults are both `0`.

### Plotting performance

Previously, frequent full Matplotlib redraws repeated expensive layout, axis, label, and grid rendering.

`LiveLineRenderer` now caches the static axes background and redraws only the changing curve on most frames. Resizing, changing axis limits, or changing a title invalidates the cache and triggers a full redraw. Axis scrolling occurs less frequently than curve updates to allow background reuse.

The requested GUI heartbeat and curve refresh rate is **30 Hz** (33 ms scheduling). The GUI separately reports measured callback Hz and actual LVDT/ATI drawing FPS. These are different from sensor acquisition rates and are not hard real-time guarantees.

## 4. LabJack ATI integration and Bias

The earlier network ATI test implementation was replaced by the LabJack T7 reader adapted from:

`Tonometry_Data_Collection/force_reading/LabjackATIReader.py`

The reader configures six differential analog channels, applies the sensor-specific calibration matrix, and returns six calibrated force/torque values. The current GUI selects sensor **44297** and requests **800 Hz** acquisition. Actual read delivery can occur in bursts and depends on the driver and device.

The LabJack handle is owned by the acquisition worker. The GUI reads snapshots rather than calling hardware read methods in its event loop. Initialization/read failures are reported, and the worker retries connections after a delay.

`Bias ATI Force` captures the current Fx/Fy/Fz offsets for display; `Remove Bias` restores the raw calibrated display. Fz in this GUI uses the reader's raw sign convention without the additional load compensation used by the separate hardware wrapper.

Displacement has matching Bias/Remove Bias controls. The underlying raw LVDT samples remain unchanged when display Bias is enabled.

## 5. Sensor availability and initialization failures

An empty graph previously gave little information about missing hardware.

The corresponding canvas is now hidden until valid sensor data is available. Its place shows an English connection/initialization message and the available error reason, such as a missing dependency, unavailable I2C bus, or device response failure. LabJack recovery restores its plot automatically. LVDT read errors hide its plot until successful reads resume; an ADC that failed initial construction still requires restarting the application after the problem is corrected.

ADC initialization errors are retained instead of silently discarded. `app.py` also disables the unused legacy ADC sample queue while retaining the per-sample saving callback. The queue remains available by default for other callers.

## 6. Recording structure and frequency

The earlier logger read the latest sensor values at approximately 20 Hz. That could omit samples collected between logger iterations, and the original combined log did not include the ATI stream.

The application now keeps the control log and records sensors independently through `SensorLog`. Acquisition code enqueues each successful sample; a background writer batches CSV rows. The bounded queue reports drops, flushes periodically, and drains accepted samples when recording stops. Writing failures are reported in the recording status.

The GUI has three recording inputs: **User #**, **Trial #**, and **Project directory**, with a directory browser. Project directory defaults to the repository root. The resulting layout is:

```text
<Project directory>/User_Data/User0/Trial0/
├── Force.csv
├── LVDT.csv
└── sequence_<timestamp>.csv  # or session_<timestamp>.csv
```

User/Trial identifiers must contain digits. Existing Force/LVDT files prevent starting another recording at the same trial path, avoiding accidental replacement. Automatic sequences do not start if automatic log creation fails. Directory edits apply to the next recording.

### Saved fields

| File | Contents |
| --- | --- |
| `Force.csv` | Host timestamp in nanoseconds, elapsed time, Fx/Fy/Fz/Tx/Ty/Tz, Bias-enabled flag, and Fx/Fy/Fz Bias offsets. Display-force columns were removed. |
| `LVDT.csv` | Host timestamp in nanoseconds, elapsed time, raw calibrated voltage and displacement. Display Bias is not applied to the saved displacement. |
| Control CSV | Encoder counts, LVDT snapshot, controller state/target/output, sequence status, nanosecond timestamp, elapsed time, actual recording interval, requested target, error, manual direction/output, PID enable state, arrival flag, and configured holding percentage. |

The control log now requests **200 Hz** snapshots, rather than 20 Hz, to expose short movement transients. It is not synchronized to individual PID iterations. Sensor files preserve their own successful read rates instead of repeating the latest value at a fixed GUI rate.

The legacy control column named `timestamp_iso` still contains Unix seconds, not an ISO-formatted date. The added `timestamp_ns` and `elapsed_time` columns make the timing explicit.

All sensor timestamps are software receipt timestamps, not hardware conversion timestamps. ATI host intervals can be very short within a batch and long between batches even when the overall average is 800 Hz. Nanosecond units do not imply nanosecond timing accuracy.

## 7. Encoder diagnosis and PID changes

The original settings included `min_output = 400`, exceeding the active movement limits (approximately 245 upward and 60 downward during the examined trials). This forced even small nonzero PID requests toward the speed limit, contributing to abrupt command reversals instead of gradual slowing.

The revised settings are:

| Setting | Previous | Current |
| --- | ---: | ---: |
| Kp | 1.0 | 1.0 |
| Ki | 0.1 | 0.0 |
| Kd | 0.04 | 0.04 |
| Minimum output | 400 | 0 |
| Gravity feedforward | 0 | 170 |

After removing the minimum output, Trial2 stalled at position 113 against target 275, with error/output both 162. A configurable feedforward value was then added after PID calculation and before output clamping. It applies to positive-position targets, while target 0 retains the existing downward policy.

Trial3 reached the target tolerance in all three lifts without recorded negative upward-movement commands. The value 170 remains an experimentally selected starting point, not a proven universal balance output.

New moves clear the old arrival flag and reset PID history. Arrival now applies the existing holding command immediately instead of temporarily leaving the last movement command active. Holding commands are also reflected in `last_cmd`, correcting logs that previously showed zero despite an active holding output.

**Remaining limitation:** arrival still exits movement PID and switches to fixed-output holding, rather than closed-loop position holding. Trial3 showed upward drift during dwell. Gravity feedforward affects movement; it does not replace the separate fixed holding output. The current calibration has Top = 315 counts; the earlier diagnostic trials used Top = 275. Shared calibration/controller changes also affect other applications using those modules.

## 8. Coast-only behavior for the current GUI

The current GUI constructs `Motoron` with `allow_braking=False`. At the driver boundary, zero-speed requests and requests for nonzero hardware braking are converted to zero-braking coast commands. The stop button is labeled `STOP (Coast)`. Stop also clears the previous holding target under this policy so an idle iteration does not resume that holding command.

The compatibility method names may still include `brake`, but the current GUI's driver policy prevents those calls from requesting nonzero short-circuit braking. Other callers retain the default braking policy unless they explicitly disable it.

Start Log remains independent of motor stopping: it supports manual recording, while sequences start recording automatically when necessary.

**Mechanical implication:** coast releases motor torque and does not hold a suspended load. Gravity-driven movement after release is possible. This change is a software policy, not a hardware interlock or a mechanical holding mechanism.

## 9. Normal shutdown and terminal Ctrl+C

SIGINT handling now records a shutdown request instead of interrupting a Tk callback with cleanup work. A periodic main-thread callback invokes the normal window-close path. Closing is guarded against duplicate requests.

The close path requests cancellation, coasts motors, drains sensor recording, stops the controller/ADC, waits for workers with their configured timeouts, closes the encoder, restores the prior SIGINT handler, and destroys the window. The paused PID worker is woken during shutdown so it can exit.

This handles Ctrl+C after normal GUI initialization. It does not guarantee cleanup after SIGKILL, a native-driver crash, or indefinitely blocked device I/O; some worker joins are time-limited.

## 10. Validation and observed results

Checks performed during development included syntax compilation, mocked movement/cancellation tests, sensor CSV column alignment and path tests, overwrite rejection, concurrent writing/draining tests, simulated device errors, Bias toggling, SIGINT dispatch, and offscreen rendering tests. Temporary test CSV files were removed. Existing experimental CSV files were analyzed without modification.

| Source | Average recorded LVDT frequency |
| --- | ---: |
| Collection repository `data/trial_0/LVDT_trial_0.csv` | 391.97 Hz |
| Current application Trial1 | 103.89 Hz |
| Current application Trial3 | 102.49 Hz |
| Current application Trial5 | 197.99 Hz |

Trial5 still had a maximum LVDT host-record interval of approximately 143.85 ms. These trials were not controlled one-variable benchmarks, so they do not establish which change caused the frequency difference.

Force recordings in Trials1 and 3 averaged approximately 800 Hz. Synthetic writer tests preserved 800 Hz LVDT plus 200 Hz ATI, and a stress test preserved 2,000 Hz for each simulated stream with no queue drops. Those tests demonstrate writer capacity, not hardware acquisition capability.

An offscreen Matplotlib test of 60 curve updates took approximately 8.18 seconds with full drawing versus 0.077 seconds with background reuse. This is an Agg-rendering benchmark, not a Raspberry Pi GUI frame-rate measurement.

Real hardware testing from the assistant environment remained limited: the exposed environment lacked `/dev/i2c-1`, and a LabJack native-driver test failed under sandbox socket restrictions. Hardware behavior was assessed using the supplied trial logs; the code is not certified as fully tuned or real-time deterministic.

## 11. Follow-up measurements

- Compare actual GUI FPS and LVDT timestamps under identical loads before increasing refresh rates further.
- Isolate LVDT-only, plotting, ATI, and motor-control loads to identify the remaining long acquisition gaps.
- Investigate I2C contention and hardware conversion freshness before claiming a higher effective sensor bandwidth.
- Evaluate closed-loop holding separately from movement PID to address dwell drift.
- Revalidate gravity compensation for changed loads, targets, or mechanics.
