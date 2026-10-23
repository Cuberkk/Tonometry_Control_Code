# PID Explained and a Practical Tuning Guide

This guide introduces PID control and explains how to tune the weight axis in this repository. The numerical examples describe the current software, not settings guaranteed to work for every load.

## 1. What is PID?

A position controller repeatedly compares a requested position with a measured position:

```text
error = target position - measured position
```

It uses that error to choose a motor command. PID combines three terms:

```text
P = Kp × error
I = Ki × accumulated(error × dt)
D = Kd × (current error - previous error) / dt
PID output = P + I + D
```

`dt` is the actual time between calculations, in seconds. In this application, position and error are encoder counts, while the output is a Motoron command. It is not a direct torque measurement.

| Term | What it responds to | Too little | Too much |
| --- | --- | --- | --- |
| P: proportional | Current position error | Slow correction or a persistent offset under load | Overshoot, oscillation, or saturation |
| I: integral | Error accumulated over time | Persistent offset may remain | Windup, slow oscillation, and overshoot |
| D: derivative | How quickly error changes | Insufficient damping in some systems | Sensitivity to encoder noise, abrupt commands, or excessive opposition to movement |

These effects are tendencies, not universal rules. Friction, gravity, gearing, motor-driver acceleration limits, and sampling delays also determine the response.

### Example: proportional control

For target 275, measured position 225, and Kp = 1:

```text
error = 275 - 225 = 50 counts
P output = 1 × 50 = 50 command units
```

If command 50 cannot lift the weight, proportional-only control may stop below the target. The controller can be computing correctly while the mechanism cannot move.

## 2. Gravity compensation is separate from PID

A vertical mechanism needs effort to support its load even when its position error is zero. A feedforward term estimates that effort:

```text
requested output = gravity feedforward + P + I + D
final output = requested output limited to the allowed range
```

Feedforward supplies a baseline; feedback corrects the remaining error. Raising Kp or imposing a large minimum output is not equivalent to correctly compensating gravity.

The current software adds `gravity_output` for positive-position targets before limiting the command. Target 0 does not receive that addition. This target-dependent rule is specific to the present setup and may not be appropriate if the coordinate system or target meanings change.

## 3. How this repository implements control

| Component | File and current location |
| --- | --- |
| PID calculation, integral clamp, and anti-windup | `device_controller/controller_app/controllers.py`, class `PIDController`, methods `reset()` and `compute()` |
| Weight controller frequency setting | `controllers.py:142`, `refresh_hz=200.0` |
| Movement command and feedforward | `controllers.py:310`, `_compute_pid_command()` |
| Fixed-output holding after arrival | `controllers.py:303`, `_apply_holding_command()` |
| Active PID settings | Root `calibration.json:38–45` |
| PID configuration loading | `device_controller/controller_app/config.py:121–128` |
| Speed and holding limits | `config.py:107`, `:110–111` |
| Arrival tolerance | `calibration.json:35`, `weights_encoder.tolerance_counts` |

Line numbers describe the source at the time this guide was written. Search for the named methods if later edits move them.

Current PID configuration:

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

These are the current experimental settings, not a completed tuning result. Preserve the rest of `calibration.json` when editing this section, then restart the application. Shared configuration changes affect other applications using these controller modules.

### Important implementation details

- **Arrival tolerance is not PID deadband.** Tolerance ends the movement when error is small enough. Although `deadband_counts` is loaded, it is not currently used by `_compute_pid_command()`.
- **Arrival ends movement PID.** The controller switches to a fixed holding command. Movement gains do not correct subsequent holding drift.
- **The 200 Hz setting is a request.** The control loop performs work and then sleeps, so its actual frequency is normally lower and may vary.
- **Control logging is separate.** The 200 Hz diagnostic logger samples controller state independently of PID updates; repeated commands in adjacent rows do not necessarily mean PID ran twice.
- **Anti-windup has limits.** The PID's internal output clamp is applied before the weight controller's speed clamp and feedforward. It does not fully account for saturation introduced by those later stages.
- **The D term uses error differences without filtering.** Encoder quantization and irregular timing can make its output noisy.
- **A new move resets PID history.** This avoids carrying integral/derivative state from the preceding move, but does not eliminate mechanical inertia.

## 4. Understand the output limits before tuning

With `MOTORON_MAX=600`, a 41% lifting limit is approximately 246 command units; floating-point truncation can produce 245. The automatic downward limit is 10%, approximately 60.

A large `min_output` can destroy smooth control. Previously, `min_output=400` exceeded both limits. Small nonzero corrections were pushed to the available limit, producing abrupt positive/negative commands.

Keep `min_output=0` while identifying the plant and feedforward. If a friction compensation scheme is later needed, design and validate it explicitly rather than restoring the old value.

Speed percentages are driver command limits, not measured RPM or motor torque percentages. Driver acceleration/deceleration settings also influence the actual movement.

## 5. What should be recorded?

For every tuning experiment, save:

- Encoder position, requested target, and position error.
- Controller state, motor command, and arrival flag.
- Timestamp and actual logging interval.
- Manual direction/output and whether the position controller is enabled.
- LVDT position and ATI force for comparison with mechanical motion.
- The exact PID/feedforward settings, speed limits, tolerance, load, and test description.

The current control CSV includes many of these fields, but it does not automatically save a complete parameter snapshot or per-iteration P/I/D components. Keep a copy of the configuration with each experiment. A logged software command is not a measurement of delivered torque.

Save each test under a new User/Trial path. Do not overwrite an earlier trial or change multiple parameters without recording what changed.

## 6. A practical tuning procedure

### Step 1: Establish the mechanical and measurement baseline

Confirm the encoder direction and coordinate zero. Verify that the intended upward command increases position, and choose targets that stay inside the usable travel.

Use the same load and a short, repeatable move for the first comparisons. Ensure the load is supported appropriately: this GUI uses coast-only stopping, which releases torque and may allow gravity-driven motion.

Check encoder continuity and timing before adjusting gains. A disconnected encoder, a reversed sign, command conflicts, or long control stalls cannot be fixed by PID tuning.

### Step 2: Separate movement from holding

Plot encoder position, target, and command over time. Mark when the state changes from movement to arrival/idle.

If position drifts only after arrival, investigate the holding output. Do not conclude that movement Kp/Ki/Kd caused a drift that occurs after movement PID has stopped.

For initial experiments, retain a consistent tolerance. A large tolerance can make a controller appear fast simply because it stops controlling early.

### Step 3: Establish conservative output limits

Select enough output to move the mechanism, while keeping motion manageable. Keep that limit unchanged when comparing gain changes.

If the command stays at the limit for nearly the entire move, increasing Kp further provides little information: the actuator is already saturated. Check load, available output, and feedforward instead.

Do not tune by deliberately forcing sustained oscillation on a suspended-load mechanism.

### Step 4: Estimate feedforward for vertical movement

With the integral disabled, compare position/velocity at different baseline outputs under the same load. Use a controlled, supported procedure to estimate the output needed to resist downward motion without sustained upward drift.

A stalled mechanism does not identify a unique balance command: static friction, motor heating, and drive conditions affect it. Likewise, a command that maintains upward movement is not necessarily the command that holds position.

In the recorded experiments, Trial2 stalled at position 113 with target 275 and command 162. Feedforward 170 was selected as an initial experiment; it was not a measured equilibrium value.

### Step 5: Tune proportional response

Keep Ki at zero. For a clean P-only experiment, set Kd to zero as well; this is a deliberate new test, not a claim that the existing Kd=0.04 is wrong.

Start with a modest Kp and compare identical short moves. Increase it gradually if correction is weak, and reduce it if the response becomes excessively abrupt or oscillatory. Record each change.

Assess the whole trace: arrival time, approach speed, overshoot, command saturation, and reversals. A faster first arrival is not necessarily a better response.

### Step 6: Add derivative damping if needed

Once proportional response is repeatable, add a small Kd if the mechanism approaches too quickly or overshoots.

For a fixed target, decreasing error produces a negative D contribution, which can slow upward motion. However, with low feedforward it can also remove too much lifting effort. Inspect the command and error together.

If commands change sharply when encoder counts change by one or control timing becomes irregular, do not keep increasing Kd. Consider derivative filtering and a more regular control period as separate code changes.

### Step 7: Introduce integral only for a demonstrated need

Add a small Ki only if a persistent error remains after feedforward and P/D are reasonable, and verify that movement PID remains active long enough for integral correction.

Integral cannot fix drift during the current fixed-output holding state. With the present early-arrival transition, it may also have little opportunity to correct an error already inside tolerance.

Watch for increased overshoot, delayed recovery, and saturation. Before relying on larger Ki, improve anti-windup so it accounts for the final speed limit and feedforward.

### Step 8: Evaluate arrival and holding separately

Repeat several cycles and compare the position at first arrival with the position at the end of dwell.

A consistent upward drift suggests excess holding effort; downward drift suggests insufficient holding effort or a release/transition issue. Fixed holding may vary with load and friction. Closed-loop holding is a separate implementation change and has not yet been added here.

Do not simply increase the movement feedforward to correct holding drift; the two settings act in different states.

### Step 9: Validate the intended operating range

After the short-move tests, repeat with the intended travel, dwell, and representative loads. Compare multiple trials and inspect long timing gaps.

Use acceptance criteria appropriate to the experiment, such as maximum allowed position error, acceptable drift during dwell, no unwanted command reversals, and repeatable arrival. Do not adopt a universal numerical criterion without considering the mechanism.

## 7. Reading common symptoms

| Symptom | Possible causes | What to inspect first |
| --- | --- | --- |
| Stops below Top while the GUI still shows movement | Insufficient feedforward/output, static friction, weak P correction | Position error, final command, output limit |
| Upward movement followed by a strong downward command | Overshoot, D response, large minimum output, sign/timing issues | Encoder trace and signed command around the reversal |
| Continues upward during dwell | Fixed holding output too high | State transition and holding command |
| Drops when stopped | Coast releases torque | Stop policy and mechanical support |
| Reports arrival too early | Tolerance too wide, stale arrival state, sparse observations | Error, arrival flag, and target at the same time |
| Noisy or rapidly alternating commands | Encoder quantization/noise, large Kd, timing jitter | Encoder increments and actual control dt |
| High average sample rate but visible gaps | Batched reads, drawing/CPU load, I2C contention | Interval distribution, not just average Hz |

A symptom can have multiple causes. This table is a diagnostic starting point, not a guaranteed gain prescription.

## 8. What the recorded trials showed

| Trial | Observation | Interpretation |
| --- | --- | --- |
| Early Trial1 | Upward movement included command -245 after exceeding target | Saturated/reversing commands required investigation; minimum output was excessive |
| Trial2 | Position remained near 113, target 275, command 162 | Removing the floor exposed insufficient lifting effort near the target |
| Trial3 | Three lifts entered tolerance without recorded negative commands during the upward phase | Feedforward improved reachability in that test |
| Trial3 dwell | Position continued upward by approximately 7, 11, and 16 counts | Fixed-output holding still needs separate work |

These observations support the identified mechanisms but do not prove global stability. Loads, targets, and configuration may have changed between experiments.

## 9. Suggested trial note

Copy this template into your experiment notes:

```text
User / Trial:
Date:
Load and mechanical setup:
Encoder zero reference:
Top / Bottom / tolerance:
Kp / Ki / Kd:
Gravity feedforward / minimum output:
Lift / downward speed limits:
Holding output percentage:
Requested and measured rates:
Parameter changed from previous trial:
Arrival time / overshoot:
Dwell drift:
Unexpected command reversals or timing gaps:
Decision for next trial:
```

For other application settings, see [NEW_USER_CONFIGURATION_GUIDE.md](NEW_USER_CONFIGURATION_GUIDE.md). For today's implementation changes and validation limits, see [CHANGELOG_2026-10-16.md](CHANGELOG_2026-10-16.md).
