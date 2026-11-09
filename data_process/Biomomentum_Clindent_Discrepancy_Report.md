# Biomomentum versus Clindent: raw-to-fit discrepancy audit

Analysis date: November 9, 2026. Scope: the six Biomomentum recordings and nine
Clindent 400 g cycles selected by the current preprocessing notebooks. These
are repeated recordings/cycles, not 15 established independent specimens.

Experimental clarification from the user: the mechanical conditions are the
same. Clindent uses free-weight-drop loading; **400 g is the additional loading
weight, not a contact-force target**. Biomomentum uses velocity/force-controlled
loading. The load difference was not intentionally chosen, but the analysis
must use Clindent's **measured steady-state force**, rather than infer contact
force from the added mass. This clarification supersedes interpreting the
400 g label as a common force setpoint or calibration reference.

The largest demonstrated comparison problem is the different amount of early
creep removed before fitting. The loads also differ by almost a factor of two.
After matching time relative to loading within a common 50 s window, the
Clindent to Biomomentum ratio of group-mean displacement increment per measured force falls from
**2.65 to 1.24**. This substantially reduces the apparent difference, but does
not establish equivalent material properties or explain all remaining variation.

## What was traced and verified

The audit follows both complete processing paths:

- `data/Biomomentum_Data/txts/Creep_*.txt` ->
  `utils/Biomomentum_preprocess.ipynb` -> `data/Biomomentum_Data/Creep_*.csv`.
- `data/Clindent_Data/400g/Trial{0,1,2}/Force.csv`, `LVDT.csv`, and sequence logs ->
  `utils/Clindent_data_preprocess.ipynb` / `utils/creep_force_preprocessing.py` ->
  `data/Clindent_Data/400g/synchronized/Trial*_*.csv`.
- Both exported datasets -> `utils/creep_force_model.py:fit_gkv_hold`, using
  the current fitting notebook's bounds, five starts, and 4,000-point
  optimization limit. Fits were recomputed for both datasets, independently of
  which dataset was selected in the notebook's displayed outputs.

All 15 current exports reproduce from the raw files and current preprocessing
settings. Biomomentum is identical in the audit's in-memory reconstruction;
Clindent differs by at most **5e-11**, consistent with ten-decimal CSV rounding.
No unexplained unit conversion, amplitude scaling, or export corruption was
found. This verifies processing consistency, not the physical accuracy of the
sensors. Original data, preprocessing notebooks, and model code were not edited.

## Confirmed comparison differences

| Quantity | Biomomentum | Clindent |
|---|---:|---:|
| Mean measured hold force, equal weight per segment | 8.894 N | 4.509 N |
| Individual mean hold forces | 8.888–8.899 N | 4.395–4.633 N |
| Export duration | 56.820–57.445 s | 60.150–61.045 s or 150.550–150.920 s |
| Position at export start | 3.031–3.168 mm | 1.814–2.313 mm |
| Export start after sustained 90% load crossing | 2.860–3.245 s | 0.490–1.040 s |
| Sustained 10–90% load rise time | 0.320–0.670 s | 0.065–0.105 s |
| Force stability detector window | 2.0 s | 0.5 s |
| Stability confirmation | 1.0 s | 0.5 s |
| Force level / slope tolerance | 2% / 1% per second | 5% / 5% per second |

Here a sustained crossing means at least 0.1 s above the chosen fraction of the
last-five-second median force. It avoids using a momentary PID overshoot as the
loading reference. The Biomomentum source origin is the first raw timestamp;
the Clindent cycle origin is a recorded indentation command. Those two origins
should not be treated as the same physical event.

Biomomentum TXT headers explicitly specify **8.8515 N**, a **5 mm/s** stage
velocity, and a **60 s** creep interval (`Creep_1.txt`, lines 18–24). Thus the
Biomomentum load level is established before preprocessing. Clindent's `400g`
label denotes extra weight; its measured steady-state force is the load
reference for this analysis. A difference between that force and `0.4*g` is
not, by itself, evidence of a setup or calibration error.

By the Biomomentum export start, approximately 88% of its final absolute
position has already been reached. Clindent exports retain substantially more
of the fast early displacement response. Subtracting each export's first
position removes a vertical offset but cannot restore the early creep that
was removed from Biomomentum.

## Controlled comparisons

All values below are arithmetic means of per-segment measurements. Define the
diagnostic as `(position_end - position_start) / mean_force_in_window`.
It has units of mm/N but is **not an identified material compliance**. Mechanical
conditions are reported matched by the user; inherited state and force
linearity remain unverified, and loading trajectories are explicitly different.

| Comparison window | Biomomentum displacement / force | Clindent displacement / force | Clindent / Biomomentum |
|---|---:|---:|---:|
| First 50 s of each existing export | 0.04672 mm/N | 0.12403 mm/N | 2.65 |
| 4–54 s after sustained 90% loading | 0.03938 mm/N | 0.04868 mm/N | 1.24 |

The second window uses the full source history and is later than every current
export start. Four seconds is a common comparison anchor, not a fitted value
or an assertion that the sample has relaxed. Both comparisons use 50 s of
observation. The first uses the same exported-local clock; the second also
matches an operational time relative to loading.

All nine Clindent windows begin **2.281–2.648 s after the first recorded motor
release**, so this comparison excludes the motor-driven descent. Force
normalization already uses the measured mean within each compared window;
the 400 g label never enters these calculations.

The corresponding unnormalized displacement increments are **0.41549 versus
0.56013 mm** for the first comparison, and **0.35024 versus 0.21957 mm** for the
second. The direction of the raw displacement difference therefore changes
when the loading age is aligned. This is direct evidence that comparing
remaining amplitudes at different ages can be misleading.

Nearby anchors at 3.5, 4.5, and 5 s produce ratios of approximately **1.23–1.24**.
Replacing individual endpoint values with medians of the first/last 20 samples
also retains a similar result. Comparing only the first Clindent cycle from
each raw trial gives approximately **1.28** against the six Biomomentum
recordings at the 4 s anchor. Thus the reduction is not explained solely by
later Clindent cycles or one noisy endpoint. These are descriptive checks,
not significance tests or a causal decomposition of the discrepancy.

Subtracting Biomomentum's small initial force offset as an additional check
changes the common-loading-age ratio from **1.236 to 1.230**. The baseline
convention therefore has little effect on this remaining difference.

![Raw-to-fit comparison](analysis_outputs/biomomentum_clindent_audit/raw_to_fit_comparison.png)

The force panel uses sign-corrected native Biomomentum measurements and
baseline-corrected, synchronized Clindent measurements before steady cropping.
The remaining plots show why force normalization alone is insufficient without
also considering the time relative to loading.

## Follow-up: measured load and the loading mechanism

The reported mechanical equivalence narrows the interpretation to measured
loading history, initial state, preprocessing, and response-model limitations.
The following additional checks use the original force and controller logs:

- Every Biomomentum file records a target of **8.8515 N**. Subtracting the
  initial `0 <= time <= 0.15 s` median force from the exported mean gives
  **8.85020–8.85115 N**, averaging **8.850681 N**. The force increment follows
  the target closely within the same instrument. This is internal consistency,
  not an independent validation of physical force calibration.
- Historical Clindent sequence logs record `holding_weight_g=400` and
  `holding_speed_pct=17.9166667`. In the controller, the weight entry sets motor
  compensation. Position control uses encoder error, not ATI force feedback
  (`app.py:_apply_holding_weight`, `controllers.py:_run_position_move_locked`).
  This agrees with the user's free-weight loading description; the field does
  not specify a force-controlled 400 g-equivalent indentation.
- After the logged `released` state, the motor command is zero through the
  principal hold. Native force averaged from **release + 1 s** through the
  export end remains **4.3949–4.6323 N**. This independently checks that the
  roughly 4.5 N plateau persists after release. The primary comparisons retain
  uniform-time averages of the synchronized measured force, not this separate
  native-sample check or a mass conversion.
- Every current Clindent force-steady export begins **0.349–1.229 s before**
  the first logged `released` state. Nonzero motor commands occupy
  **0.53–2.01%** of exported time. A force-stability detector alone therefore
  does not establish that free-weight loading has fully begun. This is a
  candidate contributor to early fitted transients, not proof of their cause.
  It does not explain the persistent difference in plateau forces.

Free-weight loading and a finite-velocity approach to a regulated force give
different force histories even under the same mechanical conditions. Dividing
displacement by plateau force removes a force scale under a linear assumption;
it does not remove the Kelvin states inherited from that history. In the
notebook's own model, the remaining branch amplitude contains
`(F_hold - q_i(start))/(2*a*E_i)`. Matching time since a force threshold improves
comparability but does not force `q_i(start)` to match. The remaining roughly
24% discrepancy cannot be read directly as a modulus difference.

Follow-up evidence is saved in `biomomentum_load_target_audit.csv` and
`controller_load_audit.csv` under the analysis-output directory. The latter
includes source log paths and CSV line numbers for each motor-release event.
These supplemental tables came from direct metadata/log inspection and are
separate from the core 75-fit script outputs.

## What the fitted parameters do and do not establish

The current notebook fits:

`h(t) = h_start + A1*(1-exp(-t/tau1)) + A2*(1-exp(-t/tau2))`.

`fit_gkv_hold` uses measured force for diagnostics only. It does not normalize
amplitudes by force or use indenter geometry. `h_start` is a free fitted
position; `A1` and `A2` are remaining displacement amplitudes. They are not
elastic moduli, viscosities, or directly comparable zero-state compliances.

| Window and dataset | A1, mm | tau1, s | A2, mm | tau2, s |
|---|---:|---:|---:|---:|
| Full export, Biomomentum | 0.19275 | 3.432 | 0.24256 | 22.605 |
| Full export, Clindent | 0.32437 | 2.267 | 0.20496 | 25.085 |
| Export 0–50 s, Biomomentum | 0.18269 | 3.218 | 0.24753 | 20.546 |
| Export 0–50 s, Clindent | 0.30321 | 1.701 | 0.25469 | 19.844 |

The full-window slow-time means differ by only about 11%; in the common 50 s
window they differ by about 3.4%. A large discrepancy in every parameter is
therefore not supported by the current recomputed results.

However, moving the fit start within the *same exported recordings* changes
the parameters substantially. In the 5–55 s window, mean tau1/tau2 become
**5.66/30.61 s** for Biomomentum and **5.60/69.49 s** for Clindent. Several
Clindent slow constants reach the 120 s bound. Fits at the common loading-age
anchor also contain bound or extrapolation warnings. Their slow-time means
must not be treated as material estimates. Matching the data window reduces
a comparison bias; it does not solve parameter identifiability.

Seven of nine full-window Clindent fits flag large initial residuals. The
largest is **0.1278 mm**, in `Trial2_2`. The full fits also report **-3.35%**
force drift for `Trial1_2`, and **+4.20%** drift with **2.26%** force CV for
`Trial1_3`. Drift is computed from first/last one-second means, following the
model helper. The constant-force fit cannot account explicitly for these
changes and can absorb them into amplitudes and time constants. The notebook
prints warnings, but its full diagnostics table is currently commented out.

## Baselines, calibration, and experimental history

The following checks narrow the remaining possibilities:

- Biomomentum position starts near zero separately in each raw file. Clindent
  subtracts one initial 0.8 s baseline for each raw three-cycle trial. Later
  Clindent cycles therefore retain approximately **0.155–0.336 mm** displacement
  before the next indentation. The two position references and histories differ.
- Clindent Trial0, Trial1, and Trial2 use recovery intervals of approximately
  **30, 60, and 150 s**, respectively. Trial2 also has a much longer hold.
  Pooling all nine cycles obscures these protocol differences.
- Biomomentum's initial force offset is only **0.038–0.049 N**, less than 0.55%
  of its hold force. It cannot explain the nearly twofold force difference.
- All Clindent native LVDT rows satisfy
  `Position = (Voltage - 0.07)*(-2.14851485149) + 22.4304950495`
  to floating-point precision. This agrees with `calibration.json` and
  `device_controller/controller_app/hardware.py`, which logs voltage with an
  ADC offset but computes position before that offset. A constant offset is
  removed by the trial baseline. Physical calibration accuracy is unverified.
- Clindent's force CSV retains calibrated raw Fz alongside separate UI bias
  metadata. `bias_enabled=True` is not evidence that the recorded Fz was
  already bias-subtracted. The current acquisition code uses `bias_mode=False`.
  Logged UI bias and preprocessing baseline differ by at most 0.043 N; this
  does not support a large double-bias error.
- Correcting Clindent Fz to a three-axis force magnitude changes the retained
  load by less than 0.051%. Choosing Fz rather than magnitude cannot explain
  the discrepancy found here.
- Biomomentum headers record June 24, 2026 and a 20 Hz third-order force
  low-pass filter. The files do not identify specimen, contact site, indenter
  dimensions, thickness, or the recovery interval between separate files.

## Remaining interpretation and next comparison

Taking the user's matched mechanical conditions as given, the residual
approximately 24% difference could involve unequal loading/recovery history,
force-dependent response, instrumentation, or the observed force drift. These
alternatives cannot be separated from the current hold-response fit. Neither
the added 400 g mass nor the residual ratio establishes that either device
measures the wrong material response.

The next comparison should retain the measured load history and distinguish
the first cycles from repeated cycles, using the measured Clindent plateau as
the load reference. If an experimental comparison at equal force is desired,
the measured Clindent plateau is the relevant reference for a Biomomentum
force target; the extra mass is not. No force channel, controller setting, or
experimental record was rescaled or modified in this audit.

For physical GKV fitting, the separate `fit_gkv_history` function needs a
justified contact reference and initial internal state, as well as full force
history and geometry. Raw records make that investigation possible, but their
availability alone does not establish a relaxed initial state. Do not feed a
steady-only export into that model as a new zero-state load event.

## Reproduce the audit

From the project root, with the Dataprocess environment active:

```bash
python data_process/utils/audit_biomomentum_clindent.py
```

The script reads literal settings from the current notebooks without executing
their export cells. It reconstructs source data in memory, verifies all exports,
performs 75 fits, and writes to
`data_process/analysis_outputs/biomomentum_clindent_audit/`:

- `provenance_checks.csv`: raw source paths and maximum reconstruction errors.
- `segment_metrics.csv`: loads, timing, position changes, and matched-age metrics.
- `fit_sensitivity.csv` and `fit_group_means.csv`: all five fit windows and warnings.
- `load_age_anchor_sensitivity.csv`: nearby anchor and median-endpoint checks.
- `raw_to_fit_comparison.png`: comparison figure.
- `audit_snapshot.json`: input SHA-256 hashes, settings, and metric definitions.

Input hashes are checked across the analysis to reject concurrent source edits.
Generated output is under the repository's existing ignored analysis-output
directory. The report and script are separate additions; the source notebook
and data remain untouched. The audit completed successfully, its figure was
visually inspected, and the existing seven model regression tests passed.
