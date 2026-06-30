#!/usr/bin/env python3
"""PD auto-tuning helper for the Tonometry weights axis."""

from __future__ import annotations

import argparse
import csv
import math
import time
from pathlib import Path
from typing import Optional

from .. import config
from ..hardware import Motoron, QuadratureEncoder


def _open_log_writer(path: Optional[Path]) -> tuple[Optional[csv.writer], Optional[object], Optional[Path]]:
    if path is None:
        return None, None, None
    path.parent.mkdir(parents=True, exist_ok=True)
    fh = path.open("w", newline="", encoding="utf-8")
    writer = csv.writer(fh)
    writer.writerow(["t_s", "encoder_counts", "error_counts", "command"])
    return writer, fh, path


class RelayAutoTuner:
    """Symmetric relay test to extract Ku and Pu."""

    def __init__(
        self,
        motor: Motoron,
        encoder: QuadratureEncoder,
        *,
        target_counts: int,
        amplitude: int = 200,
        hysteresis: int = 50,
        duration: float = 20.0,
        sample_rate_hz: float = 50.0,
        log_writer: Optional[csv.writer] = None,
        invert_encoder: bool = False,
        print_cmd: bool = False,
    ):
        if amplitude <= 0 or amplitude > config.MOTORON_MAX:
            raise ValueError(f"Amplitude must be between 1 and {config.MOTORON_MAX}.")
        if hysteresis <= 0:
            raise ValueError("Hysteresis must be > 0 counts.")
        self.motor = motor
        self.encoder = encoder
        self.target = int(target_counts)
        self.amplitude = int(amplitude)
        self.hysteresis = int(hysteresis)
        self.duration = float(duration)
        self.sample_interval = 1.0 / max(5.0, float(sample_rate_hz))
        self.log_writer = log_writer
        self._invert = -1 if invert_encoder else 1
        self._print_cmd = print_cmd

    def run(self) -> dict:
        start = time.monotonic()
        peaks_high: list[tuple[float, float]] = []
        peaks_low: list[tuple[float, float]] = []

        print(
            f"[autotune] Starting relay test for {self.duration:.1f}s | "
            f"amp={self.amplitude} | hysteresis={self.hysteresis}"
        )

        try:
            initial_pos = self._invert * self.encoder.get_position()
            initial_error = self.target - initial_pos
            cmd = self.amplitude if initial_error >= 0 else -self.amplitude
            self.motor.set_speed(config.MOTOR_ID, cmd)
            if self._print_cmd:
                print(f"[cmd] init -> {cmd:+d} | pos={initial_pos} | error={initial_error}")

            while True:
                now = time.monotonic()
                elapsed = now - start
                if elapsed >= self.duration:
                    break

                pos = self._invert * self.encoder.get_position()
                error = self.target - pos

                if error >= self.hysteresis and cmd <= 0:
                    cmd = self.amplitude
                    peaks_low.append((now, pos))
                    self.motor.set_speed(config.MOTOR_ID, cmd)
                    if self._print_cmd:
                        print(f"[cmd] +amp -> {cmd:+d} | pos={pos} | error={error}")
                elif error <= -self.hysteresis and cmd >= 0:
                    cmd = -self.amplitude
                    peaks_high.append((now, pos))
                    self.motor.set_speed(config.MOTOR_ID, cmd)
                    if self._print_cmd:
                        print(f"[cmd] -amp -> {cmd:+d} | pos={pos} | error={error}")

                if self.log_writer:
                    self.log_writer.writerow((elapsed, pos, error, cmd))

                time.sleep(self.sample_interval)
        finally:
            self.motor.set_speed(config.MOTOR_ID, 0)
            self.motor.coast_all()

        if len(peaks_high) < 2 or len(peaks_low) < 2:
            raise RuntimeError("Not enough oscillations captured; try longer duration or higher amplitude.")

        avg_high = sum(p for _, p in peaks_high) / len(peaks_high)
        avg_low = sum(p for _, p in peaks_low) / len(peaks_low)
        amplitude_counts = max(1e-3, (avg_high - avg_low) / 2.0)
        control_amp = float(self.amplitude)
        ku = (4.0 * control_amp) / (math.pi * amplitude_counts)
        periods = self._estimate_periods(peaks_high, peaks_low)
        if not periods:
            raise RuntimeError("Unable to estimate oscillation period; collected data was degenerate.")
        pu = sum(periods) / len(periods)

        kp, kd = self._ziegler_nichols_pd(ku, pu)
        return {
            "ku": ku,
            "pu": pu,
            "amplitude_counts": amplitude_counts,
            "kp": kp,
            "kd": kd,
            "peaks_high": peaks_high,
            "peaks_low": peaks_low,
        }

    @staticmethod
    def _estimate_periods(
        peaks_high: list[tuple[float, float]],
        peaks_low: list[tuple[float, float]],
    ) -> list[float]:
        periods = []
        if len(peaks_high) >= 2:
            periods.extend(
                peaks_high[i][0] - peaks_high[i - 1][0] for i in range(1, len(peaks_high))
            )
        if len(peaks_low) >= 2:
            periods.extend(peaks_low[i][0] - peaks_low[i - 1][0] for i in range(1, len(peaks_low)))
        return [p for p in periods if p > 0]

    @staticmethod
    def _ziegler_nichols_pd(ku: float, pu: float) -> tuple[float, float]:
        kp = 0.8 * ku  # ZN PD recommendation
        td = 0.125 * pu
        kd = kp * td
        return kp, kd


def _default_target(calibration: dict) -> int:
    try:
        return int(calibration.get("weights_encoder", {}).get("top_counts", 5000))
    except Exception:
        return 5000


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PD autotune utility for the weights axis.")
    parser.add_argument("--target", type=int, default=None, help="Encoder counts to oscillate around (default: top target).")
    parser.add_argument("--amplitude", type=int, default=200, help="Motoron command magnitude for relay test (default: 200).")
    parser.add_argument("--hysteresis", type=int, default=80, help="Error threshold in counts before toggling command (default: 80).")
    parser.add_argument("--duration", type=float, default=25.0, help="Test duration in seconds (default: 25).")
    parser.add_argument("--sample-rate", type=float, default=50.0, help="Logging sample rate (Hz).")
    parser.add_argument("--log", type=str, default=None, help="Optional CSV log path (default: logs/pid_autotune_*.csv).")
    parser.add_argument(
        "--min-output",
        type=float,
        default=None,
        help="Optional PD min output floor to save; defaults to 0.25 * amplitude when --save is used.",
    )
    parser.add_argument("--print-cmd", action="store_true", help="Print command toggles during the relay test.")
    parser.add_argument("--save", action="store_true", help="Save suggested PD gains back into calibration.json (weights_pid).")
    parser.add_argument("--dry-run", action="store_true", help="Skip motor commands (for scripting / unit tests).")
    parser.add_argument("--invert-encoder", action="store_true", help="Multiply encoder counts by -1 during tuning.")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None):
    args = parse_args(argv)
    calibration = config.load_calibration()
    target = args.target if args.target is not None else _default_target(calibration)

    log_path = Path(args.log) if args.log else (config.LOG_DIR / f"pid_autotune_{time.strftime('%Y%m%d_%H%M%S')}.csv")
    log_writer, log_fh, resolved_log_path = _open_log_writer(log_path)

    if args.dry_run:
        print("[dry-run] Skipping hardware interaction.")
        return

    mot = Motoron(config.MOTORON_ADDR, config.I2C_BUS)
    encoder = QuadratureEncoder(config.ENCODER_GPIO_A, config.ENCODER_GPIO_B, invert=config.ENCODER_INVERT)

    try:
        encoder.set_position(0)
        print("[setup] Encoder zeroed to 0 counts.")
    except Exception as exc:
        print(f"[setup] Warning: failed to zero encoder: {exc}")

    print(f"[setup] Using target={target} counts (encoder currently {encoder.get_position()}).")
    print("[setup] Ensure the weights can swing safely around the target before continuing.")

    tuner = RelayAutoTuner(
        mot,
        encoder,
        target_counts=target,
        amplitude=args.amplitude,
        hysteresis=args.hysteresis,
        duration=args.duration,
        sample_rate_hz=args.sample_rate,
        log_writer=log_writer,
        invert_encoder=args.invert_encoder,
        print_cmd=args.print_cmd,
    )

    try:
        result = tuner.run()
    except KeyboardInterrupt:
        print("\n[autotune] Interrupted by user.")
        mot.coast_all()
        raise SystemExit(1)
    except RuntimeError as exc:
        print(f"[autotune] Error: {exc}")
        print(f"[autotune] Encoder stopped at {encoder.get_position()} counts.")
        mot.coast_all()
        raise SystemExit(1)
    finally:
        encoder.close()
        if log_fh:
            log_fh.flush()
            log_fh.close()

    print("\n=== Relay test summary ===")
    print(f"Ultimate gain Ku : {result['ku']:.3f}")
    print(f"Ultimate period Pu: {result['pu']:.3f} s")
    print(f"Oscillation amplitude: {result['amplitude_counts']:.3f} encoder counts")
    print("\nSuggested PD gains (Ziegler-Nichols):")
    print(f"  Kp = {result['kp']:.4f}")
    print(f"  Kd = {result['kd']:.4f}")
    if resolved_log_path:
        print(f"\nLog saved to: {resolved_log_path}")

    if args.save:
        min_output = args.min_output if args.min_output is not None else args.amplitude * 0.25
        calibration.setdefault("weights_pid", {})
        calibration["weights_pid"]["kp"] = result["kp"]
        calibration["weights_pid"]["ki"] = 0.0  # Integral disabled for PD control
        calibration["weights_pid"]["kd"] = result["kd"]
        calibration["weights_pid"]["deadband_counts"] = args.hysteresis
        calibration["weights_pid"]["min_output"] = min_output
        config.save_calibration(calibration)
        config.reload_calibration()
        print(f"[save] Updated PD gains in {config.CALIBRATION_PATH} (min_output={min_output})")


if __name__ == "__main__":
    main()
