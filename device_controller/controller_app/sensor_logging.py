"""Buffered per-sample CSV recording, independent of UI refresh rates."""

import csv
import queue
import threading
import time


class SensorLog:
    def __init__(self, base_path, filenames=None):
        self._lock = threading.Lock()
        self._queue = queue.Queue(maxsize=50000)
        self._stopping = threading.Event()
        self._accepting = True
        self.dropped = 0
        self.error = None
        self.rows = {"lvdt": 0, "ati": 0}
        self._start = time.monotonic()
        self._files = {}
        try:
            for kind in self.rows:
                name = filenames[kind] if filenames else f"{base_path.stem}_{kind}.csv"
                path = base_path.with_name(name)
                self._files[kind] = path.open("x", newline="", encoding="utf-8")
            self._writers = {k: csv.writer(f) for k, f in self._files.items()}
            self._writers['lvdt'].writerow([
                'timestamp_ns', 'elapsed_time', 'Voltage (V)', 'Position (mm)',
            ])
            self._writers['ati'].writerow([
                'timestamp_ns', 'elapsed_time', 'Fx', 'Fy', 'Fz', 'Tx', 'Ty', 'Tz',
                'bias_enabled', 'bias_fx', 'bias_fy', 'bias_fz',
            ])
        except BaseException:
            for file in self._files.values():
                file.close()
            raise
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def offer(self, kind, timestamp_ns, monotonic_time, values):
        # Serialize stop with enqueue so the writer drains every accepted sample.
        with self._lock:
            if not self._accepting:
                return
            try:
                self._queue.put_nowait((kind, [timestamp_ns, monotonic_time - self._start, *values]))
            except queue.Full:
                self.dropped += 1

    def stop(self):
        with self._lock:
            self._accepting = False
            self._stopping.set()
        self._thread.join()

    def _run(self):
        flushed = time.monotonic()
        try:
            while not self._stopping.is_set() or not self._queue.empty():
                batch = []
                try:
                    batch.append(self._queue.get(timeout=0.05))
                except queue.Empty:
                    continue
                for _ in range(999):
                    try:
                        batch.append(self._queue.get_nowait())
                    except queue.Empty:
                        break
                for kind in self.rows:
                    rows = [row for source, row in batch if source == kind]
                    if rows:
                        self._writers[kind].writerows(rows)
                        self.rows[kind] += len(rows)
                if time.monotonic() - flushed >= 1.0:
                    for file in self._files.values():
                        file.flush()
                    flushed = time.monotonic()
        except Exception as exc:
            self.error = str(exc)
            with self._lock:
                self._accepting = False
        finally:
            for file in self._files.values():
                try:
                    file.close()
                except Exception as exc:
                    self.error = str(exc)
