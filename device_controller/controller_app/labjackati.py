"""ATI acquisition via LabJack T7; adapted from Tonometry_Data_Collection.

Requires the LabJack LJM driver, labjack-ljm and NumPy. No plotting.
"""

from labjack import ljm
import numpy as np
import time

## Frame Rate for Labjack ATI Reader is round 47hz +- 1hz

class LabjackATIReader:
    def __init__(self, 
                 sensor_num = '44297', 
                 bias_mode = False,
                 acquisition_rate = 1000,):
        if sensor_num == '44297':
            self.calibration_matrix = np.array([
                [-0.01852,  0.00308,  0.09314, -3.31759, -0.12893,  3.29239],
                [-0.17355,  4.01776,  0.04887, -1.93428,  0.09601, -1.89484],
                [ 3.76588, -0.07868,  3.76932, -0.06560,  3.81053, -0.07751],
                [-0.90533, 24.69259, 21.32183, -12.20765, -20.54142, -11.25907],
                [-24.54141,  0.43180, 11.58321, 20.15249, 12.91280, -20.36288],
                [-0.46556, 15.48853, -0.45853, 14.80759, -0.61616, 14.36472]
            ])
        elif sensor_num == '44298':
            self.calibration_matrix = np.array([
                [ 0.02023,  0.09133, -0.06271, -3.37843,  0.10388,  3.45523],
                [-0.05780,  3.92989, -0.00098, -1.85396, -0.08598, -2.10499],
                [ 3.84106, -0.03460,  3.82438, -0.07386,  3.83271, -0.17920],
                [-0.92585, 24.07110, 20.98591, -11.99680, -22.19816, -11.63771],
                [-25.34569, -0.13776, 11.73755, 20.41884, 11.56503, -21.88444],
                [-0.08462, 15.04212, 0.25775, 14.23111, 0.22923, 15.15714]
            ])

        else:
            raise ValueError(f"Unsupported ATI sensor number: {sensor_num}")

        # Open T7 device
        self.handle = ljm.openS("T7", "ANY", "ANY") 
        try:
            self.bias = None
            self.bias_mode = bias_mode
            # Differential channels
            ljm.eWriteName(self.handle, "AIN0_NEGATIVE_CH", 1)  # AIN0 - AIN1 (Fx)
            ljm.eWriteName(self.handle, "AIN2_NEGATIVE_CH", 3)  # AIN2 - AIN3 (Fy)
            ljm.eWriteName(self.handle, "AIN4_NEGATIVE_CH", 5)  # AIN4 - AIN5 (Fz)
            ljm.eWriteName(self.handle, "AIN6_NEGATIVE_CH", 7)  # AIN6 - AIN7 (Tx)
            ljm.eWriteName(self.handle, "AIN8_NEGATIVE_CH", 9)  # AIN8 - AIN9 (Ty)
            ljm.eWriteName(self.handle, "AIN10_NEGATIVE_CH", 11) # AIN10 - AIN11 (Tz)
    
            # Read channels
            self.channels = ["AIN0", "AIN2", "AIN4", "AIN6", "AIN8", "AIN10"]
            self.num_channels = len(self.channels)
            self.channel_address = ljm.namesToAddresses(self.num_channels, self.channels)[0]
    
            self.scan_rate = acquisition_rate  # Hz
            self.scans_per_read = 1
            try:
                ljm.eStreamStop(self.handle)
            except ljm.LJMError as e:
                if e.errorCode != 2620:
                    raise 
    
            self.actual_rate = ljm.eStreamStart(self.handle, self.scans_per_read, self.num_channels, self.channel_address, self.scan_rate)
            print(f"Stream started at {self.actual_rate:.2f} Hz\n")
        except BaseException:
            self.shutdown()
            raise

    def read(self):
        data = ljm.eStreamRead(self.handle)
        voltages = np.array(data[0]).reshape((self.scans_per_read, self.num_channels)).T

        # Calculate forces and torques
        forces_torques = np.dot(self.calibration_matrix, voltages)
        if self.bias is None:
            self.bias = forces_torques
            
        return forces_torques.reshape(-1) if not self.bias_mode else forces_torques.reshape(-1) - self.bias.reshape(-1)
    
    def clear_bias(self):
        self.bias = None
        self.bias_mode = False
        
    def enable_bias(self):
        self.bias = None
        self.bias_mode = True
        
    def shutdown(self):
        """Stop LabJack stream and close device connection."""

        if self.handle is None:
            return

        try:
            ljm.eStreamStop(self.handle)
            print("\nLabJack stream stopped.")

        except ljm.LJMError as e:
            if e.errorCode != 2620:
                print(f"Error stopping LabJack stream: {e}")

        finally:
            ljm.close(self.handle)
            self.handle = None
            print("LabJack connection closed.")

def main() -> None:
    reader = None
    try:
        reader = LabjackATIReader(
            sensor_num="44297", bias_mode=False, acquisition_rate=200,
        )
        previous = time.perf_counter()
        while True:
            values = reader.read()
            now = time.perf_counter()
            hz = 1.0 / max(now - previous, 1e-9)
            previous = now
            labels = ("Fx", "Fy", "Fz", "Tx", "Ty", "Tz")
            units = ("N", "N", "N", "Nmm", "Nmm", "Nmm")
            text = ", ".join(
                f"{label}: {value:+8.2f} {unit}"
                for label, value, unit in zip(labels, values, units)
            )
            print(f"\r{text} | Read rate: {hz:6.2f} Hz", end="", flush=True)
    except KeyboardInterrupt:
        print("\nStopping ATI acquisition.")
    finally:
        if reader is not None:
            reader.shutdown()


if __name__ == "__main__":
    main()
