#!/usr/bin/env python3
"""Reads ZBDATA lines from the coordinator's serial port and appends them to a CSV log.
Usage: python zb_logger.py COM8            (Windows)
       python zb_logger.py /dev/ttyUSB0    (Linux/macOS)
Requires: pip install pyserial
"""
import csv, sys, time
from datetime import datetime
from pathlib import Path
import serial

PORT = sys.argv[1] if len(sys.argv) > 1 else "COM8"
BAUD = 115200
LOG_DIR = Path("zigbee_logs")
LOG_DIR.mkdir(exist_ok=True)
HEADER = ["pc_timestamp", "esp_uptime_ms", "sensor_id", "short_addr",
          "endpoint", "cluster", "attribute", "value"]

def open_log():
    path = LOG_DIR / f"zigbee_{datetime.now():%Y-%m-%d}.csv"
    new = not path.exists()
    f = open(path, "a", newline="", encoding="utf-8")
    w = csv.writer(f)
    if new:
        w.writerow(HEADER)
    return path, f, w

def main():
    path, f, w = open_log()
    print(f"Logging to {path}")
    while True:
        try:
            with serial.Serial(PORT, BAUD, timeout=1) as ser:
                while True:
                    raw = ser.readline().decode("utf-8", errors="ignore")
                    idx = raw.find("ZBDATA,")          # skips any log prefix
                    if idx < 0:
                        continue
                    parts = raw[idx + 7:].strip().split(",")
                    if len(parts) != 7:
                        continue
                    uptime, sensor_id, short, ep, cluster, attr, value = parts
                    now = datetime.now()
                    if f.name != str(LOG_DIR / f"zigbee_{now:%Y-%m-%d}.csv"):
                        f.close(); path, f, w = open_log()   # new file at midnight
                    w.writerow([now.isoformat(timespec="milliseconds"),
                                uptime, sensor_id, short, ep, cluster, attr, value])
                    f.flush()
                    print(now.strftime("%H:%M:%S"), sensor_id, value)
        except serial.SerialException as e:
            print(f"Serial error: {e}, retrying in 2 s")
            time.sleep(2)

if __name__ == "__main__":
    main()