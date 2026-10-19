#!/usr/bin/env python3
"""Usage: python zb_logger.py COM8      (pip install pyserial)"""
import csv, sys, time
from datetime import datetime
from pathlib import Path
import serial
from collections import deque

PORT = sys.argv[1] if len(sys.argv) > 1 else "COM8"
BAUD = 115200
LOG_DIR = Path("zigbee_logs")
LOG_DIR.mkdir(exist_ok=True)
state = {}   # sensor_id -> {"next": expected next seq, "last_ts": ..., "offs": recent offsets}

def handle(line, w, f):
    p = line.split(",")
    if len(p) < 8:
        return
    arrival_us = time.time_ns() // 1000
    sid = p[2]
    seq, ts, period, count = int(p[4]), int(p[5]), int(p[6]), int(p[7])
    vals = p[8:8 + count]
    if len(vals) != count:
        return
    st = state.setdefault(sid, {"next": None, "last_ts": 0, "offs": deque(maxlen=300)})

    if st["next"] is not None:
        if seq < st["next"] and (st["last_ts"] - ts) <= 40 * period:
            # already seen (duplicate or overlap): keep only the new samples, if any
            skip = st["next"] - seq
            if skip >= count:
                print(f"[{sid}] duplicate batch seq {seq} ignored")
                return
            vals = vals[skip:]
            seq += skip
            ts += skip * period
            count -= skip
        elif seq < st["next"]:
            print(f"[{sid}] sensor restarted")
            st["offs"].clear()
        elif seq > st["next"]:
            print(f"[{sid}] LOST {seq - st['next']} samples")

    st["next"] = seq + count
    st["last_ts"] = ts + (count - 1) * period

    # Radio delay is always >= 0, so the smallest recent (arrival - sensor_time)
    # is the best estimate of the true clock offset.
    st["offs"].append(arrival_us - st["last_ts"])
    offset = min(st["offs"])

    for i, v in enumerate(vals):
        t_sensor = ts + i * period
        t_pc = (t_sensor + offset) / 1e6
        w.writerow([datetime.fromtimestamp(t_pc).isoformat(timespec="milliseconds"),
                    t_sensor, seq + i, sid, f"{int(v) / 100:.2f}"])
    f.flush()

def main():
    path = LOG_DIR / f"zigbee_{datetime.now():%Y-%m-%d_%H-%M-%S}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["pc_time", "sensor_ts_us", "seq", "sensor_id", "temp_C"])
        print(f"Logging to {path}")
        while True:
            try:
                with serial.Serial(PORT, BAUD, timeout=1) as ser:
                    while True:
                        raw = ser.readline().decode("utf-8", errors="ignore")
                        i = raw.find("ZBSTREAM,")
                        if i >= 0:
                            try:
                                handle(raw[i:].strip(), w, f)
                            except (ValueError, IndexError):
                                pass
            except serial.SerialException as e:
                print(f"Serial error: {e}, retrying")
                time.sleep(2)

if __name__ == "__main__":
    main()