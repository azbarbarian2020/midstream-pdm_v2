"""
Extend telemetry data from 2026-03-13 12:00 to 2026-03-20 12:00 for all 50 assets.
- Healthy assets: continue flat with normal noise
- Degrading assets: continue degradation ramp
- Assets past RUL: erratic readings then flatline (equipment failure)

Also inserts EQUIPMENT_FAILURE prediction rows for assets whose RUL reaches 0.
"""
import os
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

np.random.seed(99)

NOW = datetime(2026, 3, 13, 12, 0, 0)
EXTEND_END = datetime(2026, 3, 20, 12, 0, 0)
INTERVAL_MINUTES = 5
EXTEND_POINTS = int((EXTEND_END - NOW).total_seconds() / 60 / INTERVAL_MINUTES)

DEGRADING_ASSETS = {
    5:  {"mode": "BEARING_WEAR",  "start_offset_days": 30, "severity": 0.85, "target_rul_now": 12, "asset_type": "PUMP"},
    12: {"mode": "VALVE_FAILURE", "start_offset_days": 25, "severity": 0.80, "target_rul_now": 20, "asset_type": "COMPRESSOR"},
    18: {"mode": "SEAL_LEAK",     "start_offset_days": 20, "severity": 0.90, "target_rul_now": 14, "asset_type": "PUMP"},
    22: {"mode": "OVERHEATING",   "start_offset_days": 25, "severity": 0.75, "target_rul_now": 18, "asset_type": "COMPRESSOR"},
    27: {"mode": "BEARING_WEAR",  "start_offset_days": 15, "severity": 0.95, "target_rul_now": 5,  "asset_type": "COMPRESSOR"},
    34: {"mode": "SURGE",         "start_offset_days": 21, "severity": 1.0,  "target_rul_now": 7,  "asset_type": "COMPRESSOR"},
    35: {"mode": "SEAL_LEAK",     "start_offset_days": 15, "severity": 0.85, "target_rul_now": 22, "asset_type": "PUMP"},
    39: {"mode": "OVERHEATING",   "start_offset_days": 18, "severity": 0.85, "target_rul_now": 25, "asset_type": "COMPRESSOR"},
    41: {"mode": "BEARING_WEAR",  "start_offset_days": 20, "severity": 0.70, "target_rul_now": 28, "asset_type": "PUMP"},
    48: {"mode": "SEAL_LEAK",     "start_offset_days": 22, "severity": 0.75, "target_rul_now": 16, "asset_type": "PUMP"},
}

conn_name = os.getenv("SNOWFLAKE_CONNECTION_NAME") or "default"
conn = snowflake.connector.connect(connection_name=conn_name)
cur = conn.cursor()
cur.execute("USE ROLE ACCOUNTADMIN")
cur.execute("USE WAREHOUSE PDM_DEMO_WH")
cur.execute("USE DATABASE PDM_DEMO")

print("Fetching asset info and last telemetry readings...")
cur.execute("""
    SELECT a.ASSET_ID, a.ASSET_TYPE
    FROM RAW.ASSETS a
    ORDER BY a.ASSET_ID
""")
all_assets = cur.fetchall()
asset_types = {row[0]: row[1] for row in all_assets}

cur.execute("""
    SELECT t.*
    FROM RAW.TELEMETRY t
    WHERE t.TS >= '2026-03-13 11:00:00'
    ORDER BY t.ASSET_ID, t.TS DESC
""")
cols = [d[0] for d in cur.description]
last_readings = {}
for row in cur.fetchall():
    d = dict(zip(cols, row))
    aid = d["ASSET_ID"]
    if aid not in last_readings:
        last_readings[aid] = []
    if len(last_readings[aid]) < 12:
        last_readings[aid].append(d)

SENSOR_COLS = [
    "VIBRATION", "TEMPERATURE", "PRESSURE", "FLOW_RATE", "RPM", "POWER_DRAW",
    "DIFFERENTIAL_PRESSURE", "SUCTION_PRESSURE", "SEAL_TEMPERATURE", "CAVITATION_INDEX",
    "DISCHARGE_TEMP", "INLET_TEMP", "COMPRESSION_RATIO", "OIL_PRESSURE",
]

timestamps = [NOW + timedelta(minutes=i * INTERVAL_MINUTES) for i in range(EXTEND_POINTS)]

all_rows = []
for aid in range(1, 51):
    atype = asset_types.get(aid, "PUMP")
    is_pump = atype == "PUMP"
    readings = last_readings.get(aid, [])
    if not readings:
        continue

    baselines = {}
    noise_scales = {}
    for sc in SENSOR_COLS:
        vals = [float(r[sc]) for r in readings if r.get(sc) is not None]
        if vals:
            baselines[sc] = np.mean(vals)
            noise_scales[sc] = max(np.std(vals), 0.01)
        else:
            baselines[sc] = None

    deg = DEGRADING_ASSETS.get(aid)

    if deg:
        mode = deg["mode"]
        severity = deg["severity"]
        total_degrade_days = deg["start_offset_days"]
        days_already = total_degrade_days
        rul_now = deg["target_rul_now"]
        failure_ts = NOW + timedelta(days=rul_now)

        extend_ramp = np.linspace(0, 0.3, EXTEND_POINTS)

        for i, ts in enumerate(timestamps):
            days_past_now = i * INTERVAL_MINUTES / 60 / 24
            past_failure = ts >= failure_ts

            row = {"ASSET_ID": aid, "TS": ts.strftime("%Y-%m-%d %H:%M:%S")}

            for sc in SENSOR_COLS:
                if baselines[sc] is None:
                    row[sc] = None
                    continue

                base = baselines[sc]
                noise = np.random.normal(0, noise_scales[sc])
                ramp_add = extend_ramp[i]

                if past_failure:
                    hours_past = (ts - failure_ts).total_seconds() / 3600
                    if hours_past > 6:
                        if sc in ("VIBRATION", "TEMPERATURE", "POWER_DRAW", "SEAL_TEMPERATURE", "DISCHARGE_TEMP"):
                            row[sc] = round(base * 0.1 + np.random.normal(0, noise_scales[sc] * 0.5), 2)
                        elif sc in ("FLOW_RATE", "RPM", "PRESSURE"):
                            row[sc] = round(base * 0.05 + np.random.normal(0, noise_scales[sc] * 0.3), 2)
                        elif sc == "OIL_PRESSURE":
                            row[sc] = round(max(0, 5 + np.random.normal(0, 2)), 2)
                        elif sc == "COMPRESSION_RATIO":
                            row[sc] = round(1.0 + np.random.normal(0, 0.05), 4)
                        elif sc == "CAVITATION_INDEX":
                            row[sc] = round(max(0, np.random.normal(0.02, 0.01)), 4)
                        else:
                            row[sc] = round(base * 0.1 + np.random.normal(0, noise_scales[sc] * 0.3), 2)
                    else:
                        spike = np.random.normal(0, noise_scales[sc] * 8)
                        if sc in ("VIBRATION",):
                            spike = abs(spike) * 3
                        row[sc] = round(base + spike, 2 if sc != "CAVITATION_INDEX" and sc != "COMPRESSION_RATIO" else 4)
                else:
                    extra = 0
                    if mode == "BEARING_WEAR":
                        if sc == "VIBRATION": extra = ramp_add * 25
                        elif sc == "TEMPERATURE": extra = ramp_add * 60
                        elif sc == "POWER_DRAW": extra = ramp_add * 50
                        elif sc == "RPM": extra = np.random.normal(0, ramp_add * 100)
                        elif sc == "OIL_PRESSURE": extra = -ramp_add * 30
                    elif mode == "VALVE_FAILURE":
                        if sc == "PRESSURE": extra = np.sin(i * 0.5) * ramp_add * 150
                        elif sc == "FLOW_RATE": extra = -ramp_add * 400
                        elif sc == "VIBRATION": extra = ramp_add * 4
                        elif sc == "DIFFERENTIAL_PRESSURE": extra = ramp_add * 70
                        elif sc == "DISCHARGE_TEMP": extra = ramp_add * 60
                        elif sc == "COMPRESSION_RATIO": extra = ramp_add * 1.0
                    elif mode == "SEAL_LEAK":
                        if sc == "PRESSURE": extra = -ramp_add * 120
                        elif sc == "FLOW_RATE": extra = -ramp_add * 300
                        elif sc == "SEAL_TEMPERATURE": extra = ramp_add * 80
                        elif sc == "SUCTION_PRESSURE": extra = -ramp_add * 60
                        elif sc == "CAVITATION_INDEX": extra = ramp_add * 0.4
                        elif sc == "VIBRATION": extra = ramp_add * 6
                    elif mode == "SURGE":
                        if sc == "VIBRATION": extra = ramp_add * 20
                        elif sc == "FLOW_RATE": extra = np.sin(i * 0.8) * ramp_add * 600
                        elif sc == "PRESSURE": extra = np.sin(i * 0.8) * ramp_add * 180
                        elif sc == "COMPRESSION_RATIO": extra = ramp_add * 3.5 + np.sin(i * 0.6) * ramp_add * 2.5
                        elif sc == "DISCHARGE_TEMP": extra = ramp_add * 140
                        elif sc == "OIL_PRESSURE": extra = -ramp_add * 35
                        elif sc == "INLET_TEMP": extra = ramp_add * 50
                    elif mode == "OVERHEATING":
                        if sc == "TEMPERATURE": extra = ramp_add * 120
                        elif sc == "POWER_DRAW": extra = ramp_add * 70
                        elif sc == "SEAL_TEMPERATURE": extra = ramp_add * 90
                        elif sc == "DISCHARGE_TEMP": extra = ramp_add * 90
                        elif sc == "OIL_PRESSURE": extra = -ramp_add * 25

                    val = base + noise + extra
                    if sc == "CAVITATION_INDEX":
                        row[sc] = round(max(0, val), 4)
                    elif sc == "COMPRESSION_RATIO":
                        row[sc] = round(max(0.5, val), 4)
                    else:
                        row[sc] = round(val, 2)

            all_rows.append(row)
    else:
        for i, ts in enumerate(timestamps):
            row = {"ASSET_ID": aid, "TS": ts.strftime("%Y-%m-%d %H:%M:%S")}
            for sc in SENSOR_COLS:
                if baselines[sc] is None:
                    row[sc] = None
                else:
                    row[sc] = round(baselines[sc] + np.random.normal(0, noise_scales[sc]), 
                                    4 if sc in ("CAVITATION_INDEX", "COMPRESSION_RATIO") else 2)
            all_rows.append(row)

print(f"Generated {len(all_rows)} extended telemetry rows")

df = pd.DataFrame(all_rows)
for c in SENSOR_COLS:
    df[c] = pd.to_numeric(df[c], errors="coerce")
df["ASSET_ID"] = df["ASSET_ID"].astype(int)

cur.execute("DELETE FROM RAW.TELEMETRY WHERE TS > '2026-03-13 11:55:00'")
print("Cleared any existing future telemetry")

write_pandas(conn, df, "TELEMETRY", schema="RAW", quote_identifiers=False)
print(f"Inserted {len(df)} extended telemetry rows")

print("\nGenerating EQUIPMENT_FAILURE prediction rows...")
failure_preds = []

for aid, deg in DEGRADING_ASSETS.items():
    rul_now = deg["target_rul_now"]
    mode = deg["mode"]
    failure_ts = NOW + timedelta(days=rul_now)

    if rul_now <= 7:
        failure_day = failure_ts
        failure_probs = {
            "NORMAL": 0.01,
            "BEARING_WEAR": 0.01,
            "VALVE_FAILURE": 0.01,
            "SEAL_LEAK": 0.01,
            "OVERHEATING": 0.01,
            "SURGE": 0.01,
        }
        failure_probs["EQUIPMENT_FAILURE"] = 0.95
        failure_probs[mode] = 0.01

        failure_preds.append({
            "ASSET_ID": aid,
            "AS_OF_TS": (failure_ts + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S"),
            "PREDICTED_CLASS": "EQUIPMENT_FAILURE",
            "CLASS_PROBABILITIES": json.dumps(failure_probs),
            "PREDICTED_RUL_DAYS": 0,
            "RISK_LEVEL": "FAILED",
            "MODEL_VERSION": "v2",
            "SCORED_AT": NOW.strftime("%Y-%m-%d %H:%M:%S"),
        })

        for offset_hours in [6, 12, 24, 48, 72, 120, 168]:
            check_ts = failure_ts + timedelta(hours=offset_hours)
            if check_ts <= EXTEND_END:
                failure_preds.append({
                    "ASSET_ID": aid,
                    "AS_OF_TS": check_ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "PREDICTED_CLASS": "EQUIPMENT_FAILURE",
                    "CLASS_PROBABILITIES": json.dumps(failure_probs),
                    "PREDICTED_RUL_DAYS": 0,
                    "RISK_LEVEL": "FAILED",
                    "MODEL_VERSION": "v2",
                    "SCORED_AT": NOW.strftime("%Y-%m-%d %H:%M:%S"),
                })

if failure_preds:
    pred_df = pd.DataFrame(failure_preds)
    pred_df["ASSET_ID"] = pred_df["ASSET_ID"].astype(int)
    pred_df["PREDICTED_RUL_DAYS"] = pred_df["PREDICTED_RUL_DAYS"].astype(float)
    write_pandas(conn, pred_df, "PREDICTIONS", schema="ANALYTICS", quote_identifiers=False)
    print(f"Inserted {len(pred_df)} EQUIPMENT_FAILURE prediction rows")

cur.execute("""
    SELECT ASSET_ID, AS_OF_TS, PREDICTED_CLASS, PREDICTED_RUL_DAYS, RISK_LEVEL
    FROM ANALYTICS.PREDICTIONS
    WHERE PREDICTED_CLASS = 'EQUIPMENT_FAILURE'
    ORDER BY ASSET_ID, AS_OF_TS
""")
print("\nEquipment Failure predictions:")
for r in cur.fetchall():
    print(f"  Asset {r[0]}: {r[2]} at {r[1]}, RUL={r[3]}, {r[4]}")

cur.execute("SELECT COUNT(*) FROM RAW.TELEMETRY WHERE TS > '2026-03-13 12:00:00'")
print(f"\nFuture telemetry rows: {cur.fetchone()[0]}")

cur.execute("SELECT MIN(TS), MAX(TS) FROM RAW.TELEMETRY WHERE ASSET_ID = 27")
r = cur.fetchone()
print(f"Asset 27 telemetry range: {r[0]} to {r[1]}")

conn.close()
print("\nDone!")
