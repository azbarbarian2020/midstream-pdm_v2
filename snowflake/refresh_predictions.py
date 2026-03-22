"""Regenerate predictions table with daily historical predictions for degrading assets."""
import os
import sys
import json
import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

try:
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
except ImportError:
    print("Install: pip install snowflake-connector-python[pandas]")
    sys.exit(1)

random.seed(42)
np.random.seed(42)

DATABASE = "PDM_DEMO"
WAREHOUSE = "PDM_DEMO_WH"
NOW = datetime(2026, 3, 13, 12, 0, 0)

FAILURE_MODES_PUMP = ["NORMAL", "BEARING_WEAR", "VALVE_FAILURE", "SEAL_LEAK", "OVERHEATING"]
FAILURE_MODES_COMPRESSOR = ["NORMAL", "BEARING_WEAR", "VALVE_FAILURE", "SURGE", "OVERHEATING"]

DEGRADING_ASSETS = {
    5:  {"mode": "BEARING_WEAR",   "start_offset_days": 30, "severity": 0.85, "target_rul_now": 12, "asset_type": "PUMP"},
    12: {"mode": "VALVE_FAILURE",  "start_offset_days": 25, "severity": 0.80, "target_rul_now": 20, "asset_type": "COMPRESSOR"},
    18: {"mode": "SEAL_LEAK",      "start_offset_days": 20, "severity": 0.90, "target_rul_now": 14, "asset_type": "PUMP"},
    22: {"mode": "OVERHEATING",    "start_offset_days": 25, "severity": 0.75, "target_rul_now": 18, "asset_type": "COMPRESSOR"},
    27: {"mode": "BEARING_WEAR",   "start_offset_days": 15, "severity": 0.95, "target_rul_now": 5,  "asset_type": "COMPRESSOR"},
    34: {"mode": "SURGE",          "start_offset_days": 21, "severity": 1.0,  "target_rul_now": 7,  "asset_type": "COMPRESSOR"},
    35: {"mode": "SEAL_LEAK",      "start_offset_days": 15, "severity": 0.85, "target_rul_now": 22, "asset_type": "PUMP"},
    39: {"mode": "OVERHEATING",    "start_offset_days": 18, "severity": 0.85, "target_rul_now": 25, "asset_type": "COMPRESSOR"},
    41: {"mode": "BEARING_WEAR",   "start_offset_days": 20, "severity": 0.70, "target_rul_now": 28, "asset_type": "PUMP"},
    48: {"mode": "SEAL_LEAK",      "start_offset_days": 22, "severity": 0.75, "target_rul_now": 16, "asset_type": "PUMP"},
}

def generate_predictions():
    preds = []
    future_offsets = [
        NOW + timedelta(hours=24),
        NOW + timedelta(hours=72),
        NOW + timedelta(days=7),
    ]

    conn = snowflake.connector.connect(connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME", "default"))
    cur = conn.cursor()
    cur.execute(f"USE WAREHOUSE {WAREHOUSE}")
    cur.execute(f"USE DATABASE {DATABASE}")
    cur.execute("SELECT ASSET_ID, ASSET_TYPE FROM PDM_DEMO.RAW.ASSETS ORDER BY ASSET_ID")
    all_assets = cur.fetchall()

    for aid, atype in all_assets:
        modes = FAILURE_MODES_PUMP if atype == "PUMP" else FAILURE_MODES_COMPRESSOR
        deg = DEGRADING_ASSETS.get(aid)

        if deg:
            degrade_start = NOW - timedelta(days=deg["start_offset_days"])
            history_start = degrade_start - timedelta(days=7)
            detection_delay_days = max(2, deg["start_offset_days"] * 0.25)

            daily_timestamps = []
            day = history_start
            while day <= NOW:
                daily_timestamps.append(datetime(day.year, day.month, day.day))
                day += timedelta(days=1)

            all_timestamps = daily_timestamps + future_offsets

            for ts in all_timestamps:
                days_to_fail = max(0, (NOW - ts).total_seconds() / 86400 + deg["target_rul_now"])
                rul = max(0, round(days_to_fail, 1))

                if ts < degrade_start:
                    pred_class = "NORMAL"
                    main_prob = np.random.uniform(0.82, 0.92)
                elif ts < degrade_start + timedelta(days=detection_delay_days):
                    progress = (ts - degrade_start).total_seconds() / (timedelta(days=detection_delay_days).total_seconds())
                    pred_class = "NORMAL"
                    main_prob = max(0.45, 0.88 - 0.43 * progress)
                else:
                    full_window = (NOW - degrade_start).total_seconds()
                    if full_window > 0:
                        progress = min(1.0, (ts - degrade_start).total_seconds() / full_window)
                    else:
                        progress = 1.0
                    pred_class = deg["mode"]
                    main_prob = min(0.98, 0.35 + 0.63 * progress * deg["severity"])

                probs = {}
                remaining = 1.0 - main_prob
                for m in modes:
                    if m == pred_class:
                        probs[m] = round(main_prob, 4)
                    else:
                        share = remaining / (len(modes) - 1) * np.random.uniform(0.5, 1.5)
                        probs[m] = round(min(share, remaining), 4)
                        remaining -= probs[m]
                        remaining = max(0, remaining)

                if rul <= 7:
                    risk = "CRITICAL"
                elif rul <= 30:
                    risk = "WARNING"
                else:
                    risk = "HEALTHY"

                preds.append({
                    "ASSET_ID": aid,
                    "AS_OF_TS": ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "PREDICTED_CLASS": pred_class,
                    "CLASS_PROBABILITIES": json.dumps(probs),
                    "PREDICTED_RUL_DAYS": rul,
                    "RISK_LEVEL": risk,
                    "MODEL_VERSION": "v2",
                    "SCORED_AT": ts.strftime("%Y-%m-%d %H:%M:%S"),
                })
        else:
            for ts in [datetime(NOW.year, NOW.month, NOW.day)] + future_offsets:
                pred_class = "NORMAL"
                main_prob = np.random.uniform(0.80, 0.95)
                rul = round(np.random.uniform(45, 180), 1)

                probs = {}
                remaining = 1.0 - main_prob
                for m in modes:
                    if m == pred_class:
                        probs[m] = round(main_prob, 4)
                    else:
                        share = remaining / (len(modes) - 1) * np.random.uniform(0.5, 1.5)
                        probs[m] = round(min(share, remaining), 4)
                        remaining -= probs[m]
                        remaining = max(0, remaining)

                risk = "HEALTHY"
                preds.append({
                    "ASSET_ID": aid,
                    "AS_OF_TS": ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "PREDICTED_CLASS": pred_class,
                    "CLASS_PROBABILITIES": json.dumps(probs),
                    "PREDICTED_RUL_DAYS": rul,
                    "RISK_LEVEL": risk,
                    "MODEL_VERSION": "v2",
                    "SCORED_AT": ts.strftime("%Y-%m-%d %H:%M:%S"),
                })

    pred_df = pd.DataFrame(preds)
    print(f"Generated {len(pred_df)} prediction rows")
    print(f"  Degrading assets: {len(pred_df[pred_df['ASSET_ID'].isin(DEGRADING_ASSETS.keys())])} rows")
    print(f"  Healthy assets: {len(pred_df[~pred_df['ASSET_ID'].isin(DEGRADING_ASSETS.keys())])} rows")

    print("\nLoading into Snowflake (overwrite)...")
    cur.execute(f"USE SCHEMA {DATABASE}.ANALYTICS")
    success, nchunks, nrows, _ = write_pandas(
        conn, pred_df, "PREDICTIONS", database=DATABASE, schema="ANALYTICS",
        auto_create_table=False, overwrite=True
    )
    print(f"  -> {nrows} rows loaded ({nchunks} chunks)")

    cur.execute("SELECT ASSET_ID, MIN(AS_OF_TS) as FIRST_PRED, MAX(AS_OF_TS) as LAST_PRED, COUNT(*) as CNT FROM PDM_DEMO.ANALYTICS.PREDICTIONS WHERE ASSET_ID IN (5,27,34) GROUP BY ASSET_ID ORDER BY ASSET_ID")
    print("\nVerification (sample degrading assets):")
    for row in cur.fetchall():
        print(f"  Asset {row[0]}: {row[1]} to {row[2]} ({row[3]} predictions)")

    conn.close()
    print("\nDone!")

if __name__ == "__main__":
    generate_predictions()
