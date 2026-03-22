import json
import random
import numpy as np
import snowflake.connector
import os
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

random.seed(42)
np.random.seed(42)

conn = snowflake.connector.connect(connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "default")
conn.cursor().execute("USE WAREHOUSE PDM_DEMO_WH")
conn.cursor().execute("USE DATABASE PDM_DEMO")

FAILURE_MODES_PUMP = ["NORMAL", "BEARING_WEAR", "VALVE_FAILURE", "SEAL_LEAK", "OVERHEATING"]
FAILURE_MODES_COMPRESSOR = ["NORMAL", "BEARING_WEAR", "VALVE_FAILURE", "SURGE", "OVERHEATING"]

assets_df = pd.read_sql("SELECT ASSET_ID, ASSET_TYPE FROM RAW.ASSETS ORDER BY ASSET_ID", conn)
assets = {row.ASSET_ID: row.ASSET_TYPE for _, row in assets_df.iterrows()}

DEGRADING = {
    5:  {"mode": "BEARING_WEAR",   "now_rul": 12, "severity_now": 0.55},
    22: {"mode": "OVERHEATING",    "now_rul": 18, "severity_now": 0.40},
    27: {"mode": "BEARING_WEAR",   "now_rul": 6,  "severity_now": 0.72},
    35: {"mode": "SURGE",          "now_rul": 8,  "severity_now": 0.65},
    48: {"mode": "SEAL_LEAK",      "now_rul": 22, "severity_now": 0.35},
    12: {"mode": "VALVE_FAILURE",  "now_rul": 25, "severity_now": 0.32},
    18: {"mode": "SEAL_LEAK",      "now_rul": 28, "severity_now": 0.30},
    34: {"mode": "VALVE_FAILURE",  "now_rul": 20, "severity_now": 0.38},
    39: {"mode": "OVERHEATING",    "now_rul": 30, "severity_now": 0.30},
    41: {"mode": "BEARING_WEAR",   "now_rul": 26, "severity_now": 0.30},
}

offsets = [
    ("now",  0),
    ("+24h", 1),
    ("+72h", 3),
    ("+7d",  7),
]

predictions = []

for aid, atype in assets.items():
    modes = FAILURE_MODES_PUMP if atype == "PUMP" else FAILURE_MODES_COMPRESSOR
    deg = DEGRADING.get(aid)

    for label, days_offset in offsets:
        ts = f"2026-03-{13 + days_offset:02d} 00:00:00"

        if deg:
            rul_now = deg["now_rul"]
            rul = max(0, round(rul_now - days_offset, 1))
            sev_now = deg["severity_now"]
            progress = min(1.0, sev_now + days_offset * 0.08)

            pred_class = deg["mode"]
            main_prob = min(0.96, 0.35 + 0.60 * progress)

            if rul <= 3:
                risk = "CRITICAL"
            elif rul <= 14:
                risk = "WARNING" if rul > 7 else "CRITICAL"
            elif rul <= 30:
                risk = "WARNING"
            else:
                risk = "HEALTHY"
        else:
            pred_class = "NORMAL"
            base_rul = random.uniform(60, 180)
            rul = round(base_rul - days_offset * 0.5, 1)
            main_prob = random.uniform(0.82, 0.96)
            risk = "HEALTHY"

        probs = {}
        remaining = 1.0 - main_prob
        for m in modes:
            if m == pred_class:
                probs[m] = round(main_prob, 4)
            else:
                share = remaining / max(1, len(modes) - 1) * random.uniform(0.5, 1.5)
                share = min(share, remaining)
                probs[m] = round(max(0, share), 4)
                remaining = max(0, remaining - probs[m])

        predictions.append({
            "ASSET_ID": aid,
            "AS_OF_TS": ts,
            "PREDICTED_CLASS": pred_class,
            "CLASS_PROBABILITIES": json.dumps(probs),
            "PREDICTED_RUL_DAYS": rul,
            "RISK_LEVEL": risk,
            "MODEL_VERSION": "v1",
            "SCORED_AT": ts,
        })

pdf = pd.DataFrame(predictions)

print("Prediction summary by time offset:")
for ts in pdf["AS_OF_TS"].unique():
    sub = pdf[pdf["AS_OF_TS"] == ts]
    h = (sub["RISK_LEVEL"] == "HEALTHY").sum()
    w = (sub["RISK_LEVEL"] == "WARNING").sum()
    c = (sub["RISK_LEVEL"] == "CRITICAL").sum()
    print(f"  {ts}: {h}H / {w}W / {c}C")

print("\nDegrading assets at 'Now':")
now = pdf[(pdf["AS_OF_TS"] == "2026-03-13 00:00:00") & (pdf["RISK_LEVEL"] != "HEALTHY")]
for _, r in now.iterrows():
    print(f"  Asset {r.ASSET_ID}: {r.PREDICTED_CLASS}, RUL={r.PREDICTED_RUL_DAYS}d, {r.RISK_LEVEL}")

print("\nDegrading assets at '+7d':")
d7 = pdf[(pdf["AS_OF_TS"] == "2026-03-20 00:00:00") & (pdf["RISK_LEVEL"] != "HEALTHY")]
for _, r in d7.iterrows():
    print(f"  Asset {r.ASSET_ID}: {r.PREDICTED_CLASS}, RUL={r.PREDICTED_RUL_DAYS}d, {r.RISK_LEVEL}")

write_pandas(conn, pdf, "PREDICTIONS", database="PDM_DEMO", schema="ANALYTICS", auto_create_table=False, overwrite=True)
print(f"\nLoaded {len(pdf)} predictions")
conn.close()
