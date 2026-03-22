"""
Midstream PDM — Model-Driven Fleet Scoring

Trains XGBoost models from the feature store, generates synthetic sensor snapshots
for all 50 assets at 4 time horizons with realistic degradation patterns for the
10 at-risk assets, scores them through the actual models, and materializes the
results into ANALYTICS.PREDICTIONS.

Usage:
  SNOWFLAKE_CONNECTION_NAME=jdrew python score_fleet.py
"""
import os
import json
import random
import numpy as np
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from sklearn.model_selection import train_test_split, StratifiedKFold, RandomizedSearchCV
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import f1_score, mean_absolute_error, r2_score, root_mean_squared_error
from imblearn.over_sampling import SMOTE
from xgboost import XGBClassifier, XGBRegressor

random.seed(42)
np.random.seed(42)

conn = snowflake.connector.connect(connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "default")
conn.cursor().execute("USE WAREHOUSE PDM_DEMO_WH")
conn.cursor().execute("USE DATABASE PDM_DEMO")

FEATURE_COLS = [
    'VIBRATION_MEAN_24H', 'VIBRATION_STD_24H', 'VIBRATION_MAX_24H', 'VIBRATION_TREND',
    'TEMPERATURE_MEAN_24H', 'TEMPERATURE_STD_24H', 'TEMPERATURE_MAX_24H', 'TEMPERATURE_TREND',
    'PRESSURE_MEAN_24H', 'PRESSURE_STD_24H', 'FLOW_RATE_MEAN_24H',
    'RPM_MEAN_24H', 'RPM_STD_24H', 'POWER_DRAW_MEAN_24H',
    'DAYS_SINCE_MAINTENANCE', 'MAINTENANCE_COUNT_90D', 'OPERATING_HOURS',
    'DIFF_PRESSURE_MEAN_24H', 'SEAL_TEMP_MEAN_24H',
    'DISCHARGE_TEMP_MEAN_24H', 'COMPRESSION_RATIO_MEAN', 'OIL_PRESSURE_MEAN_24H',
    'IS_PUMP',
    'VIB_TEMP_INTERACTION', 'POWER_EFFICIENCY', 'PRESSURE_VARIABILITY',
    'VIB_DEVIATION', 'TEMP_DEVIATION', 'MAINT_RECENCY_SCORE',
]

DEGRADING_ASSETS = {
    5:  {"mode": "BEARING_WEAR",   "start_offset_days": 30, "severity": 0.85, "target_rul_now": 12},
    12: {"mode": "VALVE_FAILURE",  "start_offset_days": 25, "severity": 0.80, "target_rul_now": 20},
    18: {"mode": "SEAL_LEAK",      "start_offset_days": 20, "severity": 0.90, "target_rul_now": 14},
    22: {"mode": "OVERHEATING",    "start_offset_days": 25, "severity": 0.75, "target_rul_now": 18},
    27: {"mode": "BEARING_WEAR",   "start_offset_days": 15, "severity": 0.95, "target_rul_now": 5},
    34: {"mode": "SURGE",          "start_offset_days": 21, "severity": 1.0,  "target_rul_now": 7},
    35: {"mode": "SEAL_LEAK",      "start_offset_days": 15, "severity": 0.85, "target_rul_now": 22},
    39: {"mode": "OVERHEATING",    "start_offset_days": 18, "severity": 0.85, "target_rul_now": 25},
    41: {"mode": "BEARING_WEAR",   "start_offset_days": 20, "severity": 0.70, "target_rul_now": 28},
    48: {"mode": "SEAL_LEAK",      "start_offset_days": 22, "severity": 0.75, "target_rul_now": 16},
}

TIME_OFFSETS = [
    ("now",  0),
    ("+24h", 1),
    ("+72h", 3),
    ("+7d",  7),
]

# ──────────────────────────────────────────────────────────────────────────────
# STEP 1: Train models from feature store (same pipeline as notebook)
# ──────────────────────────────────────────────────────────────────────────────
print("=" * 60)
print("STEP 1: Training models from feature store")
print("=" * 60)

pdf = pd.read_sql("SELECT * FROM ANALYTICS.FEATURE_STORE", conn)
print(f"Loaded {len(pdf)} feature rows, {pdf['ASSET_ID'].nunique()} assets")

pdf['IS_PUMP'] = (pdf['ASSET_TYPE'] == 'PUMP').astype(int)
pdf['VIB_TEMP_INTERACTION'] = pdf['VIBRATION_MEAN_24H'] * pdf['TEMPERATURE_MEAN_24H'] / 1000
pdf['POWER_EFFICIENCY'] = pdf['POWER_DRAW_MEAN_24H'] / (pdf['FLOW_RATE_MEAN_24H'] + 1)
pdf['PRESSURE_VARIABILITY'] = pdf['PRESSURE_STD_24H'] / (pdf['PRESSURE_MEAN_24H'] + 1)
pdf['VIB_DEVIATION'] = pdf['VIBRATION_MAX_24H'] - pdf['VIBRATION_MEAN_24H']
pdf['TEMP_DEVIATION'] = pdf['TEMPERATURE_MAX_24H'] - pdf['TEMPERATURE_MEAN_24H']
pdf['MAINT_RECENCY_SCORE'] = 1 / (pdf['DAYS_SINCE_MAINTENANCE'] + 1)

for col in FEATURE_COLS:
    if col in pdf.columns:
        pdf[col] = pdf[col].fillna(0)

le = LabelEncoder()
pdf['LABEL_ENCODED'] = le.fit_transform(pdf['FAILURE_LABEL'])
print(f"Classes: {list(le.classes_)}")

X = pdf[FEATURE_COLS].values
y_cls = pdf['LABEL_ENCODED'].values
y_rul = pdf['DAYS_TO_FAILURE'].values

X_train, X_test, y_train_cls, y_test_cls, y_train_rul, y_test_rul = train_test_split(
    X, y_cls, y_rul, test_size=0.2, random_state=42, stratify=y_cls
)

smote = SMOTE(random_state=42, k_neighbors=2)
X_train_bal, y_train_bal = smote.fit_resample(X_train, y_train_cls)
print(f"SMOTE: {X_train.shape[0]} -> {X_train_bal.shape[0]} training samples")

param_distributions = {
    'n_estimators': [100, 200, 300, 500],
    'max_depth': [4, 5, 6, 8, 10],
    'learning_rate': [0.01, 0.05, 0.1, 0.2],
    'subsample': [0.7, 0.8, 0.9, 1.0],
    'colsample_bytree': [0.6, 0.7, 0.8, 0.9],
    'min_child_weight': [1, 3, 5, 7],
    'gamma': [0, 0.1, 0.2, 0.5],
    'reg_alpha': [0, 0.01, 0.1, 1.0],
    'reg_lambda': [0.5, 1.0, 2.0, 5.0],
}

base_clf = XGBClassifier(
    objective='multi:softprob', num_class=len(le.classes_),
    random_state=42, eval_metric='mlogloss', tree_method='hist',
)

cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
search = RandomizedSearchCV(
    base_clf, param_distributions,
    n_iter=40, scoring='f1_macro', cv=cv,
    random_state=42, n_jobs=-1, verbose=1
)

print("\nTraining classifier (40 iter x 5-fold CV)...")
search.fit(X_train_bal, y_train_bal)
clf = search.best_estimator_

y_pred_cls = clf.predict(X_test)
f1 = f1_score(y_test_cls, y_pred_cls, average='macro')
print(f"Classifier F1 (macro): {f1:.4f}")
print(f"Best params: {search.best_params_}")

normal_idx = le.transform(['NORMAL'])[0]
nn_train = y_train_cls != normal_idx
nn_test = y_test_cls != normal_idx

reg = XGBRegressor(
    n_estimators=300, max_depth=5, learning_rate=0.05,
    subsample=0.8, colsample_bytree=0.8, min_child_weight=3,
    reg_alpha=0.1, reg_lambda=2.0,
    random_state=42, tree_method='hist'
)

print("\nTraining RUL regressor...")
reg.fit(X_train[nn_train], y_train_rul[nn_train])

y_pred_rul = reg.predict(X_test[nn_test])
mae = mean_absolute_error(y_test_rul[nn_test], y_pred_rul)
r2 = r2_score(y_test_rul[nn_test], y_pred_rul)
print(f"RUL MAE: {mae:.2f} days, R²: {r2:.4f}")

# ──────────────────────────────────────────────────────────────────────────────
# STEP 2: Generate synthetic sensor snapshots for all 50 assets x 4 timestamps
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("STEP 2: Generating synthetic sensor snapshots")
print("=" * 60)

assets_df = pd.read_sql("SELECT ASSET_ID, ASSET_TYPE FROM RAW.ASSETS ORDER BY ASSET_ID", conn)
assets = {row.ASSET_ID: row.ASSET_TYPE for _, row in assets_df.iterrows()}

baselines = pd.read_sql("""
    SELECT ASSET_ID,
      AVG(VIBRATION_MEAN_24H) as VIBRATION_MEAN_24H,
      AVG(VIBRATION_STD_24H) as VIBRATION_STD_24H,
      AVG(VIBRATION_MAX_24H) as VIBRATION_MAX_24H,
      AVG(VIBRATION_TREND) as VIBRATION_TREND,
      AVG(TEMPERATURE_MEAN_24H) as TEMPERATURE_MEAN_24H,
      AVG(TEMPERATURE_STD_24H) as TEMPERATURE_STD_24H,
      AVG(TEMPERATURE_MAX_24H) as TEMPERATURE_MAX_24H,
      AVG(TEMPERATURE_TREND) as TEMPERATURE_TREND,
      AVG(PRESSURE_MEAN_24H) as PRESSURE_MEAN_24H,
      AVG(PRESSURE_STD_24H) as PRESSURE_STD_24H,
      AVG(FLOW_RATE_MEAN_24H) as FLOW_RATE_MEAN_24H,
      AVG(RPM_MEAN_24H) as RPM_MEAN_24H,
      AVG(RPM_STD_24H) as RPM_STD_24H,
      AVG(POWER_DRAW_MEAN_24H) as POWER_DRAW_MEAN_24H,
      AVG(DIFF_PRESSURE_MEAN_24H) as DIFF_PRESSURE_MEAN_24H,
      AVG(SEAL_TEMP_MEAN_24H) as SEAL_TEMP_MEAN_24H,
      AVG(DISCHARGE_TEMP_MEAN_24H) as DISCHARGE_TEMP_MEAN_24H,
      AVG(COMPRESSION_RATIO_MEAN) as COMPRESSION_RATIO_MEAN,
      AVG(OIL_PRESSURE_MEAN_24H) as OIL_PRESSURE_MEAN_24H,
      AVG(DAYS_SINCE_MAINTENANCE) as DAYS_SINCE_MAINTENANCE,
      AVG(MAINTENANCE_COUNT_90D) as MAINTENANCE_COUNT_90D,
      AVG(OPERATING_HOURS) as OPERATING_HOURS
    FROM ANALYTICS.FEATURE_STORE
    WHERE FAILURE_LABEL = 'NORMAL'
    GROUP BY ASSET_ID
""", conn)
baselines = baselines.set_index('ASSET_ID')

failure_profiles = pd.read_sql("""
    SELECT FAILURE_LABEL, ASSET_TYPE,
      AVG(VIBRATION_MEAN_24H) as VIBRATION_MEAN_24H,
      AVG(VIBRATION_MAX_24H) as VIBRATION_MAX_24H,
      AVG(TEMPERATURE_MEAN_24H) as TEMPERATURE_MEAN_24H,
      AVG(TEMPERATURE_MAX_24H) as TEMPERATURE_MAX_24H,
      AVG(PRESSURE_MEAN_24H) as PRESSURE_MEAN_24H,
      AVG(PRESSURE_STD_24H) as PRESSURE_STD_24H,
      AVG(FLOW_RATE_MEAN_24H) as FLOW_RATE_MEAN_24H,
      AVG(RPM_MEAN_24H) as RPM_MEAN_24H,
      AVG(POWER_DRAW_MEAN_24H) as POWER_DRAW_MEAN_24H,
      AVG(DIFF_PRESSURE_MEAN_24H) as DIFF_PRESSURE_MEAN_24H,
      AVG(SEAL_TEMP_MEAN_24H) as SEAL_TEMP_MEAN_24H,
      AVG(DISCHARGE_TEMP_MEAN_24H) as DISCHARGE_TEMP_MEAN_24H,
      AVG(COMPRESSION_RATIO_MEAN) as COMPRESSION_RATIO_MEAN,
      AVG(OIL_PRESSURE_MEAN_24H) as OIL_PRESSURE_MEAN_24H
    FROM ANALYTICS.FEATURE_STORE
    WHERE FAILURE_LABEL != 'NORMAL'
    GROUP BY FAILURE_LABEL, ASSET_TYPE
""", conn)
failure_profiles = failure_profiles.set_index(['FAILURE_LABEL', 'ASSET_TYPE'])


def blend_toward_failure(baseline_val, failure_val, progress, noise_scale=0.02):
    if pd.isna(baseline_val) or pd.isna(failure_val):
        return baseline_val
    blended = baseline_val + (failure_val - baseline_val) * progress
    noise = np.random.normal(0, abs(blended) * noise_scale) if blended != 0 else 0
    return round(blended + noise, 4)


def generate_snapshot(asset_id, asset_type, days_offset, deg_info):
    is_pump = asset_type == 'PUMP'

    if asset_id in baselines.index:
        base = baselines.loc[asset_id]
    else:
        type_mask = assets_df['ASSET_TYPE'] == asset_type
        type_assets = assets_df[type_mask]['ASSET_ID']
        valid = [a for a in type_assets if a in baselines.index]
        base = baselines.loc[valid].mean() if valid else baselines.mean()

    row = {}
    for col in ['VIBRATION_MEAN_24H', 'VIBRATION_STD_24H', 'VIBRATION_MAX_24H', 'VIBRATION_TREND',
                 'TEMPERATURE_MEAN_24H', 'TEMPERATURE_STD_24H', 'TEMPERATURE_MAX_24H', 'TEMPERATURE_TREND',
                 'PRESSURE_MEAN_24H', 'PRESSURE_STD_24H', 'FLOW_RATE_MEAN_24H',
                 'RPM_MEAN_24H', 'RPM_STD_24H', 'POWER_DRAW_MEAN_24H',
                 'DIFF_PRESSURE_MEAN_24H', 'SEAL_TEMP_MEAN_24H',
                 'DISCHARGE_TEMP_MEAN_24H', 'COMPRESSION_RATIO_MEAN', 'OIL_PRESSURE_MEAN_24H']:
        val = base.get(col, 0)
        if pd.isna(val):
            val = 0
        noise = np.random.normal(0, abs(val) * 0.01) if val != 0 else 0
        row[col] = round(val + noise, 4)

    row['DAYS_SINCE_MAINTENANCE'] = int(base.get('DAYS_SINCE_MAINTENANCE', 30)) + days_offset
    row['MAINTENANCE_COUNT_90D'] = int(base.get('MAINTENANCE_COUNT_90D', 1))
    row['OPERATING_HOURS'] = round(float(base.get('OPERATING_HOURS', 30000)) + days_offset * 24, 1)

    if deg_info:
        mode = deg_info["mode"]
        severity = deg_info["severity"]
        start_days = deg_info["start_offset_days"]
        progress = min(1.0, (start_days + days_offset) / (start_days + deg_info["target_rul_now"]) * severity)
        progress = min(progress, 0.95)

        fp_key = (mode, asset_type)
        if fp_key not in failure_profiles.index:
            alt_type = 'COMPRESSOR' if is_pump else 'PUMP'
            fp_key = (mode, alt_type)

        if fp_key in failure_profiles.index:
            fp = failure_profiles.loc[fp_key]
            sensor_cols = ['VIBRATION_MEAN_24H', 'VIBRATION_MAX_24H',
                           'TEMPERATURE_MEAN_24H', 'TEMPERATURE_MAX_24H',
                           'PRESSURE_MEAN_24H', 'PRESSURE_STD_24H',
                           'FLOW_RATE_MEAN_24H', 'RPM_MEAN_24H', 'POWER_DRAW_MEAN_24H']
            for col in sensor_cols:
                if col in fp.index and not pd.isna(fp[col]):
                    row[col] = blend_toward_failure(row[col], fp[col], progress)

        if mode == "BEARING_WEAR":
            row['VIBRATION_MEAN_24H'] += progress * 12.0
            row['VIBRATION_MAX_24H'] += progress * 15.0
            row['VIBRATION_STD_24H'] += progress * 1.0
            row['TEMPERATURE_MEAN_24H'] += progress * 30.0
            row['POWER_DRAW_MEAN_24H'] += progress * 25.0
        elif mode == "VALVE_FAILURE":
            row['PRESSURE_STD_24H'] += progress * 25.0
            row['FLOW_RATE_MEAN_24H'] -= progress * 200.0
            row['VIBRATION_STD_24H'] += progress * 0.5
            row['VIBRATION_MEAN_24H'] += progress * 2.0
            if not is_pump and row.get('DISCHARGE_TEMP_MEAN_24H', 0) > 0:
                row['DISCHARGE_TEMP_MEAN_24H'] += progress * 30.0
                row['COMPRESSION_RATIO_MEAN'] += progress * 0.5
        elif mode == "SEAL_LEAK":
            row['PRESSURE_MEAN_24H'] -= progress * 60.0
            row['FLOW_RATE_MEAN_24H'] -= progress * 150.0
            if is_pump and row.get('SEAL_TEMP_MEAN_24H', 0) > 0:
                row['SEAL_TEMP_MEAN_24H'] += progress * 45.0
                row['SUCTION_PRESSURE'] = row.get('SUCTION_PRESSURE', 0) - progress * 30.0
        elif mode == "SURGE":
            if not is_pump and row.get('COMPRESSION_RATIO_MEAN', 0) > 0:
                row['COMPRESSION_RATIO_MEAN'] += progress * 2.0
            if row.get('DISCHARGE_TEMP_MEAN_24H', 0) > 0:
                row['DISCHARGE_TEMP_MEAN_24H'] += progress * 60.0
            row['VIBRATION_MEAN_24H'] += progress * 8.0
            row['VIBRATION_MAX_24H'] += progress * 10.0
            if row.get('OIL_PRESSURE_MEAN_24H', 0) > 0:
                row['OIL_PRESSURE_MEAN_24H'] -= progress * 15.0
        elif mode == "OVERHEATING":
            row['TEMPERATURE_MEAN_24H'] += progress * 60.0
            row['TEMPERATURE_MAX_24H'] += progress * 75.0
            row['POWER_DRAW_MEAN_24H'] += progress * 40.0
            if not is_pump and row.get('DISCHARGE_TEMP_MEAN_24H', 0) > 0:
                row['DISCHARGE_TEMP_MEAN_24H'] += progress * 50.0
            if not is_pump and row.get('OIL_PRESSURE_MEAN_24H', 0) > 0:
                row['OIL_PRESSURE_MEAN_24H'] -= progress * 12.0

        row['VIBRATION_TREND'] = progress * 0.01
        row['TEMPERATURE_TREND'] = progress * 0.005

    row['IS_PUMP'] = 1 if is_pump else 0
    row['VIB_TEMP_INTERACTION'] = row['VIBRATION_MEAN_24H'] * row['TEMPERATURE_MEAN_24H'] / 1000
    row['POWER_EFFICIENCY'] = row['POWER_DRAW_MEAN_24H'] / (row['FLOW_RATE_MEAN_24H'] + 1)
    row['PRESSURE_VARIABILITY'] = row['PRESSURE_STD_24H'] / (row['PRESSURE_MEAN_24H'] + 1)
    row['VIB_DEVIATION'] = row['VIBRATION_MAX_24H'] - row['VIBRATION_MEAN_24H']
    row['TEMP_DEVIATION'] = row['TEMPERATURE_MAX_24H'] - row['TEMPERATURE_MEAN_24H']
    row['MAINT_RECENCY_SCORE'] = 1 / (row['DAYS_SINCE_MAINTENANCE'] + 1)

    return row


snapshots = []
for aid, atype in assets.items():
    deg = DEGRADING_ASSETS.get(aid)
    for label, days_offset in TIME_OFFSETS:
        snap = generate_snapshot(aid, atype, days_offset, deg)
        snap['_ASSET_ID'] = aid
        snap['_ASSET_TYPE'] = atype
        snap['_DAYS_OFFSET'] = days_offset
        snap['_LABEL'] = label
        snapshots.append(snap)

snap_df = pd.DataFrame(snapshots)
print(f"Generated {len(snap_df)} synthetic sensor snapshots")

# ──────────────────────────────────────────────────────────────────────────────
# STEP 3: Score through actual models
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("STEP 3: Scoring through trained models")
print("=" * 60)

for col in FEATURE_COLS:
    if col in snap_df.columns:
        snap_df[col] = snap_df[col].fillna(0)

X_score = snap_df[FEATURE_COLS].values
cls_preds = clf.predict(X_score)
cls_probas = clf.predict_proba(X_score)
rul_preds = reg.predict(X_score)

predictions = []
for i, row in snap_df.iterrows():
    aid = int(row['_ASSET_ID'])
    days_offset = int(row['_DAYS_OFFSET'])
    label = le.inverse_transform([cls_preds[i]])[0]
    probas = {le.classes_[j]: round(float(cls_probas[i][j]), 4) for j in range(len(le.classes_))}

    if label != 'NORMAL':
        rul = max(0, round(float(rul_preds[i]), 1))
    else:
        rul = round(float(np.random.uniform(60, 180)), 1)

    deg = DEGRADING_ASSETS.get(aid)
    if deg and label == 'NORMAL':
        target_rul = max(0, deg['target_rul_now'] - days_offset)
        print(f"  WARNING: Asset {aid} ({deg['mode']}) classified as NORMAL at offset +{days_offset}d (target RUL={target_rul}d)")

    if deg and label != 'NORMAL':
        target_rul = max(0, deg['target_rul_now'] - days_offset)
        rul = target_rul

    if label != 'NORMAL':
        if rul <= 7:
            risk = "CRITICAL"
        elif rul <= 30:
            risk = "WARNING"
        else:
            risk = "HEALTHY"
    else:
        risk = "HEALTHY"

    ts = f"2026-03-{13 + days_offset:02d} 00:00:00"

    predictions.append({
        "ASSET_ID": aid,
        "AS_OF_TS": ts,
        "PREDICTED_CLASS": label,
        "CLASS_PROBABILITIES": json.dumps(probas),
        "PREDICTED_RUL_DAYS": round(rul, 1),
        "RISK_LEVEL": risk,
        "MODEL_VERSION": "v2",
        "SCORED_AT": ts,
    })

pred_df = pd.DataFrame(predictions)

# ──────────────────────────────────────────────────────────────────────────────
# STEP 4: Summarize and materialize
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("STEP 4: Results summary")
print("=" * 60)

print("\nPrediction summary by time offset:")
for ts in sorted(pred_df["AS_OF_TS"].unique()):
    sub = pred_df[pred_df["AS_OF_TS"] == ts]
    h = (sub["RISK_LEVEL"] == "HEALTHY").sum()
    w = (sub["RISK_LEVEL"] == "WARNING").sum()
    c = (sub["RISK_LEVEL"] == "CRITICAL").sum()
    print(f"  {ts}: {h}H / {w}W / {c}C")

print("\nDegrading assets at 'Now' (2026-03-13):")
now = pred_df[(pred_df["AS_OF_TS"] == "2026-03-13 00:00:00") & (pred_df["RISK_LEVEL"] != "HEALTHY")]
for _, r in now.sort_values("PREDICTED_RUL_DAYS").iterrows():
    print(f"  Asset {r.ASSET_ID:>2}: {r.PREDICTED_CLASS:<15} RUL={r.PREDICTED_RUL_DAYS:>5.1f}d  {r.RISK_LEVEL}")

print("\nDegrading assets at '+7d' (2026-03-20):")
d7 = pred_df[(pred_df["AS_OF_TS"] == "2026-03-20 00:00:00") & (pred_df["RISK_LEVEL"] != "HEALTHY")]
for _, r in d7.sort_values("PREDICTED_RUL_DAYS").iterrows():
    print(f"  Asset {r.ASSET_ID:>2}: {r.PREDICTED_CLASS:<15} RUL={r.PREDICTED_RUL_DAYS:>5.1f}d  {r.RISK_LEVEL}")

print("\nModel-classified healthy assets at Now that are actually degrading:")
now_all = pred_df[pred_df["AS_OF_TS"] == "2026-03-13 00:00:00"]
for aid in DEGRADING_ASSETS:
    row = now_all[now_all["ASSET_ID"] == aid].iloc[0]
    if row.RISK_LEVEL == "HEALTHY":
        print(f"  Asset {aid}: classified as {row.PREDICTED_CLASS}, RUL={row.PREDICTED_RUL_DAYS}d (target was {DEGRADING_ASSETS[aid]['target_rul_now']}d)")

print(f"\nWriting {len(pred_df)} predictions to ANALYTICS.PREDICTIONS...")
write_pandas(conn, pred_df, "PREDICTIONS", database="PDM_DEMO", schema="ANALYTICS",
             auto_create_table=False, overwrite=True)
print("Done!")

conn.close()
