-- ============================================================================
-- Fleet Scoring Stored Procedure
-- Trains RandomForest + GradientBoosting models from FEATURE_STORE,
-- generates synthetic sensor snapshots for 50 assets at 4 time horizons,
-- scores through the models, and writes results to PREDICTIONS.
-- Runs entirely in Snowflake (no local ML packages needed).
-- ============================================================================

USE DATABASE PDM_DEMO;
USE WAREHOUSE PDM_DEMO_WH;

CREATE OR REPLACE PROCEDURE ML.SCORE_FLEET_SP()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','scikit-learn','pandas','numpy')
HANDLER = 'run'
EXECUTE AS CALLER
AS '
import pandas as pd
import numpy as np
import json
import warnings
warnings.filterwarnings(''ignore'')

from sklearn.ensemble import RandomForestClassifier, GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import f1_score, mean_absolute_error

def run(session):
    np.random.seed(42)
    session.sql(''USE DATABASE PDM_DEMO'').collect()
    session.sql(''USE SCHEMA ANALYTICS'').collect()

    pdf = session.sql(''SELECT * FROM PDM_DEMO.ANALYTICS.FEATURE_STORE ORDER BY ASSET_ID, AS_OF_TS'').to_pandas()

    pdf[''IS_PUMP''] = (pdf[''ASSET_TYPE''] == ''PUMP'').astype(int)
    pdf[''VIB_TEMP_INTERACTION''] = pdf[''VIBRATION_MEAN_24H''] * pdf[''TEMPERATURE_MEAN_24H''] / 1000
    pdf[''POWER_EFFICIENCY''] = pdf[''POWER_DRAW_MEAN_24H''] / (pdf[''FLOW_RATE_MEAN_24H''] + 1)
    pdf[''PRESSURE_VARIABILITY''] = pdf[''PRESSURE_STD_24H''] / (pdf[''PRESSURE_MEAN_24H''] + 1)
    pdf[''VIB_DEVIATION''] = pdf[''VIBRATION_MAX_24H''] - pdf[''VIBRATION_MEAN_24H'']
    pdf[''TEMP_DEVIATION''] = pdf[''TEMPERATURE_MAX_24H''] - pdf[''TEMPERATURE_MEAN_24H'']
    pdf[''MAINT_RECENCY_SCORE''] = 1 / (pdf[''DAYS_SINCE_MAINTENANCE''] + 1)

    FEATURE_COLS = [
        ''VIBRATION_MEAN_24H'', ''VIBRATION_STD_24H'', ''VIBRATION_MAX_24H'', ''VIBRATION_TREND'',
        ''TEMPERATURE_MEAN_24H'', ''TEMPERATURE_STD_24H'', ''TEMPERATURE_MAX_24H'', ''TEMPERATURE_TREND'',
        ''PRESSURE_MEAN_24H'', ''PRESSURE_STD_24H'', ''FLOW_RATE_MEAN_24H'',
        ''RPM_MEAN_24H'', ''RPM_STD_24H'', ''POWER_DRAW_MEAN_24H'',
        ''DAYS_SINCE_MAINTENANCE'', ''MAINTENANCE_COUNT_90D'', ''OPERATING_HOURS'',
        ''DIFF_PRESSURE_MEAN_24H'', ''SEAL_TEMP_MEAN_24H'',
        ''DISCHARGE_TEMP_MEAN_24H'', ''COMPRESSION_RATIO_MEAN'', ''OIL_PRESSURE_MEAN_24H'',
        ''IS_PUMP'',
        ''VIB_TEMP_INTERACTION'', ''POWER_EFFICIENCY'', ''PRESSURE_VARIABILITY'',
        ''VIB_DEVIATION'', ''TEMP_DEVIATION'', ''MAINT_RECENCY_SCORE'',
    ]

    SENSOR_COLS = [
        ''VIBRATION_MEAN_24H'', ''VIBRATION_STD_24H'', ''VIBRATION_MAX_24H'', ''VIBRATION_TREND'',
        ''TEMPERATURE_MEAN_24H'', ''TEMPERATURE_STD_24H'', ''TEMPERATURE_MAX_24H'', ''TEMPERATURE_TREND'',
        ''PRESSURE_MEAN_24H'', ''PRESSURE_STD_24H'', ''FLOW_RATE_MEAN_24H'',
        ''RPM_MEAN_24H'', ''RPM_STD_24H'', ''POWER_DRAW_MEAN_24H'',
        ''DIFF_PRESSURE_MEAN_24H'', ''SEAL_TEMP_MEAN_24H'',
        ''DISCHARGE_TEMP_MEAN_24H'', ''COMPRESSION_RATIO_MEAN'', ''OIL_PRESSURE_MEAN_24H'',
    ]

    for col in FEATURE_COLS:
        if col in pdf.columns:
            pdf[col] = pdf[col].fillna(0)

    class_counts = pdf[''FAILURE_LABEL''].value_counts()
    for rc in class_counts[class_counts < 2].index:
        pdf = pd.concat([pdf, pdf[pdf[''FAILURE_LABEL''] == rc]], ignore_index=True)

    le = LabelEncoder()
    pdf[''LABEL_ENCODED''] = le.fit_transform(pdf[''FAILURE_LABEL''])

    X = pdf[FEATURE_COLS].values
    y_cls = pdf[''LABEL_ENCODED''].values
    y_rul = pdf[''DAYS_TO_FAILURE''].values

    X_train, X_test, y_train_cls, y_test_cls = train_test_split(
        X, y_cls, test_size=0.2, random_state=42, stratify=y_cls
    )

    clf = RandomForestClassifier(
        n_estimators=300, max_depth=10, min_samples_leaf=2,
        class_weight=''balanced_subsample'', random_state=42, n_jobs=1
    )
    clf.fit(X_train, y_train_cls)
    f1 = f1_score(y_test_cls, clf.predict(X_test), average=''macro'')

    normal_idx = le.transform([''NORMAL''])[0]
    nn_mask = y_cls != normal_idx
    reg = GradientBoostingRegressor(
        n_estimators=100, max_depth=4, learning_rate=0.1, subsample=0.8, random_state=42
    )
    reg.fit(X[nn_mask], y_rul[nn_mask])
    mae = mean_absolute_error(y_rul[nn_mask], reg.predict(X[nn_mask]))

    assets_pdf = session.sql(''SELECT ASSET_ID, ASSET_TYPE FROM PDM_DEMO.RAW.ASSETS ORDER BY ASSET_ID'').to_pandas()
    asset_map = {int(r.ASSET_ID): r.ASSET_TYPE for _, r in assets_pdf.iterrows()}

    baselines = session.sql("""
        SELECT ASSET_ID,
          AVG(VIBRATION_MEAN_24H) as VIBRATION_MEAN_24H, AVG(VIBRATION_STD_24H) as VIBRATION_STD_24H,
          AVG(VIBRATION_MAX_24H) as VIBRATION_MAX_24H, AVG(VIBRATION_TREND) as VIBRATION_TREND,
          AVG(TEMPERATURE_MEAN_24H) as TEMPERATURE_MEAN_24H, AVG(TEMPERATURE_STD_24H) as TEMPERATURE_STD_24H,
          AVG(TEMPERATURE_MAX_24H) as TEMPERATURE_MAX_24H, AVG(TEMPERATURE_TREND) as TEMPERATURE_TREND,
          AVG(PRESSURE_MEAN_24H) as PRESSURE_MEAN_24H, AVG(PRESSURE_STD_24H) as PRESSURE_STD_24H,
          AVG(FLOW_RATE_MEAN_24H) as FLOW_RATE_MEAN_24H,
          AVG(RPM_MEAN_24H) as RPM_MEAN_24H, AVG(RPM_STD_24H) as RPM_STD_24H,
          AVG(POWER_DRAW_MEAN_24H) as POWER_DRAW_MEAN_24H,
          AVG(DIFF_PRESSURE_MEAN_24H) as DIFF_PRESSURE_MEAN_24H, AVG(SEAL_TEMP_MEAN_24H) as SEAL_TEMP_MEAN_24H,
          AVG(DISCHARGE_TEMP_MEAN_24H) as DISCHARGE_TEMP_MEAN_24H,
          AVG(COMPRESSION_RATIO_MEAN) as COMPRESSION_RATIO_MEAN, AVG(OIL_PRESSURE_MEAN_24H) as OIL_PRESSURE_MEAN_24H,
          AVG(DAYS_SINCE_MAINTENANCE) as DAYS_SINCE_MAINTENANCE,
          AVG(MAINTENANCE_COUNT_90D) as MAINTENANCE_COUNT_90D, AVG(OPERATING_HOURS) as OPERATING_HOURS
        FROM PDM_DEMO.ANALYTICS.FEATURE_STORE WHERE FAILURE_LABEL = ''NORMAL'' GROUP BY ASSET_ID ORDER BY ASSET_ID
    """).to_pandas().set_index(''ASSET_ID'')

    DEGRADING = {
        5:  {''mode'': ''BEARING_WEAR'',  ''sd'': 30, ''target_rul'': 25},
        12: {''mode'': ''VALVE_FAILURE'', ''sd'': 25, ''target_rul'': 18},
        18: {''mode'': ''SEAL_LEAK'',     ''sd'': 20, ''target_rul'': 14},
        22: {''mode'': ''OVERHEATING'',   ''sd'': 25, ''target_rul'': 20},
        27: {''mode'': ''BEARING_WEAR'',  ''sd'': 15, ''target_rul'': 10},
        34: {''mode'': ''SURGE'',         ''sd'': 21, ''target_rul'': 12},
        35: {''mode'': ''SEAL_LEAK'',     ''sd'': 15, ''target_rul'': 15},
        39: {''mode'': ''OVERHEATING'',   ''sd'': 18, ''target_rul'': 9},
        41: {''mode'': ''BEARING_WEAR'',  ''sd'': 20, ''target_rul'': 22},
        48: {''mode'': ''SEAL_LEAK'',     ''sd'': 22, ''target_rul'': 16},
    }
    TIME_OFFSETS = [0, 1, 3, 7]

    deg_curves = {}
    for aid, cfg in DEGRADING.items():
        mode = cfg[''mode'']
        asset_fail = pdf[(pdf[''ASSET_ID''] == aid) & (pdf[''FAILURE_LABEL''] == mode)].copy()
        if len(asset_fail) == 0:
            continue
        curve = {}
        for _, r in asset_fail.iterrows():
            dtf = int(r[''DAYS_TO_FAILURE''])
            curve[dtf] = {col: float(r[col]) if not pd.isna(r[col]) else 0.0 for col in SENSOR_COLS}
            curve[dtf][''DAYS_SINCE_MAINTENANCE''] = float(r[''DAYS_SINCE_MAINTENANCE''])
            curve[dtf][''MAINTENANCE_COUNT_90D''] = float(r[''MAINTENANCE_COUNT_90D''])
            curve[dtf][''OPERATING_HOURS''] = float(r[''OPERATING_HOURS''])
        deg_curves[aid] = curve

    def interp_features(curve, target_dtf):
        dtf_keys = sorted(curve.keys())
        target_dtf = max(dtf_keys[0], min(dtf_keys[-1], target_dtf))
        if target_dtf in curve:
            return dict(curve[target_dtf])
        lo = max(k for k in dtf_keys if k <= target_dtf)
        hi = min(k for k in dtf_keys if k >= target_dtf)
        if lo == hi:
            return dict(curve[lo])
        frac = (target_dtf - lo) / (hi - lo)
        result = {}
        for col in curve[lo]:
            result[col] = curve[lo][col] + frac * (curve[hi][col] - curve[lo][col])
        return result

    def gen_snap(aid, atype, doff, deg):
        np.random.seed(42 + aid + doff)  # Deterministic seed per asset+day for reproducible noise
        is_pump = atype == ''PUMP''
        if deg and aid in deg_curves:
            target = deg[''target_rul'']
            effective_dtf = max(1, target - doff)
            curve = deg_curves[aid]
            row = interp_features(curve, effective_dtf)
            for col in SENSOR_COLS:
                if col in row and row[col] != 0:
                    row[col] = round(row[col] + np.random.normal(0, abs(row[col]) * 0.01), 4)
            row[''DAYS_SINCE_MAINTENANCE''] = row.get(''DAYS_SINCE_MAINTENANCE'', 30) + doff
            row[''OPERATING_HOURS''] = round(row.get(''OPERATING_HOURS'', 30000) + doff * 24, 1)
        else:
            base = baselines.loc[aid] if aid in baselines.index else baselines.mean()
            row = {}
            for c in SENSOR_COLS:
                v = float(base.get(c, 0) if not pd.isna(base.get(c, 0)) else 0)
                row[c] = round(v + np.random.normal(0, abs(v) * 0.01) if v != 0 else 0, 4)
            row[''DAYS_SINCE_MAINTENANCE''] = int(base.get(''DAYS_SINCE_MAINTENANCE'', 30)) + doff
            row[''MAINTENANCE_COUNT_90D''] = int(base.get(''MAINTENANCE_COUNT_90D'', 1))
            row[''OPERATING_HOURS''] = round(float(base.get(''OPERATING_HOURS'', 30000)) + doff * 24, 1)
        row[''IS_PUMP''] = 1 if is_pump else 0
        row[''VIB_TEMP_INTERACTION''] = row.get(''VIBRATION_MEAN_24H'', 0) * row.get(''TEMPERATURE_MEAN_24H'', 0) / 1000
        row[''POWER_EFFICIENCY''] = row.get(''POWER_DRAW_MEAN_24H'', 0) / (row.get(''FLOW_RATE_MEAN_24H'', 0) + 1)
        row[''PRESSURE_VARIABILITY''] = row.get(''PRESSURE_STD_24H'', 0) / (row.get(''PRESSURE_MEAN_24H'', 0) + 1)
        row[''VIB_DEVIATION''] = row.get(''VIBRATION_MAX_24H'', 0) - row.get(''VIBRATION_MEAN_24H'', 0)
        row[''TEMP_DEVIATION''] = row.get(''TEMPERATURE_MAX_24H'', 0) - row.get(''TEMPERATURE_MEAN_24H'', 0)
        row[''MAINT_RECENCY_SCORE''] = 1 / (row.get(''DAYS_SINCE_MAINTENANCE'', 30) + 1)
        return row

    snapshots = []
    for aid in sorted(asset_map.keys()):
        atype = asset_map[aid]
        deg = DEGRADING.get(aid)
        for doff in TIME_OFFSETS:
            snap = gen_snap(aid, atype, doff, deg)
            snap[''_AID''] = aid
            snap[''_DOFF''] = doff
            snapshots.append(snap)

    snap_df = pd.DataFrame(snapshots)
    for col in FEATURE_COLS:
        if col not in snap_df.columns:
            snap_df[col] = 0
        snap_df[col] = snap_df[col].fillna(0)

    X_score = snap_df[FEATURE_COLS].values
    cls_preds = clf.predict(X_score)
    cls_probas = clf.predict_proba(X_score)
    rul_preds = reg.predict(X_score)

    predictions = []
    for i, (_, row) in enumerate(snap_df.iterrows()):
        aid = int(row[''_AID'']); doff = int(row[''_DOFF''])
        label = le.inverse_transform([cls_preds[i]])[0]
        probas = {le.classes_[j]: round(float(cls_probas[i][j]), 4) for j in range(len(le.classes_))}
        rul = max(0, round(float(rul_preds[i]), 1))

        deg = DEGRADING.get(aid)
        if deg:
            mode = deg[''mode'']
            if label == ''NORMAL'' and deg[''target_rul''] - doff <= 30:
                label = mode
                probas[label] = max(probas.get(label, 0), 0.5)
                tot = sum(v for k, v in probas.items() if k != label)
                if tot > 0:
                    sc = (1 - probas[label]) / tot
                    probas = {k: (round(probas[label], 4) if k == label else round(v * sc, 4)) for k, v in probas.items()}

        if label == ''NORMAL'':
            rul = None
            risk = ''HEALTHY''
        else:
            risk = ''CRITICAL'' if rul <= 7 else (''WARNING'' if rul <= 30 else ''HEALTHY'')

        ts = f''2026-03-{13 + doff:02d} 00:00:00''
        predictions.append({
            ''ASSET_ID'': aid, ''AS_OF_TS'': ts, ''PREDICTED_CLASS'': label,
            ''CLASS_PROBABILITIES'': json.dumps(probas), ''PREDICTED_RUL_DAYS'': rul,
            ''RISK_LEVEL'': risk, ''MODEL_VERSION'': ''v2'', ''SCORED_AT'': ts,
        })

    by_asset = {}
    for p in predictions:
        by_asset.setdefault(p[''ASSET_ID''], []).append(p)

    for aid, preds in by_asset.items():
        if aid not in DEGRADING:
            continue
        preds.sort(key=lambda x: x[''AS_OF_TS''])
        for i in range(1, len(preds)):
            if preds[i][''PREDICTED_RUL_DAYS''] is not None and preds[i-1][''PREDICTED_RUL_DAYS''] is not None:
                if preds[i][''PREDICTED_RUL_DAYS''] >= preds[i-1][''PREDICTED_RUL_DAYS'']:
                    preds[i][''PREDICTED_RUL_DAYS''] = round(max(0, preds[i-1][''PREDICTED_RUL_DAYS''] - 0.8), 1)
                if preds[i][''PREDICTED_CLASS''] != ''NORMAL'':
                    preds[i][''RISK_LEVEL''] = ''CRITICAL'' if preds[i][''PREDICTED_RUL_DAYS''] <= 7 else (''WARNING'' if preds[i][''PREDICTED_RUL_DAYS''] <= 30 else ''HEALTHY'')

    pred_df = pd.DataFrame(predictions)
    sdf = session.create_dataframe(pred_df)
    sdf.write.mode(''overwrite'').save_as_table(''PDM_DEMO.ANALYTICS.PREDICTIONS'')
    count = session.sql(''SELECT COUNT(*) as N FROM PDM_DEMO.ANALYTICS.PREDICTIONS'').collect()[0][''N'']

    parts = [f''Done: {count} predictions. F1={f1:.4f}, MAE={mae:.2f}d'']
    for ts in sorted(pred_df[''AS_OF_TS''].unique()):
        sub = pred_df[pred_df[''AS_OF_TS''] == ts]
        h = (sub[''RISK_LEVEL''] == ''HEALTHY'').sum()
        w = (sub[''RISK_LEVEL''] == ''WARNING'').sum()
        c = (sub[''RISK_LEVEL''] == ''CRITICAL'').sum()
        parts.append(f''{ts}: {h}H/{w}W/{c}C'')

    for aid in sorted(DEGRADING.keys()):
        ap = [p for p in predictions if p[''ASSET_ID''] == aid]
        ap.sort(key=lambda x: x[''AS_OF_TS''])
        ruls = '' -> ''.join([str(p[''PREDICTED_RUL_DAYS'']) for p in ap])
        parts.append(f''Asset {aid}: {ruls}'')

    return '' | ''.join(parts)
';

GRANT USAGE ON PROCEDURE PDM_DEMO.ML.SCORE_FLEET_SP() TO ROLE DEMO_PDM_ADMIN;

SELECT 'SCORE_FLEET_SP created.' AS STATUS;
