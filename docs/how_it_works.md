# How It Works — PDM Demo Deep Dive

This document explains the three core pillars of the Predictive Asset Navigator demo:
1. **Data Structure** — How data is organized and flows through the system
2. **ML Pipeline** — How the machine learning models are trained and make predictions
3. **Agent Architecture** — How the AI assistant orchestrates tools to answer questions

---

## 1. Data Structure

### Schema Organization

```
PDM_DEMO Database
├── RAW Schema (Source Data)
│   ├── STATIONS (10 rows) — Physical locations in Permian Basin
│   ├── ASSETS (50 rows) — Pumps and compressors with model info
│   ├── TELEMETRY (1M+ rows) — 15-minute sensor readings for 180 days
│   ├── MAINTENANCE_LOGS (500 rows) — Historical repair records
│   ├── TECHNICIANS (8 rows) — Field techs with certs and home base
│   └── PARTS_INVENTORY (50 rows) — Spare parts catalog
│
├── ANALYTICS Schema (ML Pipeline)
│   ├── FEATURE_STORE (45K rows) — 24h rolling aggregations for ML
│   ├── PREDICTIONS (11K rows) — Model outputs at multiple timestamps
│   └── FLEET_KPI_VIEW — Denormalized join for dashboards
│
├── ML Schema (Models)
│   ├── FAILURE_CLASSIFIER (v7) — XGBoost 6-class model
│   ├── RUL_REGRESSOR (v7) — XGBoost RUL prediction
│   ├── PROBABILITY_CALIBRATOR (v7) — Severity-scaled probabilities
│   └── MODEL_METADATA — Baselines and version info
│
└── APP Schema (Application Services)
    ├── MANUALS (71 rows) — Operating guides from PARSE_DOCUMENT
    ├── FLEET_SEMANTIC_VIEW — Cortex Analyst interface
    ├── MANUAL_SEARCH — Cortex Search (RAG)
    ├── PDM_AGENT — Cortex Agent with 3 tools
    └── PLAN_ROUTE — Python stored procedure
```

### Key Table Relationships

```
STATIONS (10)
    ↓ 1:N
ASSETS (50) ────────────────────┐
    ↓ 1:N                       │
TELEMETRY (1M+)                 │
    ↓ aggregated                │
FEATURE_STORE (45K)             │
    ↓ scored by ML              │
PREDICTIONS (11K) ←─────────────┘
    ↓ joined
FLEET_KPI_VIEW (50) → FLEET_SEMANTIC_VIEW → PDM_AGENT
```

### Telemetry Data (RAW.TELEMETRY)

14 sensor columns recorded every 15 minutes:

| Sensor | Unit | Normal Range | What It Indicates |
|--------|------|--------------|-------------------|
| VIBRATION | mm/s | 2.0 - 5.0 | Bearing/alignment health |
| TEMPERATURE | °F | 160 - 200 | Thermal stress, lubrication |
| PRESSURE | PSI | 450 - 600 | Discharge/system pressure |
| DIFFERENTIAL_PRESSURE | PSI | 15 - 35 | Filter/valve condition |
| FLOW_RATE | gpm | 800 - 1200 | Throughput efficiency |
| RPM | rpm | 2400 - 2600 | Motor speed |
| POWER_DRAW | kW | 250 - 340 | Energy consumption |
| OIL_PRESSURE | PSI | 50 - 70 | Lubrication system |
| SUCTION_PRESSURE | PSI | - | Pump inlet (pumps only) |
| DISCHARGE_TEMP | °F | - | Outlet temp (compressors) |
| SEAL_TEMP | °F | - | Mechanical seal (pumps) |
| COMPRESSION_RATIO | ratio | 2.8 - 3.2 | Compressor efficiency |

### Feature Store (ANALYTICS.FEATURE_STORE)

24-hour rolling aggregations computed daily for ML training:

| Feature Category | Features |
|-----------------|----------|
| **Vibration** | VIBRATION_MEAN_24H, VIBRATION_STD_24H, VIBRATION_MAX_24H, VIBRATION_TREND |
| **Temperature** | TEMPERATURE_MEAN_24H, TEMPERATURE_STD_24H, TEMPERATURE_MAX_24H, TEMPERATURE_TREND |
| **Pressure** | PRESSURE_MEAN_24H, PRESSURE_STD_24H, DIFF_PRESSURE_MEAN_24H |
| **Flow/Power** | FLOW_RATE_MEAN_24H, RPM_MEAN_24H, POWER_DRAW_MEAN_24H |
| **Maintenance** | DAYS_SINCE_MAINTENANCE, MAINTENANCE_COUNT_90D, OPERATING_HOURS |
| **Labels** | FAILURE_LABEL (6 classes), DAYS_TO_FAILURE |

### How to Demonstrate Data Structure

```sql
-- 1. Show the scale of raw telemetry
SELECT COUNT(*) AS total_readings,
       COUNT(DISTINCT ASSET_ID) AS unique_assets,
       MIN(TS)::DATE AS earliest,
       MAX(TS)::DATE AS latest
FROM PDM_DEMO.RAW.TELEMETRY;

-- 2. Show sensor readings for one asset
SELECT ASSET_ID, TS, VIBRATION, TEMPERATURE, PRESSURE, POWER_DRAW
FROM PDM_DEMO.RAW.TELEMETRY
WHERE ASSET_ID = 27
ORDER BY TS DESC
LIMIT 10;

-- 3. Show feature engineering (rolling aggregations)
SELECT ASSET_ID, AS_OF_TS,
       VIBRATION_MEAN_24H, VIBRATION_TREND,
       TEMPERATURE_MEAN_24H, TEMPERATURE_TREND,
       DAYS_SINCE_MAINTENANCE, FAILURE_LABEL
FROM PDM_DEMO.ANALYTICS.FEATURE_STORE
WHERE ASSET_ID = 27
ORDER BY AS_OF_TS DESC
LIMIT 5;

-- 4. Show prediction outputs
SELECT PUMP_ID, TS, PREDICTED_CLASS, PREDICTED_RUL_DAYS, CONFIDENCE
FROM PDM_DEMO.ANALYTICS.PREDICTIONS
WHERE PUMP_ID = 27
ORDER BY TS DESC
LIMIT 5;
```

---

## 2. ML Pipeline

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         ML TRAINING PIPELINE                             │
│  (Snowflake Notebook: ml_training.ipynb)                                │
│                                                                         │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────┐  │
│  │ FEATURE_STORE   │───▶│ Feature Eng.    │───▶│ SMOTE Oversampling  │  │
│  │ (45K rows)      │    │ (32 features)   │    │ (balance classes)   │  │
│  └─────────────────┘    └─────────────────┘    └──────────┬──────────┘  │
│                                                           │              │
│  ┌───────────────────────────────────────────────────────┼───────────┐  │
│  │                                                        ▼           │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐│  │
│  │  │ FAILURE_CLF     │  │ RUL_REGRESSOR   │  │ PROB_CALIBRATOR    ││  │
│  │  │ (XGBoost)       │  │ (XGBoost)       │  │ (LogisticReg)      ││  │
│  │  │ 6-class         │  │ Days to failure │  │ Severity probs     ││  │
│  │  └────────┬────────┘  └────────┬────────┘  └──────────┬─────────┘│  │
│  │           │                    │                       │          │  │
│  │           └────────────────────┼───────────────────────┘          │  │
│  │                                ▼                                   │  │
│  │                    SNOWFLAKE MODEL REGISTRY                        │  │
│  └────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                         ML INFERENCE PIPELINE                            │
│  (Stored Procedure: SCORE_FLEET_SP or score_fleet.ipynb)                │
│                                                                         │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────┐  │
│  │ FEATURE_STORE   │───▶│ Load Models     │───▶│ Run Inference       │  │
│  │ (new data)      │    │ from Registry   │    │ - classify()        │  │
│  │                 │    │                 │    │ - predict_rul()     │  │
│  │                 │    │                 │    │ - predict_proba()   │  │
│  └─────────────────┘    └─────────────────┘    └──────────┬──────────┘  │
│                                                           │              │
│                                                           ▼              │
│                                              ┌─────────────────────────┐ │
│                                              │ PREDICTIONS table       │ │
│                                              │ - PREDICTED_CLASS       │ │
│                                              │ - PREDICTED_RUL_DAYS    │ │
│                                              │ - CLASS_PROBABILITIES   │ │
│                                              │ - RISK_LEVEL            │ │
│                                              └─────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

### The Three Models

| Model | Type | Purpose | Key Output |
|-------|------|---------|------------|
| **FAILURE_CLASSIFIER** | XGBoost (6-class) | Predicts what will fail | BEARING_WEAR, SEAL_LEAK, VALVE_FAILURE, OVERHEATING, SURGE, NORMAL |
| **RUL_REGRESSOR** | XGBoost (regression) | Predicts when it will fail | Days until failure (0-999) |
| **PROBABILITY_CALIBRATOR** | LogisticRegression | Severity-scaled probabilities | Probability distribution across all classes |

### Why Three Models?

1. **Classifier** is great at hard classifications but overconfident on probabilities
2. **Calibrator** produces smooth probability curves that increase as sensors degrade — essential for the UI's probability charts
3. **Regressor** is trained only on non-NORMAL samples for accurate RUL estimates

### 32 Input Features

The models use 32 engineered features:

```python
# Severity scores (z-scores from NORMAL baselines)
VIB_SEVERITY = (VIBRATION_MEAN_24H - 3.4679) / 0.8777
TEMP_SEVERITY = (TEMPERATURE_MEAN_24H - 180.7056) / 11.7088
DEGRADATION_INTENSITY = sqrt(VIB_SEVERITY² + TEMP_SEVERITY²)

# Interaction features
VIB_TEMP_INTERACTION = VIBRATION_MEAN_24H * TEMPERATURE_MEAN_24H
POWER_EFFICIENCY = FLOW_RATE_MEAN_24H / POWER_DRAW_MEAN_24H
PRESSURE_VARIABILITY = PRESSURE_STD_24H / PRESSURE_MEAN_24H
```

### Failure Classes & Their Signatures

| Class | Primary Sensor | Behavior | Typical RUL at Detection |
|-------|----------------|----------|--------------------------|
| **BEARING_WEAR** | Vibration | Linear increase 3.5→12 mm/s | 7-9 days |
| **OVERHEATING** | Temperature | Exponential rise, final 72h acceleration | 5-7 days |
| **SEAL_LEAK** | Pressure | Step decreases, erratic variance | 7-10 days |
| **VALVE_FAILURE** | Diff. Pressure | Spikes, eventually constant high | 6-8 days |
| **SURGE** | Compression Ratio | Oscillations, rapid final deterioration | 4-6 days |
| **NORMAL** | All | Within baseline, no trends | N/A (healthy) |

### Risk Level Derivation

```python
if PREDICTED_CLASS == 'OFFLINE':
    RISK_LEVEL = 'FAILED'
elif PREDICTED_RUL_DAYS <= 7:
    RISK_LEVEL = 'CRITICAL'  # Red
elif PREDICTED_RUL_DAYS <= 14:
    RISK_LEVEL = 'WARNING'   # Amber
else:
    RISK_LEVEL = 'HEALTHY'   # Green
```

### How to Demonstrate ML Pipeline

```sql
-- 1. Show registered models in ML Registry
SHOW MODELS IN SCHEMA PDM_DEMO.ML;

-- 2. Show model versions and metrics
SELECT * FROM SNOWFLAKE.ML.MODELS 
WHERE DATABASE_NAME = 'PDM_DEMO' AND SCHEMA_NAME = 'ML';

-- 3. Show baselines stored for inference
SELECT * FROM PDM_DEMO.ML.MODEL_METADATA;

-- 4. Show prediction distribution
SELECT PREDICTED_CLASS, COUNT(*) AS count,
       AVG(PREDICTED_RUL_DAYS) AS avg_rul
FROM PDM_DEMO.ANALYTICS.PREDICTIONS
WHERE TS = (SELECT MAX(TS) FROM PDM_DEMO.ANALYTICS.PREDICTIONS)
GROUP BY PREDICTED_CLASS;

-- 5. Show a degrading asset's prediction history
SELECT TS, PREDICTED_CLASS, PREDICTED_RUL_DAYS, CONFIDENCE
FROM PDM_DEMO.ANALYTICS.PREDICTIONS
WHERE PUMP_ID = 27
ORDER BY TS;
```

Open `notebooks/ml_training.ipynb` in Snowsight to walk through each training step.

---

## 3. Agent Architecture

### Tool Orchestration

```
                         User Question
                              │
                              ▼
                    ┌─────────────────────┐
                    │     PDM_AGENT       │
                    │  (claude-4-sonnet)  │
                    │                     │
                    │  System prompt:     │
                    │  - Failure modes    │
                    │  - Sensor mappings  │
                    │  - Co-replacement   │
                    │  - Response format  │
                    └──────────┬──────────┘
                               │
           ┌───────────────────┼───────────────────┐
           │                   │                   │
           ▼                   ▼                   ▼
    ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
    │ fleet_      │     │ manual_     │     │ plan_route  │
    │ analyst     │     │ search      │     │             │
    │             │     │             │     │             │
    │ Cortex      │     │ Cortex      │     │ PLAN_ROUTE  │
    │ Analyst     │     │ Search      │     │ Python SP   │
    │             │     │             │     │             │
    │ FLEET_      │     │ 71 docs     │     │ Haversine   │
    │ SEMANTIC_   │     │ arctic-     │     │ routing +   │
    │ VIEW        │     │ embed-m     │     │ scheduling  │
    └──────┬──────┘     └──────┬──────┘     └──────┬──────┘
           │                   │                   │
           ▼                   ▼                   ▼
    ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
    │ Text→SQL    │     │ RAG over    │     │ Route plan  │
    │ "How many   │     │ PDFs +      │     │ with stops, │
    │ critical?"  │     │ maint logs  │     │ co-maint,   │
    │             │     │             │     │ reasoning   │
    └─────────────┘     └─────────────┘     └─────────────┘
```

### The Three Tools

#### Tool 1: fleet_analyst (Cortex Analyst)

**Purpose:** Quantitative questions about fleet data
**Implementation:** Text-to-SQL via Semantic View

```
User: "How many assets are critical?"
      ↓
Cortex Analyst generates SQL:
      SELECT COUNT(*) FROM FLEET_KPI_VIEW WHERE RISK_LEVEL = 'CRITICAL'
      ↓
Returns: 3 assets
```

**Semantic View** (`FLEET_SEMANTIC_VIEW`) defines business terms:
- "risk level" → RISK_LEVEL column
- "failure type" → PREDICTED_CLASS column
- "days until failure" → PREDICTED_RUL_DAYS column

#### Tool 2: manual_search (Cortex Search)

**Purpose:** Procedures, troubleshooting, safety info from operating guides
**Implementation:** Vector search (arctic-embed-m-v1.5) over 71 documents

```
User: "How do I fix bearing wear on a Flowserve pump?"
      ↓
Cortex Search retrieves:
      - Flowserve HPRT Operating Manual Section 8.2.3
      - Bearing replacement procedure with torque specs
      - Parts list: SKF 6310-2RS bearing, Mobil SHC 629 grease
      ↓
Agent synthesizes response with manual citations
```

**Document Sources:**
- 8 PDF manuals processed via PARSE_DOCUMENT (layout mode)
- Maintenance logs with resolution notes
- Troubleshooting guides with time estimates and parts

#### Tool 3: plan_route (Stored Procedure)

**Purpose:** Intelligent service route planning
**Implementation:** Python SP with Haversine routing

```
User: "Plan a route for Mike starting with Asset 27"
      ↓
PLAN_ROUTE SP:
      1. Get tech's home base, certs, existing schedule
      2. Get primary asset location and failure mode
      3. Find other WARNING/CRITICAL assets nearby
      4. Sort by nearest-neighbor from current location
      5. At each stop, check for co-maintenance opportunities:
         - Rising sensor trends on HEALTHY assets
         - Overdue preventive maintenance
         - High operating hours
      6. Bundle co-maintenance tasks while on-site
      7. Generate reasoning for each decision
      ↓
Returns: route with stops, parts, co-maint, explanations
```

### Route Planner Intelligence

The route planner doesn't just sequence stops — it actively looks for value:

**Co-Maintenance Triggers:**
| Trigger | Condition | Example Task |
|---------|-----------|--------------|
| `sensor_trend` | Vibration trend > 0.001/hr | "Vibration trending up — inspect bearings" |
| `sensor_threshold` | Vibration > 4.0 mm/s | "Elevated vibration — check mounting" |
| `maintenance_overdue` | Days since service > 45 | "Overdue PM — full service required" |
| `maintenance_due_soon` | Days since service > 25 | "PM due soon — lubrication check" |
| `operating_hours` | Hours > 100,000 | "High-hour inspection — wear assessment" |
| `routine` | None of above | "Visual inspection while on-site" |

**Routing Decisions Include:**
- Why each stop was added (failure pattern, RUL, proximity)
- Why each co-maintenance task was recommended (specific trigger)
- Route efficiency metrics (miles, utilization %)
- Schedule fit (respects existing commitments)

### How to Demonstrate Agent

**1. Ask a KPI Question (triggers fleet_analyst)**
```
"How many assets are in warning or critical status?"
```
→ Agent generates SQL, returns count

**2. Ask a Procedure Question (triggers manual_search)**
```
"What's the procedure for replacing bearings on a Grundfos CRN pump?"
```
→ Agent retrieves from manual, cites section numbers

**3. Ask for a Route (triggers plan_route)**
```
"Plan a 2-day service route for Mike starting with Asset 27"
```
→ Agent calls SP, explains routing logic

**4. Ask a Complex Question (triggers multiple tools)**
```
"Asset 27 is showing bearing wear. What's wrong, how do I fix it, and who should I send?"
```
→ Agent uses analyst (data), search (manual), plan_route (dispatch)

### Agent Instructions Include

The system prompt gives the agent domain expertise:

```
- Failure mode → sensor mapping (which readings indicate each failure)
- Co-replacement intelligence (bearings → seals → alignment)
- Model-specific guidance for all 8 equipment types
- Response structure: signals → manual excerpt → action list → safety
```

### How to Show Agent Architecture

```sql
-- 1. Show the Cortex Agent definition
DESCRIBE CORTEX AGENT PDM_DEMO.APP.PDM_AGENT;

-- 2. Show the Semantic View (Analyst interface)
DESCRIBE SEMANTIC VIEW PDM_DEMO.APP.FLEET_SEMANTIC_VIEW;

-- 3. Show the Cortex Search service
SHOW CORTEX SEARCH SERVICES IN SCHEMA PDM_DEMO.APP;

-- 4. Show indexed documents
SELECT DOC_ID, PUMP_MODEL, SECTION_TITLE, CHAR_LENGTH(CONTENT) AS content_len
FROM PDM_DEMO.APP.MANUALS
ORDER BY PUMP_MODEL, DOC_ID;

-- 5. Test the route planner directly
CALL PDM_DEMO.APP.PLAN_ROUTE('TECH-001', 27, 2, 5, '2026-03-13T00:00:00', FALSE);
```

---

## Demo Walkthrough Checklist

### Data Structure Demo
- [ ] Query RAW.TELEMETRY to show sensor volume (1M+ rows)
- [ ] Show one asset's sensor readings over time
- [ ] Show FEATURE_STORE rolling aggregations
- [ ] Show PREDICTIONS output with class, RUL, confidence

### ML Pipeline Demo
- [ ] Open `ml_training.ipynb` in Snowsight
- [ ] Walk through feature engineering cells
- [ ] Show SMOTE balancing (98% NORMAL → balanced)
- [ ] Show model training and metrics
- [ ] Show ML Registry with versioned models
- [ ] Query PREDICTIONS to show inference output

### Agent Demo
- [ ] Ask a KPI question → show text-to-SQL
- [ ] Ask a manual question → show RAG retrieval
- [ ] Plan a route → show intelligent bundling
- [ ] Ask a complex question → show tool orchestration
- [ ] Show the reasoning in route explanations

---

## SQL Proof Worksheets

Pre-built worksheets in `snowflake/proof/`:

| Worksheet | What It Proves |
|-----------|---------------|
| `01_explore_raw_data.sql` | Data foundation: 10 stations, 50 assets, 1M+ telemetry |
| `02_ml_pipeline_proof.sql` | Real ML: Feature Store, Registry, predictions correlate with sensors |
| `03_cortex_services_proof.sql` | AI stack: Search, Analyst, Agent, route planner |
| `04_live_predictions.sql` | Time travel: RUL counting down, risk escalation |
