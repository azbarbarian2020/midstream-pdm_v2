# Midstream Predictive Maintenance Demo

AI-powered predictive maintenance for midstream oil and gas pipeline operations in the Permian Basin. Built entirely on Snowflake with Cortex AI, SPCS, and a Next.js frontend.

## What This Demo Shows

| Feature | Technology |
|---------|------------|
| Fleet health dashboard with interactive map | SPCS + Next.js |
| ML-predicted failure modes and remaining useful life (RUL) | ML Model Registry + XGBoost |
| Natural language fleet analytics | Cortex Analyst + Semantic View |
| Equipment manual and maintenance log search | Cortex Search |
| AI maintenance assistant with tool orchestration | Cortex Agent (3 tools) |
| Optimized technician route planning with co-maintenance | Python Stored Procedure |
| Time-travel simulation (rewind/fast-forward) | Parameterized queries |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  SPCS (Snowpark Container Services)                         │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  Next.js Frontend (pdm_frontend:v1)                    │ │
│  │  ├── Fleet Dashboard (map, KPIs, alerts)               │ │
│  │  ├── Asset Detail (sensor trends, predictions)         │ │
│  │  ├── Dispatch (route planner, work orders, Gantt)      │ │
│  │  └── 20 API Routes → Snowflake SDK + REST              │ │
│  └────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│  Cortex AI Layer                                            │
│  ├── PDM_AGENT (claude-4-sonnet, 3 tools)                  │
│  ├── MANUAL_SEARCH (Cortex Search, arctic-embed-m-v1.5)    │
│  └── FLEET_SEMANTIC_VIEW (Cortex Analyst)                   │
├─────────────────────────────────────────────────────────────┤
│  Data Layer (PDM_DEMO database)                             │
│  ├── RAW: stations, assets, telemetry, maintenance, parts   │
│  ├── ANALYTICS: feature_store, predictions, fleet_kpi_view  │
│  ├── ML: model registry (classifier, regressor, calibrator) │
│  └── APP: manuals, work_orders, tech_schedules, secrets     │
└─────────────────────────────────────────────────────────────┘
```

50 assets (pumps + compressors) across 10 stations, 8 equipment models, 8 technicians, 6 failure modes, ~1M telemetry rows.

## Prerequisites

- Snowflake account on **AWS** (Cortex AI features require AWS)
- ACCOUNTADMIN role (or equivalent privileges)
- Docker Desktop (for building the container image)
- Snowflake CLI (`pip install snowflake-cli`)
- Python 3.11+ (for JSON parsing in setup script)
- openssl (for RSA key generation)

> **Note**: Seed data is included as static CSV exports in the `data/` directory. ML predictions are pre-computed — no local ML or Python packages needed.

## Quick Start

```bash
git clone https://github.com/azbarbarian2020/midstream-pdm_v2.git
cd midstream-pdm_v2
./setup.sh
```

**Estimated time**: 15-25 minutes (mostly Docker build + data loading)

The setup script will:

1. Prompt for your Snowflake CLI connection name
2. Auto-detect your account, host, and registry
3. Create all database objects (tables, views, stages, role)
4. Load seed data from static CSV exports (~1M telemetry rows)
5. Load pre-computed ML predictions (deterministic, seeded)
6. Create Cortex Search, Semantic View, and Agent
7. Prompt for PAT and set up RSA key pair (with safe key management)
8. Build and push the Docker image
9. Deploy the SPCS service
10. Print the application URL

### Safe Key Management

If you already have an RSA key configured for another SPCS app (like Digital_Twin_Truck_Configurator), the setup script will detect this and offer options:

1. **Reuse existing key** — Requires the matching private key
2. **Use RSA_PUBLIC_KEY_2** — Both apps work simultaneously (recommended)
3. **Generate new key** — Warning: breaks other SPCS apps

## Documentation

| Document | Purpose |
|----------|---------|
| [README.md](README.md) | Quick start guide and overview |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Detailed technical architecture |
| [docs/how_it_works.md](docs/how_it_works.md) | **Deep dive for demos:** Data structure, ML pipeline, Agent architecture |
| [docs/failure_signatures.md](docs/failure_signatures.md) | Failure mode sensor signatures |
| [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | Common issues and solutions |

## For Demonstrators

### Data Consistency

All data is loaded from pre-exported CSVs for consistent demo behavior across deployments. Asset 27 will always show bearing wear with the same RUL values, and the route planner will always bundle the same assets.

### Running ML Notebooks (Optional)

The notebooks use fixed random seeds (`random_state=42`, `seed=123`) for reproducibility. You CAN run them to:

- Register models in the ML schema (visible in Snowsight)
- Demonstrate the training process to your audience

**Safe to run (register models, don't overwrite predictions):**
- `notebooks/pump_training_pipeline.ipynb` — Generates training data + trains models
- `notebooks/pump_classifier_training.ipynb` — Classifier training
- `notebooks/pump_rul_training.ipynb` — RUL regressor training
- `notebooks/ml_training.ipynb` — Combined training

**Regenerates predictions (use with caution):**
- `notebooks/pump_inference_pipeline.ipynb` — Regenerates demo data
- `CALL PDM_DEMO.ML.SCORE_FLEET_SP();` — Rescores fleet

Running the inference pipeline or scoring SP will produce identical results due to seeding, but is unnecessary since predictions are pre-loaded.

## Demo Walkthrough

### Scenario 1: Fleet Overview (30 seconds)
Open the app — see the interactive map with 50 assets across 10 Permian Basin stations. KPI cards show critical/warning/healthy counts. Alert sidebar lists at-risk assets sorted by urgency.

### Scenario 2: Asset Deep-Dive (1 minute)
Click on Asset 27 (critical, bearing wear). The detail page shows sensor trend charts with ML confidence and RUL threshold reference lines. Vibration and temperature are trending up — classic bearing wear signature.

### Scenario 3: AI Assistant (1 minute)
Open the chat panel and ask: "What's wrong with asset 27 and what should I do?"

The Cortex Agent orchestrates 3 tools:
- **fleet_analyst** (Cortex Analyst) — retrieves prediction data
- **manual_search** (Cortex Search) — finds relevant Grundfos CRN bearing replacement procedures
- **plan_route** — suggests bundling nearby at-risk assets into a service trip

### Scenario 4: Route Planning with Co-Maintenance (1 minute)
Go to Dispatch Service → select a technician → plan route for Asset 27. The app bundles nearby at-risk assets into an optimized multi-stop route with parts lists and travel estimates.

**Co-Maintenance**: The route planner identifies additional maintenance tasks that can be performed opportunistically while visiting each stop based on:
- Upcoming scheduled maintenance (within 7 days)
- Preventive maintenance intervals
- Nearby assets with early warning indicators

### Scenario 5: Time Travel (30 seconds)
Use the time slider to rewind to March 6 — everything was healthy. Fast-forward day by day to watch assets degrade. This shows how early detection enables proactive maintenance.

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| DATA_NOW_TS | 2026-03-17T23:55:00 | Demo "current" timestamp |
| PDM_DEMO_WH | MEDIUM | Warehouse size |
| PDM_DEMO_POOL | CPU_X64_XS | Compute pool instance |

The demo data is frozen at 2026-03-17. The Time Travel slider simulates different points in time by filtering predictions and telemetry to the selected timestamp.

## Key Files

```
setup.sh                      # Automated setup (run this)
teardown.sh                   # Complete cleanup (preserves user RSA keys)
data/                         # Pre-exported seed data (gzipped CSVs)
snowflake/setup.sql           # DDL: database, tables, views, stages, role
snowflake/score_fleet_sp.sql  # ML scoring stored procedure (reference)
snowflake/cortex_services.sql # Cortex Search + Semantic View
snowflake/cortex_agent.sql    # PDM_AGENT definition
snowflake/route_planner_sp.sql # PLAN_ROUTE stored procedure
snowflake/semantic_model.yaml # Semantic model for Cortex Analyst
snowflake/teardown.sql        # Cleanup SQL
frontend/                     # Next.js application source
frontend/Dockerfile           # Container build
frontend/pdm_service.yaml.template # Service spec template
docs/                         # Documentation
notebooks/                    # ML training notebooks (reference)
```

## Teardown

```bash
./teardown.sh
```

The teardown script removes all PDM-specific resources but **does NOT remove user RSA keys** (to avoid breaking other SPCS apps).

To manually remove RSA keys if needed:
```sql
ALTER USER <username> UNSET RSA_PUBLIC_KEY;
ALTER USER <username> UNSET RSA_PUBLIC_KEY_2;
```

## Reset Demo State

To clear scheduled work orders and reset the dispatch state:

```sql
CALL PDM_DEMO.APP.RESET_DEMO();
```

## Proving the ML and AI is Real

This section provides SQL queries and techniques to demonstrate that the demo uses real machine learning models and AI, not pre-computed static results.

### 1. View Registered ML Models in Snowflake

```sql
-- Show all registered models in the ML schema
SHOW MODELS IN SCHEMA PDM_DEMO.ML;

-- Inspect a specific model's versions and metadata
SHOW VERSIONS IN MODEL PDM_DEMO.ML.FAILURE_CLASSIFIER;
SHOW VERSIONS IN MODEL PDM_DEMO.ML.RUL_REGRESSOR;

-- View model training metadata (accuracy, features used)
SELECT * FROM PDM_DEMO.ML.MODEL_METADATA;
```

**What you'll see**: XGBoost classifier and regressor models with version history, training timestamps, and accuracy metrics.

### 2. Run Real-Time Inference with the Model

```sql
-- Call the model directly on feature data
-- This proves the model is a real callable artifact, not pre-computed
WITH sample_features AS (
    SELECT * FROM PDM_DEMO.ANALYTICS.FEATURE_STORE
    WHERE ASSET_ID = 27 AND AS_OF_TS = (SELECT MAX(AS_OF_TS) FROM PDM_DEMO.ANALYTICS.FEATURE_STORE WHERE ASSET_ID = 27)
)
SELECT 
    ASSET_ID,
    AS_OF_TS,
    PDM_DEMO.ML.FAILURE_CLASSIFIER!PREDICT(
        VIBRATION_MEAN_24H, VIBRATION_STD_24H, VIBRATION_MAX_24H, VIBRATION_TREND,
        TEMPERATURE_MEAN_24H, TEMPERATURE_STD_24H, TEMPERATURE_MAX_24H, TEMPERATURE_TREND,
        PRESSURE_MEAN_24H, PRESSURE_STD_24H, DIFF_PRESSURE_MEAN_24H,
        FLOW_RATE_MEAN_24H, RPM_MEAN_24H, POWER_DRAW_MEAN_24H,
        DAYS_SINCE_MAINTENANCE, MAINTENANCE_COUNT_90D, OPERATING_HOURS
    ) AS PREDICTED_CLASS
FROM sample_features;
```

### 3. Examine the Training Notebooks

The `notebooks/` directory contains Jupyter notebooks you can open in Snowsight:

| Notebook | Purpose | Key Proof Points |
|----------|---------|------------------|
| `pump_training_pipeline.ipynb` | Generates synthetic training data, trains XGBoost models | See confusion matrix, feature importance, accuracy scores |
| `pump_inference_pipeline.ipynb` | Scores fleet with trained models | Shows real model.predict() calls |

**To upload and run in Snowsight**:
```bash
snow notebook create pump_training_demo --database PDM_DEMO --schema ML --from notebooks/pump_training_pipeline.ipynb
```

### 4. Verify Cortex Agent is Calling Tools

The AI assistant uses Cortex Agent with real tool orchestration. To prove it:

1. **Ask a question** in the chat: "What's wrong with asset 27?"
2. **Check the response** — it will mention:
   - Tool calls to `fleet_analyst` (Cortex Analyst → semantic model → SQL)
   - Tool calls to `manual_search` (Cortex Search → vector embeddings → RAG retrieval)
   - Tool calls to `plan_route` (Python stored procedure → route optimization)

```sql
-- View the agent definition
DESCRIBE CORTEX AGENT PDM_DEMO.APP.PDM_AGENT;

-- See the tools configured
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
```

### 5. Prove Cortex Analyst Uses Semantic Model

```sql
-- View the semantic model definition
SELECT GET_DDL('VIEW', 'PDM_DEMO.APP.FLEET_SEMANTIC_VIEW');

-- Query the semantic view directly
SELECT * FROM PDM_DEMO.APP.FLEET_SEMANTIC_VIEW LIMIT 5;

-- See what Cortex Analyst generates from natural language
-- (Via API or UI chat panel)
```

### 6. Prove Cortex Search Uses Vector Embeddings

```sql
-- View the search service definition
DESCRIBE CORTEX SEARCH SERVICE PDM_DEMO.APP.MANUAL_SEARCH;

-- Search for content (real embedding similarity)
SELECT *
FROM PDM_DEMO.APP.MANUAL_SEARCH
WHERE SEARCH('bearing replacement procedure')
LIMIT 5;
```

### 7. View Feature Engineering Pipeline

```sql
-- Show how features are computed from raw telemetry
SELECT 
    ASSET_ID,
    AS_OF_TS,
    VIBRATION_MEAN_24H,
    VIBRATION_TREND,
    TEMPERATURE_MEAN_24H,
    DAYS_SINCE_MAINTENANCE
FROM PDM_DEMO.ANALYTICS.FEATURE_STORE
WHERE ASSET_ID = 27
ORDER BY AS_OF_TS DESC
LIMIT 10;

-- Compare raw telemetry to aggregated features
SELECT 
    ASSET_ID,
    DATE_TRUNC('day', TS) AS DAY,
    AVG(VIBRATION) AS VIBRATION_AVG,
    STDDEV(VIBRATION) AS VIBRATION_STD
FROM PDM_DEMO.RAW.TELEMETRY
WHERE ASSET_ID = 27
GROUP BY 1, 2
ORDER BY DAY DESC
LIMIT 10;
```

### 8. Watch Predictions Change Over Time

```sql
-- This proves predictions are time-series based, not static
SELECT 
    ASSET_ID,
    TS AS PREDICTION_TS,
    PREDICTED_CLASS,
    PREDICTED_RUL_DAYS,
    CONFIDENCE
FROM PDM_DEMO.ANALYTICS.PREDICTIONS
WHERE ASSET_ID = 27
ORDER BY TS DESC
LIMIT 20;
```

The RUL (Remaining Useful Life) decreases over time as the asset degrades — this is the ML model detecting the failure progression.

### Summary: What Makes This Real ML/AI

| Component | Technology | Proof |
|-----------|------------|-------|
| Failure Classification | XGBoost via Snowflake Model Registry | `SHOW MODELS` + callable inference |
| RUL Prediction | XGBoost Regressor | Same — see RUL values decrease over time |
| Natural Language Analytics | Cortex Analyst + Semantic View | Ask any fleet question in chat |
| Document Search | Cortex Search (arctic-embed-m-v1.5) | Vector similarity returns relevant manual sections |
| AI Orchestration | Cortex Agent (claude-4-sonnet) | Multi-tool responses with reasoning |

## Troubleshooting

See [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for detailed solutions.

### Quick Fixes

| Issue | Cause | Fix |
|-------|-------|-----|
| Service stuck in PENDING | Compute pool not active | `DESCRIBE COMPUTE POOL PDM_DEMO_POOL;` — wait for ACTIVE |
| Cortex Agent returns errors | Missing external access | Verify PDM_CORTEX_EXTERNAL_ACCESS has the correct host |
| Map tiles not loading | OSM egress blocked | Check PDM_DEMO_EXTERNAL_ACCESS integration |
| Docker push unauthorized | Registry login expired | Re-run `snow spcs image-registry login --connection <conn>` |
| RSA key conflict | Another app using same key | Use RSA_PUBLIC_KEY_2 option during setup |
