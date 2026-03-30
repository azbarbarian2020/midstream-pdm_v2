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
git clone https://github.com/azbarbarian2020/midstream-pdm.git
cd midstream-pdm
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
| DATA_NOW_TS | 2026-03-13T00:00:00 | Demo "current" timestamp |
| PDM_DEMO_WH | MEDIUM | Warehouse size |
| PDM_DEMO_POOL | CPU_X64_XS | Compute pool instance |

The demo data is frozen at 2026-03-13. The Time Travel slider simulates different points in time by filtering predictions and telemetry to the selected timestamp.

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
