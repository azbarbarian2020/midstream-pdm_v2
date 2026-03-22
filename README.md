# Midstream Predictive Maintenance Demo

AI-powered predictive maintenance for midstream oil and gas pipeline operations in the Permian Basin. Built entirely on Snowflake with Cortex AI, SPCS, and a Next.js frontend.

## What This Demo Shows

| Capability | Snowflake Feature |
|---|---|
| Fleet health dashboard with interactive map | SPCS + Next.js |
| ML-predicted failure modes and remaining useful life (RUL) | ML Model Registry + XGBoost |
| Natural language fleet analytics | Cortex Analyst + Semantic View |
| Equipment manual and maintenance log search | Cortex Search |
| AI maintenance assistant with tool orchestration | Cortex Agent (3 tools) |
| Optimized technician route planning | Python Stored Procedure |
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

**50 assets** (pumps + compressors) across **10 stations**, 8 equipment models, 8 technicians, 6 failure modes, ~930K telemetry rows.

## Prerequisites

- **Snowflake account on AWS** (Cortex AI features require AWS)
- **ACCOUNTADMIN** role (or equivalent privileges)
- **Docker Desktop** (for building the container image)
- **Snowflake CLI** (`pip install snowflake-cli`)
- **Python 3.11+** with `snowflake-connector-python[pandas]` (for data seeding only)
- **openssl** (for RSA key generation)

### Install Python Dependencies

```bash
pip install snowflake-connector-python[pandas]
```

> **Note**: ML packages (scikit-learn, numpy, etc.) run inside Snowflake via the `SCORE_FLEET_SP()` stored procedure — no local ML installation needed.

## Service Account

User-level Programmatic Access Tokens (PATs) are being deprecated. The `setup.sh` script handles service account creation automatically:

1. **setup.sql** creates the `DEMO_PDM_ADMIN` role and all objects
2. **setup.sh** then creates `PDM_SERVICE_USER` (TYPE=SERVICE) and grants the role
3. The script pauses and prompts you to generate a PAT in Snowsight

If you prefer manual setup, create the service user **after** running `setup.sql`:

```sql
-- Run as ACCOUNTADMIN (after setup.sql has created DEMO_PDM_ADMIN)
CREATE USER IF NOT EXISTS PDM_SERVICE_USER
  TYPE = SERVICE
  DEFAULT_ROLE = DEMO_PDM_ADMIN
  DEFAULT_WAREHOUSE = PDM_DEMO_WH;

GRANT ROLE DEMO_PDM_ADMIN TO USER PDM_SERVICE_USER;
```

Then generate a PAT in Snowsight: **Admin > Users & Roles > PDM_SERVICE_USER > Authentication > Programmatic Access Tokens > Generate** (select role DEMO_PDM_ADMIN).

## Quick Start

```bash
git clone https://github.com/azbarbarian2020/midstream-pdm.git
cd midstream-pdm
./setup.sh
```

The setup script will:
1. Prompt for your Snowflake CLI connection name
2. Auto-detect your account, host, and registry
3. Create all database objects (tables, views, stages, role)
4. Generate synthetic data (~930K telemetry rows)
5. Train ML models and score the fleet (runs in Snowflake via stored procedure)
6. Create Cortex Search, Semantic View, and Agent
7. Create service user, prompt for PAT, and generate RSA key pair
8. Build and push the Docker image
9. Deploy the SPCS service
10. Print the application URL

**Estimated time**: 15-25 minutes (mostly Docker build + data generation)

## Manual Setup

If you prefer to run steps individually:

```bash
# 1. Configure your Snowflake CLI connection
snow connection add

# 2. Create infrastructure
snow sql --connection <conn> -f snowflake/setup.sql

# 3. Seed data
SNOWFLAKE_CONNECTION_NAME=<conn> python3 snowflake/seed_data.py

# 4. Generate predictions (runs in Snowflake, no local ML needed)
snow sql --connection <conn> -f snowflake/score_fleet_sp.sql
snow sql --connection <conn> -q "CALL PDM_DEMO.ML.SCORE_FLEET_SP();"

# 5. Upload semantic model and create Cortex services
snow stage copy snowflake/semantic_model.yaml @PDM_DEMO.APP.MODELS/ \
  --overwrite --database PDM_DEMO --schema APP --connection <conn>
snow sql --connection <conn> -f snowflake/cortex_services.sql

# 6. Create route planner and agent
snow sql --connection <conn> -f snowflake/route_planner_sp.sql
snow sql --connection <conn> -f snowflake/cortex_agent.sql

# 7. Create secrets (PAT + RSA key pair)
snow sql --connection <conn> -q "CREATE OR REPLACE SECRET PDM_DEMO.APP.SNOWFLAKE_PAT_SECRET
  TYPE = GENERIC_STRING SECRET_STRING = '<your-pat>';"

# Generate key pair
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out /tmp/key.p8
openssl rsa -in /tmp/key.p8 -pubout -out /tmp/key.pub
PUBLIC_KEY=$(grep -v 'BEGIN\|END' /tmp/key.pub | tr -d '\n')
snow sql --connection <conn> -q "ALTER USER <user> SET RSA_PUBLIC_KEY='$PUBLIC_KEY';"
PRIVATE_KEY=$(awk '{printf "%s\\n", $0}' /tmp/key.p8)
snow sql --connection <conn> -q "CREATE OR REPLACE SECRET PDM_DEMO.APP.SNOWFLAKE_PRIVATE_KEY_SECRET
  TYPE = GENERIC_STRING SECRET_STRING = '$PRIVATE_KEY';"

# 8. Create network rules and external access
snow sql --connection <conn> -q "CREATE OR REPLACE NETWORK RULE PDM_DEMO.APP.SNOWFLAKE_API_RULE
  TYPE = HOST_PORT MODE = EGRESS VALUE_LIST = ('<your-host>:443');"
snow sql --connection <conn> -q "CREATE OR REPLACE NETWORK RULE PDM_DEMO.APP.OSM_TILES_RULE
  TYPE = HOST_PORT MODE = EGRESS VALUE_LIST = ('tile.openstreetmap.org:443');"
snow sql --connection <conn> -q "CREATE OR REPLACE NETWORK RULE PDM_DEMO.APP.S3_RESULT_RULE
  TYPE = HOST_PORT MODE = EGRESS VALUE_LIST = ('*.s3.*.amazonaws.com:443');"
snow sql --connection <conn> -q "CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION PDM_CORTEX_EXTERNAL_ACCESS
  ALLOWED_NETWORK_RULES = (PDM_DEMO.APP.SNOWFLAKE_API_RULE, PDM_DEMO.APP.S3_RESULT_RULE)
  ALLOWED_AUTHENTICATION_SECRETS = (PDM_DEMO.APP.SNOWFLAKE_PAT_SECRET)
  ENABLED = TRUE;"
snow sql --connection <conn> -q "CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION PDM_DEMO_EXTERNAL_ACCESS
  ALLOWED_NETWORK_RULES = (PDM_DEMO.APP.OSM_TILES_RULE)
  ENABLED = TRUE;"

# 9. Build and push Docker image
snow spcs image-registry login --connection <conn>
# Get your registry URL from: SHOW IMAGE REPOSITORIES IN SCHEMA PDM_DEMO.APP;
docker buildx build --platform linux/amd64 -t <registry>/pdm_demo/app/pdm_repo/pdm_frontend:v1 -f frontend/Dockerfile frontend --load
docker push <registry>/pdm_demo/app/pdm_repo/pdm_frontend:v1

# 10. Generate service YAML from template
sed "s|__IMAGE_PATH__|<image-path>|g; s|__SNOWFLAKE_HOST__|<host>|g; s|__SNOWFLAKE_ACCOUNT__|<account>|g; s|__SNOWFLAKE_USER__|<user>|g" \
  frontend/pdm_service.yaml.template > /tmp/pdm_service.yaml
snow stage copy /tmp/pdm_service.yaml @PDM_DEMO.APP.SPECS/ --overwrite --database PDM_DEMO --schema APP --connection <conn>

# 11. Create compute pool and service
snow sql --connection <conn> -q "CREATE COMPUTE POOL IF NOT EXISTS PDM_DEMO_POOL
  MIN_NODES=1 MAX_NODES=1 INSTANCE_FAMILY=CPU_X64_XS AUTO_RESUME=TRUE AUTO_SUSPEND_SECS=3600;"
snow sql --connection <conn> -q "CREATE SERVICE IF NOT EXISTS PDM_DEMO.APP.PDM_FRONTEND
  IN COMPUTE POOL PDM_DEMO_POOL
  FROM @PDM_DEMO.APP.SPECS SPECIFICATION_FILE='pdm_service.yaml'
  EXTERNAL_ACCESS_INTEGRATIONS=(PDM_CORTEX_EXTERNAL_ACCESS, PDM_DEMO_EXTERNAL_ACCESS)
  MIN_INSTANCES=1 MAX_INSTANCES=1;"

# 12. Check status and get URL
snow sql --connection <conn> -q "SELECT SYSTEM\$GET_SERVICE_STATUS('PDM_DEMO.APP.PDM_FRONTEND');"
snow sql --connection <conn> -q "SHOW ENDPOINTS IN SERVICE PDM_DEMO.APP.PDM_FRONTEND;"
```

## Demo Walkthrough

### Scenario 1: Fleet Overview (30 seconds)
Open the app — you see the interactive map with 50 assets across 10 Permian Basin stations. KPI cards show critical/warning/healthy counts. Alert sidebar lists at-risk assets sorted by urgency.

### Scenario 2: Asset Deep-Dive (1 minute)
Click on **Asset 27** (critical, bearing wear). The detail page shows sensor trend charts with ML confidence and RUL threshold reference lines. Vibration and temperature are trending up — classic bearing wear signature.

### Scenario 3: AI Assistant (1 minute)
Open the chat panel and ask: *"What's wrong with asset 27 and what should I do?"*

The Cortex Agent orchestrates 3 tools:
- **fleet_analyst** (Cortex Analyst) — retrieves prediction data
- **manual_search** (Cortex Search) — finds relevant Grundfos CRN bearing replacement procedures
- **plan_route** — suggests bundling nearby at-risk assets into a service trip

### Scenario 4: Route Planning (30 seconds)
Go to Dispatch Service → select a technician → plan route for Asset 27. The app bundles nearby at-risk assets into an optimized multi-stop route with parts lists and travel estimates.

### Scenario 5: Time Travel (30 seconds)
Use the time slider to rewind to March 6 — everything was healthy. Fast-forward day by day to watch assets degrade. This shows how early detection enables proactive maintenance.

## Configuration

| Variable | Description | Default |
|---|---|---|
| `DATA_NOW_TS` | Demo "current" timestamp | `2026-03-13T00:00:00` |
| `PDM_DEMO_WH` | Warehouse size | MEDIUM |
| `PDM_DEMO_POOL` | Compute pool instance | CPU_X64_XS |

The demo data is frozen at **2026-03-13**. The Time Travel slider simulates different points in time by filtering predictions and telemetry to the selected timestamp.

## Key Files

```
setup.sh                      # Automated setup (run this)
teardown.sh                   # Complete cleanup
snowflake/setup.sql           # DDL: database, tables, views, stages, role
snowflake/seed_data.py        # Synthetic data generator
snowflake/score_fleet_sp.sql   # ML scoring stored procedure (runs in Snowflake)
snowflake/score_fleet.py      # Alternative: local scoring script (reference)
snowflake/cortex_services.sql # Cortex Search + Semantic View
snowflake/cortex_agent.sql    # PDM_AGENT definition
snowflake/route_planner_sp.sql # PLAN_ROUTE stored procedure
snowflake/semantic_model.yaml # Semantic model for Cortex Analyst
snowflake/teardown.sql        # Cleanup SQL
frontend/                     # Next.js application source
frontend/Dockerfile           # Container build
frontend/pdm_service.yaml.template # Service spec template
docs/pdm_architecture.drawio  # Architecture diagrams (4 tabs)
notebooks/                    # ML training notebooks (reference)
```

## Teardown

```bash
./teardown.sh
```

Or manually:
```sql
DROP SERVICE IF EXISTS PDM_DEMO.APP.PDM_FRONTEND;
DROP COMPUTE POOL IF EXISTS PDM_DEMO_POOL;
DROP AGENT IF EXISTS PDM_DEMO.APP.PDM_AGENT;
DROP CORTEX SEARCH SERVICE IF EXISTS PDM_DEMO.APP.MANUAL_SEARCH;
DROP EXTERNAL ACCESS INTEGRATION IF EXISTS PDM_CORTEX_EXTERNAL_ACCESS;
DROP EXTERNAL ACCESS INTEGRATION IF EXISTS PDM_DEMO_EXTERNAL_ACCESS;
DROP DATABASE IF EXISTS PDM_DEMO;
DROP ROLE IF EXISTS DEMO_PDM_ADMIN;
DROP WAREHOUSE IF EXISTS PDM_DEMO_WH;
```

## Troubleshooting

See [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for detailed solutions to common issues.

### Quick Fixes

| Issue | Cause | Fix |
|---|---|---|
| Service stuck in PENDING | Compute pool not active | `DESCRIBE COMPUTE POOL PDM_DEMO_POOL;` — wait for ACTIVE |
| Cortex Agent returns errors | Missing external access | Verify `PDM_CORTEX_EXTERNAL_ACCESS` has the correct host |
| Asset 27 shows wrong risk | TIMESTAMP_NTZ cast missing | Already fixed in this repo — redeploy `route_planner_sp.sql` |
| Map tiles not loading | OSM egress blocked | Check `PDM_DEMO_EXTERNAL_ACCESS` integration |
| Docker push unauthorized | Registry login expired | Re-run `snow spcs image-registry login --connection <conn>` |

## Reset Demo State

To clear scheduled work orders and reset the dispatch state:
```sql
CALL PDM_DEMO.APP.RESET_DEMO();
```
