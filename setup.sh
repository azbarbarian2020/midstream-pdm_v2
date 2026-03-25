#!/bin/bash
set -e

echo "================================================================="
echo "  Midstream Predictive Maintenance Demo - Setup"
echo "  Deploys the full PDM application on a Snowflake account"
echo "================================================================="
echo ""

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# -------------------------------------------------------------------------
# Prerequisites
# -------------------------------------------------------------------------
check_prereqs() {
    echo -e "${BOLD}Checking prerequisites...${NC}"
    local missing=0
    for cmd in snow docker python3 openssl; do
        if ! command -v "$cmd" &>/dev/null; then
            echo -e "  ${RED}✗ $cmd not found${NC}"
            missing=1
        else
            echo -e "  ${GREEN}✓ $cmd${NC}"
        fi
    done
    docker info &>/dev/null 2>&1 || { echo -e "  ${RED}✗ Docker daemon not running${NC}"; missing=1; }
    if [ $missing -eq 1 ]; then
        echo -e "\n${RED}Please install missing prerequisites and re-run.${NC}"
        exit 1
    fi
    echo ""
}

# -------------------------------------------------------------------------
# Connection setup
# -------------------------------------------------------------------------
setup_connection() {
    echo -e "${BOLD}Connection Setup${NC}"
    echo "You need a Snowflake CLI connection configured."
    echo "Available connections:"
    snow connection list 2>/dev/null || true
    echo ""
    read -p "Enter connection name to use: " CONNECTION_NAME
    echo ""

    echo "Testing connection..."
    snow connection test --connection "$CONNECTION_NAME" || {
        echo -e "${RED}Connection test failed. Check your connection config.${NC}"
        exit 1
    }
    echo ""

    ACCOUNT_INFO=$(snow sql --connection "$CONNECTION_NAME" -q "SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS ACCT" --format json 2>/dev/null)
    ACCOUNT_LOCATOR=$(echo "$ACCOUNT_INFO" | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['ACCT'])")
    ACCOUNT_LOWER=$(echo "$ACCOUNT_LOCATOR" | tr '[:upper:]' '[:lower:]')
    SNOWFLAKE_HOST="${ACCOUNT_LOWER}.snowflakecomputing.com"
    REGISTRY_HOST="${ACCOUNT_LOWER}.registry.snowflakecomputing.com"
    SNOWFLAKE_USER=$(snow sql --connection "$CONNECTION_NAME" -q "SELECT CURRENT_USER()" --format json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['CURRENT_USER()'])")

    echo -e "  Account:  ${CYAN}${ACCOUNT_LOCATOR}${NC}"
    echo -e "  Host:     ${CYAN}${SNOWFLAKE_HOST}${NC}"
    echo -e "  Registry: ${CYAN}${REGISTRY_HOST}${NC}"
    echo -e "  User:     ${CYAN}${SNOWFLAKE_USER}${NC}"
    echo ""

    PLATFORM=$(snow sql --connection "$CONNECTION_NAME" -q "SELECT SPLIT_PART(CURRENT_REGION(), '_', 1)" --format json 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d[0][list(d[0].keys())[0]])")
    if [[ "$PLATFORM" != *"AWS"* ]]; then
        echo -e "${YELLOW}⚠  Non-AWS region detected ($PLATFORM). Cortex AI features require AWS.${NC}"
        read -p "Continue anyway? (y/n): " CONT
        if [ "$CONT" != "y" ]; then exit 1; fi
    fi
}

snow_sql() {
    snow sql --connection "$CONNECTION_NAME" "$@"
}

# -------------------------------------------------------------------------
# Step 1: Infrastructure (DDL)
# -------------------------------------------------------------------------
create_infrastructure() {
    echo -e "${BOLD}[1/11] Creating database, schemas, tables, and stages...${NC}"
    snow_sql -f "$SCRIPT_DIR/snowflake/setup.sql"
    echo -e "${GREEN}✓ Infrastructure created${NC}\n"
}

# -------------------------------------------------------------------------
# Step 2: Seed data
# -------------------------------------------------------------------------
seed_data() {
    echo -e "${BOLD}[2/11] Loading seed data from static exports...${NC}"
    echo "  Uploading CSV files to DATA_STAGE..."

    snow stage copy "$SCRIPT_DIR/data/" @PDM_DEMO.APP.DATA_STAGE/seed/ --overwrite --database PDM_DEMO --schema APP --connection "$CONNECTION_NAME"

    echo "  Loading tables from CSV..."
    local CSV_FORMAT="TYPE = CSV COMPRESSION = GZIP FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1 FIELD_DELIMITER = ',' NULL_IF = ('')"

    snow_sql -q "TRUNCATE TABLE IF EXISTS PDM_DEMO.RAW.STATIONS;"
    snow_sql -q "COPY INTO PDM_DEMO.RAW.STATIONS FROM @PDM_DEMO.APP.DATA_STAGE/seed/raw_stations.csv.gz FILE_FORMAT = ($CSV_FORMAT) ON_ERROR = 'ABORT_STATEMENT';"

    snow_sql -q "TRUNCATE TABLE IF EXISTS PDM_DEMO.RAW.ASSETS;"
    snow_sql -q "COPY INTO PDM_DEMO.RAW.ASSETS FROM @PDM_DEMO.APP.DATA_STAGE/seed/raw_assets.csv.gz FILE_FORMAT = ($CSV_FORMAT) ON_ERROR = 'ABORT_STATEMENT';"

    snow_sql -q "TRUNCATE TABLE IF EXISTS PDM_DEMO.RAW.TECHNICIANS;"
    snow_sql -q "COPY INTO PDM_DEMO.RAW.TECHNICIANS FROM @PDM_DEMO.APP.DATA_STAGE/seed/raw_technicians.csv.gz FILE_FORMAT = ($CSV_FORMAT) ON_ERROR = 'ABORT_STATEMENT';"

    snow_sql -q "TRUNCATE TABLE IF EXISTS PDM_DEMO.RAW.PARTS_INVENTORY;"
    snow_sql -q "COPY INTO PDM_DEMO.RAW.PARTS_INVENTORY FROM @PDM_DEMO.APP.DATA_STAGE/seed/raw_parts_inventory.csv.gz FILE_FORMAT = ($CSV_FORMAT) ON_ERROR = 'ABORT_STATEMENT';"

    snow_sql -q "TRUNCATE TABLE IF EXISTS PDM_DEMO.RAW.MAINTENANCE_LOGS;"
    snow_sql -q "COPY INTO PDM_DEMO.RAW.MAINTENANCE_LOGS FROM @PDM_DEMO.APP.DATA_STAGE/seed/raw_maintenance_logs.csv.gz FILE_FORMAT = ($CSV_FORMAT) ON_ERROR = 'ABORT_STATEMENT';"

    echo "  Loading telemetry (~930K rows)..."
    snow_sql -q "TRUNCATE TABLE IF EXISTS PDM_DEMO.RAW.TELEMETRY;"
    snow_sql -q "COPY INTO PDM_DEMO.RAW.TELEMETRY FROM @PDM_DEMO.APP.DATA_STAGE/seed/raw_telemetry.csv.gz FILE_FORMAT = ($CSV_FORMAT) ON_ERROR = 'ABORT_STATEMENT';"

    snow_sql -q "TRUNCATE TABLE IF EXISTS PDM_DEMO.ANALYTICS.FEATURE_STORE;"
    snow_sql -q "COPY INTO PDM_DEMO.ANALYTICS.FEATURE_STORE FROM @PDM_DEMO.APP.DATA_STAGE/seed/analytics_feature_store.csv.gz FILE_FORMAT = ($CSV_FORMAT) ON_ERROR = 'ABORT_STATEMENT';"

    snow_sql -q "TRUNCATE TABLE IF EXISTS PDM_DEMO.APP.MANUALS;"
    snow_sql -q "COPY INTO PDM_DEMO.APP.MANUALS (DOC_ID, ASSET_TYPE, SECTION_TYPE, TITLE, CONTENT, SOURCE_URL, MODEL_NAME) FROM @PDM_DEMO.APP.DATA_STAGE/seed/app_manuals.csv.gz FILE_FORMAT = ($CSV_FORMAT) ON_ERROR = 'ABORT_STATEMENT';"

    snow_sql -q "TRUNCATE TABLE IF EXISTS PDM_DEMO.APP.WORK_ORDERS;"
    snow_sql -q "COPY INTO PDM_DEMO.APP.WORK_ORDERS FROM @PDM_DEMO.APP.DATA_STAGE/seed/app_work_orders.csv.gz FILE_FORMAT = ($CSV_FORMAT) ON_ERROR = 'ABORT_STATEMENT';"

    snow_sql -q "TRUNCATE TABLE IF EXISTS PDM_DEMO.APP.TECH_SCHEDULES;"
    snow_sql -q "COPY INTO PDM_DEMO.APP.TECH_SCHEDULES FROM @PDM_DEMO.APP.DATA_STAGE/seed/app_tech_schedules.csv.gz FILE_FORMAT = ($CSV_FORMAT) ON_ERROR = 'ABORT_STATEMENT';"

    echo -e "${GREEN}✓ Seed data loaded${NC}\n"
}

# -------------------------------------------------------------------------
# Step 3: Score fleet (create SP + run in Snowflake)
# -------------------------------------------------------------------------
score_fleet() {
    echo -e "${BOLD}[3/11] Creating scoring procedure and generating predictions...${NC}"
    echo "  Training ML models and scoring fleet (runs in Snowflake, ~2 min)..."
    snow_sql -f "$SCRIPT_DIR/snowflake/score_fleet_sp.sql"
    snow_sql -q "CALL PDM_DEMO.ML.SCORE_FLEET_SP();"
    echo -e "${GREEN}✓ Predictions generated${NC}\n"
}

# -------------------------------------------------------------------------
# Step 3b: Re-grant privileges on tables created by seed_data / score_fleet
# -------------------------------------------------------------------------
regrant_table_privileges() {
    echo -e "${BOLD}[3b/11] Re-granting table privileges to DEMO_PDM_ADMIN...${NC}"
    snow_sql -q "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA PDM_DEMO.RAW TO ROLE DEMO_PDM_ADMIN;"
    snow_sql -q "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA PDM_DEMO.ANALYTICS TO ROLE DEMO_PDM_ADMIN;"
    snow_sql -q "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA PDM_DEMO.ML TO ROLE DEMO_PDM_ADMIN;"
    snow_sql -q "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA PDM_DEMO.APP TO ROLE DEMO_PDM_ADMIN;"
    snow_sql -q "GRANT SELECT ON ALL VIEWS IN SCHEMA PDM_DEMO.ANALYTICS TO ROLE DEMO_PDM_ADMIN;"
    echo -e "${GREEN}✓ Table privileges re-granted${NC}\n"
}

# -------------------------------------------------------------------------
# Step 4: Cortex services
# -------------------------------------------------------------------------
create_cortex_services() {
    echo -e "${BOLD}[4/11] Creating Cortex Search service and Semantic View...${NC}"
    snow stage copy "$SCRIPT_DIR/snowflake/semantic_model.yaml" @PDM_DEMO.APP.MODELS/ --overwrite --database PDM_DEMO --schema APP --connection "$CONNECTION_NAME"
    snow_sql -f "$SCRIPT_DIR/snowflake/cortex_services.sql"

    echo "  Creating semantic view from YAML..."
    local tmp_sv_sql=$(mktemp /tmp/create_sv_XXXXXX.sql)
    echo "CALL SYSTEM\$CREATE_SEMANTIC_VIEW_FROM_YAML('PDM_DEMO.APP', \$\$" > "$tmp_sv_sql"
    cat "$SCRIPT_DIR/snowflake/semantic_model.yaml" >> "$tmp_sv_sql"
    echo "\$\$);" >> "$tmp_sv_sql"
    snow_sql -f "$tmp_sv_sql"
    rm -f "$tmp_sv_sql"
    snow_sql -q "GRANT SELECT ON SEMANTIC VIEW PDM_DEMO.APP.FLEET_SEMANTIC_VIEW TO ROLE DEMO_PDM_ADMIN;"

    echo -e "${GREEN}✓ Cortex services created${NC}\n"
}

# -------------------------------------------------------------------------
# Step 5: Route planner + Agent
# -------------------------------------------------------------------------
create_agent() {
    echo -e "${BOLD}[5/11] Creating stored procedures and Cortex Agent...${NC}"
    snow_sql -f "$SCRIPT_DIR/snowflake/route_planner_sp.sql"
    snow_sql -f "$SCRIPT_DIR/snowflake/cortex_agent.sql"
    echo -e "${GREEN}✓ Route planner and Agent created${NC}\n"
}

# -------------------------------------------------------------------------
# Step 6: Network rules + External access (needs host placeholder filled)
# -------------------------------------------------------------------------
create_network_access() {
    echo -e "${BOLD}[7/11] Creating network rules and external access integrations...${NC}"

    snow_sql -q "CREATE OR REPLACE NETWORK RULE PDM_DEMO.APP.SNOWFLAKE_API_RULE
        TYPE = HOST_PORT MODE = EGRESS
        VALUE_LIST = ('${SNOWFLAKE_HOST}:443');"

    snow_sql -q "CREATE OR REPLACE NETWORK RULE PDM_DEMO.APP.OSM_TILES_RULE
        TYPE = HOST_PORT MODE = EGRESS
        VALUE_LIST = ('tile.openstreetmap.org:443');"

    S3_HOST=$(snow_sql -q "SELECT PARSE_JSON(VALUE)['host']::VARCHAR AS host
        FROM TABLE(FLATTEN(INPUT => PARSE_JSON(SYSTEM\$ALLOWLIST())))
        WHERE PARSE_JSON(VALUE)['type']::VARCHAR = 'STAGE'
          AND PARSE_JSON(VALUE)['host']::VARCHAR LIKE '%s3.%amazonaws.com'
        LIMIT 1;" --format json 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d[0]['HOST'])" 2>/dev/null || echo "")

    if [ -n "$S3_HOST" ]; then
        echo "  S3 stage host: $S3_HOST"
        snow_sql -q "CREATE OR REPLACE NETWORK RULE PDM_DEMO.APP.S3_RESULT_RULE
            TYPE = HOST_PORT MODE = EGRESS
            VALUE_LIST = ('${S3_HOST}:443');"

        snow_sql -q "CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION PDM_CORTEX_EXTERNAL_ACCESS
            ALLOWED_NETWORK_RULES = (PDM_DEMO.APP.SNOWFLAKE_API_RULE, PDM_DEMO.APP.S3_RESULT_RULE)
            ALLOWED_AUTHENTICATION_SECRETS = (PDM_DEMO.APP.SNOWFLAKE_PAT_SECRET)
            ENABLED = TRUE;"
    else
        echo -e "  ${YELLOW}Could not detect S3 stage host; creating EAI without S3 rule${NC}"
        snow_sql -q "CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION PDM_CORTEX_EXTERNAL_ACCESS
            ALLOWED_NETWORK_RULES = (PDM_DEMO.APP.SNOWFLAKE_API_RULE)
            ALLOWED_AUTHENTICATION_SECRETS = (PDM_DEMO.APP.SNOWFLAKE_PAT_SECRET)
            ENABLED = TRUE;"
    fi

    snow_sql -q "CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION PDM_DEMO_EXTERNAL_ACCESS
        ALLOWED_NETWORK_RULES = (PDM_DEMO.APP.OSM_TILES_RULE)
        ENABLED = TRUE;"

    echo -e "${GREEN}✓ Network access configured${NC}\n"
}

# -------------------------------------------------------------------------
# Step 6: Secrets (PAT + Key-pair) - Uses deploying user directly
# -------------------------------------------------------------------------
create_secrets() {
    echo -e "${BOLD}[6/11] Setting up authentication...${NC}"
    echo ""
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  AUTHENTICATION SETUP${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo -e "  ${YELLOW}IMPORTANT: Cortex Agent REST APIs require a PAT from a regular${NC}"
    echo -e "  ${YELLOW}user account (not a TYPE=SERVICE user). We'll use your current${NC}"
    echo -e "  ${YELLOW}user: ${SNOWFLAKE_USER}${NC}"
    echo ""
    echo "  You need to generate a PAT (Programmatic Access Token) for this user."
    echo ""
    echo -e "${YELLOW}  Steps to generate PAT for ${SNOWFLAKE_USER}:${NC}"
    echo "    1. Open Snowsight as ACCOUNTADMIN"
    echo "    2. Go to Admin > Users & Roles > ${SNOWFLAKE_USER}"
    echo "    3. Under Authentication > Programmatic Access Tokens > Generate"
    echo "    4. Select role ACCOUNTADMIN (or DEMO_PDM_ADMIN), copy the token"
    echo ""
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo ""

    read -sp "Enter PAT (Programmatic Access Token) for ${SNOWFLAKE_USER}: " PAT_VALUE
    echo ""
    if [ -z "$PAT_VALUE" ]; then echo -e "${RED}PAT is required.${NC}"; exit 1; fi

    snow_sql -q "CREATE OR REPLACE SECRET PDM_DEMO.APP.SNOWFLAKE_PAT_SECRET
        TYPE = GENERIC_STRING
        SECRET_STRING = '${PAT_VALUE}';"
    echo -e "  ${GREEN}✓ PAT secret created${NC}"

    echo ""
    echo "Generating RSA key pair for SQL connections..."
    TEMP_DIR=$(mktemp -d)
    openssl genrsa 2048 2>/dev/null | openssl pkcs8 -topk8 -nocrypt -out "$TEMP_DIR/key.p8" 2>/dev/null
    openssl rsa -in "$TEMP_DIR/key.p8" -pubout -out "$TEMP_DIR/key.pub" 2>/dev/null
    PUBLIC_KEY=$(grep -v "BEGIN\|END" "$TEMP_DIR/key.pub" | tr -d '\n')

    snow_sql -q "ALTER USER ${SNOWFLAKE_USER} SET RSA_PUBLIC_KEY='${PUBLIC_KEY}';"
    echo -e "  ${GREEN}✓ Public key assigned to ${SNOWFLAKE_USER}${NC}"

    PRIVATE_KEY=$(awk '{printf "%s\\n", $0}' "$TEMP_DIR/key.p8")
    snow_sql -q "CREATE OR REPLACE SECRET PDM_DEMO.APP.SNOWFLAKE_PRIVATE_KEY_SECRET
        TYPE = GENERIC_STRING
        SECRET_STRING = '${PRIVATE_KEY}';"
    echo -e "  ${GREEN}✓ Private key secret created${NC}"

    rm -rf "$TEMP_DIR"

    snow_sql -q "GRANT READ ON SECRET PDM_DEMO.APP.SNOWFLAKE_PAT_SECRET TO ROLE DEMO_PDM_ADMIN;"
    snow_sql -q "GRANT READ ON SECRET PDM_DEMO.APP.SNOWFLAKE_PRIVATE_KEY_SECRET TO ROLE DEMO_PDM_ADMIN;"

    echo -e "${GREEN}✓ Secrets configured${NC}\n"
}

# -------------------------------------------------------------------------
# Step 8: Build and push Docker image
# -------------------------------------------------------------------------
build_and_push() {
    echo -e "${BOLD}[8/11] Building and pushing Docker image...${NC}"

    snow spcs image-registry login --connection "$CONNECTION_NAME"

    REPO_URL=$(snow_sql -q "SHOW IMAGE REPOSITORIES IN SCHEMA PDM_DEMO.APP;" --format json 2>/dev/null | python3 -c "
import sys, json
data = json.load(sys.stdin)
for row in data:
    if row.get('name','').upper() == 'PDM_REPO':
        print(row['repository_url'])
        break
")

    IMAGE_TAG="${REPO_URL}/pdm_frontend:v1"
    echo "  Building image: ${IMAGE_TAG}"

    docker buildx build --platform linux/amd64 \
        -t "$IMAGE_TAG" \
        -f "$SCRIPT_DIR/frontend/Dockerfile" \
        "$SCRIPT_DIR/frontend" \
        --load

    echo "  Pushing image..."
    docker push "$IMAGE_TAG"
    echo -e "${GREEN}✓ Image pushed to Snowflake registry${NC}\n"
}

# -------------------------------------------------------------------------
# Step 9: Create compute pool + service
# -------------------------------------------------------------------------
deploy_service() {
    echo -e "${BOLD}[9/11] Deploying SPCS service...${NC}"

    snow_sql -q "CREATE COMPUTE POOL IF NOT EXISTS PDM_DEMO_POOL
        MIN_NODES = 1 MAX_NODES = 1
        INSTANCE_FAMILY = CPU_X64_XS
        AUTO_RESUME = TRUE
        AUTO_SUSPEND_SECS = 3600;"

    REPO_URL=$(snow_sql -q "SHOW IMAGE REPOSITORIES IN SCHEMA PDM_DEMO.APP;" --format json 2>/dev/null | python3 -c "
import sys, json
data = json.load(sys.stdin)
for row in data:
    if row.get('name','').upper() == 'PDM_REPO':
        print(row['repository_url'])
        break
")
    IMAGE_PATH="${REPO_URL}/pdm_frontend:v1"

    sed "s|__IMAGE_PATH__|${IMAGE_PATH}|g; s|__SNOWFLAKE_HOST__|${SNOWFLAKE_HOST}|g; s|__SNOWFLAKE_ACCOUNT__|${ACCOUNT_LOCATOR}|g; s|__SNOWFLAKE_USER__|${SNOWFLAKE_USER}|g" \
        "$SCRIPT_DIR/frontend/pdm_service.yaml.template" > /tmp/pdm_service.yaml

    snow stage copy /tmp/pdm_service.yaml @PDM_DEMO.APP.SPECS/ --overwrite --database PDM_DEMO --schema APP --connection "$CONNECTION_NAME"

    snow_sql -q "CREATE SERVICE IF NOT EXISTS PDM_DEMO.APP.PDM_FRONTEND
        IN COMPUTE POOL PDM_DEMO_POOL
        FROM @PDM_DEMO.APP.SPECS
        SPECIFICATION_FILE = 'pdm_service.yaml'
        EXTERNAL_ACCESS_INTEGRATIONS = (PDM_CORTEX_EXTERNAL_ACCESS, PDM_DEMO_EXTERNAL_ACCESS)
        MIN_INSTANCES = 1
        MAX_INSTANCES = 1;"

    echo "  Waiting for service to start..."
    for i in $(seq 1 40); do
        STATUS=$(snow_sql -q "SELECT SYSTEM\$GET_SERVICE_STATUS('PDM_DEMO.APP.PDM_FRONTEND')" --format json 2>/dev/null | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    status_json = json.loads(data[0][list(data[0].keys())[0]])
    print(status_json[0].get('status', 'UNKNOWN'))
except:
    print('PENDING')
" 2>/dev/null || echo "PENDING")
        echo "  Status: $STATUS ($i/40)"
        if [ "$STATUS" = "READY" ]; then
            break
        fi
        sleep 15
    done
    echo -e "${GREEN}✓ Service deployed${NC}\n"
}

# -------------------------------------------------------------------------
# Step 10: Show results
# -------------------------------------------------------------------------
show_results() {
    echo -e "${BOLD}[10/11] Getting service endpoint...${NC}"
    ENDPOINT=$(snow_sql -q "SHOW ENDPOINTS IN SERVICE PDM_DEMO.APP.PDM_FRONTEND;" --format json 2>/dev/null | python3 -c "
import sys, json
data = json.load(sys.stdin)
for row in data:
    url = row.get('ingress_url', '')
    if url:
        print(url)
        break
" 2>/dev/null || echo "(endpoint not yet available)")

    echo ""
    echo -e "${GREEN}=================================================================${NC}"
    echo -e "${GREEN}  Setup Complete!${NC}"
    echo -e "${GREEN}=================================================================${NC}"
    echo ""
    echo -e "  App URL:      ${CYAN}https://${ENDPOINT}${NC}"
    echo -e "  Account:      ${ACCOUNT_LOCATOR}"
    echo -e "  Database:     PDM_DEMO"
    echo -e "  Service:      PDM_DEMO.APP.PDM_FRONTEND"
    echo -e "  Service User: ${SNOWFLAKE_USER}"
    echo -e "  Pool:         PDM_DEMO_POOL"
    echo ""
    echo "  Demo date is frozen at 2026-03-13. Use the Time Travel"
    echo "  slider in the app to simulate past and future states."
    echo ""
    echo "  To tear down: ./teardown.sh"
    echo -e "${GREEN}=================================================================${NC}"
}

# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------
main() {
    check_prereqs
    setup_connection
    create_infrastructure    # Step 1: DDL (creates DEMO_PDM_ADMIN role)
    seed_data                # Step 2: Seed data
    score_fleet              # Step 3: Create SP + generate predictions
    regrant_table_privileges # Step 3b: Re-grant after tables created/recreated
    create_cortex_services   # Step 4: Cortex Search + Semantic View
    create_agent             # Step 5: Route planner + Agent
    create_secrets           # Step 6: PAT + key-pair for deploying user
    create_network_access    # Step 7: Network rules + EAI
    build_and_push           # Step 8: Docker build + push
    deploy_service           # Step 9: SPCS service
    show_results             # Step 10: Show endpoint

    echo ""
    echo "To reset the demo state (clear scheduled work orders):"
    echo "  snow sql --connection $CONNECTION_NAME -q \"CALL PDM_DEMO.APP.RESET_DEMO();\""
}

main
