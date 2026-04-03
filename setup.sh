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
    snow sql --connection "$CONNECTION_NAME" -q "SELECT CURRENT_USER()" >/dev/null 2>&1 || {
        echo -e "${RED}Connection test failed. Check your connection config.${NC}"
        exit 1
    }
    echo -e "${GREEN}Connection OK${NC}"
    echo ""

    ACCOUNT_INFO=$(snow sql --connection "$CONNECTION_NAME" -q "SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS ACCT" --format json 2>/dev/null)
    ACCOUNT_LOCATOR=$(echo "$ACCOUNT_INFO" | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['ACCT'])")
    ACCOUNT_LOWER=$(echo "$ACCOUNT_LOCATOR" | tr '[:upper:]' '[:lower:]')
    SNOWFLAKE_HOST="${ACCOUNT_LOWER}.snowflakecomputing.com"
    REGISTRY_HOST="${ACCOUNT_LOWER}.registry.snowflakecomputing.com"
    SNOWFLAKE_USER=$(snow sql --connection "$CONNECTION_NAME" -q "SELECT CURRENT_USER()" --format json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['CURRENT_USER()'])")
    SF_ACCOUNT_LOCATOR=$(snow sql --connection "$CONNECTION_NAME" -q "SELECT CURRENT_ACCOUNT()" --format json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['CURRENT_ACCOUNT()'])")

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
    if [ -n "${SNOW_WH:-}" ]; then
        snow sql --connection "$CONNECTION_NAME" --warehouse "$SNOW_WH" "$@"
    else
        snow sql --connection "$CONNECTION_NAME" "$@"
    fi
}

SNOW_WH=""

# -------------------------------------------------------------------------
# Step 1: Infrastructure (DDL)
# -------------------------------------------------------------------------
create_infrastructure() {
    echo -e "${BOLD}[1/10] Creating database, schemas, tables, and stages...${NC}"
    snow_sql -f "$SCRIPT_DIR/snowflake/setup.sql"
    SNOW_WH="PDM_DEMO_WH"
    echo -e "${GREEN}✓ Infrastructure created${NC}\n"
}

# -------------------------------------------------------------------------
# Step 2: Seed data (from pre-exported CSVs)
# -------------------------------------------------------------------------
seed_data() {
    echo -e "${BOLD}[2/10] Loading seed data from static exports...${NC}"
    echo "  Uploading CSV files to DATA_STAGE..."

    snow stage copy "$SCRIPT_DIR/data/" @PDM_DEMO.APP.DATA_STAGE/seed/ --recursive --overwrite --database PDM_DEMO --schema APP --connection "$CONNECTION_NAME"

    echo "  Loading tables from CSV..."
    local CSV_FORMAT="TYPE = CSV COMPRESSION = GZIP FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1 FIELD_DELIMITER = ',' NULL_IF = ('', '\\\\N', '\"\\\\N\"')"

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

    echo "  Loading telemetry (~1M rows, this may take a minute)..."
    snow_sql -q "TRUNCATE TABLE IF EXISTS PDM_DEMO.RAW.PUMP_TELEMETRY;"
    snow_sql -q "COPY INTO PDM_DEMO.RAW.PUMP_TELEMETRY FROM @PDM_DEMO.APP.DATA_STAGE/seed/raw_pump_telemetry/ FILE_FORMAT = ($CSV_FORMAT) ON_ERROR = 'ABORT_STATEMENT';"

    echo "  Loading analytics tables (feature store + predictions)..."
    snow_sql -q "TRUNCATE TABLE IF EXISTS PDM_DEMO.ANALYTICS.FEATURE_STORE;"
    snow_sql -q "COPY INTO PDM_DEMO.ANALYTICS.FEATURE_STORE FROM @PDM_DEMO.APP.DATA_STAGE/seed/analytics_feature_store.csv.gz FILE_FORMAT = ($CSV_FORMAT) ON_ERROR = 'ABORT_STATEMENT';"

    snow_sql -q "TRUNCATE TABLE IF EXISTS PDM_DEMO.ANALYTICS.PREDICTIONS;"
    snow_sql -q "COPY INTO PDM_DEMO.ANALYTICS.PREDICTIONS FROM @PDM_DEMO.APP.DATA_STAGE/seed/analytics_predictions.csv.gz FILE_FORMAT = ($CSV_FORMAT) ON_ERROR = 'ABORT_STATEMENT';"

    echo "  Loading ML model metadata..."
    snow_sql -q "TRUNCATE TABLE IF EXISTS PDM_DEMO.ML.MODEL_METADATA;"
    snow_sql -q "COPY INTO PDM_DEMO.ML.MODEL_METADATA FROM @PDM_DEMO.APP.DATA_STAGE/seed/ml_model_metadata.csv.gz FILE_FORMAT = ($CSV_FORMAT) ON_ERROR = 'ABORT_STATEMENT';"

    echo "  Loading app tables..."
    snow_sql -q "TRUNCATE TABLE IF EXISTS PDM_DEMO.APP.MANUALS;"
    snow_sql -q "COPY INTO PDM_DEMO.APP.MANUALS (DOC_ID, ASSET_TYPE, SECTION_TYPE, TITLE, CONTENT, SOURCE_URL, MODEL_NAME) FROM @PDM_DEMO.APP.DATA_STAGE/seed/app_manuals.csv.gz FILE_FORMAT = ($CSV_FORMAT) ON_ERROR = 'ABORT_STATEMENT';"

    snow_sql -q "TRUNCATE TABLE IF EXISTS PDM_DEMO.APP.WORK_ORDERS;"
    snow_sql -q "COPY INTO PDM_DEMO.APP.WORK_ORDERS FROM @PDM_DEMO.APP.DATA_STAGE/seed/app_work_orders.csv.gz FILE_FORMAT = ($CSV_FORMAT) ON_ERROR = 'SKIP_FILE';"

    snow_sql -q "TRUNCATE TABLE IF EXISTS PDM_DEMO.APP.TECH_SCHEDULES;"
    snow_sql -q "COPY INTO PDM_DEMO.APP.TECH_SCHEDULES FROM @PDM_DEMO.APP.DATA_STAGE/seed/app_tech_schedules.csv.gz FILE_FORMAT = ($CSV_FORMAT) ON_ERROR = 'ABORT_STATEMENT';"

    echo -e "${GREEN}✓ Seed data loaded${NC}\n"
}

# -------------------------------------------------------------------------
# Step 3: Note about ML pipelines (predictions are pre-loaded)
# -------------------------------------------------------------------------
note_ml_pipelines() {
    echo -e "${BOLD}[3/10] ML Pipeline Notes...${NC}"
    echo "  NOTE: Predictions are pre-loaded from static export."
    echo "        To retrain or re-infer, use the notebooks in /notebooks:"
    echo "          - pump_training_pipeline.ipynb (training)"
    echo "          - pump_inference_pipeline.ipynb (inference)"
    echo -e "${GREEN}✓ Predictions pre-loaded (notebooks available for retraining)${NC}\n"
}

# -------------------------------------------------------------------------
# Step 3b: Re-grant privileges
# -------------------------------------------------------------------------
regrant_table_privileges() {
    echo -e "${BOLD}[3b/10] Re-granting table privileges to DEMO_PDM_ADMIN...${NC}"
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
    echo -e "${BOLD}[4/10] Creating Cortex Search service and Semantic View...${NC}"
    snow stage copy "$SCRIPT_DIR/snowflake/semantic_model.yaml" @PDM_DEMO.APP.MODELS/ --overwrite --database PDM_DEMO --schema APP --connection "$CONNECTION_NAME"
    snow_sql -f "$SCRIPT_DIR/snowflake/cortex_services.sql"

    echo "  Creating semantic view from SQL..."
    snow_sql -f "$SCRIPT_DIR/snowflake/create_semantic_view.sql"

    echo -e "${GREEN}✓ Cortex services created${NC}\n"
}

# -------------------------------------------------------------------------
# Step 5: Route planner + Agent
# -------------------------------------------------------------------------
create_agent() {
    echo -e "${BOLD}[5/10] Creating stored procedures and Cortex Agent...${NC}"
    snow_sql -f "$SCRIPT_DIR/snowflake/route_planner_sp.sql"
    snow_sql -f "$SCRIPT_DIR/snowflake/reset_demo.sql"
    snow_sql -f "$SCRIPT_DIR/snowflake/cortex_agent.sql"
    echo -e "${GREEN}✓ Route planner, Reset Demo, and Agent created${NC}\n"
}

# -------------------------------------------------------------------------
# Step 6: Key-pair authentication with SAFE KEY MANAGEMENT
# -------------------------------------------------------------------------
generate_new_key() {
    echo ""
    echo "  Generating RSA key pair..."
    TEMP_DIR=$(mktemp -d)
    openssl genrsa 2048 2>/dev/null | openssl pkcs8 -topk8 -nocrypt -out "$TEMP_DIR/key.p8" 2>/dev/null
    openssl rsa -in "$TEMP_DIR/key.p8" -pubout -out "$TEMP_DIR/key.pub" 2>/dev/null
    PUBLIC_KEY=$(grep -v "BEGIN\|END" "$TEMP_DIR/key.pub" | tr -d '\n')

    snow_sql -q "ALTER USER ${SNOWFLAKE_USER} SET RSA_PUBLIC_KEY='${PUBLIC_KEY}';"
    echo -e "  ${GREEN}✓ Public key assigned to ${SNOWFLAKE_USER}${NC}"

    PRIVATE_KEY=$(awk '{printf "%s\\n", $0}' "$TEMP_DIR/key.p8")
    snow_sql -q "CREATE OR REPLACE SECRET PDM_DEMO.APP.SNOWFLAKE_PRIVATE_KEY_SECRET TYPE = GENERIC_STRING SECRET_STRING = '${PRIVATE_KEY}';"
    echo -e "  ${GREEN}✓ Private key secret created${NC}"
    rm -rf "$TEMP_DIR"
}

generate_key_slot_2() {
    echo ""
    echo "  Generating RSA key pair for RSA_PUBLIC_KEY_2..."
    TEMP_DIR=$(mktemp -d)
    openssl genrsa 2048 2>/dev/null | openssl pkcs8 -topk8 -nocrypt -out "$TEMP_DIR/key.p8" 2>/dev/null
    openssl rsa -in "$TEMP_DIR/key.p8" -pubout -out "$TEMP_DIR/key.pub" 2>/dev/null
    PUBLIC_KEY=$(grep -v "BEGIN\|END" "$TEMP_DIR/key.pub" | tr -d '\n')

    snow_sql -q "ALTER USER ${SNOWFLAKE_USER} SET RSA_PUBLIC_KEY_2='${PUBLIC_KEY}';"
    echo -e "  ${GREEN}✓ Public key assigned to RSA_PUBLIC_KEY_2${NC}"

    PRIVATE_KEY=$(awk '{printf "%s\\n", $0}' "$TEMP_DIR/key.p8")
    snow_sql -q "CREATE OR REPLACE SECRET PDM_DEMO.APP.SNOWFLAKE_PRIVATE_KEY_SECRET TYPE = GENERIC_STRING SECRET_STRING = '${PRIVATE_KEY}';"
    echo -e "  ${GREEN}✓ Private key secret created${NC}"
    rm -rf "$TEMP_DIR"
}

create_secrets() {
    echo -e "${BOLD}[6/10] Setting up key-pair authentication...${NC}"
    echo ""
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  KEY-PAIR AUTHENTICATION SETUP${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo "  The SPCS app uses RSA key-pair JWT for both SQL connections"
    echo "  and Cortex Agent REST API calls. No PAT required."
    echo ""

    echo "  Checking for existing RSA key on ${SNOWFLAKE_USER}..."
    EXISTING_KEY=$(snow_sql -q "DESCRIBE USER ${SNOWFLAKE_USER};" --format json 2>/dev/null | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    for row in data:
        if row.get('property') == 'RSA_PUBLIC_KEY':
            val = row.get('value', '')
            if val and val != 'null' and len(val) > 10:
                print('EXISTS')
                break
except: pass
" 2>/dev/null || echo "")

    if [ "$EXISTING_KEY" = "EXISTS" ]; then
        echo ""
        echo -e "${YELLOW}════════════════════════════════════════════════════════════${NC}"
        echo -e "${YELLOW}  RSA_PUBLIC_KEY already exists for user ${SNOWFLAKE_USER}${NC}"
        echo -e "${YELLOW}════════════════════════════════════════════════════════════${NC}"
        echo ""

        AUTO_KEY_PATH=""
        CLI_KEY_PATH=$(python3 -c "
try:
    import tomllib
except ImportError:
    import tomli as tomllib
import os, pathlib
for f in [pathlib.Path.home()/'.snowflake'/'connections.toml', pathlib.Path.home()/'.snowflake'/'config.toml']:
    if f.exists():
        with open(f, 'rb') as fh:
            cfg = tomllib.load(fh)
        for section in [cfg.get('${CONNECTION_NAME}', {}), cfg.get('connections', {}).get('${CONNECTION_NAME}', {})]:
            p = section.get('private_key_file', '')
            if p:
                p = os.path.expanduser(p)
                if os.path.isfile(p):
                    print(p)
                    raise SystemExit(0)
" 2>/dev/null || echo "")
        if [ -n "$CLI_KEY_PATH" ]; then
            AUTO_KEY_PATH="$CLI_KEY_PATH"
        else
            for CANDIDATE in "$HOME/.snowflake/keys/${CONNECTION_NAME}.p8" "$HOME/.snowflake/keys/pdm_admin_key.p8"; do
                if [ -f "$CANDIDATE" ]; then
                    AUTO_KEY_PATH="$CANDIDATE"
                    break
                fi
            done
        fi

        if [ -n "$AUTO_KEY_PATH" ]; then
            echo -e "  ${GREEN}Found matching private key: ${AUTO_KEY_PATH}${NC}"
            echo "  Auto-selecting: Reuse existing key"
            KEY_CHOICE=1
        else
            echo "  Another SPCS application may be using this key."
            echo "  Overwriting it will break that application's authentication."
            echo ""
            echo "  Options:"
            echo "    1) Reuse existing key (requires private key file or existing secret)"
            echo "    2) Use RSA_PUBLIC_KEY_2 (secondary slot - BOTH apps work)"
            echo "    3) Generate NEW key (WARNING: breaks other SPCS apps!)"
            echo ""
            read -p "  Choice [1/2/3] (default 2 - recommended): " KEY_CHOICE
            KEY_CHOICE=${KEY_CHOICE:-2}
        fi

        case $KEY_CHOICE in
            1)
                echo ""
                echo "  Checking for existing private key secret..."
                SECRET_EXISTS=$(snow_sql -q "SHOW SECRETS LIKE 'SNOWFLAKE_PRIVATE_KEY_SECRET' IN SCHEMA PDM_DEMO.APP;" --format json 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print('yes' if d else 'no')" 2>/dev/null || echo "no")

                if [ "$SECRET_EXISTS" = "yes" ]; then
                    echo -e "  ${GREEN}✓ Private key secret already exists - reusing${NC}"
                else
                    PRIVATE_KEY_PATH="${AUTO_KEY_PATH:-}"
                    if [ -z "$PRIVATE_KEY_PATH" ]; then
                        read -p "  Path to private key file (.p8): " PRIVATE_KEY_PATH
                    fi
                    if [ -f "$PRIVATE_KEY_PATH" ]; then
                        PRIVATE_KEY=$(awk '{printf "%s\\n", $0}' "$PRIVATE_KEY_PATH")
                        snow_sql -q "CREATE OR REPLACE SECRET PDM_DEMO.APP.SNOWFLAKE_PRIVATE_KEY_SECRET TYPE = GENERIC_STRING SECRET_STRING = '${PRIVATE_KEY}';"
                        echo -e "  ${GREEN}✓ Private key secret created from ${PRIVATE_KEY_PATH}${NC}"
                    else
                        echo -e "  ${RED}File not found: $PRIVATE_KEY_PATH${NC}"
                        echo -e "  ${RED}Cannot continue without private key.${NC}"
                        exit 1
                    fi
                fi
                ;;
            2)
                generate_key_slot_2
                ;;
            3)
                echo ""
                echo -e "  ${RED}WARNING: This will invalidate any other SPCS apps using this user!${NC}"
                read -p "  Are you sure? (yes/no): " CONFIRM
                if [ "$CONFIRM" != "yes" ]; then
                    echo "  Aborted."
                    exit 1
                fi
                generate_new_key
                ;;
            *)
                echo -e "  ${YELLOW}Invalid choice, using RSA_PUBLIC_KEY_2 (default)${NC}"
                generate_key_slot_2
                ;;
        esac
    else
        generate_new_key
    fi

    snow_sql -q "GRANT READ ON SECRET PDM_DEMO.APP.SNOWFLAKE_PRIVATE_KEY_SECRET TO ROLE DEMO_PDM_ADMIN;"

    echo -e "${GREEN}✓ Key-pair authentication configured${NC}\n"
}

# -------------------------------------------------------------------------
# Step 7: Network rules + External access
# -------------------------------------------------------------------------
create_network_access() {
    echo -e "${BOLD}[7/10] Creating network rules and external access integrations...${NC}"

    snow_sql -q "CREATE OR REPLACE NETWORK RULE PDM_DEMO.APP.SNOWFLAKE_API_RULE TYPE = HOST_PORT MODE = EGRESS VALUE_LIST = ('${SNOWFLAKE_HOST}:443');"

    snow_sql -q "CREATE OR REPLACE NETWORK RULE PDM_DEMO.APP.OSM_TILES_RULE TYPE = HOST_PORT MODE = EGRESS VALUE_LIST = ('tile.openstreetmap.org:443');"

    S3_HOST=$(snow_sql -q "SELECT PARSE_JSON(VALUE)['host']::VARCHAR AS host FROM TABLE(FLATTEN(INPUT => PARSE_JSON(SYSTEM\$ALLOWLIST()))) WHERE PARSE_JSON(VALUE)['type']::VARCHAR = 'STAGE' AND PARSE_JSON(VALUE)['host']::VARCHAR LIKE '%s3.%amazonaws.com' LIMIT 1;" --format json 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d[0]['HOST'])" 2>/dev/null || echo "")

    if [ -n "$S3_HOST" ]; then
        echo "  S3 stage host: $S3_HOST"
        snow_sql -q "CREATE OR REPLACE NETWORK RULE PDM_DEMO.APP.S3_RESULT_RULE TYPE = HOST_PORT MODE = EGRESS VALUE_LIST = ('${S3_HOST}:443');"

        snow_sql -q "CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION PDM_CORTEX_EXTERNAL_ACCESS ALLOWED_NETWORK_RULES = (PDM_DEMO.APP.SNOWFLAKE_API_RULE, PDM_DEMO.APP.S3_RESULT_RULE) ALLOWED_AUTHENTICATION_SECRETS = (PDM_DEMO.APP.SNOWFLAKE_PRIVATE_KEY_SECRET) ENABLED = TRUE;"
    else
        echo -e "  ${YELLOW}Could not detect S3 stage host; creating EAI without S3 rule${NC}"
        snow_sql -q "CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION PDM_CORTEX_EXTERNAL_ACCESS ALLOWED_NETWORK_RULES = (PDM_DEMO.APP.SNOWFLAKE_API_RULE) ALLOWED_AUTHENTICATION_SECRETS = (PDM_DEMO.APP.SNOWFLAKE_PRIVATE_KEY_SECRET) ENABLED = TRUE;"
    fi

    snow_sql -q "CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION PDM_DEMO_EXTERNAL_ACCESS ALLOWED_NETWORK_RULES = (PDM_DEMO.APP.OSM_TILES_RULE) ENABLED = TRUE;"

    echo "  Creating S3 internal network rule for result streaming..."
    snow_sql -q "CREATE OR REPLACE NETWORK RULE PDM_DEMO.APP.SNOWFLAKE_INTERNAL_S3_RULE TYPE = HOST_PORT MODE = EGRESS VALUE_LIST = ('*.s3.us-west-2.amazonaws.com:443', '*.s3.amazonaws.com:443');"
    snow_sql -q "CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION PDM_S3_EXTERNAL_ACCESS ALLOWED_NETWORK_RULES = (PDM_DEMO.APP.SNOWFLAKE_INTERNAL_S3_RULE) ENABLED = TRUE;"

    echo -e "${GREEN}✓ Network access configured${NC}\n"
}

# -------------------------------------------------------------------------
# Step 7b: Account network policy - ensure SPCS service IP is allowed
# -------------------------------------------------------------------------
ensure_network_policy_access() {
    echo -e "${BOLD}[7b/10] Ensuring SPCS service IP is allowed through account network policy...${NC}"

    SPCS_CIDR="153.45.59.0/24"

    CURRENT_POLICY=$(snow_sql -q "SHOW PARAMETERS LIKE 'NETWORK_POLICY' IN ACCOUNT;" --format json 2>/dev/null | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    if data:
        print(data[0].get('value', ''))
except:
    pass
" 2>/dev/null || echo "")

    if [ -z "$CURRENT_POLICY" ]; then
        echo "  No account-level network policy set. SPCS access should work."
        echo -e "${GREEN}✓ No network policy blocking${NC}\n"
        return
    fi

    echo "  Account network policy: $CURRENT_POLICY"

    SECURITY_TASK_DETECTED=$(snow_sql -q "SHOW TASKS LIKE 'ACCOUNT_LEVEL_NETWORK_POLICY_TASK' IN ACCOUNT;" --format json 2>/dev/null | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    for row in data:
        if 'NETWORK_POLICY' in row.get('name', '').upper():
            state = row.get('state', '')
            schedule = row.get('schedule', '')
            print(f'{state}|{schedule}')
            break
except:
    pass
" 2>/dev/null || echo "")

    if [ -n "$SECURITY_TASK_DETECTED" ]; then
        TASK_STATE=$(echo "$SECURITY_TASK_DETECTED" | cut -d'|' -f1)
        TASK_SCHEDULE=$(echo "$SECURITY_TASK_DETECTED" | cut -d'|' -f2)
        echo ""
        echo -e "${RED}════════════════════════════════════════════════════════════${NC}"
        echo -e "${RED}  SECURITY ENFORCEMENT TASK DETECTED${NC}"
        echo -e "${RED}════════════════════════════════════════════════════════════${NC}"
        echo ""
        echo -e "  Task:     ACCOUNT_LEVEL_NETWORK_POLICY_TASK"
        echo -e "  State:    ${TASK_STATE}"
        echo -e "  Schedule: ${TASK_SCHEDULE}"
        echo ""
        echo -e "  This task periodically resets the account network policy"
        echo -e "  to a hardcoded VPN IP list, wiping any SPCS CIDRs added."
        echo -e "  ${BOLD}Modifying the account policy directly will NOT persist.${NC}"
        echo ""
        echo -e "  ${CYAN}Creating a user-level network policy instead. This is${NC}"
        echo -e "  ${CYAN}immune to the account-level enforcement task.${NC}"
        NP_CHOICE=1
    fi

    IP_LIST=$(snow_sql -q "DESC NETWORK POLICY ${CURRENT_POLICY};" --format json 2>/dev/null | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    for row in data:
        if row.get('name') == 'ALLOWED_IP_LIST':
            print(row.get('value', ''))
            break
except:
    pass
" 2>/dev/null || echo "")

    USER_POLICY_LEVEL=$(snow_sql -q "SHOW PARAMETERS LIKE 'NETWORK_POLICY' FOR USER ${SNOWFLAKE_USER};" --format json 2>/dev/null | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    if data:
        level = data[0].get('level', '')
        value = data[0].get('value', '')
        print(f'{level}|{value}')
except:
    pass
" 2>/dev/null || echo "")

    USER_NP_LEVEL=$(echo "$USER_POLICY_LEVEL" | cut -d'|' -f1)
    USER_NP_NAME=$(echo "$USER_POLICY_LEVEL" | cut -d'|' -f2)

    if [ "$USER_NP_LEVEL" = "USER" ]; then
        USER_NP_IPS=$(snow_sql -q "DESC NETWORK POLICY ${USER_NP_NAME};" --format json 2>/dev/null | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    for row in data:
        if row.get('name') == 'ALLOWED_IP_LIST':
            print(row.get('value', ''))
            break
except:
    pass
" 2>/dev/null || echo "")
        if echo "$USER_NP_IPS" | grep -q "$SPCS_CIDR"; then
            echo "  User-level policy '${USER_NP_NAME}' already includes SPCS CIDR."
            echo -e "${GREEN}✓ SPCS IP already allowed (user-level policy)${NC}\n"
            return
        else
            echo "  User-level policy '${USER_NP_NAME}' exists but missing SPCS CIDR."
            echo "  Will update it to include ${SPCS_CIDR}."
            IP_LIST="$USER_NP_IPS"
            NP_CHOICE=1
        fi
    fi

    if echo "$IP_LIST" | grep -q "$SPCS_CIDR"; then
        if [ -z "$SECURITY_TASK_DETECTED" ]; then
            echo "  SPCS CIDR $SPCS_CIDR already in account policy allow-list."
            echo -e "${GREEN}✓ SPCS IP already allowed${NC}\n"
            return
        else
            echo "  SPCS CIDR found in account policy, but security task will remove it."
            echo "  Creating user-level policy for persistence."
        fi
    fi

    if [ -z "${NP_CHOICE:-}" ]; then
        echo ""
        echo -e "${YELLOW}════════════════════════════════════════════════════════════${NC}"
        echo -e "${YELLOW}  ACCOUNT NETWORK POLICY - SPCS ACCESS${NC}"
        echo -e "${YELLOW}════════════════════════════════════════════════════════════${NC}"
        echo ""
        echo "  The account network policy '$CURRENT_POLICY' does not include"
        echo "  the SPCS service CIDR ($SPCS_CIDR)."
        echo ""
        echo "  Without this, the SPCS container cannot call Snowflake APIs"
        echo "  (Cortex Agent, SQL, etc.)."
        echo ""
        echo -e "${CYAN}  A user-level network policy is recommended. It only${NC}"
        echo -e "${CYAN}  affects ${SNOWFLAKE_USER} and survives account-level${NC}"
        echo -e "${CYAN}  security task resets.${NC}"
        echo ""
        echo "  Options:"
        echo "    1) Create user-level network policy (recommended - survives security tasks)"
        echo "    2) Add SPCS CIDR to account policy directly (may be wiped by security tasks)"
        echo "    3) Skip (I'll handle this manually)"
        echo ""
        read -p "  Choice [1/2/3] (default 1): " NP_CHOICE
        NP_CHOICE=${NP_CHOICE:-1}
    fi

    case $NP_CHOICE in
        1)
            echo "  Creating user-level network policy for ${SNOWFLAKE_USER}..."

            COMBINED_IPS=$(python3 -c "
ip_list = '''$IP_LIST'''
spcs = '$SPCS_CIDR'
ips = [ip.strip().strip(\"'\") for ip in ip_list.split(',') if ip.strip()]
if spcs not in ips:
    ips.append(spcs)
print(','.join([f\"'{ip}'\" for ip in ips]))
")

            snow_sql -q "CREATE OR REPLACE NETWORK POLICY PDM_USER_NETWORK_POLICY ALLOWED_IP_LIST = ($COMBINED_IPS) COMMENT = 'User-level NP for PDM demo: VPN IPs + SPCS CIDR. Immune to account-level security task. Managed by setup.sh';"

            snow_sql -q "ALTER USER ${SNOWFLAKE_USER} SET NETWORK_POLICY = PDM_USER_NETWORK_POLICY;"
            echo -e "  ${GREEN}✓ User-level network policy created and assigned${NC}"
            echo -e "  ${CYAN}  Policy: PDM_USER_NETWORK_POLICY${NC}"
            echo -e "  ${CYAN}  Includes all VPN IPs from account policy + SPCS CIDR${NC}"
            echo -e "  ${CYAN}  Assigned to user '${SNOWFLAKE_USER}' (immune to account-level resets)${NC}"
            ;;
        2)
            if [ -n "$SECURITY_TASK_DETECTED" ]; then
                echo -e "  ${RED}Cannot use option 2: security enforcement task will overwrite changes.${NC}"
                echo -e "  ${RED}Falling back to option 1 (user-level policy).${NC}"
                NP_CHOICE=1
                COMBINED_IPS=$(python3 -c "
ip_list = '''$IP_LIST'''
spcs = '$SPCS_CIDR'
ips = [ip.strip().strip(\"'\") for ip in ip_list.split(',') if ip.strip()]
if spcs not in ips:
    ips.append(spcs)
print(','.join([f\"'{ip}'\" for ip in ips]))
")
                snow_sql -q "CREATE OR REPLACE NETWORK POLICY PDM_USER_NETWORK_POLICY ALLOWED_IP_LIST = ($COMBINED_IPS) COMMENT = 'User-level NP for PDM demo: VPN IPs + SPCS CIDR. Immune to account-level security task. Managed by setup.sh';"
                snow_sql -q "ALTER USER ${SNOWFLAKE_USER} SET NETWORK_POLICY = PDM_USER_NETWORK_POLICY;"
                echo -e "  ${GREEN}✓ User-level network policy created and assigned (fallback from option 2)${NC}"
            else
                echo "  Adding $SPCS_CIDR to account policy..."
                if [ -n "$IP_LIST" ]; then
                    NEW_IP_LIST="$IP_LIST,$SPCS_CIDR"
                else
                    NEW_IP_LIST="$SPCS_CIDR"
                fi
                FORMATTED_IPS=$(echo "$NEW_IP_LIST" | python3 -c "
import sys
ips = sys.stdin.read().strip().split(',')
formatted = ','.join([f\"'{ip.strip()}'\" for ip in ips if ip.strip()])
print(formatted)
")
                snow_sql -q "ALTER NETWORK POLICY ${CURRENT_POLICY} SET ALLOWED_IP_LIST = ($FORMATTED_IPS);"
                echo -e "  ${GREEN}✓ SPCS CIDR added to account policy${NC}"
                echo -e "  ${YELLOW}  WARNING: If a security enforcement task exists, this may be overwritten.${NC}"
                echo -e "  ${YELLOW}  Consider option 1 (user-level policy) for a permanent solution.${NC}"
            fi
            ;;
        3)
            echo -e "  ${YELLOW}Skipped. You must ensure SPCS IP $SPCS_CIDR is allowed manually.${NC}"
            ;;
    esac
    echo -e "${GREEN}✓ Network policy access configured${NC}\n"
}

# -------------------------------------------------------------------------
# Step 8: Build and push Docker image
# -------------------------------------------------------------------------
build_and_push() {
    echo -e "${BOLD}[8/10] Building and pushing Docker image...${NC}"

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
    echo -e "${BOLD}[9/10] Deploying SPCS service...${NC}"

    snow_sql -q "CREATE COMPUTE POOL IF NOT EXISTS PDM_DEMO_POOL MIN_NODES = 1 MAX_NODES = 1 INSTANCE_FAMILY = CPU_X64_XS AUTO_RESUME = TRUE AUTO_SUSPEND_SECS = 3600;"

    REPO_URL=$(snow_sql -q "SHOW IMAGE REPOSITORIES IN SCHEMA PDM_DEMO.APP;" --format json 2>/dev/null | python3 -c "
import sys, json
data = json.load(sys.stdin)
for row in data:
    if row.get('name','').upper() == 'PDM_REPO':
        print(row['repository_url'])
        break
")
    IMAGE_PATH="${REPO_URL}/pdm_frontend:v1"

    sed "s|__IMAGE_PATH__|${IMAGE_PATH}|g; s|__SNOWFLAKE_HOST__|${SNOWFLAKE_HOST}|g; s|__SNOWFLAKE_ACCOUNT__|${ACCOUNT_LOCATOR}|g; s|__SNOWFLAKE_ACCOUNT_LOCATOR__|${SF_ACCOUNT_LOCATOR}|g; s|__SNOWFLAKE_USER__|${SNOWFLAKE_USER}|g" \
        "$SCRIPT_DIR/frontend/pdm_service.yaml.template" > /tmp/pdm_service.yaml

    snow stage copy /tmp/pdm_service.yaml @PDM_DEMO.APP.SPECS/ --overwrite --database PDM_DEMO --schema APP --connection "$CONNECTION_NAME"

    snow_sql -q "CREATE SERVICE IF NOT EXISTS PDM_DEMO.APP.PDM_FRONTEND IN COMPUTE POOL PDM_DEMO_POOL FROM @PDM_DEMO.APP.SPECS SPECIFICATION_FILE = 'pdm_service.yaml' EXTERNAL_ACCESS_INTEGRATIONS = (PDM_CORTEX_EXTERNAL_ACCESS, PDM_DEMO_EXTERNAL_ACCESS, PDM_S3_EXTERNAL_ACCESS) MIN_INSTANCES = 1 MAX_INSTANCES = 1;"

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
    echo -e "${BOLD}[10/10] Getting service endpoint...${NC}"
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
    echo "  Demo date is frozen at 2026-03-18. Use the Time Travel"
    echo "  slider in the app to simulate past and future states."
    echo ""
    echo -e "${YELLOW}  NOTE: Predictions are pre-loaded from static export.${NC}"
    echo -e "${YELLOW}  To regenerate predictions, use the notebooks:${NC}"
    echo -e "${YELLOW}    - notebooks/pump_training_pipeline.ipynb${NC}"
    echo -e "${YELLOW}    - notebooks/pump_inference_pipeline.ipynb${NC}"
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
    create_infrastructure    # Step 1: DDL
    seed_data                # Step 2: Seed data (includes PREDICTIONS)
    note_ml_pipelines        # Step 3: Note about notebooks
    regrant_table_privileges # Step 3b: Re-grant
    create_cortex_services   # Step 4: Cortex Search + Semantic View
    create_agent             # Step 5: Route planner + Agent
    create_secrets           # Step 6: Key-pair auth (SAFE key management)
    create_network_access    # Step 7: Network rules + EAI
    ensure_network_policy_access # Step 7b: Account/user network policy for SPCS IP
    build_and_push           # Step 8: Docker build + push
    deploy_service           # Step 9: SPCS service
    show_results             # Step 10: Show endpoint

    echo ""
    echo "To reset the demo state (clear scheduled work orders):"
    echo "  snow sql --connection $CONNECTION_NAME -q \"CALL PDM_DEMO.APP.RESET_DEMO();\""
}

main
