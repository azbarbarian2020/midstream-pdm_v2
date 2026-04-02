#!/bin/bash
set -e

echo "================================================================="
echo "  Midstream PDM Demo - Teardown"
echo "================================================================="
echo ""

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo -e "${YELLOW}WARNING: This will destroy PDM demo objects including:${NC}"
echo "  - Service: PDM_DEMO.APP.PDM_FRONTEND"
echo "  - Compute pool: PDM_DEMO_POOL"
echo "  - Database: PDM_DEMO (all tables, stages, secrets, etc.)"
echo "  - External access integrations"
echo "  - Role: DEMO_PDM_ADMIN"
echo "  - Warehouse: PDM_DEMO_WH"
echo ""
echo -e "${GREEN}NOTE: User RSA keys will NOT be removed.${NC}"
echo "      Other SPCS apps using the same user will continue to work."
echo ""
read -p "Are you sure? (type 'yes' to confirm): " CONFIRM
if [ "$CONFIRM" != "yes" ]; then echo "Cancelled."; exit 0; fi

echo ""
read -p "Enter Snowflake CLI connection name: " CONNECTION_NAME

snow_sql() {
    snow sql --connection "$CONNECTION_NAME" "$@"
}

echo ""
echo "Running teardown..."

# Drop service first (depends on compute pool)
echo "  Dropping service..."
snow_sql -q "DROP SERVICE IF EXISTS PDM_DEMO.APP.PDM_FRONTEND;" 2>/dev/null || true

# Drop compute pool
echo "  Dropping compute pool..."
snow_sql -q "DROP COMPUTE POOL IF EXISTS PDM_DEMO_POOL;" 2>/dev/null || true

# Drop Cortex services
echo "  Dropping Cortex Agent..."
snow_sql -q "DROP AGENT IF EXISTS PDM_DEMO.APP.PDM_AGENT;" 2>/dev/null || true

echo "  Dropping Cortex Search service..."
snow_sql -q "DROP CORTEX SEARCH SERVICE IF EXISTS PDM_DEMO.APP.MANUAL_SEARCH;" 2>/dev/null || true

# Unset user-level network policy before dropping database (policy lives in PDM_DEMO)
echo "  Unsetting user-level network policy..."
SNOWFLAKE_USER=$(snow_sql -q "SELECT CURRENT_USER()" --format json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['CURRENT_USER()'])" 2>/dev/null || echo "")
if [ -n "$SNOWFLAKE_USER" ]; then
    USER_NP=$(snow_sql -q "SHOW PARAMETERS LIKE 'network_policy' FOR USER ${SNOWFLAKE_USER}" --format json 2>/dev/null | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    if data and data[0].get('level') == 'USER':
        print(data[0].get('value', ''))
except: pass
" 2>/dev/null || echo "")
    if echo "$USER_NP" | grep -qi "PDM_USER_NETWORK_POLICY"; then
        snow_sql -q "ALTER USER ${SNOWFLAKE_USER} UNSET NETWORK_POLICY;" 2>/dev/null || true
        echo -e "  ${GREEN}✓ User-level network policy unset${NC}"
    fi
fi

# Drop external access integrations
echo "  Dropping external access integrations..."
snow_sql -q "DROP EXTERNAL ACCESS INTEGRATION IF EXISTS PDM_CORTEX_EXTERNAL_ACCESS;" 2>/dev/null || true
snow_sql -q "DROP EXTERNAL ACCESS INTEGRATION IF EXISTS PDM_DEMO_EXTERNAL_ACCESS;" 2>/dev/null || true
snow_sql -q "DROP EXTERNAL ACCESS INTEGRATION IF EXISTS PDM_S3_EXTERNAL_ACCESS;" 2>/dev/null || true

# Drop database (includes all schemas, tables, views, stages, secrets)
echo "  Dropping database PDM_DEMO..."
snow_sql -q "DROP DATABASE IF EXISTS PDM_DEMO;" 2>/dev/null || true

# Drop warehouse
echo "  Dropping warehouse..."
snow_sql -q "DROP WAREHOUSE IF EXISTS PDM_DEMO_WH;" 2>/dev/null || true

# Drop role
echo "  Dropping role..."
snow_sql -q "DROP ROLE IF EXISTS DEMO_PDM_ADMIN;" 2>/dev/null || true

echo ""
echo -e "${GREEN}=================================================================${NC}"
echo -e "${GREEN}  Teardown Complete${NC}"
echo -e "${GREEN}=================================================================${NC}"
echo ""
echo "  All PDM demo objects have been removed."
echo ""
echo -e "  ${YELLOW}NOTE: User RSA keys were NOT removed.${NC}"
echo "  To manually remove RSA keys if needed:"
echo "    ALTER USER <username> UNSET RSA_PUBLIC_KEY;"
echo "    ALTER USER <username> UNSET RSA_PUBLIC_KEY_2;"
echo ""
echo -e "  ${YELLOW}NOTE: User-level network policy was unset if it was PDM-managed.${NC}"
echo ""
