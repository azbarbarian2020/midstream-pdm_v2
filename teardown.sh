#!/bin/bash
set -e

echo "================================================================="
echo "  Midstream PDM Demo - Teardown"
echo "================================================================="
echo ""

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo -e "${YELLOW}WARNING: This will destroy ALL demo objects including:${NC}"
echo "  - Service: PDM_DEMO.APP.PDM_FRONTEND"
echo "  - Compute pool: PDM_DEMO_POOL"
echo "  - Database: PDM_DEMO (all tables, stages, secrets, etc.)"
echo "  - External access integrations"
echo "  - Role: DEMO_PDM_ADMIN"
echo "  - Warehouse: PDM_DEMO_WH"
echo ""
read -p "Are you sure? (type 'yes' to confirm): " CONFIRM
[ "$CONFIRM" != "yes" ] && { echo "Cancelled."; exit 0; }

echo ""
read -p "Enter Snowflake CLI connection name: " CONNECTION_NAME

echo ""
echo "Running teardown..."
snow sql --connection "$CONNECTION_NAME" -f "$SCRIPT_DIR/snowflake/teardown.sql"

echo ""
echo -e "${GREEN}Teardown complete. All demo objects have been removed.${NC}"
