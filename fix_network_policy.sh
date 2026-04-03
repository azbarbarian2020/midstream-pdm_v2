#!/bin/bash
# =============================================================================
# fix_network_policy.sh — Manual remediation for SPCS network policy issue
# =============================================================================
#
# USE THIS SCRIPT IF:
#   - You deployed midstream-pdm_v2 before the network policy fix (commit e6e3e83+)
#   - Your SPCS service stops working every ~12 hours
#   - Service logs show: "Incoming request with IP/Token 153.45.59.x is not
#     allowed to access Snowflake"
#
# WHAT IT DOES:
#   1. Adds SPCS CIDR (153.45.59.0/24) to the account-level network policy
#   2. Updates the security enforcement procedure so the 12h task preserves it
#   3. Creates a user-level network policy as belt-and-suspenders
#   4. Verifies both enforcement procedures pass
#
# PREREQUISITES:
#   - Snowflake CLI (snow) installed and configured
#   - Connection with ACCOUNTADMIN role
#   - Account has the security enforcement tasks deployed
#
# USAGE:
#   ./fix_network_policy.sh <connection_name> <username>
#
#   Example:
#   ./fix_network_policy.sh cleanbarbarian ADMIN
#
# =============================================================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

CONNECTION="${1:-}"
USERNAME="${2:-}"

if [ -z "$CONNECTION" ] || [ -z "$USERNAME" ]; then
    echo -e "${RED}Usage: $0 <connection_name> <username>${NC}"
    echo ""
    echo "  connection_name  Name from ~/.snowflake/connections.toml"
    echo "  username         Snowflake user running the SPCS service"
    echo ""
    echo "  Example: $0 cleanbarbarian ADMIN"
    exit 1
fi

SPCS_CIDR="153.45.59.0/24"

snow_sql() {
    snow sql --connection "$CONNECTION" "$@"
}

echo -e "${BOLD}=== SPCS Network Policy Remediation ===${NC}"
echo ""

# -------------------------------------------------------------------------
# Step 1: Detect enforcement task
# -------------------------------------------------------------------------
echo -e "${BOLD}[1/6] Checking for security enforcement task...${NC}"
TASK_EXISTS=$(snow_sql -q "SHOW TASKS LIKE 'ACCOUNT_LEVEL_NETWORK_POLICY_TASK' IN ACCOUNT;" --format json 2>/dev/null | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    for row in data:
        if 'NETWORK_POLICY' in row.get('name', '').upper():
            print(f\"{row.get('state', 'unknown')}|{row.get('schedule', 'unknown')}\")
            break
except:
    pass
" 2>/dev/null || echo "")

if [ -z "$TASK_EXISTS" ]; then
    echo -e "  ${YELLOW}No enforcement task found. You may not need this script.${NC}"
    echo -e "  ${YELLOW}If your SPCS service is still blocked, just add the SPCS CIDR${NC}"
    echo -e "  ${YELLOW}to your account network policy manually.${NC}"
    read -p "  Continue anyway? [y/N]: " CONTINUE
    if [ "$CONTINUE" != "y" ] && [ "$CONTINUE" != "Y" ]; then
        exit 0
    fi
else
    TASK_STATE=$(echo "$TASK_EXISTS" | cut -d'|' -f1)
    TASK_SCHEDULE=$(echo "$TASK_EXISTS" | cut -d'|' -f2)
    echo -e "  Task found: state=${CYAN}${TASK_STATE}${NC}, schedule=${CYAN}${TASK_SCHEDULE}${NC}"
fi

# -------------------------------------------------------------------------
# Step 2: Get current account policy and its IPs
# -------------------------------------------------------------------------
echo -e "${BOLD}[2/6] Reading current account network policy...${NC}"
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
    echo -e "  ${RED}No account-level network policy found. Nothing to fix.${NC}"
    exit 0
fi

echo -e "  Account policy: ${CYAN}${CURRENT_POLICY}${NC}"

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

if echo "$IP_LIST" | grep -q "$SPCS_CIDR"; then
    echo -e "  ${GREEN}SPCS CIDR already in account policy.${NC}"
else
    echo -e "  ${YELLOW}SPCS CIDR missing from account policy. Will add it.${NC}"
fi

# -------------------------------------------------------------------------
# Step 3: Add SPCS CIDR to account-level policy
# -------------------------------------------------------------------------
echo -e "${BOLD}[3/6] Adding SPCS CIDR to account-level policy...${NC}"

COMBINED_IPS=$(python3 -c "
ip_list = '''$IP_LIST'''
spcs = '$SPCS_CIDR'
ips = [ip.strip().strip(\"'\") for ip in ip_list.split(',') if ip.strip()]
if spcs not in ips:
    ips.append(spcs)
print(','.join([f\"'{ip}'\" for ip in ips]))
")

snow_sql -q "ALTER NETWORK POLICY ${CURRENT_POLICY} SET ALLOWED_IP_LIST = ($COMBINED_IPS);" 2>/dev/null
echo -e "  ${GREEN}✓ SPCS CIDR added to ${CURRENT_POLICY}${NC}"

# -------------------------------------------------------------------------
# Step 4: Update the enforcement procedure
# -------------------------------------------------------------------------
echo -e "${BOLD}[4/6] Updating enforcement procedure to include SPCS CIDR...${NC}"

DESIRED_IP_CSV=$(python3 -c "
ip_list = '''$IP_LIST'''
spcs = '$SPCS_CIDR'
ips = [ip.strip().strip(\"'\") for ip in ip_list.split(',') if ip.strip()]
if spcs not in ips:
    ips.append(spcs)
print(','.join(ips))
")

cat > /tmp/fix_np_proc.sql << SQLEOF
USE ROLE ACCOUNTADMIN;
USE DATABASE security_network_db;
USE SCHEMA security_network_db.policies;

CREATE OR REPLACE PROCEDURE security_network_db.policies.account_level_network_policy_proc()
  RETURNS STRING
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
AS
\$\$
    function exec(sqlText, binds) {
      binds = binds || [];
      var retval = [];
      var stmnt = snowflake.createStatement({sqlText: sqlText, binds: binds});
      var result;
      try { result = stmnt.execute(); }
      catch(err) { return err; }
      var columnCount = stmnt.getColumnCount();
      var columnNames = [];
      for (var i = 1; i <= columnCount; i++) { columnNames.push(stmnt.getColumnName(i)); }
      while(result.next()) {
        var o = {};
        for (var ci = 0; ci < columnNames.length; ci++) { o[columnNames[ci]] = result.getColumnValue(columnNames[ci]); }
        retval.push(o);
      }
      return retval;
    }
    var desiredIpList = '${DESIRED_IP_CSV}';
    var currentNpResult = exec("SHOW PARAMETERS LIKE 'NETWORK_POLICY' IN ACCOUNT");
    var allowedIpList = '';
    var isNetworkRuleApplied = false;
    if (currentNpResult.length > 0) {
        var currentNpName = currentNpResult[0]['value'];
        var describeNpResult = exec('DESCRIBE NETWORK POLICY ' + currentNpName);
        var allowedIpListRow = describeNpResult.filter(function(item) { return item.name === 'ALLOWED_IP_LIST'; });
        allowedIpList = allowedIpListRow.length > 0 ? allowedIpListRow[0]['value'] : '';
        var networkRuleListRow = describeNpResult.filter(function(item) { return item.name === 'ALLOWED_NETWORK_RULE_LIST'; });
        isNetworkRuleApplied = networkRuleListRow.length > 0;
    }
    if (currentNpResult.length > 0 && allowedIpList === desiredIpList && isNetworkRuleApplied === false) {
        return 'Allowed IP matches. No changes required.';
    } else {
        var policyName = 'ACCOUNT_VPN_POLICY_SE';
        exec('ALTER ACCOUNT UNSET NETWORK_POLICY');
        exec("CREATE OR REPLACE NETWORK POLICY " + policyName + " ALLOWED_IP_LIST = ('" + desiredIpList + "')");
        exec('ALTER ACCOUNT SET NETWORK_POLICY = ' + policyName);
        return 'Network policy updated to ' + policyName + ' with allowed IP ' + desiredIpList + '.';
    }
\$\$;
SQLEOF

snow_sql -f /tmp/fix_np_proc.sql 2>/dev/null
rm -f /tmp/fix_np_proc.sql
echo -e "  ${GREEN}✓ Enforcement procedure updated with SPCS CIDR${NC}"

# -------------------------------------------------------------------------
# Step 5: Create user-level policy
# -------------------------------------------------------------------------
echo -e "${BOLD}[5/6] Creating user-level network policy for ${USERNAME}...${NC}"

snow_sql -q "CREATE OR REPLACE NETWORK POLICY PDM_USER_NETWORK_POLICY ALLOWED_IP_LIST = ($COMBINED_IPS) COMMENT = 'User-level NP for PDM demo: VPN IPs + SPCS CIDR. Immune to account-level security task. Created by fix_network_policy.sh';" 2>/dev/null
snow_sql -q "ALTER USER ${USERNAME} SET NETWORK_POLICY = PDM_USER_NETWORK_POLICY;" 2>/dev/null
echo -e "  ${GREEN}✓ User-level policy created and assigned to ${USERNAME}${NC}"

# -------------------------------------------------------------------------
# Step 6: Verify
# -------------------------------------------------------------------------
echo -e "${BOLD}[6/6] Verifying both enforcement procedures pass...${NC}"

# Need a warehouse for procedure calls
WH=$(snow_sql -q "SHOW WAREHOUSES;" --format json 2>/dev/null | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    if data:
        print(data[0].get('name', ''))
except:
    pass
" 2>/dev/null || echo "")

if [ -z "$WH" ]; then
    echo -e "  ${YELLOW}No warehouse found — skipping procedure verification.${NC}"
    echo -e "  ${YELLOW}Run manually: CALL security_network_db.policies.account_level_network_policy_proc();${NC}"
else
    echo ""
    echo -e "  ${CYAN}Account-level procedure:${NC}"
    snow_sql --warehouse "$WH" -q "CALL security_network_db.policies.account_level_network_policy_proc();" 2>/dev/null

    echo ""
    echo -e "  ${CYAN}User-level procedure:${NC}"
    snow_sql --warehouse "$WH" -q "CALL security_network_db.policies.disable_users_with_np_mfa_policy_violation_proc();" 2>/dev/null
fi

echo ""
echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  REMEDIATION COMPLETE${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "  Account policy:      ${CURRENT_POLICY} now includes ${SPCS_CIDR}"
echo -e "  Enforcement proc:    Updated — 12h task will preserve SPCS CIDR"
echo -e "  User-level policy:   PDM_USER_NETWORK_POLICY assigned to ${USERNAME}"
echo ""
echo -e "  ${CYAN}If your SPCS service is currently broken, restart it:${NC}"
echo -e "  ${CYAN}  ALTER SERVICE PDM_DEMO.APP.PDM_FRONTEND SUSPEND;${NC}"
echo -e "  ${CYAN}  -- wait a few seconds --${NC}"
echo -e "  ${CYAN}  ALTER SERVICE PDM_DEMO.APP.PDM_FRONTEND RESUME;${NC}"
echo ""
