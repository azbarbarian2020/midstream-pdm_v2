-- =============================================================================
-- fix_network_policy.sql — Manual remediation for SPCS network policy issue
-- =============================================================================
--
-- USE THIS SCRIPT IF:
--   - You deployed midstream-pdm_v2 before the network policy fix (commit 034337f)
--   - Your SPCS service stops working every ~12 hours
--   - Service logs show: "Incoming request with IP/Token 153.45.59.x is not
--     allowed to access Snowflake"
--
-- WHAT IT DOES:
--   1. Adds SPCS CIDR (153.45.59.0/24) to the account-level network policy
--   2. Updates the security enforcement procedure so the 12h task preserves it
--   3. Creates a user-level network policy as belt-and-suspenders
--   4. Verifies both enforcement procedures pass
--
-- PREREQUISITES:
--   - ACCOUNTADMIN role
--   - Account has the security enforcement tasks deployed
--
-- INSTRUCTIONS:
--   1. Set the variables in Step 0 below (USERNAME, WAREHOUSE)
--   2. Run the entire script in a Snowflake worksheet or via snow sql -f
-- =============================================================================

-- =========================================================================
-- Step 0: Configuration — UPDATE THESE VALUES
-- =========================================================================
SET USERNAME    = 'ADMIN';
SET WAREHOUSE   = 'PDM_DEMO_WH';
SET SPCS_CIDR   = '153.45.59.0/24';

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE IDENTIFIER($WAREHOUSE);

-- =========================================================================
-- Step 1: Detect enforcement task
-- =========================================================================
SHOW TASKS LIKE '%NETWORK_POLICY%' IN ACCOUNT;

-- =========================================================================
-- Step 2: Read current account-level network policy
-- =========================================================================
SHOW PARAMETERS LIKE 'NETWORK_POLICY' IN ACCOUNT;

SET ACCOUNT_POLICY = (
    SELECT "value"
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    WHERE "key" = 'NETWORK_POLICY'
);

SELECT $ACCOUNT_POLICY AS current_account_policy;

-- =========================================================================
-- Step 3: Read current allowed IPs and add SPCS CIDR
-- =========================================================================
DESC NETWORK POLICY IDENTIFIER($ACCOUNT_POLICY);

SET CURRENT_IPS = (
    SELECT "value"
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    WHERE "name" = 'ALLOWED_IP_LIST'
);

SELECT $CURRENT_IPS AS current_allowed_ips;

-- Build the new IP list with SPCS CIDR appended (if not already present)
SET NEW_IPS = (
    SELECT LISTAGG(ip, ',') WITHIN GROUP (ORDER BY ip)
    FROM (
        SELECT DISTINCT TRIM(VALUE) AS ip
        FROM (
            SELECT VALUE FROM TABLE(SPLIT_TO_TABLE($CURRENT_IPS, ','))
            UNION ALL
            SELECT $SPCS_CIDR
        )
        WHERE ip != ''
    )
);

SELECT $NEW_IPS AS new_allowed_ips;

-- =========================================================================
-- Step 4: Update the account-level network policy
-- =========================================================================
-- We use a stored procedure to dynamically build the ALTER statement
-- because ALTER NETWORK POLICY doesn't accept variable substitution directly
CREATE OR REPLACE TEMPORARY PROCEDURE _fix_np_update_account_policy(
    policy_name STRING,
    ip_csv STRING
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var ips = IP_CSV.split(',').map(function(ip) {
        return "'" + ip.trim() + "'";
    }).join(',');
    var sql = "ALTER NETWORK POLICY " + POLICY_NAME + " SET ALLOWED_IP_LIST = (" + ips + ")";
    snowflake.execute({sqlText: sql});
    return 'Account policy ' + POLICY_NAME + ' updated with ' + ips.split(',').length + ' IPs (including SPCS CIDR).';
$$;

CALL _fix_np_update_account_policy($ACCOUNT_POLICY, $NEW_IPS);

-- =========================================================================
-- Step 5: Update the enforcement procedure so the 12h task preserves SPCS CIDR
-- =========================================================================
USE DATABASE security_network_db;
USE SCHEMA security_network_db.policies;

CREATE OR REPLACE PROCEDURE security_network_db.policies.account_level_network_policy_proc()
  RETURNS STRING
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
AS
$$
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

    // --- READ THE DESIRED IP LIST FROM SESSION VARIABLE ---
    var ipResult = exec("SELECT $NEW_IPS AS V");
    var desiredIpList = ipResult[0]['V'];

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
$$;

-- =========================================================================
-- Step 6: Create user-level network policy
-- =========================================================================
CREATE OR REPLACE TEMPORARY PROCEDURE _fix_np_create_user_policy(
    ip_csv STRING,
    target_user STRING
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var ips = IP_CSV.split(',').map(function(ip) {
        return "'" + ip.trim() + "'";
    }).join(',');
    var sql = "CREATE OR REPLACE NETWORK POLICY PDM_USER_NETWORK_POLICY " +
              "ALLOWED_IP_LIST = (" + ips + ") " +
              "COMMENT = 'User-level NP for PDM demo: VPN IPs + SPCS CIDR. " +
              "Immune to account-level security task. Created by fix_network_policy.sql'";
    snowflake.execute({sqlText: sql});
    snowflake.execute({sqlText: "ALTER USER " + TARGET_USER + " SET NETWORK_POLICY = PDM_USER_NETWORK_POLICY"});
    return 'User-level policy PDM_USER_NETWORK_POLICY created and assigned to ' + TARGET_USER + '.';
$$;

CALL _fix_np_create_user_policy($NEW_IPS, $USERNAME);

-- =========================================================================
-- Step 7: Verify both enforcement procedures pass
-- =========================================================================
SELECT '--- Verifying account-level procedure ---' AS step;
CALL security_network_db.policies.account_level_network_policy_proc();

SELECT '--- Verifying user-level procedure ---' AS step;
CALL security_network_db.policies.disable_users_with_np_mfa_policy_violation_proc();

-- =========================================================================
-- Step 8: Summary
-- =========================================================================
SELECT
    $ACCOUNT_POLICY AS account_policy_updated,
    'PDM_USER_NETWORK_POLICY' AS user_policy_created,
    $USERNAME AS user_policy_assigned_to,
    $SPCS_CIDR AS spcs_cidr_added,
    'If your SPCS service is currently broken, run: ALTER SERVICE PDM_DEMO.APP.PDM_FRONTEND SUSPEND; then RESUME;' AS next_step;

-- Cleanup temp procedures
DROP PROCEDURE IF EXISTS _fix_np_update_account_policy(STRING, STRING);
DROP PROCEDURE IF EXISTS _fix_np_create_user_policy(STRING, STRING);
