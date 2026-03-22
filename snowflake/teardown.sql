-- ============================================================================
-- Midstream Predictive Maintenance Demo - Teardown
-- ============================================================================
-- Run with ACCOUNTADMIN to fully remove all demo objects.
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- 1. Drop service first (releases compute pool)
DROP SERVICE IF EXISTS PDM_DEMO.APP.PDM_FRONTEND;

-- 2. Drop Cortex objects
DROP AGENT IF EXISTS PDM_DEMO.APP.PDM_AGENT;
DROP CORTEX SEARCH SERVICE IF EXISTS PDM_DEMO.APP.MANUAL_SEARCH;

-- 3. Drop external access integrations
DROP EXTERNAL ACCESS INTEGRATION IF EXISTS PDM_CORTEX_EXTERNAL_ACCESS;
DROP EXTERNAL ACCESS INTEGRATION IF EXISTS PDM_DEMO_EXTERNAL_ACCESS;

-- 4. Drop compute pool
DROP COMPUTE POOL IF EXISTS PDM_DEMO_POOL;

-- 5. Drop database (removes all schemas, tables, views, stages, secrets, image repos)
DROP DATABASE IF EXISTS PDM_DEMO;

-- 6. Drop role and warehouse
DROP ROLE IF EXISTS DEMO_PDM_ADMIN;
DROP WAREHOUSE IF EXISTS PDM_DEMO_WH;

SELECT 'Teardown complete.' AS STATUS;
