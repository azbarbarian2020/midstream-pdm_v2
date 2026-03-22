-- ============================================================================
-- Cortex Search Service + Analyst Semantic View
-- Run AFTER seed_data.py has loaded data into tables.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE PDM_DEMO;
USE WAREHOUSE PDM_DEMO_WH;

-- ----------------------------------------------------------------------------
-- 1. Cortex Search Service over manuals + maintenance logs
-- ----------------------------------------------------------------------------
CREATE OR REPLACE CORTEX SEARCH SERVICE APP.MANUAL_SEARCH
  ON CONTENT
  ATTRIBUTES ASSET_TYPE, SECTION_TYPE, TITLE, MODEL_NAME
  WAREHOUSE = PDM_DEMO_WH
  TARGET_LAG = '1 day'
  AS (
    SELECT
        CAST(DOC_ID AS VARCHAR) AS DOC_ID,
        TITLE,
        CONTENT,
        ASSET_TYPE,
        SECTION_TYPE,
        MODEL_NAME,
        SOURCE_URL
    FROM PDM_DEMO.APP.MANUALS

    UNION ALL

    SELECT
        CONCAT('LOG-', CAST(m.LOG_ID AS VARCHAR)) AS DOC_ID,
        CONCAT(a.ASSET_TYPE, ' - ', m.MAINTENANCE_TYPE, ': Asset ', m.ASSET_ID) AS TITLE,
        m.DESCRIPTION AS CONTENT,
        a.ASSET_TYPE,
        'maintenance_log' AS SECTION_TYPE,
        a.MODEL_NAME,
        NULL AS SOURCE_URL
    FROM PDM_DEMO.RAW.MAINTENANCE_LOGS m
    JOIN PDM_DEMO.RAW.ASSETS a ON m.ASSET_ID = a.ASSET_ID
  );

-- ----------------------------------------------------------------------------
-- 2. Upload semantic model to stage (do this via CLI instead:
--    snow stage copy snowflake/semantic_model.yaml @PDM_DEMO.APP.MODELS/
-- ----------------------------------------------------------------------------
-- The semantic model YAML should be uploaded to @PDM_DEMO.APP.MODELS/semantic_model.yaml

-- ----------------------------------------------------------------------------
-- 3. Create Semantic View from the YAML
-- Note: Use Snowflake CLI or UI to create semantic view from YAML.
-- Alternative: create semantic view directly via SQL
-- ----------------------------------------------------------------------------
-- After uploading the YAML, create the semantic view:
-- This uses the stage file approach:
CREATE OR REPLACE SEMANTIC VIEW PDM_DEMO.APP.FLEET_SEMANTIC_VIEW
  FROM @PDM_DEMO.APP.MODELS/semantic_model.yaml;

-- Grant access
GRANT SELECT ON SEMANTIC VIEW PDM_DEMO.APP.FLEET_SEMANTIC_VIEW TO ROLE DEMO_PDM_ADMIN;
GRANT USAGE ON CORTEX SEARCH SERVICE PDM_DEMO.APP.MANUAL_SEARCH TO ROLE DEMO_PDM_ADMIN;

SELECT 'Cortex Search + Analyst setup complete.' AS STATUS;
