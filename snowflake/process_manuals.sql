-- ============================================================================
-- Process PDF Operating Manuals via PARSE_DOCUMENT
-- Extracts text from uploaded PDFs and populates APP.MANUALS table
-- Run AFTER uploading PDFs to @PDM_DEMO.APP.DATA_STAGE/manuals/
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE PDM_DEMO;
USE WAREHOUSE PDM_DEMO_WH;

-- 1. List uploaded manuals
SELECT * FROM DIRECTORY(@APP.DATA_STAGE) WHERE RELATIVE_PATH LIKE 'manuals/%';

-- 2. Parse all PDFs and extract text sections
-- First, clear existing manual entries (we'll repopulate from PDFs)
TRUNCATE TABLE APP.MANUALS;

-- 3. Process each PDF via PARSE_DOCUMENT and insert sections
-- This uses Snowflake's built-in document AI to extract structured text from PDFs
INSERT INTO APP.MANUALS (DOC_ID, ASSET_TYPE, MODEL_NAME, SECTION_TYPE, TITLE, CONTENT, SOURCE_URL)
WITH parsed AS (
    SELECT
        RELATIVE_PATH,
        PARSE_DOCUMENT(
            @APP.DATA_STAGE,
            RELATIVE_PATH,
            {'mode': 'LAYOUT'}
        ):content::VARCHAR AS doc_content,
        CASE
            WHEN RELATIVE_PATH LIKE '%Flowserve%' THEN 'PUMP'
            WHEN RELATIVE_PATH LIKE '%Grundfos%' THEN 'PUMP'
            WHEN RELATIVE_PATH LIKE '%Sulzer%' THEN 'PUMP'
            WHEN RELATIVE_PATH LIKE '%Sundyne%' THEN 'PUMP'
            ELSE 'COMPRESSOR'
        END AS asset_type,
        CASE
            WHEN RELATIVE_PATH LIKE '%Flowserve%' THEN 'Flowserve HPRT'
            WHEN RELATIVE_PATH LIKE '%Grundfos%' THEN 'Grundfos CRN'
            WHEN RELATIVE_PATH LIKE '%Sulzer%' THEN 'Sulzer MSD'
            WHEN RELATIVE_PATH LIKE '%Sundyne%' THEN 'Sundyne LMV-311'
            WHEN RELATIVE_PATH LIKE '%Ariel%' THEN 'Ariel JGK/4'
            WHEN RELATIVE_PATH LIKE '%Atlas%' THEN 'Atlas Copco GA-90'
            WHEN RELATIVE_PATH LIKE '%Dresser%' THEN 'Dresser-Rand DATUM'
            WHEN RELATIVE_PATH LIKE '%Ingersoll%' THEN 'Ingersoll Rand Centac'
        END AS model_name
    FROM DIRECTORY(@APP.DATA_STAGE)
    WHERE RELATIVE_PATH LIKE 'manuals/%.pdf'
)
SELECT
    ROW_NUMBER() OVER (ORDER BY model_name, 'full') AS DOC_ID,
    asset_type,
    model_name,
    'full_manual' AS section_type,
    model_name || ' - Operating Manual (PDF)' AS title,
    LEFT(doc_content, 16000) AS content,
    '@PDM_DEMO.APP.DATA_STAGE/' || RELATIVE_PATH AS source_url
FROM parsed
WHERE doc_content IS NOT NULL;

-- 4. Also insert the existing detailed section entries (maintenance_procedure, troubleshooting, etc.)
-- These provide more granular search results alongside the full PDF extracts
-- (Re-seed from the original seed data sections)

-- 5. Verify results
SELECT COUNT(*) AS total_manuals, COUNT(DISTINCT MODEL_NAME) AS models FROM APP.MANUALS;
SELECT ASSET_TYPE, MODEL_NAME, SECTION_TYPE, LENGTH(CONTENT) AS content_len, SOURCE_URL
FROM APP.MANUALS ORDER BY MODEL_NAME, SECTION_TYPE;

-- 6. Note: After this, re-run cortex_services.sql to refresh the MANUAL_SEARCH Cortex Search Service
-- The search service will automatically pick up new entries via incremental refresh
SELECT 'PDF manuals processed and loaded. Cortex Search will refresh automatically.' AS STATUS;
