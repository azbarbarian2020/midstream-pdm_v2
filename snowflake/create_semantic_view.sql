-- ============================================================================
-- Create Semantic View for Fleet Analytics
-- Translates semantic_model.yaml into CREATE SEMANTIC VIEW DDL
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE PDM_DEMO;
USE WAREHOUSE PDM_DEMO_WH;

CREATE OR REPLACE SEMANTIC VIEW PDM_DEMO.APP.FLEET_SEMANTIC_VIEW

  TABLES (
    FLEET_KPI_VIEW AS PDM_DEMO.ANALYTICS.FLEET_KPI_VIEW
      PRIMARY KEY (ASSET_ID)
      COMMENT = 'Primary KPI view joining assets, stations, latest predictions, and maintenance summary.',

    TELEMETRY AS PDM_DEMO.RAW.TELEMETRY
      COMMENT = 'Raw sensor telemetry at hourly intervals.',

    MAINTENANCE_LOGS AS PDM_DEMO.RAW.MAINTENANCE_LOGS
      PRIMARY KEY (LOG_ID)
      COMMENT = 'Historical maintenance records.'
  )

  RELATIONSHIPS (
    TELEMETRY_TO_FLEET AS
      TELEMETRY(ASSET_ID) REFERENCES FLEET_KPI_VIEW,

    MAINTENANCE_TO_FLEET AS
      MAINTENANCE_LOGS(ASSET_ID) REFERENCES FLEET_KPI_VIEW
  )

  FACTS (
    FLEET_KPI_VIEW.RATED_CAPACITY AS FLEET_KPI_VIEW.RATED_CAPACITY
      COMMENT = 'Rated capacity of the asset in design units.',
    FLEET_KPI_VIEW.STATION_LAT AS FLEET_KPI_VIEW.STATION_LAT
      COMMENT = 'Station latitude coordinate.',
    FLEET_KPI_VIEW.STATION_LON AS FLEET_KPI_VIEW.STATION_LON
      COMMENT = 'Station longitude coordinate.',
    FLEET_KPI_VIEW.PREDICTED_RUL_DAYS AS FLEET_KPI_VIEW.PREDICTED_RUL_DAYS
      WITH SYNONYMS = ('RUL', 'remaining useful life', 'days to failure')
      COMMENT = 'Predicted remaining useful life in days before failure.',
    FLEET_KPI_VIEW.DAYS_SINCE_MAINTENANCE AS FLEET_KPI_VIEW.DAYS_SINCE_MAINTENANCE
      COMMENT = 'Number of days since the last maintenance event.',
    FLEET_KPI_VIEW.TOTAL_MAINTENANCE_COUNT AS FLEET_KPI_VIEW.TOTAL_MAINTENANCE_COUNT
      COMMENT = 'Total number of historical maintenance events for this asset.',

    TELEMETRY.VIBRATION AS TELEMETRY.VIBRATION
      COMMENT = 'Vibration reading in mm/s RMS.',
    TELEMETRY.TEMPERATURE AS TELEMETRY.TEMPERATURE
      COMMENT = 'Temperature reading in degrees Fahrenheit.',
    TELEMETRY.PRESSURE AS TELEMETRY.PRESSURE
      COMMENT = 'Pressure reading in PSI.',
    TELEMETRY.FLOW_RATE AS TELEMETRY.FLOW_RATE
      COMMENT = 'Flow rate.',
    TELEMETRY.RPM AS TELEMETRY.RPM
      COMMENT = 'Rotational speed in revolutions per minute.',
    TELEMETRY.POWER_DRAW AS TELEMETRY.POWER_DRAW
      COMMENT = 'Power consumption.',
    TELEMETRY.DIFFERENTIAL_PRESSURE AS TELEMETRY.DIFFERENTIAL_PRESSURE
      COMMENT = 'Differential pressure (pumps only).',
    TELEMETRY.SUCTION_PRESSURE AS TELEMETRY.SUCTION_PRESSURE
      COMMENT = 'Suction pressure (pumps only).',
    TELEMETRY.SEAL_TEMPERATURE AS TELEMETRY.SEAL_TEMPERATURE
      COMMENT = 'Seal temperature (pumps only).',
    TELEMETRY.DISCHARGE_TEMP AS TELEMETRY.DISCHARGE_TEMP
      COMMENT = 'Discharge temperature (compressors only).',
    TELEMETRY.COMPRESSION_RATIO AS TELEMETRY.COMPRESSION_RATIO
      COMMENT = 'Compression ratio (compressors only).',
    TELEMETRY.OIL_PRESSURE AS TELEMETRY.OIL_PRESSURE
      COMMENT = 'Oil system pressure (compressors only).',

    MAINTENANCE_LOGS.DURATION_HRS AS MAINTENANCE_LOGS.DURATION_HRS
      COMMENT = 'Duration of the maintenance in hours.',
    MAINTENANCE_LOGS.COST AS MAINTENANCE_LOGS.COST
      WITH SYNONYMS = ('maintenance cost', 'repair cost')
      COMMENT = 'Cost of the maintenance event in USD.'
  )

  DIMENSIONS (
    FLEET_KPI_VIEW.ASSET_ID AS FLEET_KPI_VIEW.ASSET_ID
      COMMENT = 'Unique integer identifier for the asset.',
    FLEET_KPI_VIEW.ASSET_TYPE AS FLEET_KPI_VIEW.ASSET_TYPE
      COMMENT = 'Type of asset. Values are PUMP or COMPRESSOR.',
    FLEET_KPI_VIEW.MODEL_NAME AS FLEET_KPI_VIEW.MODEL_NAME
      COMMENT = 'Equipment model name.',
    FLEET_KPI_VIEW.MANUFACTURER AS FLEET_KPI_VIEW.MANUFACTURER
      COMMENT = 'Equipment manufacturer name.',
    FLEET_KPI_VIEW.STATION_ID AS FLEET_KPI_VIEW.STATION_ID
      COMMENT = 'ID of the station where the asset is located.',
    FLEET_KPI_VIEW.STATION_NAME AS FLEET_KPI_VIEW.STATION_NAME
      COMMENT = 'Human-readable name of the station.',
    FLEET_KPI_VIEW.REGION AS FLEET_KPI_VIEW.REGION
      COMMENT = 'Geographic region.',
    FLEET_KPI_VIEW.PREDICTED_CLASS AS FLEET_KPI_VIEW.PREDICTED_CLASS
      WITH SYNONYMS = ('failure mode', 'predicted failure')
      COMMENT = 'ML-predicted failure mode.',
    FLEET_KPI_VIEW.RISK_LEVEL AS FLEET_KPI_VIEW.RISK_LEVEL
      WITH SYNONYMS = ('risk', 'health status', 'condition')
      COMMENT = 'Risk category: CRITICAL, WARNING, or HEALTHY.',
    FLEET_KPI_VIEW.INSTALL_DATE AS FLEET_KPI_VIEW.INSTALL_DATE
      COMMENT = 'Date the asset was installed.',
    FLEET_KPI_VIEW.PREDICTION_TS AS FLEET_KPI_VIEW.PREDICTION_TS
      COMMENT = 'Timestamp of the prediction.',
    FLEET_KPI_VIEW.LAST_MAINTENANCE_DATE AS FLEET_KPI_VIEW.LAST_MAINTENANCE_DATE
      COMMENT = 'Date of the most recent maintenance event.',

    TELEMETRY.ASSET_ID AS TELEMETRY.ASSET_ID
      COMMENT = 'Asset identifier.',
    TELEMETRY.TS AS TELEMETRY.TS
      WITH SYNONYMS = ('timestamp', 'reading time')
      COMMENT = 'Telemetry timestamp.',

    MAINTENANCE_LOGS.LOG_ID AS MAINTENANCE_LOGS.LOG_ID
      COMMENT = 'Unique maintenance log identifier.',
    MAINTENANCE_LOGS.ASSET_ID AS MAINTENANCE_LOGS.ASSET_ID
      COMMENT = 'Asset that was serviced.',
    MAINTENANCE_LOGS.MAINTENANCE_TYPE AS MAINTENANCE_LOGS.MAINTENANCE_TYPE
      WITH SYNONYMS = ('type of maintenance', 'work type')
      COMMENT = 'Type of maintenance.',
    MAINTENANCE_LOGS.DESCRIPTION AS MAINTENANCE_LOGS.DESCRIPTION
      COMMENT = 'Free-text description of work performed.',
    MAINTENANCE_LOGS.TECHNICIAN_ID AS MAINTENANCE_LOGS.TECHNICIAN_ID
      COMMENT = 'ID of the technician.',
    MAINTENANCE_LOGS.TS AS MAINTENANCE_LOGS.TS
      COMMENT = 'When the maintenance occurred.'
  )

  METRICS (
    FLEET_KPI_VIEW.TOTAL_ASSETS AS COUNT(*)
      WITH SYNONYMS = ('asset count', 'fleet size')
      COMMENT = 'Total number of assets.',
    FLEET_KPI_VIEW.CRITICAL_ASSET_COUNT AS SUM(CASE WHEN FLEET_KPI_VIEW.RISK_LEVEL = 'CRITICAL' THEN 1 ELSE 0 END)
      WITH SYNONYMS = ('critical count')
      COMMENT = 'Number of assets at critical risk level.',
    FLEET_KPI_VIEW.WARNING_ASSET_COUNT AS SUM(CASE WHEN FLEET_KPI_VIEW.RISK_LEVEL = 'WARNING' THEN 1 ELSE 0 END)
      COMMENT = 'Number of assets at warning risk level.',
    FLEET_KPI_VIEW.HEALTHY_ASSET_COUNT AS SUM(CASE WHEN FLEET_KPI_VIEW.RISK_LEVEL = 'HEALTHY' THEN 1 ELSE 0 END)
      COMMENT = 'Number of healthy assets.',
    FLEET_KPI_VIEW.AVG_RUL_DAYS AS AVG(FLEET_KPI_VIEW.PREDICTED_RUL_DAYS)
      WITH SYNONYMS = ('average remaining useful life')
      COMMENT = 'Average predicted remaining useful life in days.',

    TELEMETRY.AVG_VIBRATION AS AVG(TELEMETRY.VIBRATION)
      COMMENT = 'Average vibration reading.',
    TELEMETRY.MAX_VIBRATION AS MAX(TELEMETRY.VIBRATION)
      COMMENT = 'Maximum vibration reading.',
    TELEMETRY.AVG_TEMPERATURE AS AVG(TELEMETRY.TEMPERATURE)
      COMMENT = 'Average temperature reading.',
    TELEMETRY.AVG_PRESSURE AS AVG(TELEMETRY.PRESSURE)
      COMMENT = 'Average pressure reading.',

    MAINTENANCE_LOGS.TOTAL_MAINTENANCE_COST AS SUM(MAINTENANCE_LOGS.COST)
      WITH SYNONYMS = ('total cost', 'total repair cost')
      COMMENT = 'Total cost of all maintenance events.',
    MAINTENANCE_LOGS.AVG_MAINTENANCE_COST AS AVG(MAINTENANCE_LOGS.COST)
      COMMENT = 'Average cost per maintenance event.',
    MAINTENANCE_LOGS.MAINTENANCE_EVENT_COUNT AS COUNT(*)
      WITH SYNONYMS = ('number of maintenance events')
      COMMENT = 'Total number of maintenance events.'
  )

  COMMENT = 'Fleet-wide analytics for midstream pipeline operations.'
  AI_SQL_GENERATION 'Always filter telemetry data to TS <= ''2026-03-18 00:00:00'' unless the user specifies a different date range. This ensures the demo shows data as of the simulated now date.'
;

GRANT SELECT ON SEMANTIC VIEW PDM_DEMO.APP.FLEET_SEMANTIC_VIEW TO ROLE DEMO_PDM_ADMIN;

SELECT 'Semantic view FLEET_SEMANTIC_VIEW created successfully.' AS STATUS;
