-- ============================================================================
-- Route Planner Stored Procedure
-- Plans optimized service routes bundling nearby at-risk assets.
-- ============================================================================

USE DATABASE PDM_DEMO;
USE WAREHOUSE PDM_DEMO_WH;

CREATE OR REPLACE PROCEDURE APP.PLAN_ROUTE(
    TECH_ID VARCHAR,
    PRIMARY_ASSET_ID INT,
    HORIZON_DAYS INT DEFAULT 3,
    MAX_STOPS INT DEFAULT 5,
    AS_OF_TS_PARAM VARCHAR DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'plan_route'
AS $$
def plan_route(session, tech_id, primary_asset_id, horizon_days, max_stops, as_of_ts_param=None):
    import json
    import math

    tech_rows = session.sql(f"""
        SELECT HOME_BASE_LAT, HOME_BASE_LON, NAME
        FROM PDM_DEMO.RAW.TECHNICIANS
        WHERE TECH_ID = '{tech_id}'
    """).collect()

    if not tech_rows:
        return json.dumps({"error": f"Technician {tech_id} not found"})

    tech = tech_rows[0]

    ts_filter = ""
    if as_of_ts_param:
        ts_filter = f"AND AS_OF_TS <= '{as_of_ts_param}'::TIMESTAMP_NTZ"

    candidates = session.sql(f"""
        WITH latest_pred AS (
            SELECT *
            FROM PDM_DEMO.ANALYTICS.PREDICTIONS
            WHERE 1=1 {ts_filter}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ASSET_ID ORDER BY AS_OF_TS DESC) = 1
        ),
        primary_station AS (
            SELECT s.LAT, s.LON
            FROM PDM_DEMO.RAW.ASSETS a
            JOIN PDM_DEMO.RAW.STATIONS s ON a.STATION_ID = s.STATION_ID
            WHERE a.ASSET_ID = {primary_asset_id}
        )
        SELECT
            a.ASSET_ID, a.ASSET_TYPE, s.NAME AS STATION_NAME,
            s.LAT, s.LON,
            p.PREDICTED_CLASS, p.PREDICTED_RUL_DAYS, p.RISK_LEVEL,
            HAVERSINE(s.LAT, s.LON, ps.LAT, ps.LON) AS DISTANCE_MILES
        FROM PDM_DEMO.RAW.ASSETS a
        JOIN PDM_DEMO.RAW.STATIONS s ON a.STATION_ID = s.STATION_ID
        JOIN latest_pred p ON a.ASSET_ID = p.ASSET_ID
        CROSS JOIN primary_station ps
        LEFT JOIN PDM_DEMO.APP.WORK_ORDERS wo
            ON a.ASSET_ID = wo.ASSET_ID AND wo.STATUS IN ('SCHEDULED', 'IN_PROGRESS')
        WHERE (
            a.ASSET_ID = {primary_asset_id}
            OR (p.RISK_LEVEL IN ('WARNING', 'CRITICAL') AND wo.WO_ID IS NULL
                AND p.PREDICTED_RUL_DAYS <= {horizon_days})
        )
        ORDER BY
            CASE WHEN a.ASSET_ID = {primary_asset_id} THEN 0 ELSE 1 END,
            CASE p.RISK_LEVEL WHEN 'CRITICAL' THEN 0 WHEN 'WARNING' THEN 1 ELSE 2 END,
            p.PREDICTED_RUL_DAYS ASC,
            DISTANCE_MILES ASC
        LIMIT {max_stops}
    """).collect()

    home_lat = float(tech.HOME_BASE_LAT)
    home_lon = float(tech.HOME_BASE_LON)

    def haversine(lat1, lon1, lat2, lon2):
        R = 3959
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
        return R * 2 * math.asin(math.sqrt(a))

    stops = []
    prev_lat, prev_lon = home_lat, home_lon
    for i, row in enumerate(candidates):
        parts_rows = session.sql(f"""
            SELECT DISTINCT PART_NAME, CATEGORY
            FROM PDM_DEMO.RAW.PARTS_INVENTORY
            WHERE ASSET_TYPE = '{row.ASSET_TYPE}'
              AND CATEGORY IN (
                  CASE '{row.PREDICTED_CLASS}'
                      WHEN 'BEARING_WEAR' THEN 'bearing'
                      WHEN 'SEAL_LEAK' THEN 'seal'
                      WHEN 'VALVE_FAILURE' THEN 'valve'
                      WHEN 'SURGE' THEN 'valve'
                      WHEN 'OVERHEATING' THEN 'filter'
                      ELSE 'general'
                  END, 'general'
              )
            LIMIT 5
        """).collect()

        lat, lon = float(row.LAT), float(row.LON)
        leg = round(haversine(prev_lat, prev_lon, lat, lon), 1)

        stops.append({
            "stop_number": i + 1,
            "asset_id": row.ASSET_ID,
            "asset_type": row.ASSET_TYPE,
            "station": row.STATION_NAME,
            "lat": lat,
            "lon": lon,
            "predicted_class": row.PREDICTED_CLASS,
            "rul_days": float(row.PREDICTED_RUL_DAYS),
            "risk_level": row.RISK_LEVEL,
            "distance_from_primary": round(float(row.DISTANCE_MILES), 1),
            "leg_miles": leg,
            "parts_needed": [{"name": p.PART_NAME, "category": p.CATEGORY} for p in parts_rows],
            "reason": f"{row.PREDICTED_CLASS} predicted, {float(row.PREDICTED_RUL_DAYS):.1f} days RUL"
        })
        prev_lat, prev_lon = lat, lon

    total_miles = sum(s["leg_miles"] for s in stops)

    return json.dumps({
        "tech_id": tech_id,
        "tech_name": tech.NAME,
        "primary_asset_id": primary_asset_id,
        "home_lat": home_lat,
        "home_lon": home_lon,
        "route": stops,
        "total_stops": len(stops),
        "estimated_travel_miles": round(total_miles, 1)
    })
$$;

GRANT USAGE ON PROCEDURE PDM_DEMO.APP.PLAN_ROUTE(VARCHAR, INT, INT, INT, VARCHAR) TO ROLE DEMO_PDM_ADMIN;

SELECT 'PLAN_ROUTE created.' AS STATUS;
