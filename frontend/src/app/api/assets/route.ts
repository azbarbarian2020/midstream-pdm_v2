import { NextRequest, NextResponse } from "next/server";
import { query } from "@/lib/snowflake";
import { safeToISODate } from "@/lib/dates";

export async function GET(req: NextRequest) {
  const asOfTs = req.nextUrl.searchParams.get("as_of_ts");

  const predCols = `p.PREDICTED_CLASS, p.PREDICTED_RUL_DAYS, p.RISK_LEVEL, p.CONFIDENCE,
             p.TOP_FEATURE_1`;

  const predSource = asOfTs
    ? `(SELECT 
          PUMP_ID AS ASSET_ID,
          PREDICTED_CLASS,
          PREDICTED_RUL_DAYS,
          CASE 
            WHEN PREDICTED_CLASS = 'OFFLINE' THEN 'FAILED'
            WHEN PREDICTED_CLASS = 'NORMAL' THEN 'HEALTHY'
            WHEN PREDICTED_RUL_DAYS IS NULL THEN 'HEALTHY'
            WHEN PREDICTED_RUL_DAYS <= 7 THEN 'CRITICAL'
            WHEN PREDICTED_RUL_DAYS <= 14 THEN 'WARNING'
            ELSE 'HEALTHY'
          END AS RISK_LEVEL,
          CONFIDENCE,
          TOP_FEATURE AS TOP_FEATURE_1
        FROM PDM_DEMO.ANALYTICS.PREDICTIONS
        WHERE TS <= ?::TIMESTAMP_NTZ
        QUALIFY ROW_NUMBER() OVER (PARTITION BY PUMP_ID ORDER BY TS DESC) = 1)`
    : `(SELECT 
          PUMP_ID AS ASSET_ID,
          PREDICTED_CLASS,
          PREDICTED_RUL_DAYS,
          CASE 
            WHEN PREDICTED_CLASS = 'OFFLINE' THEN 'FAILED'
            WHEN PREDICTED_CLASS = 'NORMAL' THEN 'HEALTHY'
            WHEN PREDICTED_RUL_DAYS IS NULL THEN 'HEALTHY'
            WHEN PREDICTED_RUL_DAYS <= 7 THEN 'CRITICAL'
            WHEN PREDICTED_RUL_DAYS <= 14 THEN 'WARNING'
            ELSE 'HEALTHY'
          END AS RISK_LEVEL,
          CONFIDENCE,
          TOP_FEATURE AS TOP_FEATURE_1
        FROM PDM_DEMO.ANALYTICS.PREDICTIONS
        QUALIFY ROW_NUMBER() OVER (PARTITION BY PUMP_ID ORDER BY TS DESC) = 1)`;

  const sql = `SELECT a.ASSET_ID, a.ASSET_TYPE, a.MODEL_NAME, a.MANUFACTURER,
             a.INSTALL_DATE, a.RATED_CAPACITY,
             s.STATION_ID, s.NAME AS STATION_NAME, s.LAT, s.LON,
             ${predCols},
             wo.TECH_ID AS ASSIGNED_TECH_ID,
             t.NAME AS ASSIGNED_TECH_NAME
      FROM PDM_DEMO.RAW.ASSETS a
      JOIN PDM_DEMO.RAW.STATIONS s ON a.STATION_ID = s.STATION_ID
      LEFT JOIN ${predSource} p ON a.ASSET_ID = p.ASSET_ID
      LEFT JOIN (
          SELECT ASSET_ID, TECH_ID,
                 ROW_NUMBER() OVER (PARTITION BY ASSET_ID ORDER BY CREATED_AT DESC) AS RN
          FROM PDM_DEMO.APP.WORK_ORDERS
          WHERE STATUS IN ('SCHEDULED', 'IN_PROGRESS')
      ) wo ON a.ASSET_ID = wo.ASSET_ID AND wo.RN = 1
      LEFT JOIN PDM_DEMO.RAW.TECHNICIANS t ON wo.TECH_ID = t.TECH_ID
      ORDER BY a.ASSET_ID`;

  const rows = await query(sql, asOfTs ? [asOfTs] : []);

  const result = rows.map((r: any) => {
    if (r.INSTALL_DATE) {
      r.INSTALL_DATE = safeToISODate(r.INSTALL_DATE) || r.INSTALL_DATE;
    }
    return r;
  });

  return NextResponse.json(result);
}
