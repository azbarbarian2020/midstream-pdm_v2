import { NextRequest, NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

export async function GET(req: NextRequest) {
  const asOfTs = req.nextUrl.searchParams.get("as_of_ts");

  let rows;
  if (asOfTs) {
    rows = await query(
      `WITH preds AS (
        SELECT *
        FROM PDM_DEMO.ANALYTICS.PREDICTIONS
        WHERE AS_OF_TS <= ?::TIMESTAMP_NTZ
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ASSET_ID ORDER BY AS_OF_TS DESC) = 1
      )
      SELECT a.ASSET_ID, a.ASSET_TYPE, a.MODEL_NAME, a.MANUFACTURER,
             a.INSTALL_DATE, a.RATED_CAPACITY,
             s.STATION_ID, s.NAME AS STATION_NAME, s.LAT, s.LON,
             p.PREDICTED_CLASS, p.PREDICTED_RUL_DAYS, p.RISK_LEVEL,
             p.CLASS_PROBABILITIES,
             wo.TECH_ID AS ASSIGNED_TECH_ID,
             t.NAME AS ASSIGNED_TECH_NAME
      FROM PDM_DEMO.RAW.ASSETS a
      JOIN PDM_DEMO.RAW.STATIONS s ON a.STATION_ID = s.STATION_ID
      LEFT JOIN preds p ON a.ASSET_ID = p.ASSET_ID
      LEFT JOIN (
          SELECT ASSET_ID, TECH_ID,
                 ROW_NUMBER() OVER (PARTITION BY ASSET_ID ORDER BY CREATED_AT DESC) AS RN
          FROM PDM_DEMO.APP.WORK_ORDERS
          WHERE STATUS IN ('SCHEDULED', 'IN_PROGRESS')
      ) wo ON a.ASSET_ID = wo.ASSET_ID AND wo.RN = 1
      LEFT JOIN PDM_DEMO.RAW.TECHNICIANS t ON wo.TECH_ID = t.TECH_ID
      ORDER BY a.ASSET_ID`,
      [asOfTs]
    );
  } else {
    rows = await query(
      `WITH preds AS (
        SELECT *
        FROM PDM_DEMO.ANALYTICS.PREDICTIONS
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ASSET_ID ORDER BY AS_OF_TS DESC) = 1
      )
      SELECT a.ASSET_ID, a.ASSET_TYPE, a.MODEL_NAME, a.MANUFACTURER,
             a.INSTALL_DATE, a.RATED_CAPACITY,
             s.STATION_ID, s.NAME AS STATION_NAME, s.LAT, s.LON,
             p.PREDICTED_CLASS, p.PREDICTED_RUL_DAYS, p.RISK_LEVEL,
             p.CLASS_PROBABILITIES,
             wo.TECH_ID AS ASSIGNED_TECH_ID,
             t.NAME AS ASSIGNED_TECH_NAME
      FROM PDM_DEMO.RAW.ASSETS a
      JOIN PDM_DEMO.RAW.STATIONS s ON a.STATION_ID = s.STATION_ID
      LEFT JOIN preds p ON a.ASSET_ID = p.ASSET_ID
      LEFT JOIN (
          SELECT ASSET_ID, TECH_ID,
                 ROW_NUMBER() OVER (PARTITION BY ASSET_ID ORDER BY CREATED_AT DESC) AS RN
          FROM PDM_DEMO.APP.WORK_ORDERS
          WHERE STATUS IN ('SCHEDULED', 'IN_PROGRESS')
      ) wo ON a.ASSET_ID = wo.ASSET_ID AND wo.RN = 1
      LEFT JOIN PDM_DEMO.RAW.TECHNICIANS t ON wo.TECH_ID = t.TECH_ID
      ORDER BY a.ASSET_ID`
    );
  }

  const result = rows.map((r: any) => {
    if (r.CLASS_PROBABILITIES && typeof r.CLASS_PROBABILITIES === "string") {
      r.CLASS_PROBABILITIES = JSON.parse(r.CLASS_PROBABILITIES);
    }
    if (r.INSTALL_DATE) {
      const d = r.INSTALL_DATE instanceof Date ? r.INSTALL_DATE : new Date(r.INSTALL_DATE);
      r.INSTALL_DATE = d.toISOString().slice(0, 10);
    }
    return r;
  });

  return NextResponse.json(result);
}
