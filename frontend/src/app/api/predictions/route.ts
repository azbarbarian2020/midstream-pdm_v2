import { NextRequest, NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

export async function GET(req: NextRequest) {
  const asOfTs = req.nextUrl.searchParams.get("as_of_ts");

  let rows;
  if (asOfTs) {
    rows = await query(
      `SELECT PUMP_ID AS ASSET_ID, 
              TO_CHAR(TS, 'YYYY-MM-DD HH24:MI:SS') AS AS_OF_TS, 
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
              TOP_FEATURE AS TOP_FEATURE_1,
              NULL AS TOP_FEATURE_1_DELTA_PCT,
              NULL AS TOP_FEATURE_2,
              NULL AS TOP_FEATURE_2_DELTA_PCT,
              NULL AS TOP_FEATURE_3,
              NULL AS TOP_FEATURE_3_DELTA_PCT
       FROM PDM_DEMO.ANALYTICS.PREDICTIONS
       WHERE TS <= ?::TIMESTAMP_NTZ
       QUALIFY ROW_NUMBER() OVER (PARTITION BY PUMP_ID ORDER BY TS DESC) = 1`,
      [asOfTs]
    );
  } else {
    rows = await query(
      `SELECT PUMP_ID AS ASSET_ID,
              TO_CHAR(TS, 'YYYY-MM-DD HH24:MI:SS') AS AS_OF_TS, 
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
              TOP_FEATURE AS TOP_FEATURE_1,
              NULL AS TOP_FEATURE_1_DELTA_PCT,
              NULL AS TOP_FEATURE_2,
              NULL AS TOP_FEATURE_2_DELTA_PCT,
              NULL AS TOP_FEATURE_3,
              NULL AS TOP_FEATURE_3_DELTA_PCT
       FROM PDM_DEMO.ANALYTICS.PREDICTIONS
       QUALIFY ROW_NUMBER() OVER (PARTITION BY PUMP_ID ORDER BY TS DESC) = 1`
    );
  }
  return NextResponse.json(rows);
}
