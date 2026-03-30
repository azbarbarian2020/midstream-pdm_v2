import { NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

export async function GET() {
  const result = await query(
    `SELECT 
       TO_CHAR(MIN(PREDICTION_DATE), 'YYYY-MM-DD') || 'T00:00:00' AS MIN_TS,
       TO_CHAR(MAX(PREDICTION_DATE), 'YYYY-MM-DD') || 'T00:00:00' AS MAX_TS
     FROM PDM_DEMO.ANALYTICS.PREDICTION_HISTORY`
  );

  const nowResult = await query(
    `SELECT TO_CHAR(NOW_TS, 'YYYY-MM-DD"T"HH24:MI:SS') AS NOW_TS
     FROM PDM_DEMO.ANALYTICS.DATA_NOW_TS`
  );

  const row = result[0];
  const nowRow = nowResult[0];
  return NextResponse.json({
    data_min: row.MIN_TS,
    data_max: row.MAX_TS,
    now: nowRow?.NOW_TS || row.MIN_TS,
  });
}
