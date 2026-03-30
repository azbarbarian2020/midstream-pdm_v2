import { NextRequest, NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

const MAX_POINTS = 500;

export async function GET(
  req: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  const assetId = parseInt(id, 10);
  const asOfTs = req.nextUrl.searchParams.get("as_of_ts");

  let whereClause = "PUMP_ID = ?";
  const binds: any[] = [assetId];

  if (asOfTs) {
    whereClause += " AND TS <= ?::TIMESTAMP_NTZ";
    binds.push(asOfTs);
  }

  const sql = `
    WITH base AS (
      SELECT 
        TO_CHAR(TS, 'YYYY-MM-DD HH24:MI:SS') AS AS_OF_TS,
        TS::DATE AS PREDICTION_DATE,
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
        NULL AS TOP_FEATURE_3_DELTA_PCT,
        ROW_NUMBER() OVER (ORDER BY TS) AS rn,
        COUNT(*) OVER () AS total
      FROM PDM_DEMO.ANALYTICS.PREDICTIONS
      WHERE ${whereClause}
    )
    SELECT *
    FROM base
    WHERE total <= ${MAX_POINTS} OR MOD(rn - 1, CEIL(total / ${MAX_POINTS})) = 0 OR rn = total
    ORDER BY AS_OF_TS`;

  const rows: any[] = await query(sql, binds);

  return NextResponse.json(rows);
}
