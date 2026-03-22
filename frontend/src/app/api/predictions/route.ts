import { NextRequest, NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

export async function GET(req: NextRequest) {
  const asOfTs = req.nextUrl.searchParams.get("as_of_ts");

  let rows;
  if (asOfTs) {
    rows = await query(
      `SELECT ASSET_ID, AS_OF_TS, PREDICTED_CLASS, CLASS_PROBABILITIES,
              PREDICTED_RUL_DAYS, RISK_LEVEL, MODEL_VERSION, SCORED_AT
       FROM PDM_DEMO.ANALYTICS.PREDICTIONS
       WHERE AS_OF_TS <= ?::TIMESTAMP_NTZ
       QUALIFY ROW_NUMBER() OVER (PARTITION BY ASSET_ID ORDER BY AS_OF_TS DESC) = 1`,
      [asOfTs]
    );
  } else {
    rows = await query(
      `SELECT ASSET_ID, AS_OF_TS, PREDICTED_CLASS, CLASS_PROBABILITIES,
              PREDICTED_RUL_DAYS, RISK_LEVEL, MODEL_VERSION, SCORED_AT
       FROM PDM_DEMO.ANALYTICS.PREDICTIONS
       QUALIFY ROW_NUMBER() OVER (PARTITION BY ASSET_ID ORDER BY AS_OF_TS DESC) = 1`
    );
  }
  return NextResponse.json(rows);
}
