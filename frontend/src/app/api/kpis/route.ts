import { NextRequest, NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

export async function GET(req: NextRequest) {
  const asOfTs = req.nextUrl.searchParams.get("as_of_ts");

  let preds: any[];
  if (asOfTs) {
    preds = await query(
      `SELECT ASSET_ID, PREDICTED_CLASS, PREDICTED_RUL_DAYS, RISK_LEVEL
       FROM PDM_DEMO.ANALYTICS.PREDICTIONS
       WHERE AS_OF_TS <= ?::TIMESTAMP_NTZ
       QUALIFY ROW_NUMBER() OVER (PARTITION BY ASSET_ID ORDER BY AS_OF_TS DESC) = 1`,
      [asOfTs]
    );
  } else {
    preds = await query(
      `SELECT ASSET_ID, PREDICTED_CLASS, PREDICTED_RUL_DAYS, RISK_LEVEL
       FROM PDM_DEMO.ANALYTICS.PREDICTIONS
       QUALIFY ROW_NUMBER() OVER (PARTITION BY ASSET_ID ORDER BY AS_OF_TS DESC) = 1`
    );
  }

  const total = preds.length;
  const failed = preds.filter((p) => p.RISK_LEVEL === "FAILED").length;
  const critical = preds.filter((p) => p.RISK_LEVEL === "CRITICAL").length;
  const warning = preds.filter((p) => p.RISK_LEVEL === "WARNING").length;
  const healthy = preds.filter((p) => p.RISK_LEVEL === "HEALTHY").length;
  const rulValues = preds.filter((p) => p.PREDICTED_RUL_DAYS != null && (p.RISK_LEVEL === "CRITICAL" || p.RISK_LEVEL === "WARNING" || p.RISK_LEVEL === "FAILED")).map((p) => Number(p.PREDICTED_RUL_DAYS));
  const avgRul = rulValues.length ? rulValues.reduce((a, b) => a + b, 0) / rulValues.length : 0;

  return NextResponse.json({
    total_assets: total,
    failed,
    critical,
    warning,
    healthy,
    avg_rul: Math.round(avgRul * 10) / 10,
  });
}
