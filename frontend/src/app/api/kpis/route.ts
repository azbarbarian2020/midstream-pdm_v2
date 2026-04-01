import { NextRequest, NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

export async function GET(req: NextRequest) {
  const asOfTs = req.nextUrl.searchParams.get("as_of_ts");

  const nowTs = await query<{ NOW_TS: string }>(
    `SELECT TO_CHAR(NOW_TS, 'YYYY-MM-DD"T"HH24:MI:SS') AS NOW_TS FROM PDM_DEMO.ANALYTICS.DATA_NOW_TS`
  );
  const effectiveTs = asOfTs || nowTs[0]?.NOW_TS || '2026-03-18T00:00:00';

  const preds = await query<{ ASSET_ID: number; PREDICTED_CLASS: string; PREDICTED_RUL_DAYS: number | null; RISK_LEVEL: string }>(
    `SELECT 
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
       END AS RISK_LEVEL
     FROM PDM_DEMO.ANALYTICS.PREDICTIONS
     WHERE TS <= ?::TIMESTAMP_NTZ
     QUALIFY ROW_NUMBER() OVER (PARTITION BY PUMP_ID ORDER BY TS DESC) = 1`,
    [effectiveTs] as [string]
  );

  const total = preds.length;
  const offline = preds.filter((p) => p.RISK_LEVEL === "FAILED").length;
  const critical = preds.filter((p) => p.RISK_LEVEL === "CRITICAL").length;
  const warning = preds.filter((p) => p.RISK_LEVEL === "WARNING").length;
  const healthy = preds.filter((p) => p.RISK_LEVEL === "HEALTHY").length;
  const rulValues = preds
    .filter((p) => p.PREDICTED_RUL_DAYS != null && ["CRITICAL", "WARNING", "OFFLINE"].includes(p.RISK_LEVEL))
    .map((p) => Number(p.PREDICTED_RUL_DAYS));
  const avgRul = rulValues.length ? rulValues.reduce((a, b) => a + b, 0) / rulValues.length : 0;

  return NextResponse.json({
    total_assets: total,
    offline,
    critical,
    warning,
    healthy,
    avg_rul: Math.round(avgRul * 10) / 10,
  });
}
