import { NextRequest, NextResponse } from "next/server";
import { query } from "@/lib/snowflake";
import { safeToISODate, safeToISOTimestamp } from "@/lib/dates";

const formatDate = safeToISODate;

export async function GET(
  req: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  const assetId = parseInt(id, 10);
  const asOfTs = req.nextUrl.searchParams.get("as_of_ts");

  const assets = await query(
    `SELECT a.ASSET_ID, a.ASSET_TYPE, a.MODEL_NAME, a.MANUFACTURER,
            a.INSTALL_DATE, a.RATED_CAPACITY, a.ATTRIBUTES,
            s.STATION_ID, s.NAME AS STATION_NAME, s.LAT, s.LON, s.REGION
     FROM PDM_DEMO.RAW.ASSETS a
     JOIN PDM_DEMO.RAW.STATIONS s ON a.STATION_ID = s.STATION_ID
     WHERE a.ASSET_ID = ?`,
    [assetId]
  );

  if (!assets.length) return NextResponse.json({ error: "Asset not found" }, { status: 404 });

  const asset: any = { ...assets[0] };
  asset.INSTALL_DATE = formatDate(asset.INSTALL_DATE);
  if (asset.ATTRIBUTES && typeof asset.ATTRIBUTES === "string") {
    asset.ATTRIBUTES = JSON.parse(asset.ATTRIBUTES);
  }

  let preds;
  if (asOfTs) {
    preds = await query(
      `SELECT 
        PUMP_ID AS ASSET_ID,
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
        TOP_FEATURE AS TOP_FEATURE_1
       FROM PDM_DEMO.ANALYTICS.PREDICTIONS
       WHERE PUMP_ID = ? AND TS <= ?::TIMESTAMP_NTZ
       QUALIFY ROW_NUMBER() OVER (PARTITION BY PUMP_ID ORDER BY TS DESC) = 1`,
      [assetId, asOfTs]
    );
  } else {
    preds = await query(
      `SELECT 
        PUMP_ID AS ASSET_ID,
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
        TOP_FEATURE AS TOP_FEATURE_1
       FROM PDM_DEMO.ANALYTICS.PREDICTIONS
       WHERE PUMP_ID = ?
       QUALIFY ROW_NUMBER() OVER (PARTITION BY PUMP_ID ORDER BY TS DESC) = 1`,
      [assetId]
    );
  }

  const pred = preds[0] || null;
  asset.prediction = pred;

  return NextResponse.json(asset);
}
