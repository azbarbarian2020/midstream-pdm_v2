import { NextRequest, NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

function formatDate(val: any): string | null {
  if (!val) return null;
  const d = val instanceof Date ? val : new Date(val);
  return d.toISOString().slice(0, 10);
}

function formatTs(val: any): string | null {
  if (!val) return null;
  const d = val instanceof Date ? val : new Date(val);
  return d.toISOString().slice(0, 19).replace("T", " ");
}

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
      `SELECT ASSET_ID, AS_OF_TS, PREDICTED_CLASS, CLASS_PROBABILITIES,
              PREDICTED_RUL_DAYS, RISK_LEVEL, MODEL_VERSION, SCORED_AT
       FROM PDM_DEMO.ANALYTICS.PREDICTIONS
       WHERE ASSET_ID = ? AND AS_OF_TS <= ?::TIMESTAMP_NTZ
       QUALIFY ROW_NUMBER() OVER (PARTITION BY ASSET_ID ORDER BY AS_OF_TS DESC) = 1`,
      [assetId, asOfTs]
    );
  } else {
    preds = await query(
      `SELECT ASSET_ID, AS_OF_TS, PREDICTED_CLASS, CLASS_PROBABILITIES,
              PREDICTED_RUL_DAYS, RISK_LEVEL, MODEL_VERSION, SCORED_AT
       FROM PDM_DEMO.ANALYTICS.PREDICTIONS
       WHERE ASSET_ID = ?
       QUALIFY ROW_NUMBER() OVER (PARTITION BY ASSET_ID ORDER BY AS_OF_TS DESC) = 1`,
      [assetId]
    );
  }

  const pred = preds[0] || null;
  if (pred) {
    pred.AS_OF_TS = formatTs(pred.AS_OF_TS);
    pred.SCORED_AT = formatTs(pred.SCORED_AT);
    if (pred.CLASS_PROBABILITIES && typeof pred.CLASS_PROBABILITIES === "string") {
      pred.CLASS_PROBABILITIES = JSON.parse(pred.CLASS_PROBABILITIES);
    }
  }
  asset.prediction = pred;

  return NextResponse.json(asset);
}
