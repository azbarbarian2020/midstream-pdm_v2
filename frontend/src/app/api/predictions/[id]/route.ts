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

  let whereClause = "ASSET_ID = ?";
  const binds: any[] = [assetId];

  if (asOfTs) {
    whereClause += " AND AS_OF_TS <= ?::TIMESTAMP_NTZ";
    binds.push(asOfTs);
  }

  const sql = `
    WITH base AS (
      SELECT AS_OF_TS, PREDICTED_CLASS, PREDICTED_RUL_DAYS, RISK_LEVEL, MODEL_VERSION, CLASS_PROBABILITIES,
             ROW_NUMBER() OVER (ORDER BY AS_OF_TS) AS rn,
             COUNT(*) OVER () AS total
      FROM PDM_DEMO.ANALYTICS.PREDICTIONS
      WHERE ${whereClause}
    )
    SELECT AS_OF_TS, PREDICTED_CLASS, PREDICTED_RUL_DAYS, RISK_LEVEL, MODEL_VERSION, CLASS_PROBABILITIES
    FROM base
    WHERE total <= ${MAX_POINTS} OR MOD(rn - 1, CEIL(total / ${MAX_POINTS})) = 0 OR rn = total
    ORDER BY AS_OF_TS`;

  const rows: any[] = await query(sql, binds);

  rows.forEach((r) => {
    if (r.AS_OF_TS) {
      const d = r.AS_OF_TS instanceof Date ? r.AS_OF_TS : new Date(r.AS_OF_TS);
      r.AS_OF_TS = d.toISOString().slice(0, 19).replace("T", " ");
    }
    if (typeof r.CLASS_PROBABILITIES === "string") {
      try { r.CLASS_PROBABILITIES = JSON.parse(r.CLASS_PROBABILITIES); } catch {}
    }
  });

  return NextResponse.json(rows);
}
