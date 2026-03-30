import { NextRequest, NextResponse } from "next/server";
import { query } from "@/lib/snowflake";
import { safeToISOTimestamp } from "@/lib/dates";

export async function GET(
  _req: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  const assetId = parseInt(id, 10);

  const rows: any[] = await query(
    `SELECT LOG_ID, TS, MAINTENANCE_TYPE, DESCRIPTION, TECHNICIAN_ID,
            PARTS_USED, DURATION_HRS, COST
     FROM PDM_DEMO.RAW.MAINTENANCE_LOGS
     WHERE ASSET_ID = ?
     ORDER BY TS DESC`,
    [assetId]
  );

  rows.forEach((r) => {
    if (r.TS) {
      r.TS = safeToISOTimestamp(r.TS) || r.TS;
    }
    if (r.PARTS_USED && typeof r.PARTS_USED === "string") {
      r.PARTS_USED = JSON.parse(r.PARTS_USED);
    }
  });

  return NextResponse.json(rows);
}
