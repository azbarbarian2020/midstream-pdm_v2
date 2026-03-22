import { NextRequest, NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

const MAX_POINTS = 2000;

const ALL_SENSORS = [
  "VIBRATION", "TEMPERATURE", "PRESSURE", "FLOW_RATE", "RPM", "POWER_DRAW",
  "DIFFERENTIAL_PRESSURE", "SUCTION_PRESSURE", "SEAL_TEMPERATURE", "CAVITATION_INDEX",
  "DISCHARGE_TEMP", "INLET_TEMP", "COMPRESSION_RATIO", "OIL_PRESSURE",
];

export async function GET(
  req: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  const assetId = parseInt(id, 10);
  const start = req.nextUrl.searchParams.get("start");
  const end = req.nextUrl.searchParams.get("end");
  const sensorsParam = req.nextUrl.searchParams.get("sensors");
  const limitParam = req.nextUrl.searchParams.get("limit");
  const maxPts = limitParam ? Math.min(parseInt(limitParam, 10) || MAX_POINTS, 5000) : MAX_POINTS;

  const sensorCols = sensorsParam
    ? sensorsParam.split(",").map((s) => s.trim().toUpperCase()).filter((s) => ALL_SENSORS.includes(s))
    : ALL_SENSORS;

  const colList = ["TS", ...sensorCols].join(", ");

  let whereClause = "ASSET_ID = ?";
  const binds: any[] = [assetId];

  if (start) {
    whereClause += " AND TS >= ?::TIMESTAMP_NTZ";
    binds.push(start);
  }
  if (end) {
    whereClause += " AND TS <= ?::TIMESTAMP_NTZ";
    binds.push(end);
  }

  const sql = `
    WITH base AS (
      SELECT ${colList}, ROW_NUMBER() OVER (ORDER BY TS) AS rn,
             COUNT(*) OVER () AS total
      FROM PDM_DEMO.RAW.TELEMETRY
      WHERE ${whereClause}
    )
    SELECT ${colList}
    FROM base
    WHERE total <= ${maxPts} OR MOD(rn - 1, CEIL(total / ${maxPts})) = 0 OR rn = total
    ORDER BY TS`;

  const rows: any[] = await query(sql, binds);

  rows.forEach((r: any) => {
    if (r.TS) {
      const d = r.TS instanceof Date ? r.TS : new Date(r.TS);
      r.TS = d.toISOString().slice(0, 19).replace("T", " ");
    }
  });

  return NextResponse.json(rows);
}
