import { NextRequest, NextResponse } from "next/server";
import { query } from "@/lib/snowflake";
import { safeToISOTimestamp } from "@/lib/dates";

const MAX_POINTS = 2000;

const PUMP_SENSORS = [
  "SUCTION_PRESSURE", "DISCHARGE_PRESSURE", "FLOW_RATE", "MOTOR_CURRENT",
  "PUMP_SPEED", "BEARING_TEMP", "CASING_TEMP", "VIBRATION_RMS", "VALVE_POSITION", "LEAK_RATE"
];

const COMPRESSOR_SENSORS = [
  "VIBRATION", "TEMPERATURE", "PRESSURE", "FLOW_RATE", "RPM", "POWER_DRAW",
  "DISCHARGE_TEMP", "INLET_TEMP", "COMPRESSION_RATIO", "OIL_PRESSURE"
];

const COMPRESSOR_TO_PUMP_MAP: Record<string, string> = {
  "VIBRATION": "VIBRATION_RMS",
  "TEMPERATURE": "BEARING_TEMP",
  "PRESSURE": "DISCHARGE_PRESSURE",
  "RPM": "PUMP_SPEED",
  "POWER_DRAW": "MOTOR_CURRENT",
  "DISCHARGE_TEMP": "CASING_TEMP",
  "INLET_TEMP": "BEARING_TEMP",
  "COMPRESSION_RATIO": "VALVE_POSITION",
  "OIL_PRESSURE": "SUCTION_PRESSURE",
};

const ALL_SENSORS = [...new Set([...PUMP_SENSORS, ...COMPRESSOR_SENSORS])];

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

  const requestedSensors = sensorsParam
    ? sensorsParam.split(",").map((s) => s.trim().toUpperCase()).filter((s) => ALL_SENSORS.includes(s))
    : PUMP_SENSORS;

  if (requestedSensors.length === 0) {
    return NextResponse.json([]);
  }

  const dbColumns: string[] = [];
  const aliasMap: { dbCol: string; alias: string }[] = [];

  for (const sensor of requestedSensors) {
    if (PUMP_SENSORS.includes(sensor)) {
      if (!dbColumns.includes(sensor)) {
        dbColumns.push(sensor);
      }
      aliasMap.push({ dbCol: sensor, alias: sensor });
    } else if (COMPRESSOR_TO_PUMP_MAP[sensor]) {
      const mappedCol = COMPRESSOR_TO_PUMP_MAP[sensor];
      if (!dbColumns.includes(mappedCol)) {
        dbColumns.push(mappedCol);
      }
      aliasMap.push({ dbCol: mappedCol, alias: sensor });
    }
  }

  if (dbColumns.length === 0) {
    return NextResponse.json([]);
  }

  const selectCols = ["TS", ...dbColumns].join(", ");

  let whereClause = "PUMP_ID = ?";
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
      SELECT ${selectCols}, ROW_NUMBER() OVER (ORDER BY TS) AS rn,
             COUNT(*) OVER () AS total
      FROM PDM_DEMO.RAW.PUMP_TELEMETRY
      WHERE ${whereClause}
    )
    SELECT ${selectCols}
    FROM base
    WHERE total <= ${maxPts} OR MOD(rn - 1, CEIL(total / ${maxPts})) = 0 OR rn = total
    ORDER BY TS`;

  const rows: any[] = await query(sql, binds);

  rows.forEach((r: any) => {
    if (r.TS) {
      r.TS = safeToISOTimestamp(r.TS) || r.TS;
    }
    for (const { dbCol, alias } of aliasMap) {
      if (alias !== dbCol && r[dbCol] !== undefined) {
        r[alias] = r[dbCol];
      }
    }
  });

  return NextResponse.json(rows);
}
