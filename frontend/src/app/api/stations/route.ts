import { NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

export async function GET() {
  const rows = await query(
    `SELECT s.*, COUNT(a.ASSET_ID) AS ASSET_COUNT
     FROM PDM_DEMO.RAW.STATIONS s
     LEFT JOIN PDM_DEMO.RAW.ASSETS a ON s.STATION_ID = a.STATION_ID
     GROUP BY s.STATION_ID, s.NAME, s.LAT, s.LON, s.REGION, s.STATION_TYPE
     ORDER BY s.STATION_ID`
  );
  return NextResponse.json(rows);
}
