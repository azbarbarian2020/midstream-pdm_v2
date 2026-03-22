import { NextRequest, NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

export async function GET(req: NextRequest) {
  const techId = req.nextUrl.searchParams.get("tech_id");
  const startDate = req.nextUrl.searchParams.get("start_date");
  const endDate = req.nextUrl.searchParams.get("end_date");

  let sql = `SELECT s.SCHEDULE_ID, s.TECH_ID, t.NAME AS TECH_NAME,
                    TO_CHAR(s.SCHEDULE_DATE, 'YYYY-MM-DD') AS SCHEDULE_DATE,
                    s.BLOCK_TYPE, s.WO_ID, s.ASSET_ID,
                    s.STATION_NAME, s.ESTIMATED_HOURS, s.NOTES, s.IS_BASELINE
             FROM PDM_DEMO.APP.TECH_SCHEDULES s
             JOIN PDM_DEMO.RAW.TECHNICIANS t ON s.TECH_ID = t.TECH_ID
             WHERE 1=1`;
  const binds: any[] = [];

  if (techId) {
    sql += " AND s.TECH_ID = ?";
    binds.push(techId);
  }
  if (startDate) {
    sql += " AND s.SCHEDULE_DATE >= ?::DATE";
    binds.push(startDate);
  }
  if (endDate) {
    sql += " AND s.SCHEDULE_DATE <= ?::DATE";
    binds.push(endDate);
  }
  sql += " ORDER BY s.TECH_ID, s.SCHEDULE_DATE";

  const rows: any[] = await query(sql, binds);

  return NextResponse.json(rows);
}
