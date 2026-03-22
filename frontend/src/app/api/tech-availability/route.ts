import { NextRequest, NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

export async function GET(req: NextRequest) {
  const startDate = req.nextUrl.searchParams.get("start_date") || "";
  const endDate = req.nextUrl.searchParams.get("end_date") || "";

  const rows: any[] = await query(
    `WITH date_range AS (
        SELECT DATEADD('day', SEQ4(), ?::DATE) AS D
        FROM TABLE(GENERATOR(ROWCOUNT => DATEDIFF('day', ?::DATE, ?::DATE) + 1))
     ),
     booked AS (
        SELECT TECH_ID, SCHEDULE_DATE, SUM(ESTIMATED_HOURS) AS BOOKED_HOURS
        FROM PDM_DEMO.APP.TECH_SCHEDULES
        GROUP BY TECH_ID, SCHEDULE_DATE
     )
     SELECT t.TECH_ID, t.NAME, t.HOME_BASE_CITY, t.AVAILABILITY, t.CERTIFICATIONS,
            d.D AS SCHEDULE_DATE,
            COALESCE(b.BOOKED_HOURS, 0) AS BOOKED_HOURS,
            8 - COALESCE(b.BOOKED_HOURS, 0) AS AVAILABLE_HOURS
     FROM PDM_DEMO.RAW.TECHNICIANS t
     CROSS JOIN date_range d
     LEFT JOIN booked b ON t.TECH_ID = b.TECH_ID AND d.D = b.SCHEDULE_DATE
     ORDER BY t.TECH_ID, d.D`,
    [startDate, startDate, endDate]
  );

  rows.forEach((r) => {
    if (r.SCHEDULE_DATE) r.SCHEDULE_DATE = String(r.SCHEDULE_DATE);
  });

  return NextResponse.json(rows);
}
