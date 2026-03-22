import { NextRequest, NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

export async function POST(req: NextRequest) {
  const body = await req.json();
  const created: number[] = [];

  for (const stop of body.stops) {
    const priority = stop.risk_level === "CRITICAL" ? "CRITICAL" : "HIGH";
    const scheduledDate = stop.scheduled_date || "2026-03-13";
    const repairHours = stop.estimated_repair_hours || 4.0;
    const station = stop.station || "";

    await query(
      `INSERT INTO PDM_DEMO.APP.WORK_ORDERS
          (ASSET_ID, TECH_ID, PRIORITY, DESCRIPTION, PARTS_NEEDED, STATUS, SCHEDULED_DATE, ESTIMATED_HOURS, STATION_NAME)
       SELECT ?, ?, ?, ?, PARSE_JSON(?), 'SCHEDULED', ?::DATE, ?, ?`,
      [
        stop.asset_id,
        body.tech_id,
        priority,
        `Bundled service: ${stop.reason || "Predicted maintenance needed"}`,
        JSON.stringify(stop.parts_needed || []),
        scheduledDate,
        repairHours,
        station,
      ]
    );

    await query(
      `INSERT INTO PDM_DEMO.APP.TECH_SCHEDULES
          (TECH_ID, SCHEDULE_DATE, BLOCK_TYPE, ASSET_ID, STATION_NAME, ESTIMATED_HOURS, NOTES, IS_BASELINE)
       VALUES (?, ?::DATE, 'WORK_ORDER', ?, ?, ?, ?, FALSE)`,
      [
        body.tech_id,
        scheduledDate,
        stop.asset_id,
        station,
        repairHours,
        `${stop.predicted_class || "MAINTENANCE"} - ${stop.reason || ""}`,
      ]
    );

    created.push(stop.asset_id);
  }

  return NextResponse.json({ status: "created", work_orders_for_assets: created });
}
