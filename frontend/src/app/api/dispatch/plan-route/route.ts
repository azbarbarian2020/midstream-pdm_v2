import { NextRequest, NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

export async function POST(req: NextRequest) {
  const body = await req.json();

  const rows = await query(
    "CALL PDM_DEMO.APP.PLAN_ROUTE(?, ?, ?, ?, ?, ?)",
    [body.tech_id, body.primary_asset_id, body.horizon_days || 3, body.max_stops || 8, body.as_of_ts || null, body.allow_overtime || false]
  );

  let result: any = rows[0] ? Object.values(rows[0])[0] : {};
  while (typeof result === "string") {
    result = JSON.parse(result);
  }

  return NextResponse.json(result);
}
