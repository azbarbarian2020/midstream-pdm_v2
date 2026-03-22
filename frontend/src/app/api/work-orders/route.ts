import { NextRequest, NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

export async function POST(req: NextRequest) {
  const body = await req.json();
  await query(
    `INSERT INTO PDM_DEMO.APP.WORK_ORDERS (ASSET_ID, TECH_ID, PRIORITY, DESCRIPTION, PARTS_NEEDED)
     SELECT ?, ?, ?, ?, PARSE_JSON(?)`,
    [
      body.asset_id,
      body.tech_id || null,
      body.priority || "MEDIUM",
      body.description || "",
      JSON.stringify(body.parts_needed || []),
    ]
  );
  return NextResponse.json({ status: "created" });
}
