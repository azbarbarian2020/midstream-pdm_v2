import { NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

export async function POST() {
  const rows = await query("CALL PDM_DEMO.APP.RESET_DEMO()");
  const result = rows[0] ? Object.values(rows[0])[0] : "done";
  return NextResponse.json({ status: "ok", message: result });
}
