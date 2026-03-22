import { NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

export async function GET() {
  let sqlOk = false;
  try {
    await query("SELECT 1");
    sqlOk = true;
  } catch {}

  return NextResponse.json({
    status: sqlOk ? "ok" : "degraded",
    sql_connection: sqlOk,
  });
}
