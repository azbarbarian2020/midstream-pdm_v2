import { NextResponse } from "next/server";
import crypto from "crypto";

export async function POST() {
  return NextResponse.json({ thread_id: crypto.randomUUID() });
}
