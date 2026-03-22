import { NextRequest, NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

export async function GET(
  _req: NextRequest,
  { params }: { params: Promise<{ techId: string }> }
) {
  const { techId } = await params;

  const rows: any[] = await query(
    `SELECT t.TECH_ID, t.NAME, t.HOME_BASE_LAT, t.HOME_BASE_LON,
            t.HOME_BASE_CITY, t.CERTIFICATIONS, t.AVAILABILITY,
            t.YEARS_EXPERIENCE, t.SPECIALTY_NOTES, t.BIO, t.PHOTO_URL,
            t.HOURLY_RATE
     FROM PDM_DEMO.RAW.TECHNICIANS t
     WHERE t.TECH_ID = ?`,
    [techId]
  );

  if (!rows.length) return NextResponse.json({ error: "Technician not found" }, { status: 404 });

  const d: any = { ...rows[0] };
  if (d.CERTIFICATIONS && typeof d.CERTIFICATIONS === "string") {
    try {
      d.CERTIFICATIONS = JSON.parse(d.CERTIFICATIONS);
    } catch {
      d.CERTIFICATIONS = d.CERTIFICATIONS.split(",").map((s: string) => s.trim()).filter(Boolean);
    }
  }

  return NextResponse.json(d);
}
