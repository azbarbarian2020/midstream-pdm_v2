import { NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

export async function GET() {
  const rows: any[] = await query(
    `SELECT t.TECH_ID, t.NAME, t.HOME_BASE_LAT, t.HOME_BASE_LON,
            t.HOME_BASE_CITY, t.CERTIFICATIONS, t.AVAILABILITY,
            t.YEARS_EXPERIENCE, t.SPECIALTY_NOTES, t.BIO, t.PHOTO_URL,
            t.HOURLY_RATE
     FROM PDM_DEMO.RAW.TECHNICIANS t
     ORDER BY t.TECH_ID`
  );

  rows.forEach((r) => {
    if (r.CERTIFICATIONS && typeof r.CERTIFICATIONS === "string") {
      try {
        r.CERTIFICATIONS = JSON.parse(r.CERTIFICATIONS);
      } catch {
        r.CERTIFICATIONS = r.CERTIFICATIONS.split(",").map((s: string) => s.trim()).filter(Boolean);
      }
    }
  });

  return NextResponse.json(rows);
}
