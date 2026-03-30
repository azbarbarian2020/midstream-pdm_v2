import { NextRequest, NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

function haversine(lat1: number, lon1: number, lat2: number, lon2: number) {
  const R = 3959;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos((lat1 * Math.PI) / 180) * Math.cos((lat2 * Math.PI) / 180) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.asin(Math.sqrt(a));
}

const FAILURE_MODE_SKILLS: Record<string, string[]> = {
  BEARING_WEAR: ["bearing", "vibration", "alignment"],
  VALVE_FAILURE: ["valve", "calibration", "pressure"],
  SEAL_LEAK: ["seal", "pump", "alignment"],
  OVERHEATING: ["thermal", "overheating", "cooling"],
  SURGE: ["surge", "compressor", "anti-surge"],
};

export async function POST(req: NextRequest) {
  const body = await req.json();

  const asOfTs = body.as_of_ts;
  const assets: any[] = asOfTs
    ? await query(
        `SELECT a.ASSET_ID, a.ASSET_TYPE, s.LAT, s.LON, s.NAME AS STATION_NAME,
                p.PREDICTED_CLASS, p.RISK_LEVEL
         FROM PDM_DEMO.RAW.ASSETS a
         JOIN PDM_DEMO.RAW.STATIONS s ON a.STATION_ID = s.STATION_ID
         LEFT JOIN PDM_DEMO.ANALYTICS.PREDICTION_HISTORY p ON a.ASSET_ID = p.ASSET_ID
              AND p.PREDICTION_TS <= ?::TIMESTAMP_NTZ
         WHERE a.ASSET_ID = ?
         QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ASSET_ID ORDER BY p.PREDICTION_TS DESC) = 1`,
        [asOfTs, body.primary_asset_id]
      )
    : await query(
        `SELECT a.ASSET_ID, a.ASSET_TYPE, s.LAT, s.LON, s.NAME AS STATION_NAME,
                p.PREDICTED_CLASS, p.RISK_LEVEL
         FROM PDM_DEMO.RAW.ASSETS a
         JOIN PDM_DEMO.RAW.STATIONS s ON a.STATION_ID = s.STATION_ID
         LEFT JOIN PDM_DEMO.ANALYTICS.PREDICTIONS p ON a.ASSET_ID = p.ASSET_ID
         WHERE a.ASSET_ID = ?`,
        [body.primary_asset_id]
      );

  if (!assets.length) return NextResponse.json({ error: "Asset not found" }, { status: 404 });

  const asset = assets[0];
  const assetLat = Number(asset.LAT);
  const assetLon = Number(asset.LON);
  const assetType = asset.ASSET_TYPE || "PUMP";
  const predClass = asset.PREDICTED_CLASS || "NORMAL";

  const techs: any[] = await query(
    `SELECT t.TECH_ID, t.NAME, t.HOME_BASE_LAT, t.HOME_BASE_LON,
            t.HOME_BASE_CITY, t.CERTIFICATIONS, t.AVAILABILITY,
            t.YEARS_EXPERIENCE, t.SPECIALTY_NOTES
     FROM PDM_DEMO.RAW.TECHNICIANS t
     WHERE t.AVAILABILITY IN ('AVAILABLE', 'ON_CALL')
     ORDER BY t.TECH_ID`
  );

  const baseDate = body.base_date || body.as_of_ts?.slice(0, 10) || "2026-03-25";
  const horizonDays = body.horizon_days || 3;
  const booked: any[] = await query(
    `SELECT TECH_ID, TO_CHAR(SCHEDULE_DATE, 'YYYY-MM-DD') AS SCHED_DATE, SUM(ESTIMATED_HOURS) AS BOOKED
     FROM PDM_DEMO.APP.TECH_SCHEDULES
     WHERE SCHEDULE_DATE >= ?::DATE
       AND SCHEDULE_DATE < DATEADD('day', ?, ?::DATE)
     GROUP BY TECH_ID, SCHEDULE_DATE`,
    [baseDate, horizonDays, baseDate]
  );

  const bookedMap: Record<string, number> = {};
  for (const r of booked) {
    bookedMap[`${r.TECH_ID}|${r.SCHED_DATE}`] = Number(r.BOOKED);
  }

  const failureSkills = FAILURE_MODE_SKILLS[predClass] || [];
  const assetTypeLower = assetType.toLowerCase();

  const scored = techs.map((t) => {
    const dist = haversine(Number(t.HOME_BASE_LAT), Number(t.HOME_BASE_LON), assetLat, assetLon);
    const distScore = Math.max(0, 100 - dist * 1.5);

    let totalBooked = 0;
    let fullyBookedDays = 0;
    for (let d = 0; d < horizonDays; d++) {
      const dt = new Date(baseDate);
      dt.setDate(dt.getDate() + d);
      const dateStr = dt.toISOString().slice(0, 10);
      const used = bookedMap[`${t.TECH_ID}|${dateStr}`] || 0;
      totalBooked += used;
      if (used >= 7) fullyBookedDays++;
    }
    const maxHours = horizonDays * 8;
    const loadRatio = totalBooked / maxHours;
    let scheduleScore: number;
    if (fullyBookedDays >= horizonDays) {
      scheduleScore = 0;
    } else if (loadRatio > 0.8) {
      scheduleScore = 10;
    } else {
      scheduleScore = Math.max(0, (1 - loadRatio) * 100);
    }

    let certs: string[] = [];
    if (t.CERTIFICATIONS) {
      if (typeof t.CERTIFICATIONS === "string") {
        try { certs = JSON.parse(t.CERTIFICATIONS); } catch {
          certs = t.CERTIFICATIONS.split(",").map((s: string) => s.trim()).filter(Boolean);
        }
      } else if (Array.isArray(t.CERTIFICATIONS)) {
        certs = t.CERTIFICATIONS;
      }
    }
    const hasTypeCert = certs.some((c) => c.toLowerCase().includes(assetTypeLower));
    const certScore = hasTypeCert ? 100 : 0;

    const specialtyText = (t.SPECIALTY_NOTES || "").toLowerCase();
    let specialtyScore = 0;
    if (predClass !== "NORMAL" && failureSkills.length > 0) {
      const matches = failureSkills.filter((skill) => specialtyText.includes(skill));
      specialtyScore = (matches.length / failureSkills.length) * 100;
    } else {
      specialtyScore = hasTypeCert ? 50 : 0;
    }

    const onCallPenalty = t.AVAILABILITY === "ON_CALL" ? 0.65 : 1.0;

    const total = (
      distScore * 0.25 +
      scheduleScore * 0.30 +
      certScore * 0.15 +
      specialtyScore * 0.30
    ) * onCallPenalty;

    const certMatch = hasTypeCert ? 1 : 0;
    const specialtyMatches = predClass !== "NORMAL"
      ? failureSkills.filter((skill) => specialtyText.includes(skill))
      : [];

    const reasons: string[] = [];
    if (specialtyMatches.length > 0)
      reasons.push(`${specialtyMatches.join("/")} specialist`);
    else if (hasTypeCert)
      reasons.push(`certified for ${assetTypeLower}s`);
    if (dist < 5) reasons.push("on-site");
    else if (dist < 20) reasons.push(`${Math.round(dist)}mi away`);
    else reasons.push(`${Math.round(dist)}mi travel`);
    const freeHrs = Math.round((maxHours - totalBooked) * 10) / 10;
    if (fullyBookedDays >= horizonDays)
      reasons.push(`fully booked next ${horizonDays}d`);
    else if (loadRatio < 0.2) reasons.push(`${freeHrs}h free`);
    else reasons.push(`${freeHrs}h available`);
    if (t.AVAILABILITY === "ON_CALL") reasons.push("on-call only");

    return {
      tech_id: t.TECH_ID,
      name: t.NAME,
      city: t.HOME_BASE_CITY,
      distance_miles: Math.round(dist * 10) / 10,
      available_hours: freeHrs,
      booked_hours: Math.round(totalBooked * 10) / 10,
      cert_match: certMatch,
      cert_total: 1,
      specialty_match: specialtyMatches.length > 0 ? specialtyMatches.join(", ") : undefined,
      explanation: reasons.join(" · "),
      score: Math.round(total * 10) / 10,
      score_breakdown: {
        distance: Math.round(distScore * 10) / 10,
        schedule: Math.round(scheduleScore * 10) / 10,
        certification: certScore,
        specialty: Math.round(specialtyScore * 10) / 10,
      },
      availability: t.AVAILABILITY,
    };
  });

  scored.sort((a, b) => b.score - a.score);

  const top = scored[0];
  const topExplanation = top
    ? `Recommended ${top.name}: ${top.explanation}`
    : undefined;

  return NextResponse.json({
    asset_id: body.primary_asset_id,
    asset_type: assetType,
    predicted_class: predClass,
    explanation: topExplanation,
    recommendations: scored.slice(0, 3),
  });
}
